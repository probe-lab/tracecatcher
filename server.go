package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/gorilla/mux"
	"golang.org/x/exp/slog"
)

type Server struct {
	batcher *Batcher
}

func NewServer(batcher *Batcher) (*Server, error) {
	return &Server{
		batcher: batcher,
	}, nil
}

func (s *Server) ConfigureRoutes(r *mux.Router) {
	r.NotFoundHandler = http.HandlerFunc(s.NotFoundHandler)
	r.Path("/traces/_doc").Methods("POST").HandlerFunc(s.TraceHandler)
	r.Path("/traces/_bulk").Methods("POST").HandlerFunc(s.BulkTraceHandler)
	r.PathPrefix("/").Methods("GET").HandlerFunc(s.RootHandler)
}

func (s *Server) NotFoundHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte("Not Found"))
}

func (s *Server) TraceHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		slog.Error("read body", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	event := new(TraceEvent)
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		slog.Error("unmarshal body", err)
		w.WriteHeader(http.StatusBadRequest)
		return

	}
	s.batcher.Add(r.Context(), event)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) BulkTraceHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	// return vars
	iniT := time.Now()
	var total, successful, empty, failed int
	var bulkResp esutil.BulkIndexerResponse

	reqBuf := new(bytes.Buffer)
	defer r.Body.Close()
	if _, err := reqBuf.ReadFrom(r.Body); err != nil {
		slog.Error("read body", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	} else {
		reqDec := json.NewDecoder(bytes.NewReader(reqBuf.Bytes()))
	decoderLoop:
		for reqDec.More() {
			var tracEv TraceEvent
			decErr := reqDec.Decode(&tracEv)
			if decErr == io.EOF {
				break decoderLoop
			} else if decErr != nil {
				total++
				failed++
				slog.Error("decoding ES trace:", decErr)
				continue decoderLoop
			} else {
				total++
				if tracEv.Type != nil {
					s.batcher.Add(r.Context(), &tracEv)
					successful++
				} else {
					empty++
				}
				continue decoderLoop
			}
		}
		slog.Info(fmt.Sprintf("received bulk resp: total %d suc %d, emtpy %d, failed %d",
			total,
			successful,
			empty,
			failed),
		)
		// compose reply
		finT := time.Since(iniT)
		bulkResp.Took = int(finT.Seconds())
		jres, err := json.Marshal(bulkResp)
		if err != nil {
			slog.Error("unable to compose bulk response", err)
		}
		w.Write(jres)
	}
}

func (s *Server) RootHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("connection from client", "remote_addr", r.RemoteAddr)
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.WriteHeader(http.StatusOK)

	w.Write([]byte(`{
		"cluster_name": "traces",
		"cluster_uuid": "aaaabbbbccccddddaaaabbbbccccdddd",
		"name": "traces",
		"tagline": "You Know, for Search",
		"version": {
			"build_date": "2023-01-01",
			"build_flavor": "default",
			"build_hash": "",
			"build_snapshot": false,
			"build_type": "",
			"lucene_version": "",
			"minimum_index_compatibility_version": "5.6.0",
			"minimum_wire_compatibility_version": "5.6.0",
			"number": "7.0.0"
		}
	}`))
}
