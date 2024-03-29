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
	batcher                   *Batcher
	requestsReceived          *Counter
	malformedRequestsReceived *Counter
}

func NewServer(batcher *Batcher) (*Server, error) {
	s := &Server{
		batcher: batcher,
	}

	var err error
	s.requestsReceived, err = NewDimensionlessCounter("http_request_count", "Number of http requests received, tagged by endpoint", endpointTag)
	if err != nil {
		return nil, fmt.Errorf("http_request_count counter: %w", err)
	}
	s.malformedRequestsReceived, err = NewDimensionlessCounter("http_request_malformed", "Number of malformed http requests received, tagged by endpoint", endpointTag)
	if err != nil {
		return nil, fmt.Errorf("http_request_malformed counter: %w", err)
	}

	return s, nil
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

	mctx := endpointContext(r.Context(), "_doc")
	s.requestsReceived.Add(mctx, 1)

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		slog.Error("read body", err)
		s.malformedRequestsReceived.Add(mctx, 1)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	event := new(TraceEvent)
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		slog.Error("unmarshal body", err)
		s.malformedRequestsReceived.Add(mctx, 1)
		w.WriteHeader(http.StatusBadRequest)
		return

	}
	s.batcher.Add(r.Context(), event)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) BulkTraceHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	// return vars

	mctx := endpointContext(r.Context(), "_bulk")
	s.requestsReceived.Add(mctx, 1)

	iniT := time.Now()
	var total, successful, empty, failed int
	var bulkResp esutil.BulkIndexerResponse

	reqBuf := new(bytes.Buffer)
	defer r.Body.Close()
	if _, err := reqBuf.ReadFrom(r.Body); err != nil {
		slog.Error("read body", err)
		s.malformedRequestsReceived.Add(mctx, 1)
		w.WriteHeader(http.StatusBadRequest)
		return
	} else {
		reqDec := json.NewDecoder(bytes.NewReader(reqBuf.Bytes()))
		for reqDec.More() {
			var tracEv TraceEvent
			decErr := reqDec.Decode(&tracEv)
			if decErr == io.EOF {
				break
			} else if decErr != nil {
				total++
				failed++
				slog.Error("decoding ES trace:", decErr)
				s.malformedRequestsReceived.Add(mctx, 1)
				continue
			} else {
				total++
				if tracEv.Type != nil {
					s.batcher.Add(r.Context(), &tracEv)
					successful++
				} else {
					empty++
				}
				continue
			}
		}
		slog.Debug("received bulk request",
			"total", total,
			"successful", successful,
			"empty", empty,
			"failed", failed,
		)
		// compose reply
		finT := time.Since(iniT)
		bulkResp.Took = int(finT.Seconds())
		jres, err := json.Marshal(bulkResp)
		if err != nil {
			slog.Error("unable to compose bulk response", err)
			s.malformedRequestsReceived.Add(mctx, 1)
			w.WriteHeader(http.StatusBadRequest)
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
