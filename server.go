package main

import (
	"bytes"
	"encoding/json"
	"net/http"

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
