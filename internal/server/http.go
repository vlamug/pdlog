package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/vlamug/pdlog/api/v1"
	logpkg "github.com/vlamug/pdlog/internal/log"
)

func NewHTTPServer(addr string, dir string, cfg logpkg.Config) (*http.Server, error) {
	srv, err := newHTTPServer(dir, cfg)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter()
	r.HandleFunc("/", srv.handleProduce).Methods(http.MethodPost)
	r.HandleFunc("/", srv.handleConsume).Methods(http.MethodGet)

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}, nil
}

type httpServer struct {
	Log *logpkg.Log
}

func newHTTPServer(dir string, cfg logpkg.Config) (*httpServer, error) {
	log, err := logpkg.NewLog(dir, cfg)
	if err != nil {
		return nil, err
	}

	return &httpServer{Log: log}, nil
}

// Record contains log item
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type ProduceRequest struct {
	Record *Record `json:"record"`
}

type ProducerResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record *Record `json:"record"`
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record := &api.Record{
		Value:  req.Record.Value,
		Offset: req.Record.Offset,
	}

	offset, err := s.Log.Append(record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProducerResponse{Offset: offset}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: &Record{
		Value:  record.Value,
		Offset: record.Offset,
	}}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
