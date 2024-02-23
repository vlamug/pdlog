package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/vlamug/pdlog/api/v1"
)

func NewHTTPServer(addr string, serverConfig *Config) (*http.Server, error) {
	srv, err := newHTTPServer(serverConfig)
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
	*Config
}

func newHTTPServer(config *Config) (*httpServer, error) {
	return &httpServer{Config: config}, nil
}

// Record contains log item
type Record struct {
	Value  string `json:"value"`
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
		Value:  []byte(req.Record.Value),
		Offset: req.Record.Offset,
	}

	offset, err := s.CommitLog.Append(record)
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

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: &Record{
		Value:  string(record.Value),
		Offset: record.Offset,
	}}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
