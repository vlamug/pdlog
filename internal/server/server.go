package server

import (
	"context"
	"google.golang.org/grpc"

	"github.com/vlamug/pdlog/api/v1"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog Log
}

var _ api.LogServer = (*GRPCServer)(nil)

type GRPCServer struct {
	api.UnimplementedLogServer
	*Config
}

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newGRPCServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func newGRPCServer(config *Config) (*GRPCServer, error) {
	return &GRPCServer{
		Config: config,
	}, nil
}

func (s *GRPCServer) Produce(_ context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	record := NewRecord(req.Record.Offset, req.Record.Value)
	offset, err := s.CommitLog.Append(record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *GRPCServer) Consume(_ context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: &api.Record{Offset: record.Offset, Value: record.Value}}, nil
}

func (s *GRPCServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *GRPCServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err := stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
