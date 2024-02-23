package agent

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/vlamug/pdlog/api/v1"
	"github.com/vlamug/pdlog/internal/discovery"
	"github.com/vlamug/pdlog/internal/log"
	"github.com/vlamug/pdlog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Agent runs on every service instance, setting up and connecting all the different components
type (
	Agent struct {
		Config

		log        *log.Log
		httpServer *http.Server
		grpcServer *grpc.Server
		membership *discovery.Membership
		replicator *log.Replicator

		shutdown     bool
		shutdowns    chan struct{}
		shutdownLock sync.Mutex
	}

	Config struct {
		DataDir        string
		HTTPBindAddr   string
		RPCBindAddr    string
		NodeName       string
		StartJoinAddrs []string
		ACLModelFile   string
		ACLPolicyFile  string
	}
)

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(a.Config.DataDir, log.Config{})
	return err
}

func (a *Agent) setupServer() error {
	serverConfig := &server.Config{
		CommitLog: a.log,
	}

	if err := a.setupGRPC(serverConfig); err != nil {
		return err
	}

	return a.setupHTTP(serverConfig)
}

func (a *Agent) setupHTTP(serverConfig *server.Config) error {
	var err error
	a.httpServer, err = server.NewHTTPServer(a.HTTPBindAddr, serverConfig)
	if err != nil {
		return err
	}

	go func() {
		if err := a.httpServer.ListenAndServe(); err != nil {
			withTimeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			_ = a.httpServer.Shutdown(withTimeoutCtx)
		}
	}()

	return nil
}

func (a *Agent) setupGRPC(serverConfig *server.Config) error {
	var err error
	a.grpcServer, err = server.NewGRPCServer(serverConfig)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", a.RPCBindAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := a.grpcServer.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()
	return nil
}

func (a *Agent) setupMembership() error {
	conn, err := grpc.Dial(a.RPCBindAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		LocalServer: client,
	}
	a.membership, err = discovery.New(a.replicator, &discovery.Config{
		NodeName: a.NodeName,
		BindAddr: a.RPCBindAddr,
		Tags: map[string]string{
			discovery.RpcAddrTagKey: a.RPCBindAddr,
		},
		StartJoinAddrs: a.StartJoinAddrs,
	})
	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.grpcServer.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
