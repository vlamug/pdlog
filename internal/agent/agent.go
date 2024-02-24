package agent

import (
	"fmt"
	"net"
	"sync"

	"github.com/vlamug/pdlog/api/v1"
	"github.com/vlamug/pdlog/internal/discovery"
	"github.com/vlamug/pdlog/internal/log"
	"github.com/vlamug/pdlog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	DataDir        string
	BindAddr       string
	RPCPort        int
	NodeName       string
	StartJoinAddrs []string
}

func (c *Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

type Agent struct {
	cfg *Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func New(cfg *Config) (*Agent, error) {
	a := &Agent{
		cfg:       cfg,
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
	a.log, err = log.NewLog(
		a.cfg.DataDir,
		log.Config{},
	)

	return err
}

func (a *Agent) setupServer() error {
	var err error
	a.server, err = server.NewGRPCServer(a.log)

	addr, err := a.cfg.RPCAddr()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		if err := a.server.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.cfg.RPCAddr()
	if err != nil {
		return err
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.Dial(rpcAddr, opts...)
	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		DialOption:  opts,
		LocalServer: client,
	}

	a.membership, err = discovery.New(a.replicator, &discovery.Config{
		NodeName: a.cfg.NodeName,
		BindAddr: a.cfg.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.cfg.StartJoinAddrs,
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

	shutdowns := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			// @todo this code is blocked forever during tests running, try to investigate the problem
			// a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdowns {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}
