package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/vlamug/pdlog/internal/agent"
	"go.uber.org/zap"
)

const (
	defaultHTTPAddr = ":9099"
	defaultGRPCAddr = ":9098"
	defaultSerfAddr = ":9097"
	defaultStoreDir = "/store"
)

var (
	httpAddr = flag.String("http_addr", defaultHTTPAddr, "addr to run http server on")
	grpcAddr = flag.String("grpc_addr", defaultGRPCAddr, "addr to run grpc server on")
	serfAddr = flag.String("serf_addr", defaultSerfAddr, "addr to run serf on")
	storeDir = flag.String("dir", defaultStoreDir, "directory to store data into")
)

func main() {
	flag.Parse()
	notifyCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()

	logger := zap.L().Named("main")

	agentConfig := agent.Config{
		DataDir:      *storeDir,
		HTTPBindAddr: *httpAddr,
		RPCBindAddr:  *grpcAddr,
		SerfBindAddr: *serfAddr,
	}
	a, err := agent.New(agentConfig)
	if err != nil {
		log.Fatal(err)
	}

	select {
	case <-notifyCtx.Done():
		logger.Info("caught signal")
		_ = a.Shutdown()
	}
}
