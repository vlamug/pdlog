package main

import (
	"log"

	logpkg "github.com/vlamug/pdlog/internal/log"
	"github.com/vlamug/pdlog/internal/server"
)

const (
	defaultServerAddr = ":9099"
	defaultStoreDir   = "/store"
)

func main() {
	cfg := logpkg.Config{}
	srv, err := server.NewHTTPServer(defaultStoreDir, defaultServerAddr, cfg)
	if err != nil {
		log.Fatalln("failed to run http server", err)
	}
	log.Printf("serve on %s\n", defaultServerAddr)
	log.Fatal(srv.ListenAndServe())
}
