package main

import (
	"github.com/vlamug/pdlog/internal/server"
	"log"
)

const defaultServerAddr = ":9099"

func main() {
	srv := server.NewHTTPServer(defaultServerAddr)
	log.Printf("serve on %s\n", defaultServerAddr)
	log.Fatal(srv.ListenAndServe())
}
