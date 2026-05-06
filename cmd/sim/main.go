package main

import (
	"Distributed_Key_Value_Store/pkg/sim"
	"context"
	"flag"
	"log"
	"net/http"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "simulator backend address")
	flag.Parse()

	manager := sim.NewManager()
	if err := manager.EnsureNodeBinary(context.Background()); err != nil {
		log.Fatal(err)
	}

	log.Printf("[sim] listening=%s", *addr)
	if err := http.ListenAndServe(*addr, manager.Handler()); err != nil {
		log.Fatal(err)
	}
}
