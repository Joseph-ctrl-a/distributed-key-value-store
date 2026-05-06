package main

import (
	"Distributed_Key_Value_Store/pkg/raft"
	"Distributed_Key_Value_Store/pkg/wal"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	id := flag.String("id", "", "the node's ip:port")
	peersFlag := flag.String("peers", "", "comma separated list of peer addresses")
	control := flag.String("control", "", "optional HTTP control api address")
	flag.Parse()

	if *id == "" {
		log.Fatal("id is required")
	}

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	idSanitized := strings.ReplaceAll(*id, ":", "_")
	dataDir := fmt.Sprintf("./data/%s", idSanitized)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatal(err)
	}

	w, err := wal.NewWal(filepath.Join(dataDir, "wal.log"))
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	node, err := raft.NewNode(*id, peers, w)
	if err != nil {
		log.Fatal(err)
	}

	if *control != "" {
		node.StartControlServer(*control)
		log.Printf("[control] node=%s listening=%s", *id, *control)
	}

	select {}
}
