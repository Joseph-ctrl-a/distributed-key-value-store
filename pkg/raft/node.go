package raft

import (
	"Distributed_Key_Value_Store/pkg/wal"
	"flag"
	"strings"
)

type Node struct {
	id            string
	role          string
	currentLeader string
	currentTerm   int
	votedFor      string
	peers         []string
	log           *wal.Wal
}

func NewNode(wal *wal.Wal) *Node {
	id := flag.String("id", "", "the node's ip:port")
	peers := flag.String("peers", "", "comma separated list of peer addresses")

	flag.Parse()

	peerList := strings.Split(*peers, ",")
	return &Node{id: *id, peers: peerList, role: "follower", currentLeader: "", votedFor: "", log: wal}

}
