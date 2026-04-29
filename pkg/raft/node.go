package raft

import (
	"Distributed_Key_Value_Store/pkg/wal"
	"flag"
	"strings"
	"time"
)

type Node struct {
	id            string
	role          string
	currentLeader string
	currentTerm   int
	votedFor      string
	peers         []string
	log           *wal.Wal
	ElectionTimer time.Timer
}

// NewNode defines how a Node should look
func NewNode(wal *wal.Wal) *Node {
	id := flag.String("id", "", "the node's ip:port")
	peers := flag.String("peers", "", "comma separated list of peer addresses")

	flag.Parse()

	peerList := strings.Split(*peers, ",")

	node := &Node{id: *id, peers: peerList, role: "follower", currentLeader: "", votedFor: "", log: wal, ElectionTimer: *time.NewTimer(time.Millisecond * 150)}

	node.startElectionTimer()
	return node
}

func (n *Node) startElection() {
	defer n.startElectionTimer()

}
func (n *Node) startElectionTimer() {
	go func() {
		defer n.resetElectionTimer()

		<-n.ElectionTimer.C

		n.startElection()
	}()
}

func (n *Node) resetElectionTimer() {
	defer n.ElectionTimer.Reset(time.Millisecond * 150)
}
