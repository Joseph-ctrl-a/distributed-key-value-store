package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"
	"flag"
	"strings"
	"time"

	"google.golang.org/grpc"
)

type Node struct {
	id            string
	role          string
	currentLeader string
	currentTerm   int
	votedFor      string
	peers         []string
	log           *wal.Wal
	electionTimer time.Timer
	clients       map[string]transport.RaftClient
}

// NewNode defines how a Node should look
func NewNode(wal *wal.Wal) (*Node, error) {
	id := flag.String("id", "", "the node's ip:port")
	peers := flag.String("peers", "", "comma separated list of peer addresses")

	flag.Parse()

	peerList := strings.Split(*peers, ",")

	node := &Node{id: *id, peers: peerList, role: "follower", currentLeader: "", votedFor: "", log: wal, electionTimer: *time.NewTimer(time.Millisecond * 150)}

	err := node.createConnections()
	if err != nil {
		return nil, err
	}
	node.startElectionTimer()
	return node, nil
}

func (n *Node) createConnections() error {
	for _, peerAddress := range n.peers {
		connection, err := grpc.NewClient(peerAddress)

		if err != nil {
			return err
		}
		n.clients[peerAddress] = transport.NewRaftClient(connection)
	}
	return nil
}
func (n *Node) startElection() {
	defer n.startElectionTimer()

}
func (n *Node) startElectionTimer() {
	go func() {
		defer n.resetElectionTimer()

		<-n.electionTimer.C

		n.startElection()
	}()
}

func (n *Node) resetElectionTimer() {
	defer n.electionTimer.Reset(time.Millisecond * 150)
}
