package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"
	"context"
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
)

type Node struct {
	transport.UnimplementedRaftServer
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

	node := &Node{id: *id, peers: strings.Split(*peers, ","), role: "follower", currentLeader: "", votedFor: "", log: wal, electionTimer: *time.NewTimer(time.Millisecond * 150)}

	err := node.init()
	if err != nil {
		return nil, err
	}
	return node, nil
}
func (n *Node) init() error {
	err := n.createConnections()
	if err != nil {
		return err
	}

	err = n.startServer()
	if err != nil {
		return err
	}
	n.startElectionTimer()
	return nil
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

func (n *Node) startServer() error {
	listen, err := net.Listen("tcp", n.id)

	if err != nil {
		return err
	}
	server := grpc.NewServer()
	transport.RegisterRaftServer(server, n)

	go func() {
		err := server.Serve(listen)
		if err != nil {
			log.Printf("Server error: %v", err)
		}
	}()
	return nil
}

func (n *Node) RequestVote(c context.Context, req *transport.RequestVoteRequest) (res *transport.RequestVoteResponse, err error) {
	return
}

func (n *Node) AppendEntries(c context.Context, req *transport.AppendEntriesRequest) (response *transport.AppendEntriesResponse, err error) {
	return
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
