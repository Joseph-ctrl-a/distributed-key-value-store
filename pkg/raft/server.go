package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"log"
	"net"

	"google.golang.org/grpc"
)

// createConnections establishes gRPC client connections to all peer nodes.
func (n *Node) createConnections() error {
	n.clients = make(map[string]transport.RaftClient)
	for _, peerAddress := range n.peers {
		connection, err := grpc.NewClient(peerAddress)

		if err != nil {
			return err
		}
		n.clients[peerAddress] = transport.NewRaftClient(connection)
	}
	return nil
}

// startServer starts the gRPC server and registers the Raft service on the node's address.
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
