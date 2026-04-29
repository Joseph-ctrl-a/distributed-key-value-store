package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"time"
)

func (n *Node) RequestVote(c context.Context, req *transport.RequestVoteRequest) (res *transport.RequestVoteResponse, err error) {
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
