package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"time"
)

func (n *Node) RequestVote(c context.Context, req *transport.RequestVoteRequest) (res *transport.RequestVoteResponse, err error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	res = &transport.RequestVoteResponse{}
	if req.Term < n.currentTerm {
		res.VoteGranted = false
		res.Term = n.currentTerm
		return

	} else if req.Term > n.currentTerm {
		n.currentTerm = req.Term
	}

	lastLogTerm, err := n.log.LastLogTerm()

	if err != nil {
		return nil, err
	}

	if n.votedFor == req.CandidateId {
		n.voteForCandidate(req, res)
		return
	}
	if n.hasVoted() {
		res.VoteGranted = false
		res.Term = n.currentTerm
		return
	}

	if lastLogTerm == req.LastLogTerm {
		if req.LastLogIndex >= n.log.LastLogIndex() {
			n.voteForCandidate(req, res)
		} else {
			res.VoteGranted = false
			res.Term = n.currentTerm
		}
	} else if lastLogTerm < req.LastLogTerm {
		n.voteForCandidate(req, res)
	} else {
		res.VoteGranted = false
		res.Term = n.currentTerm
	}

	return
}

func (n *Node) voteForCandidate(req *transport.RequestVoteRequest, res *transport.RequestVoteResponse) error {
	res.VoteGranted = true
	res.Term = n.currentTerm
	n.votedFor = req.CandidateId

	err := n.persistentState.writeCurrentState(n.currentTerm, n.votedFor)

	return err
}

func (n *Node) hasVoted() bool {
	return n.votedFor != ""
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
