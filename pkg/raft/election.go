package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
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
		n.currentLeader = ""
		n.role = "follower"
		n.currentTerm = req.Term
	}

	lastLogTerm, err := n.log.LastLogTerm()

	if err != nil {
		return nil, err
	}

	if n.votedFor == req.CandidateId {
		err = n.voteForCandidate(req, res)
		if err != nil {
			return nil, err
		}
		return
	}
	if n.hasVoted() {
		res.VoteGranted = false
		res.Term = n.currentTerm
		return
	}

	if lastLogTerm == req.LastLogTerm {
		if req.LastLogIndex >= n.log.LastLogIndex() {
			err = n.voteForCandidate(req, res)
			if err != nil {
				return nil, err
			}
		} else {
			res.VoteGranted = false
			res.Term = n.currentTerm
		}
	} else if lastLogTerm < req.LastLogTerm {
		err = n.voteForCandidate(req, res)
		if err != nil {
			return nil, err
		}
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
	n.role = "follower"
	n.resetElectionTimer()
	err := n.persistentState.writeCurrentState(n.currentTerm, n.votedFor)

	return err
}

func (n *Node) hasVoted() bool {
	return n.votedFor != ""
}
