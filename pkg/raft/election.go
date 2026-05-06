package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequestVote handles an incoming vote request from a candidate, granting a vote if the candidate's log is up-to-date and the node hasn't already voted this term.
func (n *Node) RequestVote(c context.Context, req *transport.RequestVoteRequest) (res *transport.RequestVoteResponse, err error) {
	if n.isPeerBlocked(req.CandidateId) {
		return nil, status.Error(codes.Unavailable, "peer blocked by simulated network policy")
	}

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
		n.votedFor = ""
		if err := n.persistentState.writeCurrentState(n.currentTerm, n.votedFor); err != nil {
			return nil, err
		}
	}

	lastLogTerm, err := n.log.LastLogTerm()

	if err != nil {
		return nil, err
	}

	if n.votedFor == req.CandidateId {
		err = n.voteForCandidate(req, res)
		if err != nil {
			return &transport.RequestVoteResponse{}, err
		}
		return
	}
	if n.hasVoted() {
		res.VoteGranted = false
		res.Term = n.currentTerm
		return res, err
	}

	if lastLogTerm == req.LastLogTerm {
		if req.LastLogIndex >= n.log.LastLogIndex() {
			err = n.voteForCandidate(req, res)
			if err != nil {
				return &transport.RequestVoteResponse{}, err
			}
		} else {
			res.VoteGranted = false
			res.Term = n.currentTerm
		}
	} else if lastLogTerm < req.LastLogTerm {
		err = n.voteForCandidate(req, res)
		if err != nil {
			return &transport.RequestVoteResponse{}, err
		}
	} else {
		res.VoteGranted = false
		res.Term = n.currentTerm
	}

	return
}

// voteForCandidate grants the vote, updates node state, and persists votedFor to disk.
func (n *Node) voteForCandidate(req *transport.RequestVoteRequest, res *transport.RequestVoteResponse) error {
	res.VoteGranted = true
	res.Term = n.currentTerm
	n.votedFor = req.CandidateId
	n.role = "follower"
	n.resetElectionTimer()
	log.Printf("[election] node=%s granted vote to candidate=%s term=%d", n.id, req.CandidateId, n.currentTerm)
	err := n.persistentState.writeCurrentState(n.currentTerm, n.votedFor)

	return err
}

// hasVoted returns true if the node has already cast a vote this term.
func (n *Node) hasVoted() bool {
	return n.votedFor != ""
}
