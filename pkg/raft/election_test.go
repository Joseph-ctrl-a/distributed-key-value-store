package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"testing"
)

func TestHasVoted(t *testing.T) {
	n := newTestNode(t)

	if n.hasVoted() {
		t.Error("expected hasVoted=false before any vote")
	}

	n.votedFor = "candidate-1"
	if !n.hasVoted() {
		t.Error("expected hasVoted=true after setting votedFor")
	}
}

func TestRandomElectionTime(t *testing.T) {
	n := newTestNode(t)
	for range 500 {
		got := n.RandomTime(150, 150)
		if got < 150 || got > 300 {
			t.Errorf("expected time in [150, 300], got %d", got)
		}
	}
}

func TestRequestVote(t *testing.T) {
	t.Run("lower term is rejected", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 5
		res, err := n.RequestVote(context.Background(), voteReq(3, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if res.VoteGranted {
			t.Error("expected vote denied for lower term")
		}
		if res.Term != 5 {
			t.Errorf("expected response term 5, got %d", res.Term)
		}
	})

	t.Run("higher term causes step down to follower", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.role = "leader"
		_, err := n.RequestVote(context.Background(), voteReq(5, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if n.role != "follower" {
			t.Errorf("expected follower after seeing higher term, got %q", n.role)
		}
		if n.currentTerm != 5 {
			t.Errorf("expected term updated to 5, got %d", n.currentTerm)
		}
	})

	t.Run("grants vote when log is up to date", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		res, err := n.RequestVote(context.Background(), voteReq(1, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if !res.VoteGranted {
			t.Error("expected vote granted for up-to-date log")
		}
		if n.votedFor != "c1" {
			t.Errorf("expected votedFor c1, got %q", n.votedFor)
		}
		if n.role != "follower" {
			t.Errorf("expected role follower after granting vote, got %q", n.role)
		}
	})

	t.Run("denies vote when already voted for a different candidate", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.votedFor = "other"
		res, err := n.RequestVote(context.Background(), voteReq(1, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if res.VoteGranted {
			t.Error("expected vote denied when already voted for different candidate")
		}
	})

	t.Run("re-grants vote to the same candidate", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.votedFor = "c1"
		res, err := n.RequestVote(context.Background(), voteReq(1, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if !res.VoteGranted {
			t.Error("expected vote re-granted for same candidate")
		}
	})

	t.Run("denies vote when candidate log is behind", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 5)
		res, err := n.RequestVote(context.Background(), voteReq(5, "c1", 2, 2))
		if err != nil {
			t.Fatal(err)
		}
		if res.VoteGranted {
			t.Error("expected vote denied when candidate log is behind")
		}
	})

	t.Run("grants vote when candidate has higher last log term", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		res, err := n.RequestVote(context.Background(), voteReq(5, "c1", 3, 5))
		if err != nil {
			t.Fatal(err)
		}
		if !res.VoteGranted {
			t.Error("expected vote granted when candidate has higher last log term")
		}
	})

	t.Run("denies vote when candidate has lower last log term", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 5)
		n.currentTerm = 5
		res, err := n.RequestVote(context.Background(), voteReq(5, "c1", 10, 2))
		if err != nil {
			t.Fatal(err)
		}
		if res.VoteGranted {
			t.Error("expected vote denied when candidate has lower last log term")
		}
	})
}

func TestTallyVotes(t *testing.T) {
	t.Run("counts granted votes", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.peers = []string{"p1", "p2", "p3", "p4"}

		ch := make(chan *transport.RequestVoteResponse, 4)
		ch <- &transport.RequestVoteResponse{VoteGranted: true, Term: 1}
		ch <- &transport.RequestVoteResponse{VoteGranted: true, Term: 1}
		ch <- &transport.RequestVoteResponse{VoteGranted: false, Term: 1}
		ch <- &transport.RequestVoteResponse{VoteGranted: false, Term: 1}

		votes, err := n.tallyVotes(ch)
		if err != nil {
			t.Fatal(err)
		}
		if votes != 3 {
			t.Errorf("expected 3 votes including self vote, got %d", votes)
		}
	})

	t.Run("nil response is skipped", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.peers = []string{"p1", "p2"}

		ch := make(chan *transport.RequestVoteResponse, 2)
		ch <- nil
		ch <- &transport.RequestVoteResponse{VoteGranted: true, Term: 1}

		votes, err := n.tallyVotes(ch)
		if err != nil {
			t.Fatal(err)
		}
		if votes != 2 {
			t.Errorf("expected 2 votes including self vote (nil skipped), got %d", votes)
		}
	})

	t.Run("higher term response causes step down", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.role = "candidate"
		n.peers = []string{"p1"}

		ch := make(chan *transport.RequestVoteResponse, 1)
		ch <- &transport.RequestVoteResponse{VoteGranted: false, Term: 10}

		_, err := n.tallyVotes(ch)
		if err != nil {
			t.Fatal(err)
		}
		if n.currentTerm != 10 {
			t.Errorf("expected term updated to 10, got %d", n.currentTerm)
		}
		if n.role != "follower" {
			t.Errorf("expected follower after step down, got %q", n.role)
		}
	})
}

func TestStepDown(t *testing.T) {
	n := newTestNode(t)
	n.currentTerm = 3
	n.role = "leader"
	n.votedFor = "self"

	if err := n.stepDown(7); err != nil {
		t.Fatal(err)
	}

	if n.currentTerm != 7 {
		t.Errorf("expected term 7, got %d", n.currentTerm)
	}
	if n.role != "follower" {
		t.Errorf("expected follower, got %q", n.role)
	}
	if n.votedFor != "" {
		t.Errorf("expected votedFor cleared, got %q", n.votedFor)
	}
}
