package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"math/rand"
	"time"
)

func (n *Node) startElection() error {
	n.mutex.Lock()

	defer func() {
		n.resetElectionTimer()
	}()

	n.currentTerm++
	n.role = "candidate"
	n.votedFor = n.id

	err := n.persistentState.writeCurrentState(n.currentTerm, n.votedFor)

	if err != nil {
		n.mutex.Unlock()
		return err
	}
	lastLogIndex := n.log.LastLogIndex()

	lastLogTerm, err := n.log.LastLogTerm()

	if err != nil {
		n.mutex.Unlock()
		return err
	}

	votes := make(chan *transport.RequestVoteResponse, len(n.peers))

	for _, peerAddress := range n.peers {
		n.sendVote(votes, peerAddress, lastLogIndex, lastLogTerm)
	}
	n.mutex.Unlock()
	votesReceived, err := n.tallyVotes(votes)
	if err != nil {
		return err
	}
	n.mutex.Lock()
	if votesReceived > len(n.peers)/2 && n.role == "candidate" {
		n.role = "leader"
		n.currentLeader = n.id
	}
	n.mutex.Unlock()
	return nil
}

func (n *Node) tallyVotes(channel chan *transport.RequestVoteResponse) (int, error) {

	votesReceived := 0
	for range len(n.peers) {
		res := <-channel
		n.mutex.Lock()
		if res == nil {
			n.mutex.Unlock()
			continue
		}
		if res.Term > n.currentTerm {
			err := n.stepDown(res.Term)
			n.mutex.Unlock()

			return 0, err
		}
		n.mutex.Unlock()
		if res.VoteGranted {
			votesReceived++
		}
	}
	return votesReceived, nil
}
func (n *Node) sendVote(channel chan *transport.RequestVoteResponse, address string, logIndex int32, logTerm int32) {
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)

	request := &transport.RequestVoteRequest{Term: n.currentTerm, CandidateId: n.id, LastLogIndex: logIndex, LastLogTerm: logTerm}

	go func() {
		res, err := n.clients[address].RequestVote(ctx, request)
		cancel()
		if err != nil {
			channel <- nil
		} else {
			channel <- res
		}
	}()
}

func (n *Node) stepDown(newTerm int32) error {

	n.currentTerm = newTerm
	n.role = "follower"
	n.votedFor = ""
	err := n.persistentState.writeCurrentState(n.currentTerm, n.votedFor)
	if err != nil {
		return err
	}
	n.resetElectionTimer()
	return nil
}
func (n *Node) startElectionTimer() {
	go func() {

		<-n.electionTimer.C

		n.startElection()
	}()

}

func (n *Node) resetElectionTimer() {
	n.electionTimer.Reset(time.Millisecond * time.Duration(n.RandomElectionTime()))
}

func (n *Node) RandomElectionTime() int {
	return (rand.Intn(150) + 150)
}
