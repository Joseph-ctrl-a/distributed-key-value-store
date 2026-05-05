package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"math/rand"
	"time"
)

// startElection increments the term, votes for itself, and sends RequestVote RPCs to all peers concurrently.
func (n *Node) startElection() error {
	n.mutex.Lock()

	defer func() {
		n.resetElectionTimer()
	}()

	n.currentTerm++
	n.role = "candidate"
	n.votedFor = n.id

	n.mutex.Unlock()
	err := n.persistentState.writeCurrentState(n.currentTerm, n.votedFor)
	n.mutex.Lock()

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
	clusterSize := len(n.peers) + 1
	majority := clusterSize/2 + 1
	if votesReceived >= majority && n.role == "candidate" {
		n.role = "leader"
		n.currentLeader = n.id

		n.initLeaderState()

		go n.runLeaderReplicationLoop()
	}
	n.mutex.Unlock()
	return nil
}

// tallyVotes reads vote responses from the channel, stepping down if a higher term is seen.
func (n *Node) tallyVotes(channel chan *transport.RequestVoteResponse) (int, error) {

	votesReceived := 1
	for range len(n.peers) {
		res := <-channel
		n.mutex.Lock()
		if res == nil {
			n.mutex.Unlock()
			continue
		}
		if res.Term > n.currentTerm {
			n.mutex.Unlock()
			err := n.stepDown(res.Term)

			return 0, err
		}
		n.mutex.Unlock()
		if res.VoteGranted {
			votesReceived++
		}
	}
	return votesReceived, nil
}

// sendVote sends a RequestVote RPC to a single peer in a goroutine and sends the response to the channel.
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

// stepDown reverts the node to follower, updates the term, clears votedFor, and persists the state.
func (n *Node) stepDown(newTerm int32) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

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

// startElectionTimer waits for the election timer to fire then starts an election.
func (n *Node) startElectionTimer() {
	go func() {

		<-n.electionTimer.C

		n.startElection()
	}()

}

// resetElectionTimer resets the election timer to a new random duration between 150-300ms.
func (n *Node) resetElectionTimer() {
	n.electionTimer.Reset(time.Millisecond * time.Duration(n.RandomTime(150, 150)))
}

// RandomElectionTime returns a random election timeout between start and end milliseconds.
func (n *Node) RandomTime(start, end int) int {
	return (rand.Intn(start) + end)
}
