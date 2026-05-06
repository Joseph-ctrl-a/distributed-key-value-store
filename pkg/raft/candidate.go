package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"log"
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
	currentTerm := n.currentTerm
	candidateID := n.id
	peers := append([]string(nil), n.peers...)
	log.Printf("[election] node=%s became candidate term=%d", candidateID, currentTerm)

	n.mutex.Unlock()
	err := n.persistentState.writeCurrentState(currentTerm, candidateID)

	if err != nil {
		return err
	}
	lastLogIndex := n.log.LastLogIndex()

	lastLogTerm, err := n.log.LastLogTerm()

	if err != nil {
		return err
	}

	votes := make(chan *transport.RequestVoteResponse, len(peers))

	for _, peerAddress := range peers {
		n.sendVote(votes, peerAddress, currentTerm, candidateID, lastLogIndex, lastLogTerm)
	}
	votesReceived, err := n.tallyVotes(votes, len(peers))
	if err != nil {
		return err
	}
	n.mutex.Lock()
	clusterSize := len(peers) + 1
	majority := clusterSize/2 + 1
	if votesReceived >= majority && n.role == "candidate" {
		n.role = "leader"
		n.currentLeader = n.id
		term := n.currentTerm
		n.mutex.Unlock()

		n.initLeaderState()
		log.Printf("[leader] node=%s became leader term=%d votes=%d/%d", n.id, term, votesReceived, clusterSize)
		go n.runLeaderReplicationLoop()
		return nil
	}
	n.mutex.Unlock()
	return nil
}

// tallyVotes reads vote responses from the channel, stepping down if a higher term is seen.
func (n *Node) tallyVotes(channel chan *transport.RequestVoteResponse, expectedResponses int) (int, error) {

	votesReceived := 1
	for range expectedResponses {
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
	log.Printf("[election] node=%s received votes=%d", n.id, votesReceived)
	return votesReceived, nil
}

// sendVote sends a RequestVote RPC to a single peer in a goroutine and sends the response to the channel.
func (n *Node) sendVote(channel chan *transport.RequestVoteResponse, address string, term int32, candidateID string, logIndex int32, logTerm int32) {
	if n.isPeerBlocked(address) {
		log.Printf("[election] node=%s blocked vote request to peer=%s", n.id, address)
		channel <- nil
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)

	request := &transport.RequestVoteRequest{Term: term, CandidateId: candidateID, LastLogIndex: logIndex, LastLogTerm: logTerm}

	go func() {
		res, err := n.clients[address].RequestVote(ctx, request)
		cancel()
		if err != nil {
			log.Printf("[election] node=%s vote request to peer=%s failed: %v", n.id, address, err)
			channel <- nil
		} else {
			log.Printf("[election] node=%s vote response from peer=%s granted=%t term=%d", n.id, address, res.VoteGranted, res.Term)
			channel <- res
		}
	}()
}

// stepDown reverts the node to follower, updates the term, clears votedFor, and persists the state.
func (n *Node) stepDown(newTerm int32) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	oldTerm := n.currentTerm
	oldRole := n.role
	n.currentTerm = newTerm
	n.role = "follower"
	n.votedFor = ""
	log.Printf("[stepdown] node=%s role=%s oldTerm=%d newTerm=%d", n.id, oldRole, oldTerm, newTerm)
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
		for {

			<-n.electionTimer.C
			n.mutex.RLock()
			role := n.role
			n.mutex.RUnlock()
			if role != "leader" {
				n.startElection()
			}

		}
	}()
}

// resetElectionTimer resets the election timer to a new random duration between 1500-3000ms.
func (n *Node) resetElectionTimer() {
	n.electionTimer.Reset(time.Millisecond * time.Duration(n.RandomTime(1500, 1500)))
}

// RandomElectionTime returns a random election timeout between start and end milliseconds.
func (n *Node) RandomTime(start, end int) int {
	return (rand.Intn(start) + end)
}
