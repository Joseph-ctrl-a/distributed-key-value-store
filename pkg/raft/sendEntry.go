package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"errors"
	"time"
)

type appendEntryResult struct {
	peer string
	res  *transport.AppendEntriesResponse
	err  error
}

func (n *Node) sendEntry(channel chan *appendEntryResult, req *transport.AppendEntriesRequest, address string) {

	n.mutex.RLock()
	client := n.clients[address]
	n.mutex.RUnlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
		defer cancel()
		res, err := client.AppendEntries(ctx, req)
		if err != nil {
			channel <- nil
			return
		}
		channel <- &appendEntryResult{address, res, nil}
	}()
}

func (n *Node) sendEntriesToFollowers(c context.Context) {

	channel := make(chan *appendEntryResult, len(n.peers))

	sentCount := 0
	for _, peer := range n.peers {
		req, err := n.newAppendEntriesRequest(peer)
		if err != nil {
			continue
		}

		n.sendEntry(channel, req, peer)
		sentCount++
	}
	for range sentCount {
		res := <-channel
		n.handleEntryResult(res)
	}
}

func (n *Node) handleEntryResult(result *appendEntryResult) {
	if result == nil {
		return
	}

	n.mutex.RLock()
	currentTerm := n.currentTerm
	n.mutex.RUnlock()

	if result.res.Term > currentTerm {
		_ = n.stepDown(result.res.Term)
		return
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()
	if !result.res.Success {
		if n.nextIndex[result.peer] > 1 {
			n.nextIndex[result.peer]--
		}
	} else {
		n.nextIndex[result.peer] = n.log.LastLogIndex() + 1
		n.matchIndex[result.peer] = n.log.LastLogIndex()
	}
	return
}

func (n *Node) newAppendEntriesRequest(peerId string) (*transport.AppendEntriesRequest, error) {
	n.mutex.RLock()
	currentTerm := n.currentTerm
	id := n.id
	nextPeerIndex := n.nextIndex[peerId]
	commitIndex := n.commitIndex
	n.mutex.RUnlock()

	req := transport.AppendEntriesRequest{}
	req.Term = currentTerm
	req.LeaderId = id

	var prevLogTerm int32
	var prevLogIndex int32
	if nextPeerIndex == 0 {
		return nil, errors.New("nextIndex not initialized")
	} else {
		prevLogIndex = nextPeerIndex - 1
	}
	if prevLogIndex == 0 {
		prevLogTerm = 0
	} else {
		var err error
		prevLogTerm, err = n.log.GetTermAtIndex(prevLogIndex)
		if err != nil {
			return nil, err
		}
	}

	req.PrevLogIndex = prevLogIndex
	req.PrevLogTerm = prevLogTerm
	req.LeaderCommit = commitIndex

	entries, err := n.log.EntriesFromIndex(int(nextPeerIndex))

	if err != nil {
		return nil, err
	}
	req.Entries = entries

	return &req, nil
}
