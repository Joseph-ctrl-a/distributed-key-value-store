package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"
)

const (
	appendEntriesTimeout      = 750 * time.Millisecond
	leaderReplicationInterval = 500 * time.Millisecond
)

type appendEntryResult struct {
	peer     string
	request  *transport.AppendEntriesRequest
	response *transport.AppendEntriesResponse
	err      error
}

func (n *Node) runLeaderReplicationLoop() error {
	ticker := time.NewTicker(leaderReplicationInterval)
	defer ticker.Stop()

	for {
		<-ticker.C

		if !n.isLeader() {
			return nil
		}

		for _, peer := range n.deepCopyPeers() {
			request, err := n.newAppendEntriesRequest(peer)
			if err != nil {
				continue
			}
			if err := n.sendAppendEntriesToFollower(request, peer); err != nil {
				continue
			}
		}
		if err := n.tryAdvanceCommitIndex(); err != nil {
			return err
		}
		if err := n.applyCommittedEntries(); err != nil {
			return err
		}
	}
}

func (n *Node) sendAppendEntriesToFollower(request *transport.AppendEntriesRequest, peer string) error {
	if request == nil {
		return errors.New("append entries request is nil")
	}
	if peer == "" {
		return errors.New("peer is empty")
	}
	if n.isPeerBlocked(peer) {
		return fmt.Errorf("peer %q blocked by simulated network policy", peer)
	}

	n.mutex.RLock()
	client, ok := n.clients[peer]
	n.mutex.RUnlock()
	if !ok || client == nil {
		return fmt.Errorf("raft client for peer %q not found", peer)
	}

	ctx, cancel := context.WithTimeout(context.Background(), appendEntriesTimeout)
	defer cancel()

	response, err := client.AppendEntries(ctx, request)
	if err != nil {
		log.Printf("[replicate] node=%s peer=%s append failed entries=%d prevIndex=%d: %v", n.id, peer, len(request.Entries), request.PrevLogIndex, err)
	} else {
		log.Printf("[replicate] node=%s peer=%s append response success=%t term=%d entries=%d prevIndex=%d", n.id, peer, response.Success, response.Term, len(request.Entries), request.PrevLogIndex)
	}
	return n.handleAppendEntriesResult(&appendEntryResult{
		peer:     peer,
		request:  request,
		response: response,
		err:      err,
	})
}

func (n *Node) handleAppendEntriesResult(result *appendEntryResult) error {
	if result == nil {
		return nil
	}
	if result.err != nil {
		return result.err
	}
	if result.request == nil {
		return errors.New("append entries request is nil")
	}
	if result.response == nil {
		return errors.New("append entries response is nil")
	}

	n.mutex.RLock()
	currentTerm := n.currentTerm
	n.mutex.RUnlock()

	if result.response.Term > currentTerm {
		return n.stepDown(result.response.Term)
	}
	if result.response.Term < currentTerm {
		return nil
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.nextIndex == nil || n.matchIndex == nil {
		return errors.New("leader replication state is not initialized")
	}

	if !result.response.Success {
		if n.nextIndex[result.peer] > 1 {
			n.nextIndex[result.peer]--
		}
		log.Printf("[replicate] node=%s peer=%s backtrack nextIndex=%d", n.id, result.peer, n.nextIndex[result.peer])
		return nil
	}

	matchIndex := result.request.PrevLogIndex + int32(len(result.request.Entries))
	if matchIndex > n.matchIndex[result.peer] {
		n.matchIndex[result.peer] = matchIndex
	}
	if nextIndex := matchIndex + 1; nextIndex > n.nextIndex[result.peer] {
		n.nextIndex[result.peer] = nextIndex
	}
	log.Printf("[replicate] node=%s peer=%s matched=%d nextIndex=%d", n.id, result.peer, n.matchIndex[result.peer], n.nextIndex[result.peer])
	return nil
}

func (n *Node) isLeader() bool {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.role == "leader"
}

func (n *Node) deepCopyPeers() []string {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	peers := make([]string, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (n *Node) initLeaderState() {
	lastLogIndex := n.lastLogIndex()

	n.mutex.Lock()
	n.nextIndex = make(map[string]int32)
	n.matchIndex = make(map[string]int32)

	for _, peer := range n.peers {
		n.nextIndex[peer] = lastLogIndex + 1
		n.matchIndex[peer] = 0
	}
	n.mutex.Unlock()
}

func (n *Node) newAppendEntriesRequest(peerId string) (*transport.AppendEntriesRequest, error) {
	n.mutex.RLock()
	currentTerm := n.currentTerm
	id := n.id
	nextPeerIndex := n.nextIndex[peerId]
	commitIndex := n.commitIndex
	n.mutex.RUnlock()

	if nextPeerIndex < 1 {
		return nil, errors.New("nextIndex not initialized")
	}

	prevLogIndex := nextPeerIndex - 1
	var prevLogTerm int32
	if prevLogIndex > 0 {
		var err error
		prevLogTerm, err = n.termAtIndex(prevLogIndex)
		if err != nil {
			return nil, err
		}
	}

	entries, err := n.entriesFromIndex(nextPeerIndex)
	if err != nil {
		return nil, err
	}

	return &transport.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     id,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: commitIndex,
	}, nil
}

func (n *Node) tryAdvanceCommitIndex() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	indexes := []int{}
	snapshotIndex := n.lastSnapshotIndex
	snapshotTerm := n.lastSnapshotTerm
	indexes = append(indexes, int(snapshotIndex+n.log.LastLogIndex()))

	for _, peer := range n.peers {
		indexes = append(indexes, int(n.matchIndex[peer]))
	}
	slices.SortFunc(indexes, func(a, b int) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	// Sort ascending and take the element at position len - majority to get the
	// highest index that at least a majority of nodes have matched.
	candidateIndex := int32(indexes[len(indexes)-((len(indexes)/2)+1)])
	if candidateIndex <= n.commitIndex || candidateIndex == 0 {
		return nil
	}

	var candidateTerm int32
	if candidateIndex == snapshotIndex {
		candidateTerm = snapshotTerm
	} else {
		var err error
		candidateTerm, err = n.log.GetTermAtIndex(candidateIndex - snapshotIndex)
		if err != nil {
			return err
		}
	}
	// Only commit entries from the current term. Entries from prior terms are
	// committed indirectly once a current-term entry advances past them (Raft §5.4.2).
	if candidateTerm == n.currentTerm {
		oldCommitIndex := n.commitIndex
		n.commitIndex = candidateIndex
		log.Printf("[commit] node=%s advanced commitIndex %d->%d", n.id, oldCommitIndex, n.commitIndex)
	}

	return nil
}

func (n *Node) applyCommittedEntries() error {
	n.mutex.Lock()

	start := n.lastApplied + 1
	localStart := start - n.lastSnapshotIndex
	if localStart <= 0 {
		n.mutex.Unlock()
		return fmt.Errorf("get entries from index %d: log index compacted into snapshot at index %d", start, n.lastSnapshotIndex)
	}
	entries, err := n.log.EntriesFromIndex(int(localStart))
	if err != nil {
		n.mutex.Unlock()
		return fmt.Errorf("get entries from index %d: %w", start, err)
	}

	for i, entry := range entries {
		index := int(start) + i

		if index > int(n.commitIndex) {
			break
		}
		if err := n.applyEntry(entry); err != nil {
			n.mutex.Unlock()
			return fmt.Errorf("apply entry at index %d: %w", index, err)
		}
		n.lastApplied = int32(index)
		log.Printf("[apply] node=%s applied index=%d", n.id, n.lastApplied)
	}

	n.mutex.Unlock()

	if err := n.maybeSnapshot(); err != nil {
		return fmt.Errorf("maybe snapshot after applying leader entries: %w", err)
	}
	return nil
}
