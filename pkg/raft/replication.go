package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"
	"context"
	"errors"
	"fmt"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AppendEntries handles incoming AppendEntries RPCs, routing to heartbeat or entry replication logic.
func (n *Node) AppendEntries(c context.Context, req *transport.AppendEntriesRequest) (res *transport.AppendEntriesResponse, err error) {
	if n.isPeerBlocked(req.LeaderId) {
		return nil, status.Error(codes.Unavailable, "peer blocked by simulated network policy")
	}

	if len(req.Entries) == 0 {
		return n.handleHeartBeat(req)
	} else {
		return n.handleEntry(req)
	}

}

// HeartBeat processes a heartbeat from the leader, stepping down if a higher term is seen and resetting the election timer.
func (n *Node) handleHeartBeat(req *transport.AppendEntriesRequest) (res *transport.AppendEntriesResponse, err error) {
	res = &transport.AppendEntriesResponse{}
	n.mutex.RLock()
	currentTerm := n.currentTerm
	n.mutex.RUnlock()

	if req.Term > currentTerm {
		err = n.stepDown(req.Term)
		if err != nil {
			return res, err
		}
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()
	if req.Term < n.currentTerm {
		res.Term = n.currentTerm
		res.Success = false
		log.Printf("[append] node=%s rejected heartbeat leader=%s reqTerm=%d currentTerm=%d", n.id, req.LeaderId, req.Term, n.currentTerm)
	} else {
		res.Term = n.currentTerm
		res.Success = true
		n.resetElectionTimer()
		// Cap at our own log length — we may not have received all entries yet.
		n.commitIndex = min(req.LeaderCommit, n.log.LastLogIndex())
		if err := n.applyCommittedEntriesFromLog(); err != nil {
			return res, err
		}
		log.Printf("[append] node=%s accepted heartbeat leader=%s term=%d commit=%d", n.id, req.LeaderId, req.Term, req.LeaderCommit)
	}

	return res, nil
}

// handleEntry processes a log replication request, checking log consistency before appending new entries.
func (n *Node) handleEntry(req *transport.AppendEntriesRequest) (res *transport.AppendEntriesResponse, err error) {

	Success := func(term int32) (*transport.AppendEntriesResponse, error) {
		return &transport.AppendEntriesResponse{Success: true, Term: term}, nil
	}
	Failure := func(term int32) (*transport.AppendEntriesResponse, error) {
		return &transport.AppendEntriesResponse{Success: false, Term: term}, nil
	}
	Error := func(err error) (*transport.AppendEntriesResponse, error) {
		return &transport.AppendEntriesResponse{}, err
	}
	// 1. Check if sending term is greater than ours
	n.mutex.RLock()
	currentTerm := n.currentTerm
	n.mutex.RUnlock()

	if req.Term > currentTerm {
		err = n.stepDown(req.Term)
		if err != nil {
			return Error(err)
		}
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()
	// 2. Check if sending term is less than ours
	if req.Term < n.currentTerm {
		log.Printf("[append] node=%s rejected entries leader=%s reqTerm=%d currentTerm=%d", n.id, req.LeaderId, req.Term, n.currentTerm)
		return Failure(n.currentTerm)
	}

	// 3. Check if term at index match
	if req.PrevLogIndex != 0 {
		termAtIndex, err := n.log.GetTermAtIndex(req.PrevLogIndex)
		if err != nil {
			log.Printf("[append] node=%s rejected entries leader=%s missingPrevIndex=%d", n.id, req.LeaderId, req.PrevLogIndex)
			return Failure(n.currentTerm)
		}
		if termAtIndex != req.PrevLogTerm {
			log.Printf("[append] node=%s rejected entries leader=%s prevIndex=%d localTerm=%d leaderTerm=%d", n.id, req.LeaderId, req.PrevLogIndex, termAtIndex, req.PrevLogTerm)
			return Failure(n.currentTerm)
		}
	}

	// 4. Check If theres extra entries in your log
	if n.log.LastLogIndex() > req.PrevLogIndex {
		err := n.log.SpliceInPlace(req.PrevLogIndex)
		if err != nil {
			return Error(err)
		}
	}
	// 5. Add new entries
	for _, entry := range req.Entries {
		logEntry, err := wal.ParseToLogEntry(entry)
		if err != nil {
			return Error(err)
		}
		err = n.log.Append(logEntry)

		if err != nil {
			return Error(err)
		}

	}
	// 6. Advance the commit and update your own
	n.commitIndex = min(req.LeaderCommit, n.log.LastLogIndex())

	// 7. Apply entries
	if err := n.applyCommittedEntriesFromLog(); err != nil {
		return Error(err)
	}
	log.Printf("[append] node=%s accepted entries leader=%s count=%d lastIndex=%d commit=%d", n.id, req.LeaderId, len(req.Entries), n.log.LastLogIndex(), n.commitIndex)
	return Success(n.currentTerm)
}

func (n *Node) applyCommittedEntriesFromLog() error {
	return n.log.ForEach(func(i int, entry string) error {
		index := int32(i + 1)
		if index <= n.commitIndex && index > n.lastApplied {
			if err := n.applyEntry(entry); err != nil {
				return err
			}
			n.lastApplied = index
		}
		return nil
	})
}

// applyEntry will apply an entry to the hashmap
func (n *Node) applyEntry(entry string) error {
	entryParsed, err := wal.Parse(entry)
	if err != nil {
		return err
	}

	if entryParsed.MethodName == "SET" {
		if len(entryParsed.MethodParams) != 2 {
			errorMessage := fmt.Sprintf("Expected 2 param to call Hashmap.Set got %d", len(entryParsed.MethodParams))
			return errors.New(errorMessage)
		}
		n.store.Set(entryParsed.MethodParams[0], entryParsed.MethodParams[1])
	} else {
		if len(entryParsed.MethodParams) != 1 {
			errorMessage := fmt.Sprintf("Expected 1 param to call Hashmap.Delete got %d", len(entryParsed.MethodParams))
			return errors.New(errorMessage)
		}
		n.store.Delete(entryParsed.MethodParams[0])
	}
	return nil
}
