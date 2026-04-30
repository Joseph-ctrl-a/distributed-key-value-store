package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"
	"context"
	"errors"
	"fmt"
)

// AppendEntries handles incoming AppendEntries RPCs, routing to heartbeat or entry replication logic.
func (n *Node) AppendEntries(c context.Context, req *transport.AppendEntriesRequest) (res *transport.AppendEntriesResponse, err error) {
	if len(req.Entries) == 0 {
		return n.handleHeartBeat(req)
	} else {
		return n.handleEntry(req)
	}

}

// handleHeartBeat processes a heartbeat from the leader, stepping down if a higher term is seen and resetting the election timer.
func (n *Node) handleHeartBeat(req *transport.AppendEntriesRequest) (res *transport.AppendEntriesResponse, err error) {
	n.mutex.Lock()
	res = &transport.AppendEntriesResponse{}
	if req.Term > n.currentTerm {
		err = n.stepDown(req.Term)
		if err != nil {
			n.mutex.Unlock()
			return res, err
		}
	}

	if req.Term < n.currentTerm {
		res.Term = n.currentTerm
		res.Success = false
	} else {
		res.Term = n.currentTerm
		res.Success = true
		n.resetElectionTimer()
	}

	n.mutex.Unlock()
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
	n.mutex.Lock()
	defer n.mutex.Unlock()
	// 1. Check if sending term is greater than ours
	if req.Term > n.currentTerm {
		err = n.stepDown(req.Term)
		if err != nil {
			return Error(err)
		}
	}
	// 2. Check if sending term is less than ours
	if req.Term < n.currentTerm {
		return Failure(n.currentTerm)
	}

	// 3. Check if term at index match
	if req.PrevLogIndex != 0 {
		termAtIndex, err := n.log.GetTermAtIndex(req.PrevLogIndex)
		if err != nil {
			return Error(err)
		}
		if termAtIndex != req.PrevLogTerm {
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
	callbackFunction := func(i int, entry string) (err error) {
		if i+1 <= int(n.commitIndex) && i+1 > int(n.lastApplied) {
			err = n.applyEntry(entry)
			if err != nil {
				return err
			}
			n.lastApplied++

		}
		return err
	}
	err = n.log.ReadLine(callbackFunction)
	if err != nil {
		return Error(err)
	}
	return Success(n.currentTerm)
}

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
