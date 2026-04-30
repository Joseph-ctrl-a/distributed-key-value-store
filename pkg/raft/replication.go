package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
)

func (n *Node) AppendEntries(c context.Context, req *transport.AppendEntriesRequest) (res *transport.AppendEntriesResponse, err error) {
	if len(req.Entries) == 0 {
		res, err = n.handleHeartBeat(req)
		if err != nil {
			return nil, err
		}
		return res, err
	} else {
		n.handleEntry(req)
	}
}

func (n *Node) handleHeartBeat(req *transport.AppendEntriesRequest) (res *transport.AppendEntriesResponse, err error) {
	n.mutex.Lock()
	res = &transport.AppendEntriesResponse{}
	if req.Term > n.currentTerm {
		err = n.stepDown(req.Term)
		if err != nil {
			n.mutex.Unlock()
			return nil, err
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

func (n *Node) handleEntry(req *transport.AppendEntriesRequest) {

}
