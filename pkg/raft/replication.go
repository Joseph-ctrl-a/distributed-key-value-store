package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
)

func (n *Node) AppendEntries(c context.Context, req *transport.AppendEntriesRequest) (response *transport.AppendEntriesResponse, err error) {
	return

}
