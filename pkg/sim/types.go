package sim

import (
	"Distributed_Key_Value_Store/pkg/raft"
	"time"
)

type NodeConfig struct {
	Name        string `json:"name"`
	RaftAddr    string `json:"raftAddr"`
	ControlAddr string `json:"controlAddr"`
}

type NodeSnapshot struct {
	Name        string           `json:"name"`
	RaftAddr    string           `json:"raftAddr"`
	ControlAddr string           `json:"controlAddr"`
	Running     bool             `json:"running"`
	PID         int              `json:"pid,omitempty"`
	Status      *raft.NodeStatus `json:"status,omitempty"`
	Error       string           `json:"error,omitempty"`
}

type ClusterSnapshot struct {
	Nodes      []NodeSnapshot `json:"nodes"`
	Partitions [][]string     `json:"partitions"`
}

type Event struct {
	ID        int64     `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Node      string    `json:"node,omitempty"`
	Message   string    `json:"message"`
	Data      any       `json:"data,omitempty"`
}

type Envelope struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type PartitionRequest struct {
	Groups [][]string `json:"groups"`
}

type KVSetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KVDeleteRequest struct {
	Key string `json:"key"`
}
