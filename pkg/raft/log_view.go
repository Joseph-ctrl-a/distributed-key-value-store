package raft

import "fmt"

func (n *Node) lastLogIndex() int32 {
	n.mutex.RLock()
	snapshotIndex := n.lastSnapshotIndex
	n.mutex.RUnlock()

	return snapshotIndex + n.log.LastLogIndex()
}

func (n *Node) lastLogTerm() (int32, error) {
	if n.log.LastLogIndex() == 0 {
		n.mutex.RLock()
		defer n.mutex.RUnlock()
		return n.lastSnapshotTerm, nil
	}

	return n.log.LastLogTerm()
}

func (n *Node) termAtIndex(index int32) (int32, error) {
	n.mutex.RLock()
	snapshotIndex := n.lastSnapshotIndex
	snapshotTerm := n.lastSnapshotTerm
	n.mutex.RUnlock()

	if index == snapshotIndex {
		return snapshotTerm, nil
	}
	if index < snapshotIndex {
		return 0, fmt.Errorf("log index %d compacted into snapshot at index %d", index, snapshotIndex)
	}

	return n.log.GetTermAtIndex(index - snapshotIndex)
}
