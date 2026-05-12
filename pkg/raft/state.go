package raft

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

type PersistentState struct {
	CurrentTerm int32
	VotedFor    string
	mutex       sync.Mutex
	file        *os.File
}

// NewPersistentState loads existing state from disk if available, otherwise creates a fresh state file.
func NewPersistentState(filePath string) (state *PersistentState, err error) {

	var file *os.File
	state = &PersistentState{}

	if _, statErr := os.Stat(filePath); statErr == nil {

		file, err = os.OpenFile(filePath, os.O_RDWR, 0644)

		if err != nil {
			return nil, err
		}

		err = json.NewDecoder(file).Decode(state)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		state.file = file

	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	} else {
		file, err = os.Create(filePath)

		if err != nil {
			return nil, err
		}
		state.file = file

	}

	return state, nil

}

// writeCurrentState updates the in-memory state and atomically overwrites the state file on disk.
func (p *PersistentState) writeCurrentState(term int32, votedFor string) (err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.updateState(term, votedFor)

	state := make(map[string]any)
	state["votedFor"] = p.VotedFor
	state["CurrentTerm"] = p.CurrentTerm

	p.file.Seek(0, 0)
	p.file.Truncate(0)
	err = json.NewEncoder(p.file).Encode(state)
	return err
}

// updateState updates the in-memory term and votedFor fields.
func (p *PersistentState) updateState(term int32, votedFor string) {
	p.CurrentTerm = term
	p.VotedFor = votedFor
}
