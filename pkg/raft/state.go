package raft

import (
	"Distributed_Key_Value_Store/pkg/wal"
	"encoding/json"
	"os"
	"sync"
)

type PersistentState struct {
	CurrentTerm int32
	VotedFor    string
	mutex       sync.Mutex
	file        *os.File
}

func NewPersistentState() (state *PersistentState, err error) {

	var file *os.File
	state = &PersistentState{}

	filePath := "./data/votedFor.json"
	if wal.FileExists(filePath) {

		file, err = os.OpenFile(filePath, os.O_RDWR, 0644)

		if err != nil {
			return nil, err
		}

		err = json.NewDecoder(file).Decode(state)
		if err != nil {
			return nil, err
		}
		state.file = file

	} else {
		file, err = os.Create(filePath)

		if err != nil {
			return nil, err
		}
		state.file = file

	}

	return state, nil

}

func (p *PersistentState) writeCurrentState(term int32, votedFor string) (err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.updateState(term, votedFor)

	state := make(map[string]interface{})
	state["votedFor"] = p.VotedFor
	state["CurrentTerm"] = p.CurrentTerm

	p.file.Seek(0, 0)
	p.file.Truncate(0)
	err = json.NewEncoder(p.file).Encode(state)
	return err
}

func (p *PersistentState) updateState(term int32, votedFor string) {
	p.CurrentTerm = term
	p.VotedFor = votedFor
}
