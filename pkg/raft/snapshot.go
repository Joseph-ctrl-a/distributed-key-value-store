package raft

import (
	"encoding/json"
	"errors"
	"os"
)

type Snapshot struct {
	LastIncludedIndex int32             `json:"lastIncludedIndex"`
	LastIncludedTerm  int32             `json:"lastIncludedTerm"`
	State             map[string]string `json:"state"`
}

func loadSnapShot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	defer file.Close()

	var snapshot Snapshot
	if err := json.NewDecoder(file).Decode(&snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}
