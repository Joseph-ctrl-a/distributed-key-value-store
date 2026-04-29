package wal

import (
	"os"
	"strconv"
	"strings"
	"sync"
)

// Wal defines how a Wal should look
type Wal struct {
	file      *os.File
	mutex     sync.RWMutex
	lastEntry string
	lineCount int
}

// WriteMethodCall formats and writes any given method to the WAL, other writes are blocked during this.
func (w *Wal) WriteMethodCall(entry *LogEntry) (err error) {

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	log := entry.FormatEntry()

	_, err = w.file.WriteString(log)
	if err != nil {
		return err
	}
	w.lastEntry = log
	w.lineCount++
	return
}

// Closes the WAL
func (w *Wal) Close() {
	w.file.Close()
}

func (w *Wal) LastLogTerm() (int, error) {
	lastTerm := strings.Split(w.lastEntry, ":")[2]
	term, err := strconv.Atoi(strings.TrimSpace(lastTerm))
	if err != nil {
		return 0, err
	}
	return term, nil
}

func (w *Wal) LastLogIndex() int {
	return w.lineCount
}

// Creates a new WAL
func NewWal(filepath string) (*Wal, error) {
	var file *os.File
	var err error
	if !fileExists(filepath) {
		file, err = os.Create(filepath)
		if err != nil {
			return nil, err
		}
	} else {
		file, err = os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
	}

	return &Wal{file: file}, nil
}
