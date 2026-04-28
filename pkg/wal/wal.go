package wal

import (
	"os"
	"sync"
)

// Wal defines how a WAL should look
type Wal struct {
	file  *os.File
	mutex sync.RWMutex
}

// WriteMethodCall formats and writes any given method to the WAL, other writes are blocked during this.
func (w *Wal) WriteMethodCall(entry *LogEntry) error {

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	log := entry.FormatEntry()

	_, err := w.file.WriteString(log)

	return err

}

// Closes the WAL
func (w *Wal) Close() {
	w.file.Close()
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
