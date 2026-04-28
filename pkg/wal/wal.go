package wal

import (
	"os"
	"sync"
)

type Wal struct {
	file  *os.File
	mutex sync.RWMutex
}

func (w *Wal) WriteMethodCall(entry *LogEntry) error {
	log := entry.FormatEntry()

	_, err := w.file.WriteString(log)

	return err

}

func (w *Wal) Close() {
	w.file.Close()
}
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
