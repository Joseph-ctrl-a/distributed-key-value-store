package wal

import (
	"bufio"
	"errors"
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

type mapResult struct {
	res []interface{}
	err error
}

// Append formats and writes any given method to the WAL, other writes are blocked during this.
func (w *Wal) Append(entry *LogEntry) (err error) {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	log := entry.FormatEntry()

	_, err = w.file.Seek(0, 2)
	if err != nil {
		return err
	}

	_, err = w.file.WriteString(log)
	if err != nil {
		return err
	}
	w.lastEntry = log
	w.lineCount++
	return
}

// Creates a new WAL
func NewWal(filepath string) (*Wal, error) {

	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	w := &Wal{file: file}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			w.lastEntry = line
			w.lineCount++
		}
	}
	if err := scanner.Err(); err != nil {
		file.Close()
		return nil, err
	}

	return w, nil
}

// Closes the WAL
func (w *Wal) Close() {
	w.file.Close()
}

// LastLogTerm returns the Raft term of the last entry written to the WAL.
func (w *Wal) LastLogTerm() (int32, error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	if w.lastEntry == "" {
		return 0, nil
	}
	lastTerm := strings.Split(w.lastEntry, ":")[2]
	term, err := strconv.Atoi(strings.TrimSpace(lastTerm))
	if err != nil {
		return 0, err
	}
	return int32(term), nil
}

// LastLogIndex returns the index of the last entry in the WAL (1-based line count).
func (w *Wal) LastLogIndex() int32 {

	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return int32(w.lineCount)
}

// GetTermAtIndex returns the term of a specific index
func (w *Wal) GetTermAtIndex(index int32) (int32, error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return 0, err
	}
	scanner := bufio.NewScanner(w.file)

	var i int32 = 0
	for scanner.Scan() {
		i++
		if i == index {
			entry := scanner.Text()
			if entry == "" {
				return 0, errors.New("no entry at that index")
			}

			lastTerm := strings.Split(entry, ":")[2]
			term, err := strconv.Atoi(strings.TrimSpace(lastTerm))
			if err != nil {
				return 0, err
			}
			return int32(term), nil
		}
	}
	return 0, errors.New("no entry at that index")
}

// SpliceInPlace will delete every entry in the log past the given index. It does not create a new log
func (w *Wal) SpliceInPlace(index int32) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(w.file)
	splice := []string{}

	var i int32 = 0
	for scanner.Scan() {
		entry := scanner.Text()
		splice = append(splice, entry)
		if i == index-1 {
			break
		}
		i++
	}
	if len(splice) == 0 {
		return errors.New("no entries found at or before index")
	}
	err = w.file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = w.file.Seek(0, 0)
	if err != nil {
		return err
	}
	for _, entry := range splice {
		_, err := w.file.WriteString(entry + "\n")

		if err != nil {
			return err
		}
	}

	w.lastEntry = splice[len(splice)-1]
	w.lineCount = int(index)
	return nil
}

func (w *Wal) EntriesFromIndex(index int) ([]string, error) {
	return w.Filter(func(i int, entry string) bool {
		return i+1 >= int(index)
	})
}
