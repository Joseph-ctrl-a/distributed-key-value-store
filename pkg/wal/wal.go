package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Wal is an append-oriented write-ahead log backed by a single file.
// Indexes exposed by Wal are 1-based local WAL indexes. Raft is responsible
// for mapping those local indexes to absolute log indexes after snapshotting.
type Wal struct {
	file      *os.File
	mutex     sync.RWMutex
	lastEntry string
	lineCount int
	offsets   []int64
}

// mapResult is kept for map-style helpers that need to return values plus an error.
type mapResult struct {
	res []any
	err error
}

// Append writes one entry to the end of the WAL and records its byte offset.
// The offset index lets point lookups seek directly to a line instead of
// scanning from the beginning of the file.
func (w *Wal) Append(entry *LogEntry) (err error) {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	log := entry.FormatEntry()

	offset, err := w.file.Seek(0, 2)
	if err != nil {
		return err
	}
	_, err = w.file.WriteString(log)
	if err != nil {
		return err
	}

	w.offsets = append(w.offsets, offset)
	w.lastEntry = log
	w.lineCount++
	return
}

// NewWal opens or creates a WAL file and rebuilds its in-memory index.
// Existing log entries are scanned once to restore lineCount, lastEntry, and
// byte offsets used by GetTermAtIndex.
func NewWal(filepath string) (*Wal, error) {

	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	w := &Wal{file: file}

	var offset int64 = 0
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		line := scanner.Text()

		if line != "" {
			w.offsets = append(w.offsets, offset)
			w.lastEntry = line
			w.lineCount++
			offset += int64(len(scanner.Bytes())) + 1
		}
	}
	if err := scanner.Err(); err != nil {
		file.Close()
		return nil, err
	}

	return w, nil
}

// Close closes the underlying WAL file.
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

// GetTermAtIndex returns the term at a 1-based local WAL index.
// It uses the in-memory offset table to seek directly to the requested entry.
func (w *Wal) GetTermAtIndex(index int32) (int32, error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	if index <= 0 || int(index) > len(w.offsets) {
		return 0, errors.New("no entry at that index")
	}

	offset := w.offsets[int(index)-1]

	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}

	reader := bufio.NewReader(w.file)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if line == "" {
		return 0, errors.New("no entry at that index")
	}

	parts := strings.Split(line, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("malformed log entry at index %d", index)
	}

	term, err := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil {
		return 0, err
	}

	return int32(term), nil
}

// EntriesFromIndex returns all entries from a 1-based local WAL index onward.
func (w *Wal) EntriesFromIndex(index int) ([]string, error) {

	return w.Filter(func(i int, entry string) bool {
		return i+1 >= int(index)
	})
}

// rewriteEntriesLocked replaces the WAL file with entries and rebuilds metadata.
// Caller must hold w.mutex. This is used by truncation and compaction so the
// file contents, offsets, lastEntry, and lineCount stay consistent.
func (w *Wal) rewriteEntriesLocked(entries []string) error {
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	w.offsets = w.offsets[:0]
	w.lastEntry = ""
	w.lineCount = 0

	for _, entry := range entries {
		offset, err := w.file.Seek(0, 1)
		if err != nil {
			return err
		}

		if _, err := w.file.WriteString(entry + "\n"); err != nil {
			return err
		}

		w.offsets = append(w.offsets, offset)
		w.lastEntry = entry
		w.lineCount++
	}

	return nil
}
