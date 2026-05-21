package wal

import (
	"strconv"
	"strings"
)

// LogEntry is the in-memory representation of one command stored in the WAL.
// The entry is persisted as METHOD:param1,param2:term.
type LogEntry struct {
	MethodName string
	Params     []string
	Term       int
}

// NewLogEntry creates a LogEntry for a state-machine command at a Raft term.
func NewLogEntry(methodName string, params []string, term int) *LogEntry {
	return &LogEntry{MethodName: methodName, Params: params, Term: term}
}

// FormatEntry serializes a LogEntry as one newline-terminated WAL record.
func (l *LogEntry) FormatEntry() string {
	return l.MethodName + ":" + strings.Join(l.Params, ",") + ":" + strconv.Itoa(l.Term) + "\n"
}
