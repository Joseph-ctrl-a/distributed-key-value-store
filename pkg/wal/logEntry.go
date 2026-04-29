package wal

import (
	"strconv"
	"strings"
)

// LogEntry defines the structure of a how a log inside the WAL should look
type LogEntry struct {
	MethodName string
	Params     []string
	Term       int
}

// NewLogEntry Creates a new LogEntry returning its memory address
func NewLogEntry(methodName string, params []string, term int) *LogEntry {
	return &LogEntry{MethodName: methodName, Params: params, Term: term}
}

// FormatEntry formats the LogEntry into a standard format.
func (l *LogEntry) FormatEntry() string {
	return l.MethodName + ":" + strings.Join(l.Params, ",") + ":" + strconv.Itoa(l.Term) + "\n"
}
