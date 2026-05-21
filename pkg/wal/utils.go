package wal

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

// FileExists checks whether a file exists at the given path.
func FileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return !errors.Is(err, os.ErrNotExist)
}

// Parse splits a raw WAL record into command name, params, and term.
// Expected format is METHOD:param1,param2:term.
func Parse(s string) (*entryParsed, error) {
	entrySplice := strings.Split(s, ":")
	methodName := entrySplice[0]
	methodParams := strings.Split(entrySplice[1], ",")
	logTerm, err := strconv.Atoi(entrySplice[2])
	if err != nil {
		return nil, err
	}
	return &entryParsed{MethodName: methodName, MethodParams: methodParams, Term: int32(logTerm)}, nil
}

// ParseToLogEntry converts a raw WAL record back into a LogEntry.
func ParseToLogEntry(s string) (*LogEntry, error) {
	parsedEntry, err := Parse(s)
	if err != nil {
		return nil, err
	}
	return NewLogEntry(parsedEntry.MethodName, parsedEntry.MethodParams, int(parsedEntry.Term)), nil
}

// entryParsed holds the parsed fields from a raw WAL record.
type entryParsed struct {
	MethodName   string
	MethodParams []string
	Term         int32
}
