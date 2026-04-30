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

// Parse splits a formatted WAL entry string into its components.
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

// ParseToLogEntry parses a formatted WAL entry string into a LogEntry.
func ParseToLogEntry(s string) (*LogEntry, error) {
	parsedEntry, err := Parse(s)
	if err != nil {
		return nil, err
	}
	return NewLogEntry(parsedEntry.MethodName, parsedEntry.MethodParams, int(parsedEntry.Term)), nil
}

// entryParsed holds the raw parsed fields from a WAL entry string.
type entryParsed struct {
	MethodName   string
	MethodParams []string
	Term         int32
}
