package wal

import (
	"os"
	"testing"
)

func TestWriteMethodCall(t *testing.T) {
	w, err := NewWal(t.TempDir() + "/test.log")

	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		w.Close()
	})
	entry := NewLogEntry("SET", []string{"name", "joseph"}, 1)
	w.WriteMethodCall(entry)

	w.file.Seek(0, 0)

	log, err := os.ReadFile(w.file.Name())

	if err != nil {
		t.Fatal(err)
	}
	got := string(log)
	expected := "SET:name,joseph:1\n"
	if got != expected {
		t.Errorf("expected log to be %s instead got %s", got, expected)
	}
}

func TestFormatEntry(t *testing.T) {
	logEntry := NewLogEntry("SET", []string{"name", "joseph"}, 1)

	got := logEntry.FormatEntry()
	expected := "SET:name,joseph:1\n"
	if got != expected {
		t.Errorf("expected log to be %s instead got %s", expected, got)
	}

}

func TestLastLogTerm(t *testing.T) {
	w, err := NewWal(t.TempDir() + "/test.log")
	if err != nil {
		t.Fatal(err)
	}
	entry := NewLogEntry("SET", []string{"name", "joseph"}, 1)
	w.WriteMethodCall(entry)

	got, err := w.LastLogTerm()
	expected := 1
	if err != nil {
		t.Fatal(err)
	}
	if got != expected {
		t.Errorf("expected lastLogTerm to be %d instead got %d", expected, got)
	}
	t.Cleanup(func() {
		w.Close()
	})
}

func TestLastLogIndex(t *testing.T) {
	w, err := NewWal(t.TempDir() + "/test.log")
	if err != nil {
		t.Fatal(err)
	}
	entry := NewLogEntry("SET", []string{"name", "joseph"}, 1)
	w.WriteMethodCall(entry)

	got := w.LastLogIndex()
	expected := 1

	if got != expected {
		t.Errorf("expected LastLogIndex to return %d instead got %d", expected, got)
	}
	t.Cleanup(func() {
		w.Close()
	})
}
