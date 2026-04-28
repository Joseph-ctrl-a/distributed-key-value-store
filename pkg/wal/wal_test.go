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
	entry := NewLogEntry("SET", []string{"name", "joseph"})
	w.WriteMethodCall(entry)

	w.file.Seek(0, 0)

	log, err := os.ReadFile(w.file.Name())

	if err != nil {
		t.Fatal(err)
	}
	got := string(log)
	expected := "SET name joseph\n"
	if got != expected {
		t.Errorf("expected log to be %s instead got %s", got, expected)
	}
}

func TestFormatEntry(t *testing.T) {
	logEntry := NewLogEntry("SET", []string{"name", "joseph"})

	got := logEntry.FormatEntry()
	expected := "SET name joseph\n"
	if got != expected {
		t.Errorf("expected log to be %s instead got %s", got, expected)
	}

}
