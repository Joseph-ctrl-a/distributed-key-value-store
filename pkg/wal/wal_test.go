package wal

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

// --- helpers ---

func newTestWal(t *testing.T) *Wal {
	t.Helper()
	w, err := NewWal(filepath.Join(t.TempDir(), "test.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func writeTestEntries(t *testing.T) *Wal {
	t.Helper()
	w := newTestWal(t)
	for i := range 10 {
		entry := NewLogEntry("SET", []string{strconv.Itoa(i), "Value" + strconv.Itoa(i)}, i+1)
		if err := w.Append(entry); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

// --- LogEntry ---

func TestNewLogEntry(t *testing.T) {
	e := NewLogEntry("SET", []string{"k", "v"}, 3)
	if e.MethodName != "SET" {
		t.Errorf("expected MethodName SET, got %q", e.MethodName)
	}
	if e.Term != 3 {
		t.Errorf("expected Term 3, got %d", e.Term)
	}
	if len(e.Params) != 2 {
		t.Errorf("expected 2 params, got %d", len(e.Params))
	}
}

func TestFormatEntry(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		params   []string
		term     int
		expected string
	}{
		{"set two params", "SET", []string{"name", "joseph"}, 1, "SET:name,joseph:1\n"},
		{"delete one param", "DELETE", []string{"key"}, 2, "DELETE:key:2\n"},
		{"three params", "SET", []string{"a", "b", "c"}, 5, "SET:a,b,c:5\n"},
		{"high term", "SET", []string{"k", "v"}, 100, "SET:k,v:100\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewLogEntry(tt.method, tt.params, tt.term).FormatEntry()
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// --- Parse / ParseToLogEntry ---

func TestParse(t *testing.T) {
	t.Run("valid set entry", func(t *testing.T) {
		p, err := Parse("SET:name,joseph:1")
		if err != nil {
			t.Fatal(err)
		}
		if p.MethodName != "SET" {
			t.Errorf("expected MethodName SET, got %q", p.MethodName)
		}
		if p.Term != 1 {
			t.Errorf("expected Term 1, got %d", p.Term)
		}
		if len(p.MethodParams) != 2 || p.MethodParams[0] != "name" || p.MethodParams[1] != "joseph" {
			t.Errorf("unexpected params: %v", p.MethodParams)
		}
	})

	t.Run("valid delete entry", func(t *testing.T) {
		p, err := Parse("DELETE:mykey:7")
		if err != nil {
			t.Fatal(err)
		}
		if p.MethodName != "DELETE" || p.Term != 7 {
			t.Errorf("unexpected parsed entry: %+v", p)
		}
	})

	t.Run("invalid term returns error", func(t *testing.T) {
		_, err := Parse("SET:name,joseph:notanumber")
		if err == nil {
			t.Error("expected error for non-numeric term")
		}
	})
}

func TestParseToLogEntry(t *testing.T) {
	t.Run("valid entry", func(t *testing.T) {
		e, err := ParseToLogEntry("DELETE:key1:3")
		if err != nil {
			t.Fatal(err)
		}
		if e.MethodName != "DELETE" {
			t.Errorf("expected DELETE, got %q", e.MethodName)
		}
		if e.Term != 3 {
			t.Errorf("expected term 3, got %d", e.Term)
		}
		if len(e.Params) != 1 || e.Params[0] != "key1" {
			t.Errorf("unexpected params: %v", e.Params)
		}
	})

	t.Run("round-trip format then parse", func(t *testing.T) {
		original := NewLogEntry("SET", []string{"foo", "bar"}, 9)
		formatted := original.FormatEntry()
		// FormatEntry appends \n; strip it before parsing
		parsed, err := ParseToLogEntry(formatted[:len(formatted)-1])
		if err != nil {
			t.Fatal(err)
		}
		if parsed.MethodName != original.MethodName || parsed.Term != original.Term {
			t.Errorf("round-trip mismatch: original %+v, parsed %+v", original, parsed)
		}
	})
}

// --- FileExists ---

func TestFileExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	if FileExists(path) {
		t.Error("expected false for non-existent file")
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	if !FileExists(path) {
		t.Error("expected true for existing file")
	}
}

// --- Append ---

func TestAppend(t *testing.T) {
	t.Run("single entry written to file", func(t *testing.T) {
		w := newTestWal(t)
		if err := w.Append(NewLogEntry("SET", []string{"name", "joseph"}, 1)); err != nil {
			t.Fatal(err)
		}
		log, err := os.ReadFile(w.file.Name())
		if err != nil {
			t.Fatal(err)
		}
		expected := "SET:name,joseph:1\n"
		if string(log) != expected {
			t.Errorf("expected %q, got %q", expected, string(log))
		}
	})

	t.Run("multiple entries increment lineCount", func(t *testing.T) {
		w := newTestWal(t)
		for i := 1; i <= 5; i++ {
			if err := w.Append(NewLogEntry("SET", []string{strconv.Itoa(i), "v"}, i)); err != nil {
				t.Fatal(err)
			}
		}
		if got := w.LastLogIndex(); got != 5 {
			t.Errorf("expected LastLogIndex 5, got %d", got)
		}
	})

	t.Run("concurrent appends do not panic", func(t *testing.T) {
		w := newTestWal(t)
		var wg sync.WaitGroup
		for i := 1; i <= 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				w.Append(NewLogEntry("SET", []string{strconv.Itoa(i), "v"}, i))
			}(i)
		}
		wg.Wait()
		if got := w.LastLogIndex(); got != 100 {
			t.Errorf("expected 100 entries after concurrent appends, got %d", got)
		}
	})
}

// --- LastLogTerm ---

func TestLastLogTermEmpty(t *testing.T) {
	w := newTestWal(t)
	term, err := w.LastLogTerm()
	if err != nil {
		t.Fatal(err)
	}
	if term != 0 {
		t.Errorf("expected 0 for empty WAL, got %d", term)
	}
}

func TestLastLogTerm(t *testing.T) {
	w := newTestWal(t)
	w.Append(NewLogEntry("SET", []string{"k", "v"}, 2))
	w.Append(NewLogEntry("SET", []string{"k", "v"}, 7))

	got, err := w.LastLogTerm()
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Errorf("expected lastLogTerm 7, got %d", got)
	}
}

// --- LastLogIndex ---

func TestLastLogIndexEmpty(t *testing.T) {
	w := newTestWal(t)
	if got := w.LastLogIndex(); got != 0 {
		t.Errorf("expected 0 for empty WAL, got %d", got)
	}
}

func TestLastLogIndex(t *testing.T) {
	w := writeTestEntries(t)
	if got := w.LastLogIndex(); got != 10 {
		t.Errorf("expected 10, got %d", got)
	}
}

// --- GetTermAtIndex ---

func TestGetTermAtIndex(t *testing.T) {
	tests := []struct {
		name        string
		index       int32
		expectTerm  int32
		expectError bool
	}{
		{"first index", 1, 1, false},
		{"middle index", 5, 5, false},
		{"last index", 10, 10, false},
		{"specific index 7", 7, 7, false},
		{"out of range", 100, 0, true},
		{"index zero", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := writeTestEntries(t)
			got, err := w.GetTermAtIndex(tt.index)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error for index %d, got term %d", tt.index, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expectTerm {
				t.Errorf("expected term %d, got %d", tt.expectTerm, got)
			}
		})
	}
}

// --- SpliceInPlace ---

func TestSpliceInPlace(t *testing.T) {
	t.Run("splice truncates past index", func(t *testing.T) {
		w := writeTestEntries(t)

		if err := w.SpliceInPlace(5); err != nil {
			t.Fatal(err)
		}

		if got := w.LastLogIndex(); got != 5 {
			t.Errorf("expected LastLogIndex 5 after splice, got %d", got)
		}

		term, err := w.LastLogTerm()
		if err != nil {
			t.Fatal(err)
		}
		if term != 5 {
			t.Errorf("expected LastLogTerm 5 after splice, got %d", term)
		}

		_, err = w.GetTermAtIndex(6)
		if err == nil {
			t.Error("expected error for index past splice point")
		}
	})

	t.Run("splice at index 1 keeps only first entry", func(t *testing.T) {
		w := writeTestEntries(t)

		if err := w.SpliceInPlace(1); err != nil {
			t.Fatal(err)
		}

		if got := w.LastLogIndex(); got != 1 {
			t.Errorf("expected LastLogIndex 1, got %d", got)
		}
	})
}

// --- ReadLine ---

func TestReadLine(t *testing.T) {
	t.Run("iterates all entries in order", func(t *testing.T) {
		w := newTestWal(t)
		for i := 1; i <= 3; i++ {
			w.Append(NewLogEntry("SET", []string{strconv.Itoa(i), "v"}, i))
		}

		var lines []string
		err := w.ReadLine(func(_ int, entry string) error {
			lines = append(lines, entry)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) != 3 {
			t.Errorf("expected 3 lines, got %d", len(lines))
		}
		expected0 := "SET:1,v:1"
		if lines[0] != expected0 {
			t.Errorf("expected first line %q, got %q", expected0, lines[0])
		}
	})

	t.Run("empty WAL yields zero callbacks", func(t *testing.T) {
		w := newTestWal(t)
		count := 0
		w.ReadLine(func(_ int, _ string) error {
			count++
			return nil
		})
		if count != 0 {
			t.Errorf("expected 0 callbacks for empty WAL, got %d", count)
		}
	})
}

// --- NewWal with existing file ---

func TestNewWalPreservesExistingContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.log")

	w1, err := NewWal(path)
	if err != nil {
		t.Fatal(err)
	}
	w1.Append(NewLogEntry("SET", []string{"k", "v"}, 1))
	w1.Close()

	w2, err := NewWal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "SET:k,v:1\n" {
		t.Errorf("expected preserved content after reopen, got %q", string(content))
	}
}
