package wal

import "fmt"

// TruncateAfter removes entries after index, keeping entries 1..index.
// The index is a 1-based local WAL index. TruncateAfter(0) clears the WAL.
func (w *Wal) TruncateAfter(index int32) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if index < 0 {
		return fmt.Errorf("cannot truncate after negative index %d", index)
	}
	if index > int32(w.lineCount) {
		return fmt.Errorf("cannot truncate after index %d; log only has %d entries", index, w.lineCount)
	}
	if index == int32(w.lineCount) {
		return nil
	}

	splice, err := w.filterLocked(func(i int, entry string) bool {
		return int32(i+1) <= index
	})
	if err != nil {
		return err
	}
	return w.rewriteEntriesLocked(splice)
}

// CompactPrefix removes entries 1..index, keeping entries after index.
// The index is a 1-based local WAL index. CompactPrefix(0) is a no-op.
func (w *Wal) CompactPrefix(index int32) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if index < 0 {
		return fmt.Errorf("cannot compact negative index %d", index)
	}
	if index > int32(w.lineCount) {
		return fmt.Errorf("cannot compact %d entries; log only has %d entries", index, w.lineCount)
	}
	if index == 0 {
		return nil
	}

	splice, err := w.filterLocked(func(i int, entry string) bool {
		return int32(i+1) > index
	})
	if err != nil {
		return err
	}
	return w.rewriteEntriesLocked(splice)
}
