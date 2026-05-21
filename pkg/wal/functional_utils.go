package wal

import "bufio"

// ForEach calls callback for every WAL entry in local index order.
// The callback receives a zero-based loop index and the raw entry string.
func (w *Wal) ForEach(callback func(i int, entry string) error) (err error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	w.file.Seek(0, 0)
	scanner := bufio.NewScanner(w.file)

	for i := 0; scanner.Scan(); i++ {
		entry := scanner.Text()
		err = callback(i, entry)
		if err != nil {
			return
		}
	}
	return
}

// Map transforms every WAL entry into a string result.
// It is safe for concurrent callers because it takes the WAL read lock.
func (w *Wal) Map(callback func(i int, entry string) (string, error)) ([]string, error) {
	var res []string

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	w.file.Seek(0, 0)
	scanner := bufio.NewScanner(w.file)

	for i := 0; scanner.Scan(); i++ {
		mapped, err := callback(i, scanner.Text())
		if err != nil {
			return nil, err
		}

		res = append(res, mapped)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

// Filter returns all WAL entries for which callback returns true.
// It is safe for concurrent callers because it takes the WAL read lock.
func (w *Wal) Filter(callback func(i int, entry string) bool) ([]string, error) {
	var res []string

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	w.file.Seek(0, 0)
	scanner := bufio.NewScanner(w.file)

	for i := 0; scanner.Scan(); i++ {
		entry := scanner.Text()

		if callback(i, entry) {
			res = append(res, entry)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

// filterLocked is the lock-free implementation of Filter.
// Caller must hold either w.mutex.RLock or w.mutex.Lock.
func (w *Wal) filterLocked(callback func(i int, entry string) bool) ([]string, error) {
	var res []string

	err := w.forEachLocked(func(i int, entry string) error {
		if callback(i, entry) {
			res = append(res, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// forEachLocked is the lock-free implementation of ForEach.
// Caller must hold either w.mutex.RLock or w.mutex.Lock.
func (w *Wal) forEachLocked(callback func(i int, entry string) error) error {
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(w.file)
	for i := 0; scanner.Scan(); i++ {
		if err := callback(i, scanner.Text()); err != nil {
			return err
		}
	}

	return scanner.Err()
}
