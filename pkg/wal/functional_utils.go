package wal

import "bufio"

func (w *Wal) ForEach(callback func(i int, entry string) error) (err error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	w.file.Seek(0, 0)
	scanner := bufio.NewScanner(w.file)

	var i int
	for scanner.Scan() {
		entry := scanner.Text()
		err = callback(i, entry)
		if err != nil {
			return
		}
		i++
	}
	return
}

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
