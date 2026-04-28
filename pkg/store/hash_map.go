package store

import (
	"Distributed_Key_Value_Store/pkg/wal"
	"sync"
)

// HashMap is a thread-safe in-memory key-value store.

type HashMap struct {
	data  map[string]string
	mutex sync.RWMutex
	wal   *wal.Wal
}

// NewHashMap Returns an instance of a HashMap with empty data map property
func NewHashMap(wal *wal.Wal) (hashMap *HashMap) {
	return &HashMap{data: make(map[string]string), wal: wal}
}

// Get retrieves the value for a given key, blocking writes during the read.
func (h *HashMap) Get(key string) (string, bool) {

	h.mutex.RLock()
	defer h.mutex.RUnlock()

	value, ok := h.data[key]

	if !ok {
		return "", false
	}

	return value, true

}

// Set sets a key to a given value, blocks any reads or writes during this.

func (h *HashMap) Set(key, value string) {
	entry := wal.NewLogEntry("SET", []string{key, value})
	h.wal.WriteMethodCall(entry)

	h.mutex.Lock()

	defer h.mutex.Unlock()

	h.data[key] = value
}

// Delete an entry with a given key, blocks any reads or writes during this.
func (h *HashMap) Delete(key string) {
	entry := wal.NewLogEntry("DELETE", []string{key})
	h.wal.WriteMethodCall(entry)
	h.mutex.Lock()

	defer h.mutex.Unlock()

	delete(h.data, key)
}

func (h *HashMap) Has(key string) bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	_, has := h.data[key]
	return has
}
