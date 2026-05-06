package store

import (
	"sync"
)

// HashMap is a thread-safe in-memory key-value store.

type HashMap struct {
	data  map[string]string
	mutex sync.RWMutex
}

// NewHashMap Returns an instance of a HashMap with empty data map property
func NewHashMap() (hashMap *HashMap) {
	return &HashMap{data: make(map[string]string)}
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
	h.mutex.Lock()

	defer h.mutex.Unlock()

	h.data[key] = value
}

// Delete an entry with a given key, blocks any reads or writes during this.
func (h *HashMap) Delete(key string) {
	h.mutex.Lock()

	defer h.mutex.Unlock()

	delete(h.data, key)
}

// All returns a snapshot of all key-value pairs.
func (h *HashMap) All() map[string]string {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	out := make(map[string]string, len(h.data))
	for k, v := range h.data {
		out[k] = v
	}
	return out
}

// Has checks if a given exists, blocks any writes during this. Returns a boolean.
func (h *HashMap) Has(key string) bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	_, has := h.data[key]
	return has
}
