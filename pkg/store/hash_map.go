package store

import (
	"errors"
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
func (h *HashMap) Get(key string) (string, error) {
	h.lockWrites()
	defer h.unlockWrites()
	value, ok := h.data[key]

	if !ok {
		return "", errors.New("Could not find key")
	}

	return value, nil

}

// Set sets a key to a given value, blocks any reads or writes during this.

func (h *HashMap) Set(key, value string) {
	h.lockReadAndWrite()
	defer h.unlockReadAndWrite()

	h.data[key] = value
}

func (h *HashMap) Delete(key string) {
	h.lockReadAndWrite()

	defer h.unlockReadAndWrite()

	delete(h.data, key)
}

func (h *HashMap) Has(key string) bool {
	h.lockWrites()
	defer h.unlockWrites()
	_, has := h.data[key]
	return has
}

// util functions only to be used within the class itself
func (h *HashMap) lockWrites() {
	h.mutex.RLock()
}

func (h *HashMap) unlockWrites() {
	h.mutex.RUnlock()
}

func (h *HashMap) lockReadAndWrite() {
	h.mutex.Lock()
}

func (h *HashMap) unlockReadAndWrite() {
	h.mutex.Unlock()
}
