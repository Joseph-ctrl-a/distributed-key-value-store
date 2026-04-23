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
	h.lockRead()
	defer h.unlockRead()
	value, ok := h.data[key]

	if !ok {
		return "", errors.New("Could not find key")
	}

	return value, nil

}

// Set sets a key to a given value, blocks any reads or writes during this.

func (h *HashMap) Set(key, value string) {
	h.lockWrite()
	defer h.unlockWrite()

	h.data[key] = value
}

// util functions only to be used within the class itself
func (h *HashMap) lockRead() {
	h.mutex.RLock()
}

func (h *HashMap) unlockRead() {
	h.mutex.RUnlock()
}

func (h *HashMap) lockWrite() {
	h.mutex.Lock()
}

func (h *HashMap) unlockWrite() {
	h.mutex.Unlock()
}
