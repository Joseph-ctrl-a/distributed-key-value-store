package store

import (
	"errors"
	"sync"
)

type HashMap struct {
	data  map[string]string
	mutex sync.RWMutex
}

func (h *HashMap) Get(key string) (string, error) {
	h.lockRead()
	defer h.unlockRead()
	value, ok := h.data[key]

	if !ok {
		return "", errors.New("Could not find key")
	}

	return value, nil

}

func NewHashMap() (hashMap *HashMap) {
	return &HashMap{data: make(map[string]string)}
}
func (h *HashMap) Set(key, value string) {
	h.lockWrite()
	defer h.unlockWrite()

	h.data[key] = value
}

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
