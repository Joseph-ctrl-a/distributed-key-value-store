package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	hashmap := NewHashMap()

	hashmap.Set("name", "joseph")

	got, err := hashmap.Get("name")
	expected := "joseph"

	if got != expected {
		t.Errorf("expected %q got %q", expected, got)
	}

	if err != nil {
		t.Fatalf("expected err to be nil instead %q", err)
	}
}

func TestMissingKey(t *testing.T) {
	hashmap := NewHashMap()

	_, err := hashmap.Get("name")

	if err == nil {
		t.Error("expected an err got nil")
	}
}

func TestConcurrency(t *testing.T) {

	setAndGet := func(h Store, count int, wg *sync.WaitGroup) {
		defer wg.Done()

		key := "hotkey"
		value := fmt.Sprintf("value%d", count)

		h.Set(key, value)
		h.Get(key)
	}

	h := NewHashMap()
	var wg sync.WaitGroup

	for i := range 10000 {
		wg.Add(1)
		go setAndGet(h, i, &wg)
	}
	wg.Wait()
}

func TestDelete(t *testing.T) {

}
