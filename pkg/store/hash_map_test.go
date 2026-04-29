package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	hashmap := NewHashMap()

	hashmap.Set("name", "joseph")

	got, has := hashmap.Get("name")
	expected := "joseph"

	if got != expected {
		t.Errorf("expected %q got %q", expected, got)
	}

	if !has {
		t.Fatal("expected has to be true instead got false")
	}
}

func TestMissingKey(t *testing.T) {
	hashmap := NewHashMap()

	_, has := hashmap.Get("name")

	if has {
		t.Error("expected an has to be false got true")
	}
}

func TestConcurrency(t *testing.T) {

	setAndGet := func(hashmap *HashMap, count int, wg *sync.WaitGroup) {
		defer wg.Done()

		key := "hotkey"
		value := fmt.Sprintf("value%d", count)

		hashmap.Set(key, value)
		hashmap.Get(key)
	}

	hashmap := NewHashMap()
	var wg sync.WaitGroup

	for i := range 10000 {
		wg.Add(1)
		go setAndGet(hashmap, i, &wg)
	}
	wg.Wait()
}

func TestDelete(t *testing.T) {
	hashmap := NewHashMap()

	hashmap.Set("name", "joseph")

	hashmap.Delete("name")

	_, has := hashmap.Get("name")

	if has {
		t.Errorf("Delete did not delete key 'name'!")
	}

}

func TestHas(t *testing.T) {
	hashmap := NewHashMap()

	hashmap.Set("name", "joseph")

	has := hashmap.Has("name")

	if !has {
		t.Error("expected has to be true got false")
	}
}
