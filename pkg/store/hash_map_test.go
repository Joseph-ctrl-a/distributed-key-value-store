package store

import (
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
