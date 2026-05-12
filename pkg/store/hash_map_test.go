package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewHashMap(t *testing.T) {
	h := NewHashMap()
	if h == nil {
		t.Fatal("NewHashMap returned nil")
	}
	if h.Has("any") {
		t.Error("new HashMap should be empty")
	}
}

func TestSet(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"basic", "name", "joseph"},
		{"empty value", "key", ""},
		{"empty key", "", "value"},
		{"unicode", "日本語", "テスト"},
		{"overwrite", "name", "updated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHashMap()
			h.Set(tt.key, tt.value)
			got, ok := h.Get(tt.key)
			if !ok {
				t.Errorf("Get after Set: expected ok=true")
			}
			if got != tt.value {
				t.Errorf("expected %q, got %q", tt.value, got)
			}
		})
	}
}

func TestSetOverwrite(t *testing.T) {
	h := NewHashMap()
	h.Set("key", "first")
	h.Set("key", "second")
	got, ok := h.Get("key")
	if !ok {
		t.Fatal("expected key to exist after overwrite")
	}
	if got != "second" {
		t.Errorf("expected %q after overwrite, got %q", "second", got)
	}
}

func TestGet(t *testing.T) {
	t.Run("existing key", func(t *testing.T) {
		h := NewHashMap()
		h.Set("k", "v")
		got, ok := h.Get("k")
		if !ok {
			t.Error("expected ok=true for existing key")
		}
		if got != "v" {
			t.Errorf("expected %q, got %q", "v", got)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		h := NewHashMap()
		got, ok := h.Get("missing")
		if ok {
			t.Error("expected ok=false for missing key")
		}
		if got != "" {
			t.Errorf("expected empty string for missing key, got %q", got)
		}
	})

	t.Run("empty string value vs missing key", func(t *testing.T) {
		h := NewHashMap()
		h.Set("empty", "")
		got, ok := h.Get("empty")
		if !ok {
			t.Error("expected ok=true for key with empty string value")
		}
		if got != "" {
			t.Errorf("expected empty string, got %q", got)
		}

		_, missingOk := h.Get("not-set")
		if missingOk {
			t.Error("expected ok=false for unset key")
		}
	})
}

func TestDelete(t *testing.T) {
	t.Run("existing key", func(t *testing.T) {
		h := NewHashMap()
		h.Set("name", "joseph")
		h.Delete("name")

		_, ok := h.Get("name")
		if ok {
			t.Error("Get: expected ok=false after Delete")
		}
		if h.Has("name") {
			t.Error("Has: expected false after Delete")
		}
	})

	t.Run("non-existent key does not panic", func(t *testing.T) {
		h := NewHashMap()
		h.Delete("ghost")
	})

	t.Run("delete one key leaves others", func(t *testing.T) {
		h := NewHashMap()
		h.Set("a", "1")
		h.Set("b", "2")
		h.Delete("a")

		if h.Has("a") {
			t.Error("expected 'a' to be deleted")
		}
		if !h.Has("b") {
			t.Error("expected 'b' to still exist")
		}
	})
}

func TestHas(t *testing.T) {
	t.Run("existing key", func(t *testing.T) {
		h := NewHashMap()
		h.Set("name", "joseph")
		if !h.Has("name") {
			t.Error("expected Has=true for existing key")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		h := NewHashMap()
		if h.Has("missing") {
			t.Error("expected Has=false for missing key")
		}
	})

	t.Run("after delete", func(t *testing.T) {
		h := NewHashMap()
		h.Set("key", "val")
		h.Delete("key")
		if h.Has("key") {
			t.Error("expected Has=false after Delete")
		}
	})
}

func TestConcurrentWrites(t *testing.T) {
	h := NewHashMap()
	var wg sync.WaitGroup

	for i := range 10000 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			h.Set(key, fmt.Sprintf("value%d", i))
		}(i)
	}
	wg.Wait()
}

func TestConcurrentReads(t *testing.T) {
	h := NewHashMap()
	h.Set("shared", "constant")

	var wg sync.WaitGroup
	for range 10000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, ok := h.Get("shared")
			if !ok {
				t.Errorf("expected ok=true on concurrent read")
			}
			if got != "constant" {
				t.Errorf("expected %q, got %q", "constant", got)
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentMixed(t *testing.T) {
	h := NewHashMap()
	var wg sync.WaitGroup

	// writers
	for i := range 1000 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h.Set(fmt.Sprintf("key%d", i%100), fmt.Sprintf("val%d", i))
		}(i)
	}

	// readers
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Get(fmt.Sprintf("key%d", 42))
		}()
	}

	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h.Delete(fmt.Sprintf("key%d", i))
		}(i)
	}

	wg.Wait()
}

func TestReplaceAll(t *testing.T) {
	h := NewHashMap()
	h.Set("old", "value")

	source := map[string]string{
		"a": "1",
		"b": "2",
	}

	h.ReplaceAll(source)

	if h.Has("old") {
		t.Error("expected ReplaceAll to remove old keys")
	}

	got, ok := h.Get("a")
	if !ok || got != "1" {
		t.Errorf("expected a=1, got %q ok=%t", got, ok)
	}

	source["a"] = "changed"

	got, ok = h.Get("a")
	if !ok || got != "1" {
		t.Errorf("expected store to copy input map, got %q ok=%t", got, ok)
	}
}
