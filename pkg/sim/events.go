package sim

import (
	"strings"
	"sync"
	"time"
)

type EventBus struct {
	mu          sync.RWMutex
	nextID      int64
	limit       int
	events      []Event
	subscribers map[chan Event]struct{}
}

func NewEventBus(limit int) *EventBus {
	if limit <= 0 {
		limit = 500
	}
	return &EventBus{subscribers: make(map[chan Event]struct{}), limit: limit}
}

func (b *EventBus) Publish(event Event) Event {
	b.mu.Lock()
	b.nextID++
	event.ID = b.nextID
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	b.events = append(b.events, event)
	if len(b.events) > b.limit {
		b.events = append([]Event(nil), b.events[len(b.events)-b.limit:]...)
	}
	subscribers := make([]chan Event, 0, len(b.subscribers))
	for ch := range b.subscribers {
		subscribers = append(subscribers, ch)
	}
	b.mu.Unlock()

	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
	return event
}

func (b *EventBus) Snapshot() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return append([]Event(nil), b.events...)
}

func (b *EventBus) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 64)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		if _, ok := b.subscribers[ch]; ok {
			delete(b.subscribers, ch)
			close(ch)
		}
		b.mu.Unlock()
	}
}

type lineEventWriter struct {
	mu     sync.Mutex
	buffer string
	emit   func(string)
}

func newLineEventWriter(emit func(string)) *lineEventWriter {
	return &lineEventWriter{emit: emit}
}

func (w *lineEventWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer += string(p)
	parts := strings.Split(w.buffer, "\n")
	w.buffer = parts[len(parts)-1]
	for _, line := range parts[:len(parts)-1] {
		line = strings.TrimSpace(line)
		if line != "" {
			w.emit(line)
		}
	}
	return len(p), nil
}
