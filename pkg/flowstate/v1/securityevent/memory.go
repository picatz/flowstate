package securityevent

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// MemoryStore is a bounded single-node Store. It deliberately does not claim
// cross-node consistency; clustered deployments must provide a linearizable
// durable implementation instead.
type MemoryStore struct {
	mu      sync.RWMutex
	max     int
	entries map[Key]Entry
}

func NewMemoryStore(max int) (*MemoryStore, error) {
	if max <= 0 {
		return nil, errors.New("securityevent: store bound must be positive")
	}
	return &MemoryStore{max: max, entries: make(map[Key]Entry)}, nil
}
func (s *MemoryStore) Apply(_ context.Context, e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.entries[e.Key]; ok && old.IssuedAt.After(e.IssuedAt) {
		return ErrRefused
	}
	if _, ok := s.entries[e.Key]; !ok && len(s.entries) >= s.max {
		return ErrUnavailable
	}
	s.entries[e.Key] = e
	return nil
}
func (s *MemoryStore) Lookup(_ context.Context, k Key, _ bool) (Entry, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[k]
	return e, ok, nil
}
func (s *MemoryStore) Snapshot(_ context.Context, _ bool, limit int) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		return nil, ErrRefused
	}
	out := make([]Entry, 0, min(limit, len(s.entries)))
	for _, e := range s.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].IssuedAt.Before(out[b].IssuedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *MemoryStore) Compact(_ context.Context, before time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, e := range s.entries {
		if n == limit {
			break
		}
		if !e.ExpiresAt.After(before) {
			delete(s.entries, k)
			n++
		}
	}
	return n, nil
}
