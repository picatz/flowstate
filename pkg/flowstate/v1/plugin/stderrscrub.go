package plugin

import (
	"sync"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

const (
	// stderrSecretRetention covers diagnostics emitted shortly after the call
	// that delivered a secret has returned. The registry belongs to one plugin
	// process and is discarded with it.
	stderrSecretRetention = 5 * time.Minute

	// maxStderrSecrets bounds retained deliveries. Entries, rather than encoded
	// forms, are counted because one delivered value is the resource the plugin
	// controls; each entry's Scrubber owns the fixed set of encodings.
	maxStderrSecrets = 256
)

// stderrSecretScrubber retains a bounded tail of values delivered to one plugin
// process. Each entry hides its plaintext inside secrets.Scrubber's closure, so
// formatting this registry cannot itself disclose the values it protects.
type stderrSecretScrubber struct {
	mu       sync.Mutex
	entries  []stderrSecretEntry
	combined *secrets.Scrubber
	nextID   uint64
	now      func() time.Time
}

type stderrSecretEntry struct {
	id       uint64
	expires  time.Time
	active   bool
	scrubber *secrets.Scrubber
}

func newStderrSecretScrubber(now func() time.Time) *stderrSecretScrubber {
	if now == nil {
		now = time.Now
	}
	return &stderrSecretScrubber{now: now}
}

func (s *stderrSecretScrubber) add(secret secrets.Secret) func() {
	now := s.now()

	s.mu.Lock()
	s.prune(now)
	s.nextID++
	id := s.nextID
	s.entries = append(s.entries, stderrSecretEntry{id: id, active: true, scrubber: secrets.NewScrubber(secret)})
	if extra := len(s.entries) - maxStderrSecrets; extra > 0 {
		clear(s.entries[:extra])
		s.entries = s.entries[extra:]
	}
	s.rebuild()
	s.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() { s.release(id) })
	}
}

func (s *stderrSecretScrubber) release(id uint64) {
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.entries {
		if s.entries[i].id == id {
			s.entries[i].active = false
			s.entries[i].expires = now.Add(stderrSecretRetention)
			return
		}
	}
}

func (s *stderrSecretScrubber) scrub(text string) (string, bool) {
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.prune(now) {
		s.rebuild()
	}
	original := text
	if s.combined != nil {
		text = s.combined.Scrub(text)
	}
	return text, text != original
}

func (s *stderrSecretScrubber) hasEntries() bool {
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.prune(now) {
		s.rebuild()
	}
	return len(s.entries) > 0
}

func (s *stderrSecretScrubber) prune(now time.Time) bool {
	kept := s.entries[:0]
	for _, entry := range s.entries {
		if !entry.active && !now.Before(entry.expires) {
			continue
		}
		kept = append(kept, entry)
	}
	clear(s.entries[len(kept):])
	changed := len(kept) != len(s.entries)
	s.entries = kept
	return changed
}

func (s *stderrSecretScrubber) rebuild() {
	combined := secrets.NewScrubber()
	for _, entry := range s.entries {
		combined.AddScrubber(entry.scrubber)
	}
	s.combined = combined
}
