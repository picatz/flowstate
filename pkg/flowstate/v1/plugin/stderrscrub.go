package plugin

import (
	"errors"
	"slices"
	"strings"
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

	// maxStderrSecretBytes bounds raw retained material before Scrubber expands
	// each value into its encoded forms. It admits the provider-wide 1 MiB
	// maximum for several concurrent values without allowing count × maximum.
	maxStderrSecretBytes = 8 << 20
)

// stderrSecretScrubber retains a bounded set of values delivered to one plugin
// process, preferring in-flight entries over the post-call tail and failing
// closed if active deliveries fill the bound. Each entry hides its plaintext
// inside secrets.Scrubber's closure, so formatting this registry cannot itself
// disclose the values it protects.
type stderrSecretScrubber struct {
	mu            sync.Mutex
	entries       []stderrSecretEntry
	combined      *secrets.Scrubber
	saturated     bool
	retainedBytes int
	multiline     int
	nextID        uint64
	now           func() time.Time
}

type stderrSecretEntry struct {
	id        uint64
	expires   time.Time
	active    bool
	bytes     int
	multiline bool
	scrubber  *secrets.Scrubber
}

func newStderrSecretScrubber(now func() time.Time) *stderrSecretScrubber {
	if now == nil {
		now = time.Now
	}
	return &stderrSecretScrubber{now: now}
}

func (s *stderrSecretScrubber) add(secret secrets.Secret) func() {
	now := s.now()
	value := secret.Reveal()

	s.mu.Lock()
	if s.saturated {
		s.mu.Unlock()
		return func() {}
	}
	if s.prune(now) {
		s.rebuild()
	}
	for len(s.entries) == maxStderrSecrets || s.retainedBytes+len(value) > maxStderrSecretBytes {
		oldestInactive := slices.IndexFunc(s.entries, func(entry stderrSecretEntry) bool { return !entry.active })
		if oldestInactive < 0 {
			// Forgetting an in-flight value would allow its next log line out.
			// Suppress plugin-controlled log text for this process from now on
			// instead; the instance boundary resets this fail-closed state.
			s.saturated = true
			s.mu.Unlock()
			return func() {}
		}
		clear(s.entries[oldestInactive : oldestInactive+1])
		s.entries = slices.Delete(s.entries, oldestInactive, oldestInactive+1)
		s.rebuild()
	}
	s.nextID++
	id := s.nextID
	s.entries = append(s.entries, stderrSecretEntry{
		id: id, active: true, bytes: len(value), multiline: strings.ContainsAny(value, "\r\n"),
		scrubber: secrets.NewScrubber(secret),
	})
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
	if s.saturated {
		return secrets.Redacted, true
	}
	original := text
	if s.combined != nil {
		text = s.combined.Scrub(text)
	}
	return text, text != original
}

func (s *stderrSecretScrubber) scrubError(err error) (error, bool) {
	if err == nil {
		return nil, false
	}
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.prune(now) {
		s.rebuild()
	}
	if s.saturated {
		return errors.New(secrets.Redacted), true
	}
	if s.combined == nil {
		return err, false
	}
	scrubbed := s.combined.ScrubError(err)
	return scrubbed, scrubbed.Error() != err.Error()
}

func (s *stderrSecretScrubber) hasEntries() bool {
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.prune(now) {
		s.rebuild()
	}
	return s.saturated || len(s.entries) > 0
}

func (s *stderrSecretScrubber) scrubFramedLine(text string, truncated bool) (string, bool) {
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.prune(now) {
		s.rebuild()
	}
	if s.saturated || s.multiline > 0 || (truncated && len(s.entries) > 0) {
		return secrets.Redacted, true
	}
	original := text
	if s.combined != nil {
		text = s.combined.Scrub(text)
	}
	return text, text != original
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
	retainedBytes := 0
	multiline := 0
	for _, entry := range s.entries {
		combined.AddScrubber(entry.scrubber)
		retainedBytes += entry.bytes
		if entry.multiline {
			multiline++
		}
	}
	s.combined = combined
	s.retainedBytes = retainedBytes
	s.multiline = multiline
}
