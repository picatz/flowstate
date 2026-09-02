package secrets

import (
	"cmp"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"sync"
)

const maxScrubCompareBytes = 8 << 20

// Scrubber removes known secret values from text on its way out of an activity.
//
// [Secret] stops a value leaking through anything that formats the secret itself.
// A Scrubber covers the other direction: code that never saw a Secret and has no
// reason to be careful. An HTTP client puts a URL with embedded credentials into
// its error message, a server reflects a token back in a response body, a driver
// includes a connection string in a failure. Any of those can end up in a task
// error, and a task error is surfaced to users and written to workflow history,
// where it is durable and replayable.
//
// So the rule for an activity that resolves a secret is: register it, and pass
// anything the activity returns through [Scrubber.ScrubError].
//
//	scrubber := secrets.NewScrubber(secret)
//
//	resp, err := client.Do(req)
//	if err != nil {
//		return nil, scrubber.ScrubError(err)
//	}
//
// Construct one per activity. Registered values are kept for the scrubber's
// lifetime, so a long-lived one accumulates every plaintext it has ever been
// given; [Scrubber.Reset] drops them.
//
// # What it catches
//
// Each registered secret is matched as its literal value and as the encodings a
// value picks up in transit: percent-encoding with either upper or lower case
// escapes, JSON string escaping, base64 in the standard and URL alphabets with
// and without padding, and hex in either case. Matches are replaced with
// [Redacted], longest first, so an encoded form that contains a shorter one does
// not leave a fragment behind.
//
// It is best effort by construction and not a substitute for keeping a revealed
// value close to its use. A value that has been transformed some other way —
// hashed, encrypted, compressed, split across a header fold, or base64'd as part
// of a larger string, as HTTP basic auth does — will not be recognized. Treat it
// as the last line of defense rather than the first.
//
// The zero Scrubber is valid and empty. A Scrubber is safe for concurrent use.
type Scrubber struct {
	mu sync.RWMutex

	// state closes over the registered values rather than holding them in a
	// field, for the reason given on [Secret]: a field would be reachable by
	// reflection, and printing a Scrubber with %v would then dump every
	// plaintext it holds. A closure has nothing for reflection to find.
	state func() *scrubState
}

// scrubState is the set of strings to redact.
type scrubState struct {
	// seen deduplicates registrations.
	seen map[string]struct{}

	// needles is sorted longest first.
	needles []string

	// byFirst keeps the same order within the only needles that can match at
	// one byte, avoiding a scan of every registered encoding per input byte.
	byFirst map[byte][]string
}

// NewScrubber returns a scrubber that redacts the given secrets.
func NewScrubber(secrets ...Secret) *Scrubber {
	scrubber := &Scrubber{}
	for _, secret := range secrets {
		scrubber.Add(secret)
	}

	return scrubber
}

// Add registers a secret's value, and its encoded forms, for redaction. A zero
// secret registers nothing.
func (s *Scrubber) Add(secret Secret) {
	s.AddValue(secret.Reveal())
}

// AddValue registers a raw value for redaction. It exists for a value that never
// became a [Secret] — one read from a configuration file, or derived from a secret
// by combining it with something else. Prefer [Scrubber.Add].
//
// An empty value registers nothing: it appears in every string, and redacting it
// would destroy the text while protecting nothing.
func (s *Scrubber) AddValue(value string) {
	if value == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.stateLocked()

	var added bool
	for _, form := range encodedForms(value) {
		if _, ok := state.seen[form]; ok {
			continue
		}
		state.seen[form] = struct{}{}
		state.needles = append(state.needles, form)
		added = true
	}

	if !added {
		return
	}

	// Longest first, so replacing one form cannot leave part of a longer one
	// behind. Ties break on content to keep the order stable.
	slices.SortFunc(state.needles, func(a, b string) int {
		return cmp.Or(cmp.Compare(len(b), len(a)), strings.Compare(a, b))
	})
	indexNeedles(state)
}

// AddScrubber registers every value and encoded form held by other without
// exposing them to the caller. It is for composing independently scoped
// scrubbers into one matching pass; adding a scrubber to itself is a no-op.
func (s *Scrubber) AddScrubber(other *Scrubber) {
	if s == other {
		return
	}

	other.mu.RLock()
	needles := slices.Clone(other.readLocked())
	other.mu.RUnlock()
	if len(needles) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.stateLocked()
	for _, needle := range needles {
		if _, ok := state.seen[needle]; ok {
			continue
		}
		state.seen[needle] = struct{}{}
		state.needles = append(state.needles, needle)
	}
	slices.SortFunc(state.needles, func(a, b string) int {
		return cmp.Or(cmp.Compare(len(b), len(a)), strings.Compare(a, b))
	})
	indexNeedles(state)
}

func indexNeedles(state *scrubState) {
	state.byFirst = make(map[byte][]string)
	for _, needle := range state.needles {
		state.byFirst[needle[0]] = append(state.byFirst[needle[0]], needle)
	}
}

// Reset drops every registered value.
func (s *Scrubber) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = nil
}

// stateLocked returns the mutable state, creating it if this is the first
// registration. The caller must hold s.mu for writing.
func (s *Scrubber) stateLocked() *scrubState {
	if s.state != nil {
		return s.state()
	}

	state := &scrubState{seen: make(map[string]struct{})}
	s.state = func() *scrubState { return state }

	return state
}

// readLocked returns the registered needles, or nil when nothing is registered.
// The caller must hold s.mu for reading.
func (s *Scrubber) readLocked() []string {
	if s.state == nil {
		return nil
	}

	return s.state().needles
}

// Len returns how many distinct strings the scrubber will redact, counting encoded
// forms. It is for tests and diagnostics, and never exposes the strings.
func (s *Scrubber) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.readLocked())
}

// Scrub replaces every registered value in text with [Redacted].
func (s *Scrubber) Scrub(text string) string {
	return s.ScrubWith(text, Redacted)
}

// ScrubWith replaces every registered value in text with replacement. It is
// useful when a caller must distinguish newly redacted spans while composing
// several bounded scrubbers; most callers should use [Scrubber.Scrub]. If
// matching would exceed its comparison budget, it returns [Redacted] for the
// whole text rather than risk spending unbounded work on attacker-controlled
// common prefixes.
func (s *Scrubber) ScrubWith(text, replacement string) string {
	if text == "" {
		return text
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.state == nil {
		return text
	}
	state := s.state()

	var out strings.Builder
	compareBytes := 0
	for i := 0; i < len(text); {
		matched := ""
		for _, needle := range state.byFirst[text[i]] {
			// Charge the full candidate length before comparing. This is a
			// conservative bound — HasPrefix may reject sooner — but prevents
			// many long values with a shared prefix from multiplying work.
			if len(needle) > maxScrubCompareBytes-compareBytes {
				return Redacted
			}
			compareBytes += len(needle)
			if strings.HasPrefix(text[i:], needle) {
				matched = needle
				break
			}
		}
		if matched != "" {
			out.WriteString(replacement)
			i += len(matched)
			continue
		}
		out.WriteByte(text[i])
		i++
	}
	return out.String()
}

// Contains reports whether text holds any registered value. Use it to assert that
// something is safe before recording it, rather than to decide whether to scrub —
// scrubbing unconditionally is cheaper than being wrong.
func (s *Scrubber) Contains(text string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, needle := range s.readLocked() {
		if strings.Contains(text, needle) {
			return true
		}
	}

	return false
}

// Format implements [fmt.Formatter], so printing a Scrubber reports how much it
// holds rather than what. Without this, %v would dump every registered plaintext:
// a Scrubber is the object holding every secret an activity resolved, which makes
// it the worst thing in the package to print by accident.
func (s *Scrubber) Format(f fmt.State, verb rune) {
	io.WriteString(f, s.String())
}

// String implements [fmt.Stringer], reporting the count and never the values.
func (s *Scrubber) String() string {
	return fmt.Sprintf("secrets.Scrubber(%d values)", s.Len())
}

// GoString implements [fmt.GoStringer], so %#v does not expose the values.
func (s *Scrubber) GoString() string {
	return s.String()
}

// LogValue implements [slog.LogValuer], so a logged scrubber does not expose the
// values.
func (s *Scrubber) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

// MarshalText implements [encoding.TextMarshaler], emitting the count.
func (s *Scrubber) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// MarshalJSON implements [json.Marshaler], emitting the count as a JSON string.
func (s *Scrubber) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// encodedForms returns the value and the encodings it might appear as.
func encodedForms(value string) []string {
	forms := []string{value}
	jsonString, _ := json.Marshal(value) // encoding a string cannot fail

	candidates := []string{
		string(jsonString[1 : len(jsonString)-1]),
		url.QueryEscape(value),
		url.PathEscape(value),
		lowerPercentEscapes(url.QueryEscape(value)),
		lowerPercentEscapes(url.PathEscape(value)),
		base64.StdEncoding.EncodeToString([]byte(value)),
		base64.RawStdEncoding.EncodeToString([]byte(value)),
		base64.URLEncoding.EncodeToString([]byte(value)),
		base64.RawURLEncoding.EncodeToString([]byte(value)),
		hex.EncodeToString([]byte(value)),
		strings.ToUpper(hex.EncodeToString([]byte(value))),
	}

	for _, encoded := range candidates {
		if encoded != "" && !slices.Contains(forms, encoded) {
			forms = append(forms, encoded)
		}
	}

	return forms
}

// lowerPercentEscapes lowercases the hex digits of percent escapes, leaving the
// rest of the text alone. Go writes %2F where many servers write %2f, and an error
// message quoting a URL back reproduces whichever the server used.
func lowerPercentEscapes(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}

	out := []byte(s)
	for i := 0; i+2 < len(out); i++ {
		if out[i] != '%' {
			continue
		}
		out[i+1] = lowerHex(out[i+1])
		out[i+2] = lowerHex(out[i+2])
	}

	return string(out)
}

// lowerHex lowercases an ASCII hex digit.
func lowerHex(c byte) byte {
	if c >= 'A' && c <= 'F' {
		return c + ('a' - 'A')
	}

	return c
}

// ScrubError returns an error whose message has every registered value redacted.
//
// The result deliberately does not implement Unwrap, so nothing can walk from it
// back to the original text. That matters more than it looks: Temporal's failure
// converter records every level of an error's chain into workflow history, so an
// Unwrap reaching the unredacted original would write the value into history
// despite the scrubbing. Typed extraction with errors.As is unavailable for the
// same reason — a typed error such as [net/url.Error] holds the unredacted URL in
// an exported field.
//
// Classification still works: errors.Is reaches the sentinels and any other
// comparable error in the original chain, which is what a caller needs to decide
// how to handle a failure.
//
// A nil error returns nil, and an error with nothing to redact is returned
// unchanged. Note that only the error's own message is inspected, so an error type
// whose Error method omits its cause's text can still carry a value in a level
// this cannot see; that is another reason to keep a revealed value close to its
// use.
func (s *Scrubber) ScrubError(err error) error {
	if err == nil {
		return nil
	}

	message := err.Error()

	scrubbed := s.Scrub(message)
	if scrubbed == message {
		return err
	}

	return &scrubbedError{message: scrubbed, err: err}
}

// scrubbedError is an error with a redacted message. It keeps the original only to
// answer errors.Is, and never exposes it.
type scrubbedError struct {
	message string
	err     error
}

// Error implements the error interface, returning the redacted message.
func (e *scrubbedError) Error() string {
	return e.message
}

// Is reports whether the original error matches target, so classification survives
// scrubbing without exposing the original through Unwrap.
func (e *scrubbedError) Is(target error) bool {
	return errors.Is(e.err, target)
}

// Format implements [fmt.Formatter] so no verb can reach past the redacted message
// into the original error's own formatting. %q still quotes, because the message
// contains upstream-controlled text and a caller relying on %q for escaping must
// keep it.
func (e *scrubbedError) Format(f fmt.State, verb rune) {
	if verb == 'q' {
		fmt.Fprintf(f, "%q", e.message)
		return
	}

	io.WriteString(f, e.message)
}

// LogValue implements [slog.LogValuer], so logging the error as a structured
// attribute records the redacted message.
func (e *scrubbedError) LogValue() slog.Value {
	return slog.StringValue(e.message)
}
