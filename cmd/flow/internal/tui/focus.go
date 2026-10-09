package tui

import "slices"

// Ring is the order focus moves through. It is a value: moving returns the
// ring that results.
type Ring struct {
	names []string
	at    int
}

// NewRing returns a ring over names, focused on the first.
func NewRing(names ...string) Ring { return Ring{names: slices.Clone(names)} }

// Current is the focused name, or "" for an empty ring.
func (r Ring) Current() string {
	if len(r.names) == 0 {
		return ""
	}

	return r.names[r.at]
}

// Names are the members, in order.
func (r Ring) Names() []string { return slices.Clone(r.names) }

// Next focuses the following name, wrapping.
func (r Ring) Next() Ring { return r.step(1) }

// Prev focuses the preceding name, wrapping.
func (r Ring) Prev() Ring { return r.step(-1) }

func (r Ring) step(delta int) Ring {
	if len(r.names) == 0 {
		return r
	}
	r.at = ((r.at+delta)%len(r.names) + len(r.names)) % len(r.names)

	return r
}

// Set focuses name, and leaves the ring alone when it is not a member.
func (r Ring) Set(name string) Ring {
	if i := slices.Index(r.names, name); i >= 0 {
		r.at = i
	}

	return r
}

// Is reports whether name is focused.
func (r Ring) Is(name string) bool { return r.Current() == name }
