package authtest

import (
	"sync"
	"time"
)

// Clock is a clock a test moves by hand.
//
// Token lifetimes are the part of an authentication policy that a test cannot
// wait for: a ten minute maximum token age is ten minutes. Pass a Clock's [Now]
// to both the issuer ([WithClock]) and the verifier under test, and the same
// question becomes a call to [Clock.Advance].
//
// A Clock is safe for concurrent use, which it has to be: the verifier reads it
// from whatever goroutine is handling a request.
//
// The zero value reads as the zero time. Use [NewClock].
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock returns a clock stopped at the given instant.
//
// Prefer a fixed instant over [time.Now] where a test asserts on a timestamp,
// so that what the test claims does not depend on when it runs.
func NewClock(now time.Time) *Clock {
	return &Clock{now: now}
}

// Now returns the instant the clock is stopped at. It is the method to pass
// wherever a clock function is wanted.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward. A negative duration moves it back, which is
// how a clock running behind an issuer's is modelled.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
