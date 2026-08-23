package plugin

import (
	"fmt"
	"sync"
	"time"
)

// stderrLimiter bounds how many stderr lines per interval the host will
// relay into its own log stream.
//
// pumpPluginLog already bounds one line, at MaxStderrLine — the whole memory
// cost of a line with no newline in it. That says nothing about how many
// lines arrive per second, and a plugin that loops printing short lines
// never trips it: "bounding one resource does not bound another the peer
// controls the ratio to" (see the package's CLAUDE.md). This is the second
// resource — the rate at which the host formats and emits log records — and
// the peer choosing to flood it costs the worker's CPU on slog formatting
// and whatever the deployment's log pipeline charges per record.
//
// It bounds what the host *relays*, not what the plugin may write:
// pumpPluginLog keeps draining every line regardless, exactly as the
// stdout-after-handshake pump already does, because a full pipe looks like a
// hung plugin rather than a noisy one.
type stderrLimiter struct {
	max    int
	window time.Duration
	now    func() time.Time

	mu          sync.Mutex
	windowStart time.Time
	used        int
	suppressed  int
}

// newStderrLimiter returns a limiter admitting up to max lines per window. A
// nil now uses [time.Now]; a test supplies one to control the window without
// sleeping.
func newStderrLimiter(max int, window time.Duration, now func() time.Time) *stderrLimiter {
	if now == nil {
		now = time.Now
	}
	return &stderrLimiter{max: max, window: window, now: now}
}

// allow reports whether the caller may relay the line it is holding. When a
// window has just rolled over with lines suppressed in the one before it,
// summary is the one line to log describing that flood — reported once, at a
// rate the host chooses, rather than once per suppressed line.
//
// A rate rather than a lifetime cap: a long-lived healthy plugin never ages
// into silence, and a plugin restarted after a crash loop re-earns a fresh
// budget, the same reasoning [stableRun] applies to restart counts.
func (l *stderrLimiter) allow() (ok bool, summary string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	if l.windowStart.IsZero() || now.Sub(l.windowStart) >= l.window {
		if l.suppressed > 0 {
			summary = fmt.Sprintf("plugin log suppressed: %d lines in the last %s", l.suppressed, l.window)
		}
		l.windowStart = now
		l.used = 0
		l.suppressed = 0
	}

	if l.used < l.max {
		l.used++
		return true, summary
	}

	l.suppressed++
	return false, summary
}

// flush returns the summary for whatever the current window has suppressed
// so far, as if its window had just rolled over, and resets the count so a
// caller cannot double-report it.
//
// allow only reports a window's suppression total when another line arrives
// to roll the window over — a plugin that floods and then goes quiet (the
// crash this limiter exists to survive is a common cause) leaves its last
// window's count stranded with nothing left to trigger the report. The pump
// calls this once, when it sees EOF, so the flood a plugin's exit is often
// diagnosing is never the one line this limiter drops.
func (l *stderrLimiter) flush() (summary string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.suppressed == 0 {
		return ""
	}
	summary = fmt.Sprintf("plugin log suppressed: %d lines in the last %s", l.suppressed, l.window)
	l.suppressed = 0
	return summary
}
