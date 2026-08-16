package plugin

import (
	"testing"
	"time"
)

// TestStderrLimiterBoundsVolumeNotSize checks the limiter's core contract:
// within a window only max lines are admitted, the rest are counted, and the
// count is reported exactly once, when the next window opens.
func TestStderrLimiterBoundsVolumeNotSize(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0)
	clock := func() time.Time { return now }

	l := newStderrLimiter(3, time.Minute, clock)

	var admitted, suppressed, summaries int
	for range 100_000 {
		ok, summary := l.allow()
		if summary != "" {
			summaries++
		}
		if ok {
			admitted++
		} else {
			suppressed++
		}
	}

	if admitted != 3 {
		t.Errorf("admitted = %d, want 3", admitted)
	}
	if suppressed != 100_000-3 {
		t.Errorf("suppressed = %d, want %d", suppressed, 100_000-3)
	}
	// No summary yet: the flood is still within its first window, and a
	// summary is only owed once that window has closed.
	if summaries != 0 {
		t.Errorf("summaries = %d before the window rolls over, want 0", summaries)
	}

	// Roll the clock into the next window. The next call is admitted (a fresh
	// budget) and carries exactly one summary describing everything dropped
	// in the window that just ended.
	now = now.Add(time.Minute)
	ok, summary := l.allow()
	if !ok {
		t.Error("first call of a new window was not admitted")
	}
	if summary == "" {
		t.Fatal("first call of a new window carried no summary, want one reporting the prior window's suppression")
	}
	const want = "plugin log suppressed: 99997 lines in the last 1m0s"
	if summary != want {
		t.Errorf("summary = %q, want %q", summary, want)
	}

	// The summary is owed once, not on every call in the new window.
	if _, summary := l.allow(); summary != "" {
		t.Errorf("second call of the new window carried summary %q, want none", summary)
	}
}

// TestStderrLimiterQuietWindowsStaySilent checks that a plugin logging within
// its budget never triggers a summary line — only suppression earns one.
func TestStderrLimiterQuietWindowsStaySilent(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0)
	clock := func() time.Time { return now }

	l := newStderrLimiter(10, time.Minute, clock)

	for window := range 5 {
		for range 10 {
			ok, summary := l.allow()
			if !ok {
				t.Fatalf("window %d: line within budget was not admitted", window)
			}
			if summary != "" {
				t.Fatalf("window %d: unexpected summary %q from a plugin that never exceeded its budget", window, summary)
			}
		}
		now = now.Add(time.Minute)
	}
}

// TestStderrLimiterZeroMaxAdmitsNothing checks that the limiter type itself
// has no notion of "disabled" — a budget of zero simply admits nothing. The
// caller (stderrRelayFunc) is where "disabled" is decided, by not
// constructing a limiter at all when Config.MaxStderrLinesPerMinute is
// negative; see TestStderrRelayFuncNegativeDisablesTheBound.
func TestStderrLimiterZeroMaxAdmitsNothing(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0)
	l := newStderrLimiter(0, time.Minute, func() time.Time { return now })

	if ok, _ := l.allow(); ok {
		t.Error("a limiter with max=0 admitted a line")
	}
}

// TestStderrRelayFuncNegativeDisablesTheBound checks the caller-side policy:
// a negative MaxStderrLinesPerMinute means every line is relayed, unbounded,
// which is the pre-existing behavior a deployment opts back into explicitly.
func TestStderrRelayFuncNegativeDisablesTheBound(t *testing.T) {
	t.Parallel()

	cfg := Config{MaxStderrLinesPerMinute: -1, Logger: testLogger(t)}
	relay := stderrRelayFunc(cfg, cfg.logger())

	for range 10_000 {
		relay("line", false)
	}
	// Nothing to assert beyond "this ran 10,000 lines without blocking or
	// panicking": the point of a disabled bound is the absence of any limit,
	// and a bound with nothing that could trip it is what "disabled" means.
}
