package plugin

import (
	"context"
	"testing"
	"time"
)

// These tests cover all deadline relationships: a caller deadline longer than,
// shorter than, or absent relative to Config.CallTimeout. The first two must
// select the earlier deadline, while the last must still acquire a deadline.

// TestCallContextCapsALongerCallerDeadline checks the literal default without
// making the suite wait for it. The subprocess test below proves the returned
// context actually governs plugin work using smaller bounds.
func TestCallContextCapsALongerCallerDeadline(t *testing.T) {
	t.Parallel()

	p := &Plugin{cfg: Config{CallTimeout: DefaultCallTimeout}}

	const budget = 2 * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	callCtx, callCancel := p.callContext(ctx)
	defer callCancel()

	deadline, ok := callCtx.Deadline()
	if !ok {
		t.Fatal("the call context carries no deadline, want the host's bound")
	}

	remaining := time.Until(deadline)
	if remaining > DefaultCallTimeout {
		t.Errorf("the call was left %s, want no more than the host's %s bound",
			remaining, DefaultCallTimeout)
	}
}

// TestCallContextBoundsACallerWithNoDeadline is the backstop the rule above
// exists to preserve: nothing changes for the caller that brought no deadline,
// because there is nothing else to end that call.
func TestCallContextBoundsACallerWithNoDeadline(t *testing.T) {
	t.Parallel()

	p := &Plugin{cfg: Config{CallTimeout: DefaultCallTimeout}}

	callCtx, cancel := p.callContext(context.Background())
	defer cancel()

	deadline, ok := callCtx.Deadline()
	if !ok {
		t.Fatal("a call with no deadline of its own was left unbounded, want CallTimeout")
	}

	if remaining := time.Until(deadline); remaining > DefaultCallTimeout {
		t.Errorf("the call was left %s, want no more than CallTimeout (%s)", remaining, DefaultCallTimeout)
	}
}

// TestCallContextKeepsAShorterCallerDeadline ensures a step with a five second
// `timeout:` does not wait for the host's longer bound.
func TestCallContextKeepsAShorterCallerDeadline(t *testing.T) {
	t.Parallel()

	p := &Plugin{cfg: Config{CallTimeout: DefaultCallTimeout}}

	const budget = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	callCtx, callCancel := p.callContext(ctx)
	defer callCancel()

	deadline, ok := callCtx.Deadline()
	if !ok {
		t.Fatal("the call context carries no deadline, want the caller's own")
	}

	if remaining := time.Until(deadline); remaining > budget {
		t.Errorf("the call was left %s against a caller that allowed %s", remaining, budget)
	}
}

// TestACallIsCappedByCallTimeoutWhenCallerAllowsMoreTime proves the host bound
// reaches the real plugin subprocess path, even when the step permits longer.
func TestACallIsCappedByCallTimeoutWhenCallerAllowsMoreTime(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "sleepy"))

	// Deliberately shorter than the fixture's own work, standing in for the
	// thirty seconds a shipped host applies and the minutes a codex run takes.
	cfg.CallTimeout = 200 * time.Millisecond

	host := openHost(t, cfg)

	defs := host.TaskDefs()
	if len(defs) != 1 {
		t.Fatalf("host provides %d tasks, want 1", len(defs))
	}

	// The deadline a step's `timeout:` becomes, ample for the work.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()
	_, err := defs[0].Fn(ctx, nil, nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("a %s task under a caller allowing 30s succeeded after %s against a %s CallTimeout, want the host bound to end it",
			sleepyTaskDuration, elapsed, cfg.CallTimeout)
	}

	if elapsed < cfg.CallTimeout {
		t.Errorf("the call returned after %s, before its %s CallTimeout; something other than the host bound ended it", elapsed, cfg.CallTimeout)
	}
	if elapsed >= sleepyTaskDuration {
		t.Errorf("the call returned after %s, want it stopped before the plugin completed %s of work", elapsed, sleepyTaskDuration)
	}
}

// TestACallWithNoDeadlineStillDiesAtCallTimeout is the backstop over the same
// real path: the "slow" fixture blocks until its context ends, and with no
// caller deadline the host's own bound is the only thing that can end it.
func TestACallWithNoDeadlineStillDiesAtCallTimeout(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "slow"))
	cfg.CallTimeout = time.Second

	host := openHost(t, cfg)

	defs := host.TaskDefs()
	if len(defs) != 1 {
		t.Fatalf("host provides %d tasks, want 1", len(defs))
	}

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		// context.Background(), not t.Context(): a context the test would
		// cancel at cleanup is a deadline of sorts, and the shape under test
		// is the caller that brought nothing at all.
		_, err := defs[0].Fn(context.Background(), nil, nil)
		done <- err
	}()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		if err == nil {
			t.Fatalf("a call with no deadline of its own succeeded after %s, want CallTimeout to end it", elapsed)
		}
		if elapsed < cfg.CallTimeout {
			t.Errorf("the call ended after %s, before its %s CallTimeout; something other than the "+
				"bound under test ended it", elapsed, cfg.CallTimeout)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a call with no deadline of its own was still running 15s in, want it bounded by CallTimeout")
	}
}
