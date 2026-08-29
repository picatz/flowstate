package plugin

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The three directions [Plugin.callContext] has to get right, and #1130's bug
// was one of them going unasked for as long as the package existed.
//
// The host stacked Config.CallTimeout on top of whatever deadline the caller
// already carried, so the shorter of the two won. Two of the three directions
// were covered — a caller with a shorter deadline (which the host was not the
// reason for), and a caller with none at all
// (TestTaskServiceExecuteStreamIsBoundedByCallTimeout, service_test.go). The
// third, a caller whose deadline is *longer* than the host's bound, is the one
// every real step takes, because both drivers hand a plugin task a deadline
// drawn from its step's `timeout:` — and it was the one nothing asked. Thirty
// seconds is far enough out that no fixture in this package ever reached it,
// which is CLAUDE.md's "a bound nothing reaches is a bound nothing tests"
// wearing its other face: a bound nothing reaches is also a bound nothing
// notices is wrong.
//
// So all three are written down here together. Read apart they are three
// timeout tests; read together they are the rule.

// TestCallContextKeepsALongerCallerDeadline pins the defect exactly as #1130
// stated it — a task that would run past DefaultCallTimeout under a two minute
// caller deadline — without spending thirty seconds to do it.
//
// It asserts on the deadline callContext hands the call rather than on how long
// a call takes, which is what makes the literal 30 seconds affordable to pin:
// the sleeping version of this claim is
// [TestACallOutlivesCallTimeoutWhenItsCallerAllowedTheTime] below, at bounds a
// test suite can wait for. Both are needed. This one would still pass if
// nothing ever used the context it returns; that one would still pass against
// a CallTimeout raised to cover its own fixture.
func TestCallContextKeepsALongerCallerDeadline(t *testing.T) {
	t.Parallel()

	p := &Plugin{cfg: Config{CallTimeout: DefaultCallTimeout}}

	const budget = 2 * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	callCtx, callCancel := p.callContext(ctx)
	defer callCancel()

	deadline, ok := callCtx.Deadline()
	if !ok {
		t.Fatal("the call context carries no deadline, want the caller's own")
	}

	remaining := time.Until(deadline)
	if remaining <= DefaultCallTimeout {
		t.Errorf("the call was left %s, want the caller's %s — the host's %s bound was applied "+
			"beneath a deadline the caller had already chosen (#1130)",
			remaining, budget, DefaultCallTimeout)
	}

	// And the caller's deadline is the one that survived, rather than some
	// third number: a host that raised its own bound to two minutes would
	// satisfy the check above and still be deciding this for the author.
	if want, _ := ctx.Deadline(); !deadline.Equal(want) {
		t.Errorf("call deadline = %s, want the caller's own %s", deadline, want)
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

// TestCallContextKeepsAShorterCallerDeadline is the direction the old rule was
// defending, and it still holds — a step with a five second `timeout:` must not
// wait thirty seconds for a plugin.
//
// It holds for a different reason than it used to, which is why it is worth
// asserting rather than assuming: nothing shortens the caller's deadline any
// more, it is simply passed through, and a passed-through five seconds is five
// seconds.
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

// TestACallOutlivesCallTimeoutWhenItsCallerAllowedTheTime is the same claim
// against a real plugin process, over the whole path a step takes: a task that
// works for longer than the host's CallTimeout completes, because its caller
// said it could.
//
// The elapsed check is the bound being *reached* rather than merely not
// exceeded — CLAUDE.md's rule for a bound, one directory over from the paging
// case that taught it. A fixture that answered instantly would satisfy "the
// call succeeded" whether the host's bound had been skipped or not.
func TestACallOutlivesCallTimeoutWhenItsCallerAllowedTheTime(t *testing.T) {
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
	outputs, err := defs[0].Fn(ctx, nil, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("a %s task under a caller allowing 30s failed after %s against a %s CallTimeout, "+
			"want the caller's deadline to govern (#1130): %v",
			sleepyTaskDuration, elapsed, cfg.CallTimeout, err)
	}

	if elapsed < sleepyTaskDuration {
		t.Errorf("the call returned after %s, before its %s of work could have finished; "+
			"the fixture is not proving what it was written to prove", elapsed, sleepyTaskDuration)
	}

	if got := outputs.GetNamedValues()["result"].GetLiteral().GetStringValue(); got != "awake" {
		t.Errorf("result = %q, want %q", got, "awake")
	}
}

// TestACallWithNoDeadlineStillDiesAtCallTimeout is the backstop over the same
// real path: the "slow" fixture blocks until its context ends, and with no
// caller deadline the host's own bound is the only thing that can end it. The
// wire error identifies that bound without comparing the host's clock to the
// deadline Connect propagates to the plugin process.
func TestACallWithNoDeadlineStillDiesAtCallTimeout(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "slow"))
	cfg.CallTimeout = time.Second

	host := openHost(t, cfg)

	p, ok := host.Lookup("slow")
	if !ok {
		t.Fatal("plugin was not launched")
	}
	service, err := p.TaskService()
	if err != nil {
		t.Fatalf("TaskService: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		// context.Background(), not t.Context(): a context the test would
		// cancel at cleanup is a deadline of sorts, and the shape under test
		// is the caller that brought nothing at all.
		_, err := service.Execute(context.Background(), connect.NewRequest(&pluginv1.ExecuteRequest{
			Task: &flowstatev1.Task{Name: "slow_task"},
		}))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a call with no deadline of its own succeeded, want CallTimeout to end it")
		}
		if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
			t.Errorf("call error code = %s, want %s from CallTimeout (err: %v)",
				connect.CodeOf(err), connect.CodeDeadlineExceeded, err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a call with no deadline of its own was still running 15s in, want it bounded by CallTimeout")
	}
}
