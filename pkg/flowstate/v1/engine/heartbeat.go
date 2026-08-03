package engine

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Heartbeats: a long step saying it is still working, and hearing that it should
// stop.
//
// # Two things, and the second is the one that was missing
//
// The obvious half is observability. Until this, a step that took four minutes was
// opaque for four minutes: `flow watch` could say which step a run was on and what
// a *retrying* activity last failed with, and had nothing at all to say about an
// activity on its first attempt that simply had not come back.
//
// The half that matters more is cancellation. Temporal delivers a cancellation to
// a running activity through the *response to a heartbeat* — an activity that never
// heartbeats never learns it was cancelled, and runs to its `StartToCloseTimeout`.
// Which means the guarantee added alongside compensation-on-cancel — that a
// cancelled run waits for the work it started, rather than compensating over the
// top of it — cost up to two minutes per in-flight step and could not be shortened
// by anything the operator did. With this, an http request in flight has its
// context cancelled, returns, and the run stops in about as long as the network
// takes to notice. Those two changes are one feature reported as two.
//
// # Periodic, not per-phase
//
// The heartbeat is a ticker, and the phase is what it happens to be carrying. That
// is the only arrangement that works: a heartbeat *timeout* fails an activity that
// has not heartbeated recently enough, so if heartbeats were emitted only when a
// task changed phase, the timeout would have to exceed the longest a task can
// legitimately spend inside one phase — which for an http request is the whole
// request, which is the whole activity, which would make the timeout meaningless.
//
// Ticking on a clock decouples "how often does the worker say it is alive" from
// "how often does the task do something worth reporting", and only the first is a
// property a timeout can be set against.
//
// # What is carried, and what cannot be
//
// A [v1.Phase], whose name is one of a closed set of constants. Heartbeat details
// are written into workflow history, and history is durable and broadly readable —
// invariant 7's territory exactly. `v1.Phase` has no constructor and an unexported
// field, so nothing derived from a task's inputs can reach here even by mistake;
// the reasoning for that shape lives on the type.

const (
	// heartbeatInterval is how often a running activity says it is alive.
	//
	// Ten seconds is chosen against the timeout below rather than for its own
	// sake: it is the number that makes three consecutive missed heartbeats — a
	// worker genuinely gone, not a worker briefly busy — the thing that fails a
	// step.
	heartbeatInterval = 10 * time.Second

	// heartbeatTimeout is how long Temporal waits before deciding a heartbeating
	// activity has died.
	//
	// Three intervals, deliberately generous. This timeout is the one that can
	// *cause* failures rather than merely detect them, and a step killed because a
	// worker was briefly descheduled is a worse outcome than a dead worker taken
	// thirty seconds to notice. The asymmetry is the whole reason for the factor:
	// a missed heartbeat is cheap to wait through and expensive to act on.
	heartbeatTimeout = 3 * heartbeatInterval
)

// withHeartbeat installs a phase reporter and starts the ticker that sends it,
// returning the context a task should run under and a function that stops it.
//
// The returned stop must be called before the activity returns — always deferred at
// the call site. A ticker outliving its activity would heartbeat against a finished
// activity id, which Temporal answers with an error nobody reads.
//
// The phase is guarded because it is written by the task's goroutine and read by
// the ticker's. That is a mutex over one word rather than an atomic because what it
// protects is a struct, and because this is not on any path where a lock is the
// expensive part — it is contended once every ten seconds against a step doing I/O.
func withHeartbeat(ctx context.Context) (context.Context, func()) {
	// Not an activity in a unit test that calls the function directly, and
	// `activity.RecordHeartbeat` outside an activity context is a panic rather
	// than a no-op. Tests of a task should not have to know that.
	if !activity.IsActivity(ctx) {
		return ctx, func() {}
	}

	var (
		mu sync.Mutex

		// The phase a heartbeat carries before the task has said anything, which is
		// the honest answer for the window between the activity starting and the
		// task reaching its first report: the step is running and has not said what
		// it is doing.
		phase = v1.Phase{}
	)

	ctx = v1.ContextWithProgress(ctx, func(reached v1.Phase) {
		mu.Lock()
		phase = reached
		mu.Unlock()
	})

	done := make(chan struct{})
	var once sync.Once

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				// The activity's own context, cancelled when the run is cancelled
				// or the activity times out. Heartbeating after that is answered
				// with an error and tells nobody anything.
				return
			case <-ticker.C:
				mu.Lock()
				current := phase
				mu.Unlock()

				// The details are the phase's name and nothing else — see the
				// package comment above for why that is a rule and not a
				// convenience.
				activity.RecordHeartbeat(ctx, current.String())
			}
		}
	}()

	return ctx, func() { once.Do(func() { close(done) }) }
}
