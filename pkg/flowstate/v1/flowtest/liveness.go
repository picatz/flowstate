package flowtest

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// livenessSettle is how long a quiescent run must stay quiescent, in real time,
// before it is called stuck. The state is exact (see [watchLiveness]); the
// settle covers the one thing the clock cannot see, a goroutine of the run
// itself that is between two blocking calls and about to register again, so it
// is a margin against a false verdict and never a bound on a true one.
const livenessSettle = 25 * time.Millisecond

// stuckError is the case's verdict when its run is held at a gate that nothing
// can ever answer: the failure a hung production run would be, found in the time
// it takes to look rather than in the case's wall-clock limit.
type stuckError struct {
	// waits are the signals the run is blocked on, sorted.
	waits []string
	// dropped are the signals a fault lost at least one delivery of, sorted.
	dropped []string
}

func (e *stuckError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "stuck: the run waits for signal %s and nothing pending can deliver it", quoteNames(e.waits))
	if len(e.dropped) > 0 {
		fmt.Fprintf(&b, " (a fault dropped a delivery of %s)", quoteNames(e.dropped))
	}

	return b.String()
}

func quoteNames(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}

	return strings.Join(quoted, ", ")
}

// watchLiveness cancels the run with a [stuckError] when it can no longer make
// progress: every goroutine registered with the clock has left, no timer is
// pending, and a receive is blocked on a signal nothing is queued for. In that
// state nothing the harness owns can wake the run (a scripted sender is a clock
// participant until it has sent its last delivery, and a delayed one is a
// pending timer), so waiting for the case's wall-clock limit would only spend
// real time to reach the same verdict.
//
// It is event-driven: the clock and the signal queue nudge it when either could
// have just become true, and it reads both afresh each time, so a nudge that
// arrives early costs a look. It only observes the local driver's harness; the
// durable driver has no virtual clock and holds the same run at the same gate.
func watchLiveness(ctx context.Context, clock *v1.VirtualClock, signals *v1.LocalSignals, outcomes *signalOutcomes, runFinished <-chan struct{}, cancel context.CancelCauseFunc) {
	nudge := make(chan struct{}, 1)
	clock.NotifyIdle(nudge)
	signals.NotifyBlocked(nudge)

	stuckWaits := func() []string {
		if !clock.Idle() {
			return nil
		}

		return signals.StuckWaits()
	}

	go func() {
		for {
			select {
			case <-nudge:
			case <-runFinished:
				return
			case <-ctx.Done():
				return
			}

			first := stuckWaits()
			if len(first) == 0 {
				continue
			}
			select {
			case <-time.After(livenessSettle):
			case <-runFinished:
				return
			case <-ctx.Done():
				return
			}
			if again := stuckWaits(); len(again) > 0 {
				cancel(&stuckError{waits: again, dropped: outcomes.droppedNames()})

				return
			}
		}
	}()
}
