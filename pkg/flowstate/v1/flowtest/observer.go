package flowtest

import (
	"context"
	"sync"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// observerFor decides who hears the engine's account of one case.
//
// A context carries one [v1.RunObserver], and a debugged case has two
// interested parties: the transcript's recorder, and a debugging session that
// wants to print what each step produced. Neither is optional in the other's
// favour — the transcript is what a failing case reports afterward, and the
// session is what an author is watching now — so where both are present they
// are teed. Where neither is, nil comes back and nothing is installed, which
// is what keeps a run nobody is listening to from cloning outputs at all.
//
// recorder may be nil (an account being discarded and nothing to gather), or a
// [sensitiveGatherer] where the account is discarded but what the run withholds
// is still needed. The debugger is read from the context because that is where
// [runSuite] put it.
func observerFor(ctx context.Context, recorder v1.RunObserver) v1.RunObserver {
	watching, _ := v1.DebuggerFromContext(ctx).(v1.RunObserver)

	switch {
	case recorder == nil && watching == nil:
		return nil
	case recorder == nil:
		return watching
	case watching == nil:
		return recorder
	default:
		return teeObserver{first: recorder, second: watching}
	}
}

// withholdingGatherer is what a case reads, once its run is over, of what the
// run's steps withheld: the transcript's recorder, or a [sensitiveGatherer]
// where the account is discarded.
type withholdingGatherer interface {
	withheld() sensitiveInputs
}

// sensitiveGatherer hears only what each step withholds, for a case whose
// account is discarded but whose failures must still render as a recorded
// run's do (#2211, Codex on #2215). It keeps the gathered set, nothing else.
type sensitiveGatherer struct {
	mu       sync.Mutex
	gathered v1.SensitiveAccumulator
}

func (*sensitiveGatherer) StepFinished(string, *v1.Node_Outputs, error, bool) {}
func (*sensitiveGatherer) StepSkipped(string)                                 {}
func (*sensitiveGatherer) WaitStarted(string, string, time.Duration, bool)    {}

// StepWithheld implements [v1.WithholdingOnlyRunObserver]: a discarded
// account has no use for the outputs, so the engine copies none for it.
func (g *sensitiveGatherer) StepWithheld(_ string, withhold v1.SensitiveValues) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gathered.Add(withhold)
}

func (g *sensitiveGatherer) withheld() sensitiveInputs {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.gathered.Values()
}

// widenedBy is base with gathered added, each value held once, so a case's
// posture and what its run withheld do not together reach the bound that
// withholds everything merely by repeating each other.
func widenedBy(base, gathered sensitiveInputs) sensitiveInputs {
	if gathered.Empty() {
		return base
	}
	var both v1.SensitiveAccumulator
	both.Add(base)
	both.Add(gathered)

	return both.Values()
}

// teeObserver forwards one account to two listeners, in order.
//
// The recorder goes first, always. It is this repository's own bookkeeping and
// the thing a failing case's report is built from, while the second listener is
// a caller's object that prints to somebody's terminal; if the two ever
// contend, the record is what must not be the casualty. The engine already
// isolates a panicking observer (observeSafely), and that isolation covers this
// type as one observer, so a panic in either listener drops the other's
// remaining call — the reason to put the one that matters first rather than a
// reason to catch panics again here.
//
// One consequence of teeing worth stating: the engine clones a step's outputs
// once, before the callback, so both listeners here share that single copy
// rather than getting one each. [v1.RunObserver] promises an observer its own
// copy, and against two listeners that promise is only as good as both of them
// reading it. That is why this type is unexported and why the second listener
// is discovered rather than registered — the only one that exists formats the
// outputs and forgets them.
type teeObserver struct {
	first  v1.RunObserver
	second v1.RunObserver
}

// TaskNoted implements [v1.TaskNoter] for whichever listener wants notes, so
// teeing a debugger with the recorder does not silence a task's own account.
func (t teeObserver) TaskNoted(step, text string) {
	for _, listener := range []v1.RunObserver{t.first, t.second} {
		if noter, ok := listener.(v1.TaskNoter); ok {
			noter.TaskNoted(step, text)
		}
	}
}

func (t teeObserver) StepFinished(id string, outputs *v1.Node_Outputs, err error, tolerated bool) {
	t.first.StepFinished(id, outputs, err, tolerated)
	t.second.StepFinished(id, outputs, err, tolerated)
}

// StepFinishedWithholding implements [v1.WithholdingRunObserver] for whichever
// listener renders with it, so teeing a debugger with the recorder costs
// neither what a step's rendering must withhold. Each records the outcome as
// it is: what either withholds changes a rendering, never a verdict.
func (t teeObserver) StepFinishedWithholding(id string, outputs *v1.Node_Outputs, err error, tolerated bool, withhold v1.SensitiveValues) {
	for _, listener := range []v1.RunObserver{t.first, t.second} {
		switch listener := listener.(type) {
		case v1.WithholdingRunObserver:
			listener.StepFinishedWithholding(id, outputs, err, tolerated, withhold)
		case v1.WithholdingOnlyRunObserver:
			listener.StepWithheld(id, withhold)
		default:
			listener.StepFinished(id, outputs, err, tolerated)
		}
	}
}

func (t teeObserver) StepSkipped(id string) {
	t.first.StepSkipped(id)
	t.second.StepSkipped(id)
}

// StepSkippedBy implements [v1.GuardRunObserver] for whichever listener quotes
// the condition, so teeing a debugger with the recorder does not cost the
// debugger its account of why a step was skipped.
func (t teeObserver) StepSkippedBy(id, account string, withhold v1.SensitiveValues) {
	for _, listener := range []v1.RunObserver{t.first, t.second} {
		if guard, ok := listener.(v1.GuardRunObserver); ok {
			guard.StepSkippedBy(id, account, withhold)
		} else {
			listener.StepSkipped(id)
		}
	}
}

// GuardFailed implements [v1.GuardRunObserver] for whichever listener hears
// about a condition that could not be evaluated.
func (t teeObserver) GuardFailed(id string, err error, withhold v1.SensitiveValues) {
	for _, listener := range []v1.RunObserver{t.first, t.second} {
		if guard, ok := listener.(v1.GuardRunObserver); ok {
			guard.GuardFailed(id, err, withhold)
		}
	}
}

func (t teeObserver) WaitStarted(id, signal string, timeout time.Duration, bounded bool) {
	t.first.WaitStarted(id, signal, timeout, bounded)
	t.second.WaitStarted(id, signal, timeout, bounded)
}
