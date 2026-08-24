package flowtest

import (
	"context"
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
// recorder may be nil (an account being discarded). The debugger is read from
// the context because that is where [runSuite] put it.
func observerFor(ctx context.Context, recorder *runRecorder) v1.RunObserver {
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

func (t teeObserver) StepFinished(id string, outputs *v1.Node_Outputs, err error, tolerated bool) {
	t.first.StepFinished(id, outputs, err, tolerated)
	t.second.StepFinished(id, outputs, err, tolerated)
}

func (t teeObserver) StepSkipped(id string) {
	t.first.StepSkipped(id)
	t.second.StepSkipped(id)
}

func (t teeObserver) WaitStarted(id, signal string, timeout time.Duration, bounded bool) {
	t.first.WaitStarted(id, signal, timeout, bounded)
	t.second.WaitStarted(id, signal, timeout, bounded)
}
