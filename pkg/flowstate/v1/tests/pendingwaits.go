package tests

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a run parked on a gate reports about that gate, held to the same
// expectations on both drivers.
//
// Deliberately shaped like [AssertSignalSenderShape] rather than like a [Case]:
// a [Case] compares final outputs, and everything interesting here is true only
// *while* the run is parked and gone by the time it has any outputs to compare.
// So the workflows and the expectations are shared, and each driver's own test
// file supplies the two things that are irreducibly driver-specific - how to
// observe a running run (a Temporal query durably, a [v1.PendingWaits] registry
// locally) and how to deliver a signal to one.
//
// One shape is missing from this table on purpose. A run holding two gates open
// at once - two `parallel:` branches each parked on a different signal - is a
// durable-driver test only, because the local driver runs branches sequentially
// (see eval.go's runParallel) and can therefore only ever hold one gate at a
// time. That is a pre-existing, documented difference in how the local driver
// schedules, not a difference in what a parked wait reports, so the join
// direction is pinned in engine/waits_test.go where it can actually happen.

// WantWait is what one parked wait must report, on either driver.
//
// HasDeadline rather than a deadline, because the moment itself differs by
// design: durably it is computed from the workflow's replay-safe clock and
// locally from the run's own, which under `flow test` is virtual. What must
// agree is that a gate the author bounded reports a deadline and a gate they
// did not reports none.
type WantWait struct {
	StepID      string
	Path        []string
	SignalName  string
	Policed     bool
	HasDeadline bool
}

// PendingWaitCase is a workflow that parks, what it must say while parked, and
// what releases it.
type PendingWaitCase struct {
	// Name of the case, used for test identification.
	Name string

	// Workflow parks on at least one signal wait and finishes once Release has
	// been delivered.
	Workflow *v1.Workflow

	// Release names the signals to deliver, in order, to let the run finish.
	Release []string

	// Want is the set of waits the run must report while it is parked. Order is
	// not part of it: [AssertPendingWaits] compares as a set, since which
	// coroutine parked first is a scheduling detail on one driver and not a
	// choice on the other.
	Want []WantWait
}

// PendingWaitCases are the shapes both drivers must report identically.
func PendingWaitCases() []PendingWaitCase {
	return []PendingWaitCase{
		{
			Name: "a bounded, policed gate reports its name, its deadline and its policy",
			Workflow: &v1.Workflow{
				Name: "policed-gate",
				Signals: map[string]*v1.SignalPolicy{
					"approve": {Allow: []*v1.SignalPolicyRule{{Subject: "https://idp.example#release-manager"}}},
				},
				Steps: []*v1.Node{
					says("before", "starting"),
					signalGate("approve_gate", "approve", time.Hour),
					says("after", "released"),
				},
			},
			Release: []string{"approve"},
			Want: []WantWait{{
				StepID:      "approve_gate",
				SignalName:  "approve",
				Policed:     true,
				HasDeadline: true,
			}},
		},
		{
			Name: "an unbounded, unpoliced gate reports no deadline and no policy",
			Workflow: &v1.Workflow{
				Name: "open-gate",
				Steps: []*v1.Node{
					signalGate("hold", "nudge", 0),
				},
			},
			Release: []string{"nudge"},
			Want: []WantWait{{
				StepID:     "hold",
				SignalName: "nudge",
				// Unset rather than "very far away": a gate with no `timeout:`
				// waits for a person for as long as the run may live, and a
				// reader must be able to tell that from a deadline that has not
				// arrived yet.
				HasDeadline: false,
				Policed:     false,
			}},
		},
		{
			Name: "a gate inside a sequential loop reports the loop as its path",
			Workflow: &v1.Workflow{
				Name: "gate-in-loop",
				Steps: []*v1.Node{{
					Id: "each",
					Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items: v1.NewLiteralList("only"),
						Body:  []*v1.Node{signalGate("inner_gate", "go", time.Hour)},
					}},
				}},
			},
			Release: []string{"go"},
			Want: []WantWait{{
				StepID: "inner_gate",
				// The enclosing step, outermost first, and never the waiting
				// step itself. A sequential loop has a position on both
				// drivers, which is what makes this shareable at all - the
				// concurrent spellings deliberately report none.
				Path:        []string{"each"},
				SignalName:  "go",
				HasDeadline: true,
			}},
		},
	}
}

// signalGate returns a step that waits for a signal, bounded when timeout is
// positive and unbounded when it is zero.
func signalGate(id, name string, timeout time.Duration) *v1.Node {
	wait := &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: name}}}
	if timeout > 0 {
		wait.Timeout = durationpb.New(timeout)
	}

	return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: wait}}
}

// AssertPendingWaits checks that what a driver reported a run parked on is
// exactly what the case says it must be.
//
// Compared as a set keyed by step id, because the order two gates park in is a
// scheduling detail durably and not a choice locally, and asserting on it would
// make a passing test a statement about coroutines.
func AssertPendingWaits(t testing.TB, got []*v1.PendingWait, want []WantWait) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("the run reported %d parked wait(s), want %d: %s", len(got), len(want), describeWaits(got))
	}

	byStep := make(map[string]*v1.PendingWait, len(got))
	for _, wait := range got {
		if previous, ok := byStep[wait.GetStepId()]; ok {
			t.Fatalf("two parked waits reported the same step %q (%s and %s), so neither can be acted on",
				wait.GetStepId(), describeWaits([]*v1.PendingWait{previous}), describeWaits([]*v1.PendingWait{wait}))
		}
		byStep[wait.GetStepId()] = wait
	}

	for _, expected := range want {
		wait, ok := byStep[expected.StepID]
		if !ok {
			t.Fatalf("the run is parked on step %q and did not report it: %s",
				expected.StepID, describeWaits(got))
		}

		if wait.GetSignalName() != expected.SignalName {
			t.Errorf("step %q reported signal %q, want %q, which is the name an operator has to send",
				expected.StepID, wait.GetSignalName(), expected.SignalName)
		}

		if diff := comparePaths(wait.GetPath(), expected.Path); diff != "" {
			t.Errorf("step %q reported path %v, want %v (%s)",
				expected.StepID, wait.GetPath(), expected.Path, diff)
		}

		if wait.GetPoliced() != expected.Policed {
			t.Errorf("step %q reported policed=%v, want %v. A refused delivery and a delivery "+
				"nobody sent leave the run looking identical, which is why this is reported at all",
				expected.StepID, wait.GetPoliced(), expected.Policed)
		}

		switch {
		case expected.HasDeadline && wait.GetDeadline() == nil:
			t.Errorf("step %q declared a timeout and reported no deadline", expected.StepID)
		case !expected.HasDeadline && wait.GetDeadline() != nil:
			t.Errorf("step %q declared no timeout and reported a deadline of %s. A gate that blocks "+
				"until somebody acts must not look like one that lapses",
				expected.StepID, wait.GetDeadline().AsTime())
		}
	}
}

// comparePaths reports how two paths differ, or "" when they do not.
func comparePaths(got, want []string) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d entries, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			return fmt.Sprintf("entry %d is %q, want %q", i, got[i], want[i])
		}
	}

	return ""
}

// describeWaits renders an answer for a failure message, sorted so the sentence
// does not depend on scheduling.
func describeWaits(waits []*v1.PendingWait) string {
	if len(waits) == 0 {
		return "nothing parked"
	}

	described := make([]string, 0, len(waits))
	for _, wait := range waits {
		deadline := "no deadline"
		if wait.GetDeadline() != nil {
			deadline = wait.GetDeadline().AsTime().String()
		}
		described = append(described, fmt.Sprintf("{step %q path %v signal %q policed=%v %s}",
			wait.GetStepId(), wait.GetPath(), wait.GetSignalName(), wait.GetPoliced(), deadline))
	}
	sort.Strings(described)

	return fmt.Sprint(described)
}
