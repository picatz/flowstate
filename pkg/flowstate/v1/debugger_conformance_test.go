package flowstatev1_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRunWorkflowDebuggerBoundaries is the local half of
// [conformance.DebuggerCases]. The engine package runs the same workflows
// against the durable driver with no debugger installed, which is what makes
// "the same answer" a fact about the pair rather than one driver agreeing with
// itself — see the corpus doc for why the halves are asymmetric.
//
// Each case runs twice. That is the whole design: a single debugged run would
// assert that a debugged run produces *these* outputs, which is satisfied by a
// debugger that changes the answer and a corpus that was written down after it
// did. Running the same workflow plain and then debugged, and comparing the two
// to each other as well as to the declaration, is what makes the claim
// "attaching a debugger changed nothing".
func TestRunWorkflowDebuggerBoundaries(t *testing.T) {
	cases := conformance.DebuggerCases()
	require.NotEmpty(t, cases,
		"the debugger corpus is empty, so every claim below is vacuous")

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			plain, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			require.NoError(t, err, "the undebugged run is the baseline and must succeed")
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, plain, protocmp.Transform()),
				"the undebugged run disagrees with the corpus, so the debugged comparison below "+
					"would be against the wrong answer")

			offers := &recordingDebugger{}
			ctx := v1.NewContextWithDebugger(t.Context(), offers)

			debugged, err := v1.RunWithInputs(ctx, test.Workflow, test.Inputs)
			require.NoError(t, err)

			assert.Empty(t, cmp.Diff(plain, debugged, protocmp.Transform()),
				"the same workflow produced different outputs with a debugger attached, which is "+
					"the one thing a debugger must never do")

			assert.Equal(t, test.Offered, offers.seen(),
				"the boundaries offered are not the ones the corpus states")

			// The local half of [conformance.DebuggerCase.Held], and the link
			// that makes the corpus's own invariant mean something about a real
			// run: `TestEveryHeldBoundaryIsOneTheCorpusOffers` next to the
			// corpus says every held id is an offered one, in order, and the
			// assertion above says the offered list is what this driver
			// actually does. Restating the subsequence rule here would be that
			// check written twice, in the weaker of the two places — this one
			// cannot see the other cases and would drift into a subset test,
			// which is what it was.
			require.NotEmpty(t, test.Held,
				"a case with no holdable boundary states nothing about the durable driver")
			assert.Contains(t, offers.seen(), test.Held[0],
				"the boundary a durable lease holds first is not one this driver ever offered, "+
					"so the two disagree about which boundaries a run has")
		})
	}
}

// TestADebuggerRefusalStopsTheRunRatherThanChangingIt is the negative
// direction, and it is the direction that matters.
//
// The test above proves a debugger that says yes changes nothing. It cannot
// distinguish that from a seam nothing is ever asked at: a driver that never
// called [v1.Debugger.BeforeStep] would pass every case above, because the
// outputs would match and — if the corpus had been written against that driver
// — so would the offers. What separates the two is a debugger that says *no*:
// only a run that is really asking can be stopped by the answer.
func TestADebuggerRefusalStopsTheRunRatherThanChangingIt(t *testing.T) {
	workflow := conformance.DebuggerCases()[0].Workflow

	refusal := &recordingDebugger{stopAfter: 1}
	ctx := v1.NewContextWithDebugger(t.Context(), refusal)

	_, err := v1.RunWithInputs(ctx, workflow, nil)

	require.ErrorIs(t, err, v1.ErrDebugSessionEnded,
		"a debugger that refused a boundary did not end the run, so nothing is actually asking "+
			"at the seam the corpus above measures")
	assert.Len(t, refusal.seen(), 1,
		"the run continued past the refusal, so later steps ran with no answer to whether they may")
}

// recordingDebugger accepts every boundary and remembers it, or refuses once it
// has seen stopAfter of them.
//
// Concurrency-safe because it has to be: a `for_each:` with `max_parallel:`
// above one offers its body from several goroutines, and a recorder that
// happened to be safe only for the sequential cases would be a data race
// waiting for the first case that is not.
type recordingDebugger struct {
	// stopAfter is how many boundaries are allowed before this refuses. Zero
	// means never refuse, which is every case in the corpus.
	stopAfter int

	mu      sync.Mutex
	offered []string
}

func (d *recordingDebugger) BeforeStep(_ context.Context, node *v1.Node, _ *v1.Scope) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.offered = append(d.offered, node.GetId())

	if d.stopAfter > 0 && len(d.offered) >= d.stopAfter {
		return v1.ErrDebugSessionEnded
	}

	return nil
}

func (d *recordingDebugger) seen() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	return append([]string(nil), d.offered...)
}
