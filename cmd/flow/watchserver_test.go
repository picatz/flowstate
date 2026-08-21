package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// The join nothing else covers: a real run, a real query, the real server, the real
// wire, and this command's renderer at the end of it.
//
// Everything else in watch_test.go answers from a fake, which is right for the state
// machine and useless for this particular claim. The renderer used to be exercised
// entirely by responses these tests built themselves — a shape nothing produced, at
// the time — and that is exactly the arrangement in which a field can be renamed, left
// unset by the server, or filled with something other than what a test assumed, while
// every test stays green. So one test asks a running workload where it is and prints
// the answer through the same code `flow watch` prints it through.
//
// It costs a Temporal dev server, which is why it is behind -short like every other
// dev-server test in this repo. CI runs the full suite, so it is not optional there.

// releaseFirstSignal is the gate the watched run parks on for its first step,
// held open until the watcher has reported seeing it there.
const releaseFirstSignal = "release-first"

// TestWatchFollowsARealRunningExecution drives a watch against a workload that is
// genuinely parked on a step.
//
// The workflow parks on one step and then parks on another, so there is a position
// that changes and a run that stays RUNNING while it does — which is the state this
// whole feature is about, and the one a fake can only assert about itself.
//
// # Why the first step is a gate rather than a log
//
// It used to be a `log` task, and that made the test a race it could lose. A log task
// finishes in a millisecond or two, while the watcher polls every
// [minWatchInterval] — 250ms — through a Temporal query, an RPC and the wire. So
// whether anybody ever *saw* the run on `first` depended entirely on the poller
// getting scheduled before the worker finished a step it had no reason to wait for.
// On a quiet box the first poll usually landed in time; under a full parallel
// `-race` load it did not, and the test failed having seen only `waiting` — never
// `first` — so `distinct()` never reached two positions, the poller never stopped
// itself, and the watch ran to its 60-second deadline before failing.
//
// The fix is the ordering edge the test always needed, rather than a wider window:
// `first` is now a `wait_for_signal:`, and the signal that releases it is sent by
// the poller *because* it has seen the run parked there. The run therefore cannot
// leave `first` until `first` has been observed and reported, which is precisely the
// claim — that a live view is live — stated as a happens-before rather than inferred
// from a step being slower than an RPC. Nothing about what is asserted changes; what
// changes is that the first position is now guaranteed instead of likely.
func TestWatchFollowsARealRunningExecution(t *testing.T) {
	temporal := newTemporalNamespace(t)

	w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{})
	engine.Register(w)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	flowstate := mustNewFlowstateServer(t, temporal)

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(flowstate))
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)

	// Started through the service rather than through Temporal directly, so the run
	// this watches is the kind of run the service creates — the memo it filters by
	// included.
	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name: "parked",
			Steps: []*v1.Node{
				{
					Id: "first",
					Kind: &v1.Node_Wait{Wait: &v1.Wait{
						// Parked until the watcher has seen it here and says so
						// by signalling. The timeout is the backstop for a
						// watcher that never sees it at all — which is a
						// failure, and one this test should reach its
						// assertions to report rather than hang on.
						Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: releaseFirstSignal}},
						Timeout: durationpb.New(90 * time.Second),
					}},
				},
				{
					Id: "waiting",
					Kind: &v1.Node_Wait{Wait: &v1.Wait{
						// Long enough that the run is reliably still on it while
						// this watches, and bounded so a failing test does not leave
						// a worker holding a run for an hour.
						Kind: &v1.Wait_Duration{Duration: durationpb.New(90 * time.Second)},
					}},
				},
			},
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	// Terminated however this ends, including a failure, so the dev server is not
	// left holding a run for the rest of the process.
	t.Cleanup(func() {
		_, _ = flowstate.Terminate(context.Background(), connect.NewRequest(&v1.TerminateRequest{
			WorkflowId: workflowID,
		}))
	})

	// Bounded, because the run itself never finishes inside this test: the watch is
	// stopped from inside the poller once it has reported a position twice, and the
	// deadline is the backstop for a worker that never answers.
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	poller := &movedOn{
		inner: clientPoller{
			workflowID: workflowID,
			server:     serverFlags{address: httpServer.URL},
		},
		stop: cancel,
		// The happens-before edge: the run leaves `first` because this test saw
		// it on `first`, not because a step happened to finish quickly enough.
		// Sent once, on the first sighting; releasing a gate that is already
		// open would be harmless but is not what is being claimed.
		release: func() {
			_, err := flowstate.Signal(context.Background(), connect.NewRequest(&v1.SignalRequest{
				WorkflowId: workflowID,
				Name:       releaseFirstSignal,
			}))
			// Reported rather than fatal: this runs on the poller's goroutine,
			// and a failure here shows up as the position never moving, which
			// the assertions below describe far better than a bare error would.
			if err != nil {
				t.Errorf("releasing the %q gate: %v", releaseFirstSignal, err)
			}
		},
	}

	surface, out, errOut := plainSurface()

	// The plain shape, because it is the one whose whole output is assertable: the
	// live view draws the same state through the same helpers, and its own tests
	// cover the drawing.
	require.NoError(t, followPlainly(ctx, surface, renderingOf(FormatText), poller, minWatchInterval,
		workflowID, nil))

	require.Equal(t, []string{"first", "waiting"}, poller.distinct(),
		"a real run was not observed moving between steps, so nothing here joins up; saw %v",
		poller.positions)

	// The step ids an author wrote, having travelled from the workflow's own query
	// through Describe, the service, the schema and the wire onto two lines of prose —
	// one per move, which is the discipline this shape holds.
	account := errOut.String()
	lines := reportedLines(account)
	require.Len(t, lines, 2,
		"a real run on two steps under one status produced %d line(s):\n%s", len(lines), account)
	require.Contains(t, lines[0], "on first")
	require.Contains(t, lines[1], "on waiting",
		"the run moved and the account said nothing, which is the whole feature")

	for _, line := range lines {
		require.Contains(t, line, "RUNNING")
		require.Contains(t, line, workflowID)
	}

	// And it is still an account rather than an answer: a run that has produced
	// nothing writes nothing to stdout.
	require.Empty(t, out.String(),
		"a run still going wrote outputs it does not have")
}

// movedOn polls for real and stops the watch once the run has been seen on two
// different steps, plus one poll.
//
// Two steps rather than one, because the claim is that a *live* view is live: a single
// position proves the field arrives, and a run observed on one step and then another
// proves the thing that made this worth building. The extra poll is because
// followPlainly checks for cancellation *before* folding an answer in — an interrupted
// poll is the watcher stopping, not the run changing — so stopping on the sighting
// itself would end the watch before that position had been reported to anybody.
type movedOn struct {
	inner clientPoller
	stop  context.CancelFunc

	// release lets the run move off the step it is parked on, called once the
	// run has been seen parked there. This is what makes the first position an
	// ordering guarantee rather than a race against a fast step — see the test's
	// own doc comment.
	release func()

	// released records that release has already been called, so a gate is
	// opened once however many polls observe it standing open afterwards.
	released bool

	// positions are what the server actually answered, in order and with repeats,
	// kept so a test that fails can say what it saw rather than only that it did not
	// see what it wanted.
	positions []string
}

func (p *movedOn) Poll(ctx context.Context) (*v1.GetResponse, error) {
	response, err := p.inner.Poll(ctx)

	position := positionPath(response.GetProgress())
	if position == "" {
		return response, err
	}

	p.positions = append(p.positions, position)

	// Released only after the position has been recorded, which is the edge
	// that matters: the sighting is what causes the run to move on, so the run
	// cannot have moved on before the sighting.
	if !p.released && p.release != nil {
		p.released = true
		p.release()
	}

	if len(p.distinct()) >= 2 && p.positions[len(p.positions)-1] == p.positions[len(p.positions)-2] {
		p.stop()
	}

	return response, err
}

// distinct is the positions seen, in order, without the repeats.
func (p *movedOn) distinct() []string {
	var seen []string
	for _, position := range p.positions {
		if len(seen) == 0 || seen[len(seen)-1] != position {
			seen = append(seen, position)
		}
	}

	return seen
}
