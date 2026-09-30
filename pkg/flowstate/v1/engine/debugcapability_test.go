package engine_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheDurableDriverDoesWhatItAdvertises is the durable half of
// [conformance.CapabilityCase]: for every capability, a session attached to a
// run of each case's workflow is sent the case's commands, and the value the
// run's snapshot advertised must equal what became of them. The root package
// puts the same cases to the local driver.
func TestTheDurableDriverDoesWhatItAdvertises(t *testing.T) {
	t.Parallel()

	cases := conformance.CapabilityCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Field, func(t *testing.T) {
			t.Parallel()

			conformance.AssertCapabilityCase(t, "durable", test, observeDurableCapability(t, test))
		})
	}
}

// observeDurableCapability attaches a session to a run of test's workflow,
// sends test's probe, and reports what the run answered, judging none of it.
//
// Every step is a delayed callback on the workflow clock, spaced so the run has
// reached its next boundary before the next command lands, which is how the
// other durable debug tests are timed.
func observeDurableCapability(t *testing.T, test conformance.CapabilityCase) conformance.CapabilityObserved {
	t.Helper()

	spec := proto.CloneOf(test.Workflow)
	spec.Debug = debugSpec(spec.GetName()).GetDebug()
	// Kept alive past the last command so the run can still be read.
	spec.Steps = append(spec.Steps, sleepStep("linger", time.Hour))

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	var (
		observed conformance.CapabilityObserved
		at       time.Duration
		last     string
	)
	next := func() time.Duration {
		at += 10 * time.Second

		return at
	}

	tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.read(next(), "held", "attach")

	if set := test.Probe.Breakpoints; set != nil {
		tl.ask(next(), sre, &v1.DebugAsk{Verb: v1.DebugVerbBreakpoints, Session: "s1", Request: "set", Breakpoints: set})
		tl.read(at+time.Second, "set", "set")
	}
	if test.Probe.Pause {
		tl.ask(next(), sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause", Lease: 5 * time.Minute})
		tl.read(at+time.Second, "pause", "pause")
	}
	if ask := test.Probe.Inspect; ask != nil {
		tl.env.RegisterDelayedCallback(func() {
			request := proto.CloneOf(ask)
			request.SessionId = "s1"
			request.Revision = querySnapshot(t, tl.env, "").GetRevision()
			encoded, err := tl.env.QueryWorkflow(v1.DebugInspectQuery, request)
			if err != nil {
				observed.InspectError = err

				return
			}
			observed.Inspect = &v1.DebugInspectResponse{}
			require.NoError(t, encoded.Get(observed.Inspect))
		}, next())
	}
	for i, move := range test.Probe.Moves {
		last = fmt.Sprintf("move-%d", i)
		tl.ask(next(), sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: last,
			Action: move.GetAction(), Until: move.GetUntil()})
		tl.read(at+9*time.Second, last, last)
		at += 10 * time.Second
	}
	tl.read(next(), "after", last)

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted(), "the run did not end")
	require.NoError(t, tl.env.GetWorkflowError())

	observed.Advertised = tl.reads["held"].GetCapabilities()
	observed.After = tl.reads["after"]
	if tl.reads["set"] != nil {
		observed.Set, observed.Breakpoints = tl.reads["set"].GetReceipt(), tl.reads["set"].GetBreakpoints()
	}
	if tl.reads["pause"] != nil {
		observed.Pause = tl.reads["pause"].GetReceipt()
	}
	for i := range test.Probe.Moves {
		observed.Moves = append(observed.Moves, tl.reads[fmt.Sprintf("move-%d", i)].GetReceipt())
	}

	return observed
}
