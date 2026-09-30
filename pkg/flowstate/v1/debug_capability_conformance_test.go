package flowstatev1_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheLocalDriverDoesWhatItAdvertises is the local half of
// [conformance.CapabilityCase]: for every capability, a controlled session
// holding each case's workflow is sent the case's commands, and the value its
// snapshot advertised must equal what became of them. The engine package puts
// the same cases to the durable driver.
func TestTheLocalDriverDoesWhatItAdvertises(t *testing.T) {
	t.Parallel()

	cases := conformance.CapabilityCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Field, func(t *testing.T) {
			t.Parallel()

			conformance.AssertCapabilityCase(t, "local", test, observeLocalCapability(t, test))
		})
	}
}

// observeLocalCapability sends test's probe to a controlled local session held
// at its first boundary and reports what came back, judging none of it.
func observeLocalCapability(t *testing.T, test conformance.CapabilityCase) conformance.CapabilityObserved {
	t.Helper()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Workflow: test.Workflow, SourceMap: test.SourceMap})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	go func() {
		ctx := v1.NewContextWithRunObserver(v1.NewContextWithDebugger(t.Context(), session), session)
		_, runErr := v1.RunWithInputs(ctx, test.Workflow, nil)
		session.Finished(runErr)
	}()

	held := awaitDebugState(t, session, 0, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
	observed := conformance.CapabilityObserved{Advertised: held.GetCapabilities(), After: held}

	if set := test.Probe.Breakpoints; set != nil {
		request := proto.CloneOf(set)
		request.RequestId = "set"
		response, err := session.ReplaceBreakpoints(t.Context(), request)
		require.NoError(t, err)
		observed.Set, observed.Breakpoints, observed.After = response.GetReceipt(), response.GetBreakpoints(), response.GetSnapshot()
	}
	if test.Probe.Pause {
		observed.Pause, err = session.Pause(t.Context(), "pause")
		require.NoError(t, err)
	}
	if ask := test.Probe.Inspect; ask != nil {
		request := proto.CloneOf(ask)
		request.Revision = observed.After.GetRevision()
		observed.Inspect, observed.InspectError = session.Inspect(t.Context(), request)
	}
	for i, move := range test.Probe.Moves {
		request := proto.CloneOf(move)
		request.RequestId = fmt.Sprintf("move-%d", i)
		receipt, err := session.Resume(t.Context(), request)
		require.NoError(t, err)
		observed.Moves = append(observed.Moves, receipt)
		if !flowdebug.Accepted(receipt) {
			break
		}
		observed.After = awaitNextStop(t, session, receipt.GetRevision())
	}

	return observed
}
