package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// What this file is for.
//
// #526's third instrument: a refusal by a deny-by-default surface is a rate an
// operator can see, rather than a pattern somebody has to find in logs. The
// question it answers is the one that separates "traffic stopped" from "we are
// refusing all of it", and today only the second half of that pair has any
// signal at all.
//
// It is recorded in [v1.CheckTaskPolicy], the check *both* drivers call — the
// local driver above its retry loop, the durable driver once per activity entry
// — so the driver label is the parameter that function already receives rather
// than a fact each side asserts separately. That is what these tests pin: the
// mapping from that one parameter to the two label values, in both directions.

// TestADeniedDispatchIsCounted runs a workflow the deployment's policy refuses
// and reads the counter.
//
// Through a real run rather than by calling the check directly, because the
// claim is that a refused *dispatch* is counted once — a check called in a loop,
// or one counted per attempt, would satisfy a direct call and lie to an operator
// about how often the policy is being hit.
func TestADeniedDispatchIsCounted(t *testing.T) {
	denying, err := v1.TaskPolicyConfig{Deny: []string{"true"}}.Policy()
	require.NoError(t, err)

	reader := conformance.RecordMetrics(t)

	ctx := v1.NewContextWithTaskPolicy(t.Context(), denying)
	_, err = v1.Run(ctx, oneStep("log"))
	require.Error(t, err, "the policy denies every task")

	collected := conformance.CollectFlowstateMetrics(t, reader)

	points := collected[metricschema.InstrumentPolicyDenials]
	require.Len(t, points, 1, "the denial counter recorded %v", points)
	require.Equal(t, uint64(1), points[0].Count,
		"one refused dispatch, counted once — not once per attempt")
	require.Equal(t, map[string]string{
		metricschema.PolicySurface: metricschema.SurfaceTaskDispatch,
		metricschema.TaskName:      "log",
		metricschema.Driver:        metricschema.DriverLocal,
	}, points[0].Attributes)

	// And the execution the refusal replaced is counted as a failure, with the
	// classification and not the sentence. Without this an operator's error rate
	// would omit exactly the failures the deployment itself caused.
	executions := collected[metricschema.InstrumentTaskExecutions]
	require.Len(t, executions, 1, "the execution counter recorded %v", executions)
	require.Equal(t, map[string]string{
		metricschema.TaskName:    "log",
		metricschema.Driver:      metricschema.DriverLocal,
		metricschema.TaskOutcome: metricschema.OutcomeError,
		metricschema.ErrorType:   v1.ErrorKindPolicyDenied.String(),
	}, executions[0].Attributes)
}

// TestTheDenialCounterLabelsTheDriverItWasToldAbout is the mapping itself, both
// directions.
//
// The durable arm cannot be reached from a local run — `local` is false there
// because the durable driver always has a server in front of it — so the check
// is called directly with each value. A single-direction test would pass just as
// well against a recording site that hard-coded "local", which is the mistake
// worth catching: production traffic labelled as somebody's rehearsal.
func TestTheDenialCounterLabelsTheDriverItWasToldAbout(t *testing.T) {
	denying, err := v1.TaskPolicyConfig{Deny: []string{"true"}}.Policy()
	require.NoError(t, err)

	for _, tc := range []struct {
		local bool
		want  string
	}{
		{local: true, want: metricschema.DriverLocal},
		{local: false, want: metricschema.DriverDurable},
	} {
		reader := conformance.RecordMetrics(t)

		ctx := v1.NewContextWithTaskPolicy(t.Context(), denying)
		require.Error(t, v1.CheckTaskPolicy(ctx, "log", nil, tc.local))

		points := conformance.CollectFlowstateMetrics(t, reader)[metricschema.InstrumentPolicyDenials]
		require.Len(t, points, 1)
		require.Equal(t, tc.want, points[0].Attributes[metricschema.Driver])
	}
}
