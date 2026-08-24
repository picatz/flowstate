package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a local run reports when nobody said how it started.
//
// This is the one half of trigger context the shared corpus deliberately cannot
// hold, because it is the one place the two drivers honestly differ — see
// [conformance.TriggerContextCases]'s own note. The durable driver's empty context
// means "no trigger recorded", which is a gap in a record; the local driver's
// means nothing at all, because there is only one way a local run can happen: a
// person ran it.
//
// So the answer here is a fact rather than a default, and it is the fact the
// whole slice turns on. If a local run reported an empty kind, then
// `if: ${trigger.kind == "manual"}` would be false on the one machine an author
// can watch it being taken on, and the feature would ship conditional behaviour
// that only manifests in production — which is precisely what it exists to avoid.
func TestALocalRunReportsAManualStart(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Name:    "local-default-trigger",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   "how",
			Kind: &v1.Node_Value{Value: v1.NewExpr("trigger.kind")},
		}},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "kind", Value: v1.NewExpr("trigger.kind")},
			{Name: "principal", Value: v1.NewExpr("trigger.principal")},
		},
	}

	outputs, err := v1.Run(t.Context(), workflow)
	require.NoError(t, err)

	values := outputs.GetRunOutputs().GetValues()
	require.Equal(t, v1.TriggerKindManual, values["kind"].GetLiteral().GetStringValue(),
		"a local run is a person at a keyboard, so it reports a manual start; an empty kind here would "+
			"make every `${trigger.kind == \"manual\"}` branch unreachable in rehearsal")

	// And nothing attested, honestly. A local run has no server in front of it to
	// attest anybody, exactly as `run.identity` is empty and `run.local` true —
	// invariant 3's rule that a rehearsal must never look like an attested
	// production run.
	require.Empty(t, values["principal"].GetLiteral().GetStringValue(),
		"a local run attests nobody, so the principal is empty rather than invented")
}

// A stated context wins over that default, which is what `flow test` uses to
// exercise both sides of a branch with no real trigger anywhere.
func TestALocalRunReportsTheTriggerItWasGiven(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Name:    "local-stated-trigger",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   "how",
			Kind: &v1.Node_Value{Value: v1.NewExpr("trigger.kind + \":\" + trigger.name")},
		}},
	}

	ctx := v1.NewContextWithTrigger(t.Context(), v1.NewScheduleTriggerContext("nightly", "ops"))

	outputs, err := v1.Run(ctx, workflow)
	require.NoError(t, err)
	require.Equal(t, "schedule:nightly",
		outputs.GetStepValues()["how"].GetNamedValues()[v1.ValueOutput].GetLiteral().GetStringValue())
}
