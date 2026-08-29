package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The derivation and the bound, on their own terms. `server/concurrency_test.go`
// is the same mechanism through a real Temporal; this is what can be asked
// without one.

func exclusiveSpec(key *v1.Value) *v1.Workflow {
	return &v1.Workflow{
		Name:        "drain",
		Profile:     v1.CurrentProfile,
		Concurrency: &v1.Concurrency{Key: key},
	}
}

// TestResolveConcurrencyKeyReadsInputsAndNothingElse pins what the resolution
// sees: the run's bound arguments, at the one moment there is nothing else to
// see.
func TestResolveConcurrencyKeyReadsInputsAndNothingElse(t *testing.T) {
	t.Parallel()

	inputs := map[string]*v1.Value{"cluster": v1.NewLiteral("prod-eu")}

	key, err := v1.ResolveConcurrencyKey(t.Context(), exclusiveSpec(v1.NewExpr("inputs.cluster")), inputs)
	require.NoError(t, err)
	assert.Equal(t, "prod-eu", key)

	// A literal is the other spelling, for a workflow exclusive of itself.
	key, err = v1.ResolveConcurrencyKey(t.Context(), exclusiveSpec(v1.NewLiteral("schema-migration")), nil)
	require.NoError(t, err)
	assert.Equal(t, "schema-migration", key)

	// And a workflow that declares nothing resolves to nothing, which the server
	// reads as "compose an ordinary id".
	key, err = v1.ResolveConcurrencyKey(t.Context(), &v1.Workflow{Name: "plain"}, nil)
	require.NoError(t, err)
	assert.Empty(t, key)
}

// TestResolveConcurrencyKeyReachesItsBound is the bound asserted in both
// directions: a key exactly at [v1.MaxConcurrencyKeyLen] is accepted, and one
// byte past it is refused. A test that only checked the refusal would also pass
// against a limit of one.
func TestResolveConcurrencyKeyReachesItsBound(t *testing.T) {
	t.Parallel()

	atBound := strings.Repeat("k", v1.MaxConcurrencyKeyLen)
	key, err := v1.ResolveConcurrencyKey(t.Context(),
		exclusiveSpec(v1.NewLiteral(atBound)), nil)
	require.NoError(t, err, "a key exactly at the bound is legal; the bound is reached, not merely not exceeded")
	assert.Len(t, key, v1.MaxConcurrencyKeyLen)

	_, err = v1.ResolveConcurrencyKey(t.Context(),
		exclusiveSpec(v1.NewLiteral(atBound+"k")), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "past the")
}

// TestResolveConcurrencyKeyRefusesWhatNamesNoResource covers the three shapes
// that would compose a permit meaning something other than what the author
// wrote.
func TestResolveConcurrencyKeyRefusesWhatNamesNoResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     *v1.Workflow
		inputs   map[string]*v1.Value
		contains string
	}{
		{
			name:     "an empty key names nothing",
			spec:     exclusiveSpec(v1.NewLiteral("")),
			contains: "names no resource",
		},
		{
			// Not stringified: a key is compared for equality against other
			// runs' keys, and choosing a spelling on the author's behalf is
			// two spellings of one value, which is two permits.
			name:     "a key that is not a string is refused rather than rendered",
			spec:     exclusiveSpec(v1.NewExpr("size(inputs.cluster)")),
			inputs:   map[string]*v1.Value{"cluster": v1.NewLiteral("prod-eu")},
			contains: "must evaluate to a string",
		},
		{
			// Reachable only from a hand-built specification, since
			// protovalidate marks the field required — and treated as a
			// refusal rather than as "no key", because inventing an empty one
			// would compose a permit every keyless run of this workflow shares.
			name:     "a block with no key at all is refused rather than ignored",
			spec:     &v1.Workflow{Name: "drain", Concurrency: &v1.Concurrency{}},
			contains: "concurrency.key is unset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := v1.ResolveConcurrencyKey(t.Context(), tt.spec, tt.inputs)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.contains)
		})
	}
}

// TestCheckConcurrencyRefusesTheTwoTriggersThatOwnARunsID is the shape check the
// compiler and the server both ask, so a hand-built specification is refused
// exactly as a Flowfile is.
func TestCheckConcurrencyRefusesTheTwoTriggersThatOwnARunsID(t *testing.T) {
	t.Parallel()

	t.Run("a webhook trigger", func(t *testing.T) {
		wf := exclusiveSpec(v1.NewLiteral("k"))
		wf.Triggers = &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{Name: "payments"}}}

		err := v1.CheckConcurrency(wf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "webhook trigger")
	})

	t.Run("a schedule trigger, naming where exclusion between firings lives", func(t *testing.T) {
		wf := exclusiveSpec(v1.NewLiteral("k"))
		wf.Triggers = &v1.Triggers{Schedule: &v1.ScheduleTrigger{Cron: []string{"0 3 * * *"}}}

		err := v1.CheckConcurrency(wf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "triggers.schedule.overlap:")
	})

	t.Run("a manual trigger is fine: it addresses nothing", func(t *testing.T) {
		wf := exclusiveSpec(v1.NewLiteral("k"))
		wf.Triggers = &v1.Triggers{Manual: &v1.ManualTrigger{Denied: true}}

		require.NoError(t, v1.CheckConcurrency(wf))
	})

	t.Run("a workflow declaring no concurrency is unaffected", func(t *testing.T) {
		require.NoError(t, v1.CheckConcurrency(&v1.Workflow{
			Name:     "plain",
			Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{Name: "payments"}}},
		}))
	})
}

// TestConcurrencyArmNamesComeFromTheSchema asserts the spelling is derived
// rather than written down twice — the same guard `OverlapNames` carries, and
// the reason an arm added to the schema is spelled in every diagnostic without
// anybody editing a list.
func TestConcurrencyArmNamesComeFromTheSchema(t *testing.T) {
	t.Parallel()

	names := v1.ConcurrencyOnConflictNames()
	assert.Equal(t, []string{"reject", "join", "terminate_other"}, names)

	for _, name := range names {
		arm, known := v1.ParseConcurrencyOnConflict(name)
		require.True(t, known, "%q round-trips", name)
		assert.Equal(t, name, v1.ConcurrencyOnConflictName(arm))
	}

	_, known := v1.ParseConcurrencyOnConflict("unspecified")
	assert.False(t, known, "the zero arm is not a word an author writes")

	_, known = v1.ParseConcurrencyOnConflict("buffer_one")
	assert.False(t, known, "a schedule's queueing policies are not arms here")
}
