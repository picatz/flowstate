package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// #913's grammar: `concurrency:` declares what at most one run of this workflow
// may hold at a time. See [v1.Concurrency] for the mechanism this describes and
// `server/server.go`'s `Run` for where it is enforced.
const exclusiveSource = `edition: v2026.3
name: cluster-drain
inputs:
  cluster:
    type: string
    required: true
concurrency:
  key: ${inputs.cluster}
  on_conflict: reject
steps:
  - id: drain
    log:
      message: ${"draining " + inputs.cluster}
`

// concurrencyRefusals collects everything `flow validate` says about a source, from both
// places it can say it: a compiler diagnostic stops the parse and comes back as
// an error, and a validator diagnostic comes back in the list. A test that read
// only one of the two would pass a file the other refuses.
//
// Every message here has to carry a position, which is checked rather than
// assumed — a diagnostic with no line is one an editor cannot place.
func concurrencyRefusals(t *testing.T, source string) string {
	t.Helper()

	diagnostics, err := flowfile.ValidateSource([]byte(source))

	var reported strings.Builder
	if err != nil {
		reported.WriteString(err.Error())
		reported.WriteString("\n")
		assert.Regexp(t, `\d+:\d+:`, err.Error(), "a compiler diagnostic carried no line and column")
	}
	for _, d := range diagnostics {
		reported.WriteString(d.Message)
		reported.WriteString("\n")
		assert.Positive(t, d.Line, "a diagnostic carried no line: %s", d.Message)
		assert.Positive(t, d.Column, "a diagnostic carried no column: %s", d.Message)
	}

	require.NotEmpty(t, reported.String(), "the file was accepted")

	return reported.String()
}

// TestParsingAConcurrencyBlock pins what the block compiles to, and that every
// key an author wrote has a position a diagnostic could be placed on.
func TestParsingAConcurrencyBlock(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(exclusiveSource))
	require.NoError(t, err)

	concurrency := workflow.GetConcurrency()
	require.NotNil(t, concurrency)
	assert.NotNil(t, concurrency.GetKey().GetExpr(), "a fenced key compiles to an expression")
	assert.Equal(t, v1.Concurrency_ON_CONFLICT_REJECT, concurrency.GetOnConflict())

	for _, path := range []string{"concurrency", "concurrency.key", "concurrency.on_conflict"} {
		_, ok := positions.At(path)
		assert.True(t, ok, "no recorded position for %q", path)
	}
}

// TestAConcurrencyBlockValidates is the property every diagnostic test below has
// to be distinguishable from.
func TestAConcurrencyBlockValidates(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(exclusiveSource))
	require.NoError(t, err)
	require.Empty(t, diagnostics, "a well-formed concurrency block reported a diagnostic")
}

// TestAConcurrencyBlockRoundTrips is the property `flow fmt` depends on: the
// writer is the parser's inverse, so a key one of them knows about and the other
// does not would be a formatter that silently deletes an author's exclusion.
func TestAConcurrencyBlockRoundTrips(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse([]byte(exclusiveSource))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	require.Contains(t, string(written), "concurrency:")
	require.Contains(t, string(written), "on_conflict: reject")

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	assert.Equal(t, v1.Concurrency_ON_CONFLICT_REJECT, again.GetConcurrency().GetOnConflict())
	assert.Equal(t,
		workflow.GetConcurrency().GetKey().GetExpr().String(),
		again.GetConcurrency().GetKey().GetExpr().String())
}

// TestAnUnwrittenOnConflictStaysUnwritten keeps the default distinguishable in
// the file, the way `triggers.schedule.overlap:` already is: `flow fmt` must not
// materialize a word the author did not write.
func TestAnUnwrittenOnConflictStaysUnwritten(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse([]byte(strings.ReplaceAll(exclusiveSource, "  on_conflict: reject\n", "")))
	require.NoError(t, err)
	require.Equal(t, v1.Concurrency_ON_CONFLICT_UNSPECIFIED, workflow.GetConcurrency().GetOnConflict())

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	assert.NotContains(t, string(written), "on_conflict:")
}

// TestConcurrencyDiagnostics covers what `flow validate` says about the shapes a
// workflow id cannot honour — each with a line and a column, and each naming
// what to do instead.
func TestConcurrencyDiagnostics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		source   string
		contains []string
	}{
		{
			// The three queueing policies, refused by name rather than by
			// falling through to "not one of the three": they are real
			// policies with a real spelling in this language, and an author
			// has every reason to be sent to the key that has them.
			name:     "buffer_one is refused and points at the schedule's overlap",
			source:   strings.Replace(exclusiveSource, "on_conflict: reject", "on_conflict: buffer_one", 1),
			contains: []string{"cannot queue", "triggers.schedule.overlap: buffer_one", "reject, join, terminate_other"},
		},
		{
			name:     "buffer_all is refused the same way",
			source:   strings.Replace(exclusiveSource, "on_conflict: reject", "on_conflict: buffer_all", 1),
			contains: []string{"cannot queue", "triggers.schedule.overlap: buffer_all"},
		},
		{
			name:     "cancel_other is refused the same way, because it waits too",
			source:   strings.Replace(exclusiveSource, "on_conflict: reject", "on_conflict: cancel_other", 1),
			contains: []string{"cannot queue", "triggers.schedule.overlap: cancel_other"},
		},
		{
			name:     "a word that is not a policy at all lists the three that are",
			source:   strings.Replace(exclusiveSource, "on_conflict: reject", "on_conflict: refuse", 1),
			contains: []string{`is "refuse"`, "reject, join, terminate_other"},
		},
		{
			name: "a key that reads a step is refused: no step has run at submit",
			source: strings.Replace(exclusiveSource,
				"key: ${inputs.cluster}", "key: ${steps.drain.message}", 1),
			contains: []string{"may not read a step", "resolved at submit", "${inputs.<name>}"},
		},
		{
			name: "a key that reads a var is refused: vars are evaluated after the run starts",
			source: strings.Replace(exclusiveSource,
				"key: ${inputs.cluster}", "key: ${vars.region}", 1),
			contains: []string{"may not read a var", "${inputs.<name>}"},
		},
		{
			name: "a key that reads the run is refused: there is no run yet to have an id",
			source: strings.Replace(exclusiveSource,
				"key: ${inputs.cluster}", "key: ${run.id}", 1),
			contains: []string{"may not read `run`", "before there is a run"},
		},
		{
			name: "a key that reads the trigger is refused: the key holds across start paths",
			source: strings.Replace(exclusiveSource,
				"key: ${inputs.cluster}", "key: ${trigger.name}", 1),
			contains: []string{"may not read `trigger`", "every start path"},
		},
		{
			name: "a key that reads a name nothing declares is refused",
			source: strings.Replace(exclusiveSource,
				"key: ${inputs.cluster}", "key: ${nonesuch}", 1),
			contains: []string{`unknown name "nonesuch"`, "resolved at submit"},
		},
		{
			name: "a block with no key names nothing to hold",
			source: strings.Replace(exclusiveSource,
				"  key: ${inputs.cluster}\n", "", 1),
			contains: []string{"declares no `key:`"},
		},
		{
			// A misspelled member is reported, not ignored: a key that does
			// nothing at run time and gives the author no reason to doubt it is
			// the worst of both outcomes.
			name: "a misspelled member is reported rather than ignored",
			source: strings.Replace(exclusiveSource,
				"on_conflict: reject", "onconflict: reject", 1),
			contains: []string{"onconflict"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reported := concurrencyRefusals(t, tt.source)
			for _, want := range tt.contains {
				assert.Contains(t, reported, want)
			}
		})
	}
}

// TestConcurrencyAlongsideAWebhookTriggerIsRefused and its schedule sibling are
// the two combinations where another addressing scheme already owns the run's
// workflow id. Both are refusals rather than a precedence rule: honouring either
// silently discards the other.
func TestConcurrencyAlongsideAWebhookTriggerIsRefused(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.3
name: paid
inputs:
  order:
    type: string
    required: true
concurrency:
  key: ${inputs.order}
triggers:
  - webhook: payments
    verify:
      stripe: ${secret("env:STRIPE_WEBHOOK")}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order: ${event.body.order}
steps:
  - id: settle
    log:
      message: ${"settling " + inputs.order}
`

	reported := concurrencyRefusals(t, source)
	assert.Contains(t, reported, "webhook trigger")
	assert.Contains(t, reported, "idempotency key")
}

func TestConcurrencyAlongsideAScheduleTriggerIsRefused(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.3
name: nightly-sweep
concurrency:
  key: sweep
triggers:
  schedule:
    cron: "0 3 * * *"
steps:
  - id: sweep
    log:
      message: sweeping
`

	reported := concurrencyRefusals(t, source)
	assert.Contains(t, reported, "schedule trigger")
	assert.Contains(t, reported, "triggers.schedule.overlap:",
		"the refusal names where exclusion between firings actually lives")
}

// TestASecretCannotBeAConcurrencyKey: the key is resolved by the server at
// submit, where there is no activity to resolve a reference in, and the resolved
// value is digested into a workflow id that is durable and broadly readable.
func TestASecretCannotBeAConcurrencyKey(t *testing.T) {
	t.Parallel()

	source := strings.Replace(exclusiveSource,
		"key: ${inputs.cluster}", `key: ${secret("env:CLUSTER_NAME")}`, 1)

	reported := concurrencyRefusals(t, source)
	assert.Contains(t, reported, "secret reference cannot be used as a `concurrency:` key")
}

// TestALiteralConcurrencyKeyIsAllowed covers the other spelling: a workflow that
// is globally exclusive of itself has one key and no expression to resolve.
func TestALiteralConcurrencyKeyIsAllowed(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.3
name: global-migration
concurrency:
  key: schema-migration
steps:
  - id: migrate
    log:
      message: migrating
`

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Empty(t, diagnostics)

	workflow, _, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)
	assert.Equal(t, "schema-migration", workflow.GetConcurrency().GetKey().GetLiteral().GetStringValue())
}
