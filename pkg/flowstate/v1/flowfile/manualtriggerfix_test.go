package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` and `flow fmt` against the two things this slice added to the
// grammar: a `manual:` block, and the `trigger` root read in a body.
//
// Asserted on **bytes**, never on the result still validating. That distinction is
// the whole lesson of CLAUDE.md's rewriter section: every file `flow fix` has ever
// corrupted came out valid, because a whole-step reference with no output name is
// legal — so the file simply computed something else, and nothing downstream could
// see it. A byte comparison is the only assertion that can.
//
// `trigger` is a *root* rather than a name the grammar binds bare, which is what
// makes it safe here for the same reason `run` was: the rewriter only ever roots a
// name that is a declared step id, and a step may not be called `trigger` (see
// [TestTriggerIsRefusedAsANameAFileBinds]). This pins that reasoning against the
// rewriter rather than leaving it as an argument.

// manualAndTriggerFile is on the current edition with nothing left to migrate: a
// narrowing, a refusal's alternative, and the root read in three positions.
const manualAndTriggerFile = `edition: v2026.3
name: trigger-aware
inputs:
  order_id: { type: string, required: true }
triggers:
  - webhook: payments
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order_id: ${event.body.order_id}
  - manual:
      require_reason: true
      allowed_principals:
        - oncall@example.com
steps:
  - id: notify
    if: ${trigger.kind != "schedule"}
    log:
      message: ${"started by " + trigger.kind}
  - id: record
    log:
      message: ${'order ' + inputs.order_id + ' via ' + trigger.name}
`

// TestFixLeavesAManualBlockAndTriggerReferencesByteForByte is the unchanged
// direction, and the important one: nothing here is fix's to touch.
func TestFixLeavesAManualBlockAndTriggerReferencesByteForByte(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(manualAndTriggerFile))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.Equal(t, manualAndTriggerFile, string(result.Source),
		"a file with a `manual:` block and `trigger.` references was rewritten anyway")
}

// TestFixDoesNotRootTriggerAlongsideAStepOfEveryOtherName is the specific
// corruption class, probed rather than assumed.
//
// The rewriter roots a bare identifier when it names a declared step. `trigger` is
// written bare as the operand of a selection (`trigger.kind` parses as a select
// over the identifier `trigger`), so it reaches the same walk that rewrote a
// loop's item and `now` into references to steps of those names. What keeps it
// safe is that no step may be named `trigger` — this file names a step every
// *other* interesting thing to prove the rewriter is doing its ordinary work at
// the same time, so a byte-identical result is evidence rather than a vacuous pass.
func TestFixDoesNotRootTriggerAlongsideAStepOfEveryOtherName(t *testing.T) {
	t.Parallel()

	const source = `edition: v2026.3
name: nearly-shadowed
steps:
  - id: kind
    log:
      message: a step whose id is kind
  - id: name
    log:
      message: a step whose id is name
  - id: principal
    log:
      message: a step whose id is principal
  - id: read
    log:
      message: ${trigger.kind + trigger.name + trigger.principal + steps.kind.message}
`

	result, err := flowfile.Fix([]byte(source))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.Equal(t, source, string(result.Source),
		"`trigger.kind` was rewritten into a reference to the step called `kind`, or the root itself "+
			"was rooted; either way the file now computes something its author did not write")
}

// TestFmtRoundTripsAManualBlockInBothSpellings is [Marshal] being the exact
// inverse of [Parse], which is what stops `flow fmt` deleting what an author
// wrote.
//
// Both spellings, because there are two and a writer that knew one would silently
// migrate every file using the other. The refusal in particular has to come back
// as the scalar it went in as: written as `{denied: true}` it would still parse,
// still mean the same thing, and no longer be found by the `grep` that is the
// entire argument for having a greppable spelling.
func TestFmtRoundTripsAManualBlockInBothSpellings(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name: "a narrowing in the mapping spelling",
			source: `edition: v2026.3
name: break-glass
triggers:
  manual:
    require_reason: true
    allowed_principals:
      - oncall@example.com
      - sre@example.com
steps:
  - id: rotate
    log:
      message: rotating
`,
		},
		{
			name: "a refusal beside the source that does start it",
			source: `edition: v2026.3
name: payments-only
inputs:
  order_id: { type: string, required: true }
triggers:
  - webhook: payments
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order_id: ${event.body.order_id}
  - manual: denied
steps:
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`,
		},
		{
			name: "a single principal written bare",
			source: `edition: v2026.3
name: one-principal
triggers:
  manual:
    allowed_principals: oncall@example.com
steps:
  - id: rotate
    log:
      message: rotating
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			workflow, err := flowfile.Unmarshal([]byte(test.source))
			require.NoError(t, err)

			written, err := flowfile.Marshal(workflow)
			require.NoError(t, err)

			// Compared by *re-reading* rather than by comparing bytes to the input:
			// the writer chooses its own key order and its own spelling of a single
			// principal, and a formatter is allowed to. What it is not allowed to do
			// is lose anything, which is what a second parse catches.
			again, err := flowfile.Unmarshal(written)
			require.NoError(t, err, "what the writer produced does not parse:\n%s", written)

			require.Empty(t, flowfile.Validate(again),
				"what the writer produced does not validate:\n%s", written)

			assert.Equal(t, workflow.GetTriggers().GetManual().GetDenied(),
				again.GetTriggers().GetManual().GetDenied(),
				"the refusal did not survive the round trip:\n%s", written)
			assert.Equal(t, workflow.GetTriggers().GetManual().GetRequireReason(),
				again.GetTriggers().GetManual().GetRequireReason(),
				"`require_reason:` did not survive the round trip:\n%s", written)
			assert.Equal(t, workflow.GetTriggers().GetManual().GetAllowedPrincipals(),
				again.GetTriggers().GetManual().GetAllowedPrincipals(),
				"the allowed principals did not survive the round trip:\n%s", written)
		})
	}
}
