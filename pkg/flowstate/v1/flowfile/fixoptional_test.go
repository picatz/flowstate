package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestFixRewritesGuardedReadsAcrossTheEditionBoundary is the whole migration in
// one document: an older-edition file whose expressions carry the three decided
// idiom shapes (issue #412) comes back stamped with the current edition and
// rewritten, compared byte for byte — "still validates" is the assertion that
// let earlier rewriter corruptions through, per CLAUDE.md.
func TestFixRewritesGuardedReadsAcrossTheEditionBoundary(t *testing.T) {
	t.Parallel()

	in := `edition: v2026.2
name: guarded
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 3s
      outputs:
        approved: ${has(payload.approved) && payload.approved}
  - id: review
    wait_for_signal:
      name: review
      timeout: 3s
  - id: halt
    if: ${!(has(steps.review.payload.ok) && steps.review.payload.ok)}
    log:
      message: stop
outputs:
  days:
    value: '${has(steps.review.payload.days) ? steps.review.payload.days : -1}'
`

	want := `edition: v2026.3
name: guarded
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 3s
      outputs:
        approved: ${payload.?approved.orValue(false)}
  - id: review
    wait_for_signal:
      name: review
      timeout: 3s
  - id: halt
    if: ${!steps.review.payload.?ok.orValue(false)}
    log:
      message: stop
outputs:
  days:
    value: '${steps.review.payload.?days.orValue(-1)}'
`

	result, err := flowfile.Fix([]byte(in))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)
	assert.Equal(t, want, string(result.Source))

	// And the result is a Flowfile this build accepts.
	wf, _, err := flowfile.Parse(result.Source)
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(wf))

	// Running the migration on its own output changes nothing: the fixed point
	// is the file, not another round of edits.
	again, err := flowfile.Fix(result.Source)
	require.NoError(t, err)
	assert.Equal(t, string(result.Source), string(again.Source))
	assert.Empty(t, again.Changes)
}

// TestFixKeepsACurrentFilesGuardedReads pins the gate: `has(x.y) && x.y` is
// legal in the current edition — has() is not retired — so a current file that
// writes it has written what its author meant, and `flow fix` leaves it byte
// for byte.
func TestFixKeepsACurrentFilesGuardedReads(t *testing.T) {
	t.Parallel()

	in := `edition: ` + flowfile.CurrentEdition + `
name: deliberate
steps:
  - id: check
    if: ${has(vars.cfg.flag) && vars.cfg.flag}
    log:
      message: on
vars:
  cfg:
    flag: true
`

	result, err := flowfile.Fix([]byte(in))
	require.NoError(t, err)
	assert.Empty(t, result.Changes)
	assert.Equal(t, in, string(result.Source))
}

// TestFixLeavesNearMissIdiomsAloneWhileStampingTheEdition is the document-level
// mutation proof: shapes one edit away from the idiom migrate with only their
// edition line changed, so the expression bytes an author wrote survive.
func TestFixLeavesNearMissIdiomsAloneWhileStampingTheEdition(t *testing.T) {
	t.Parallel()

	expressions := []string{
		`${has(steps.a.url)}`,                                // presence alone
		`${has(steps.a.payload.ok) && !steps.a.payload.ok}`,  // answered no
		`${!has(steps.a.payload.ok) && steps.a.payload.ok}`,  // negated guard, bare read
		`${has(steps.a.payload.ok) && steps.a.payload.okay}`, // paths differ
		`${"has(a.b) && a.b"}`,                               // prose
		// The operand-boundary reversal (PR #483's P1): `==` binds tighter than
		// `&&`, so the read is `steps.a.payload.ok == false` and the textual
		// idiom is a fragment of it; rewriting would turn an absent field's
		// false into true.
		`${has(steps.a.payload.ok) && steps.a.payload.ok == false}`,
	}

	for _, expr := range expressions {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()

			in := "edition: v2026.2\nname: nearmiss\nsteps:\n  - id: a\n    log:\n      message: hi\n  - id: b\n    if: '" + expr + "'\n    log:\n      message: gated\n"
			result, err := flowfile.Fix([]byte(in))
			require.NoError(t, err)
			require.Empty(t, result.Refusals)

			assert.Equal(t,
				strings.Replace(in, "edition: v2026.2", "edition: "+flowfile.CurrentEdition, 1),
				string(result.Source),
				"only the edition line may change")
		})
	}
}

// TestFixCheckReportsGuardedReadsAsPending covers the --check spelling: the
// rewrite is announced without being performed.
func TestFixCheckReportsGuardedReadsAsPending(t *testing.T) {
	t.Parallel()

	in := `edition: v2026.2
name: pending
steps:
  - id: gate
    if: ${has(vars.cfg.flag) && vars.cfg.flag}
    log:
      message: on
`
	result, err := flowfile.Fix([]byte(in))
	require.NoError(t, err)

	var found bool
	for _, change := range result.Changes {
		if strings.Contains(change.Message, "optional traversal") {
			found = true
			assert.NotEmpty(t, change.Pending)
			assert.Contains(t, change.Pending, "would be")
		}
	}
	assert.True(t, found, "the rewrite must be reported as a change")
}
