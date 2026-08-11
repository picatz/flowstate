package flowfile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// auditOf compiles a Flowfile and returns what the audit makes of it.
func auditOf(t *testing.T, src string) []RepeatedExpr {
	t.Helper()

	wf, positions, err := Parse([]byte(src))
	require.NoError(t, err, "fixture did not compile:\n%s", src)

	return Audit(wf, positions)
}

// findAudit returns the finding for an exact rendered expression.
func findAudit(t *testing.T, found []RepeatedExpr, want string) RepeatedExpr {
	t.Helper()

	for _, repeat := range found {
		if repeat.Expr == want {
			return repeat
		}
	}

	rendered := make([]string, 0, len(found))
	for _, repeat := range found {
		rendered = append(rendered, repeat.Expr)
	}
	t.Fatalf("no finding for %q; found %q", want, rendered)

	return RepeatedExpr{}
}

// TestAuditMarksOnlyWhatTheNegationDirectlyCovers is the rule that keeps the
// hand-negated pair meaningful.
//
// `!(A && B)` states the negation of `A && B` and says nothing about `A` on its
// own. Marking every descendant of a `!` would report a `filter(...)` as a
// negation of itself the moment something wrapped around it was negated, and the
// mark would then be on so many findings that the pair it exists to surface would
// be unfindable.
func TestAuditMarksOnlyWhatTheNegationDirectlyCovers(t *testing.T) {
	found := auditOf(t, `edition: v2026.2
name: negation
steps:
  - id: yes
    if: ${has(inputs.x) && inputs.x}
    log:
      message: yes
  - id: no
    if: ${!(has(inputs.x) && inputs.x)}
    log:
      message: no
`)

	pair := findAudit(t, found, "has(inputs.x) && inputs.x")
	assert.Equal(t, 2, pair.Count())
	assert.True(t, pair.Negated, "one half is written as the `!` of the other")
	assert.False(t, pair.Sites[0].Negated)
	assert.True(t, pair.Sites[1].Negated)

	// `has(inputs.x)` sits under the same `!` and is not itself negated by it. It
	// is dropped here for occurring exactly as often as the conjunction it never
	// appears outside of, which is the other half of the same restraint.
	for _, repeat := range found {
		if repeat.Expr == "has(inputs.x)" {
			t.Fatalf("`has(inputs.x)` occurs only inside a conjunction that occurs as often, and should not be reported separately")
		}
	}
}

// TestAuditReadsMacrosAsTheyWereWritten checks the resolution a repeated `filter`
// depends on.
//
// A macro is expanded by the parser, so what a Flowfile stores is a comprehension
// over an accumulator nobody typed. Counting that tree reports how often a corpus
// repeats `@result + [r]`, which is every time anyone writes `filter`, and never
// reports the `filter` two fields share.
func TestAuditReadsMacrosAsTheyWereWritten(t *testing.T) {
	found := auditOf(t, `edition: v2026.2
name: macros
steps:
  - id: one
    log:
      message: ${string(inputs.rows.filter(r, has(r.id)))}
  - id: two
    log:
      message: ${string(size(inputs.rows.filter(r, has(r.id))))}
`)

	// Reported at the filter rather than at either wrapper, because the two
	// wrappers differ: this is the shape `examples/enterprise-access-review/`
	// carries, where one output renders the filtered rows and another counts what
	// the filter left out.
	repeat := findAudit(t, found, "inputs.rows.filter(r, has(r.id))")
	assert.Equal(t, 2, repeat.Count())
	assert.False(t, repeat.Negated)

	for _, other := range found {
		assert.NotContains(t, other.Expr, "@result",
			"the accumulator the expander introduced is not something an author wrote")
	}
}

// TestAuditCountsWithinOneFileOnly states the boundary of this slice: two
// workflows sharing a predicate cannot share a held entry either way, so the
// count is per file and the report is keyed by file.
func TestAuditCountsWithinOneFileOnly(t *testing.T) {
	const one = `edition: v2026.2
name: one
steps:
  - id: gate
    if: ${size(inputs.rows) > 0}
    log:
      message: one
`

	require.Empty(t, auditOf(t, one), "one statement of a predicate is not a repetition")

	found := auditOf(t, `edition: v2026.2
name: two
steps:
  - id: gate
    if: ${size(inputs.rows) > 0}
    log:
      message: one
  - id: also
    if: ${size(inputs.rows) > 0}
    log:
      message: two
`)
	assert.Equal(t, 2, findAudit(t, found, "size(inputs.rows) > 0").Count())
}

// TestAuditSurvivesNoPositions checks the nil [Positions] path, which is what a
// caller that compiled a workflow without asking for source positions gets. The
// counts are the answer; the lines are what is lost.
func TestAuditSurvivesNoPositions(t *testing.T) {
	wf, _, err := Parse([]byte(`edition: v2026.2
name: unplaced
steps:
  - id: gate
    if: ${size(inputs.rows) > 0}
    log:
      message: one
  - id: also
    if: ${size(inputs.rows) > 0}
    log:
      message: two
`))
	require.NoError(t, err)

	found := Audit(wf, nil)
	require.Len(t, found, 1)
	assert.Equal(t, 2, found[0].Count())
	for _, site := range found[0].Sites {
		assert.Zero(t, site.Line, "a position nobody recorded must read as unknown rather than as line 1")
	}
}
