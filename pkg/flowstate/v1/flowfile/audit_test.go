package flowfile

import (
	"os"
	"path/filepath"
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

// TestAuditSeesCallArguments covers the position the node switch reaches last:
// a call's `with:` binds arguments with expressions this file wrote, so two
// call sites sharing one computed argument are a repetition the report must
// carry. The callee's own expressions must NOT be counted against the caller,
// even though the compiler embeds the callee's spec whole: a library workflow
// audited through its three callers would otherwise report its internals three
// times in files that never wrote them.
func TestAuditSeesCallArguments(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeAuditFile(t, dir, "callee.yaml", `edition: v2026.2
name: callee
inputs:
  tenant:
    type: string
    required: true
steps:
  - id: a
    log:
      message: ${'hi ' + inputs.tenant}
outputs:
  greeting:
    value: ${'hello ' + inputs.tenant}
`)
	caller := writeAuditFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
inputs:
  region:
    type: string
    required: true
steps:
  - id: primary
    call: ./callee.yaml
    with:
      tenant: ${inputs.region + '-prod'}
  - id: secondary
    call: ./callee.yaml
    with:
      tenant: ${inputs.region + '-prod'}
`)

	wf, positions, err := ParseFile(caller)
	require.NoError(t, err)

	found := Audit(wf, positions)

	finding := findAudit(t, found, `inputs.region + "-prod"`)
	require.Equal(t, 2, finding.Count())
	require.Equal(t, "primary", finding.Sites[0].Step)
	require.Equal(t, "with.tenant", finding.Sites[0].Field)
	require.Equal(t, "secondary", finding.Sites[1].Step)

	// The callee's own repetition ('hi/hello ' + inputs.tenant share the
	// inputs.tenant read, but each computation appears once) must not leak into
	// the caller's report: nothing from the embedded spec is a site here.
	for _, repeat := range found {
		for _, site := range repeat.Sites {
			require.NotEqual(t, "a", site.Step,
				"the embedded callee's step leaked into the caller's audit: %q", repeat.Expr)
		}
	}
}

// TestAuditSeesComputedSignalSubjects covers the workflow-level position
// outside vars, steps and outputs: a signal rule's `subject:` written as an
// expression lands in subject_from, and two rules resolving the same computed
// subject are a repetition like any other.
func TestAuditSeesComputedSignalSubjects(t *testing.T) {
	t.Parallel()

	found := auditOf(t, `edition: v2026.2
name: gated
inputs:
  expected_approver:
    type: string
    required: true
signals:
  deploy-approved:
    allow:
      - subject: "${'https://issuer.example.com#' + inputs.expected_approver}"
        namespace: release-managers-ns
  teardown-approved:
    allow:
      - subject: "${'https://issuer.example.com#' + inputs.expected_approver}"
        namespace: release-managers-ns
steps:
  - id: gate
    wait_for_signal: deploy-approved
`)

	finding := findAudit(t, found, `"https://issuer.example.com#" + inputs.expected_approver`)
	require.Equal(t, 2, finding.Count())
	require.Equal(t, "", finding.Sites[0].Step)
	require.Equal(t, "signals.deploy-approved.allow[0].subject", finding.Sites[0].Field)
	require.Equal(t, "signals.teardown-approved.allow[0].subject", finding.Sites[1].Field)
}

// writeAuditFile is call_test.go's writeFile, restated here because that helper
// lives in the external test package and this file is the internal one.
func writeAuditFile(t *testing.T, dir, name, content string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}
