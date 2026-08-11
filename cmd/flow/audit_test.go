package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// corpus is the shipped examples, reached from this package's own directory the
// way every other test here reaches them.
const corpus = "../../examples/"

// auditJSON runs `flow audit -o json` over the arguments and returns the parsed
// report.
//
// Through the real command rather than through [flowfile.Audit] under it, because
// the walk, the flag plumbing and the rendering are as much of this verb as the
// counting is: a `-o json` that parses and is then ignored would pass every test
// written one level down.
func auditJSON(t *testing.T, args ...string) auditReport {
	t.Helper()

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"audit", "-o", "json"}, args...))

	require.NoError(t, root.Execute(), "audit should not fail on a corpus that compiles")

	var report auditReport
	require.NoError(t, json.Unmarshal([]byte(out.String()), &report),
		"the machine format should be JSON a program can read: %s", out.String())

	return report
}

// findingFor returns the finding whose expression contains want, for the file at
// path.
func findingFor(t *testing.T, report auditReport, path, want string) auditRepeat {
	t.Helper()

	for _, file := range report.Files {
		if file.Path != path {
			continue
		}
		for _, finding := range file.Findings {
			if strings.Contains(finding.Expr, want) {
				return finding
			}
		}
		t.Fatalf("%s reported no finding containing %q; it reported %d others", path, want, len(file.Findings))
	}

	t.Fatalf("%s is not in the report at all", path)

	return auditRepeat{}
}

// siteLines is the lines a finding's occurrences sit on, in report order.
func siteLines(finding auditRepeat) []int {
	lines := make([]int, 0, len(finding.Sites))
	for _, site := range finding.Sites {
		lines = append(lines, site.Line)
	}

	return lines
}

// TestAuditReproducesTheManualAudit is the gate this command exists to hold.
//
// The repetition #411 is about was found twice by reading the corpus by hand, and
// a number found by hand is one nobody can check and nobody can watch move. These
// files are the residue that survived the rewrites since: each states one
// question in several places because the language gives it no way to state it
// once. This asserts that the mechanical reading finds every one of them, at the
// line, with the count and the hand-negated pair the manual reading found.
//
// The lines are the first line of the *expression* holding each occurrence, which
// is one below the `if:` or `value:` key it is written under whenever the file
// wraps. Two of the hand-recorded numbers named that key line instead; where they
// differ by one, the number here is the one the file bears out.
//
// Nothing here asserts a limit on what else the corpus repeats. This command is a
// measurement, so a corpus that grows a repetition is not a test failure, and a
// gate that failed on one would push somebody to rewrite a file around a feature
// the language does not have yet.
//
// Onboarding and access-review are gone from this list, on purpose. Issue #411's
// design-pass comment (finding 1) named both as *not* `value:` evidence: onboarding's
// triplicate is a single-wait predicate `outputs:` shaping already reaches, and
// access-review's shared filter has one producer the `http` task's own `outputs:`
// already names. The sweep that landed alongside this comment collapsed both, so
// what remains here is exactly the residue only a held entry would reach — a
// predicate spanning more than one step, or mixing `inputs:` with a step.
func TestAuditReproducesTheManualAudit(t *testing.T) {
	report := auditJSON(t, corpus)

	t.Run("incident response states one timeout predicate four times", func(t *testing.T) {
		const path = corpus + "enterprise-incident-response/workflow.yaml"

		finding := findingFor(t, report, path, "steps.escalated_ack.timed_out")
		assert.Equal(t, 4, finding.Count)
		assert.Equal(t, []int{185, 193, 229, 244}, siteLines(finding))
		assert.False(t, finding.Negated, "the four are all written plainly")

		// Two of them are bare `if:` conditions and two are conjuncts of a longer
		// one, which is why the walk cannot stop at whole fields.
		assert.Equal(t, "claimed", finding.Sites[0].Step)
		assert.Equal(t, "authorized", finding.Sites[2].Step)
	})

	t.Run("incident response also carries a hand-negated pair", func(t *testing.T) {
		const path = corpus + "enterprise-incident-response/workflow.yaml"

		finding := findingFor(t, report, path,
			"steps.responder_ack.timed_out && steps.escalated_ack.timed_out")
		assert.Equal(t, 2, finding.Count)
		assert.True(t, finding.Negated)
		assert.Equal(t, []int{254, 265}, siteLines(finding))

		// Both sit inside a ternary in an `outputs:` entry rather than at the top
		// of one, so neither is a whole field and neither is a top-level conjunct.
		assert.Equal(t, "outputs.outcome", finding.Sites[0].Field)
		assert.False(t, finding.Sites[0].Negated)
		assert.Equal(t, "outputs.authorized_by", finding.Sites[1].Field)
		assert.True(t, finding.Sites[1].Negated)
	})

	t.Run("fund transfer states one threshold predicate at each of its three gates", func(t *testing.T) {
		const path = corpus + "enterprise-fund-transfer/workflow.yaml"

		finding := findingFor(t, report, path,
			"inputs.amount_cents < inputs.approval_threshold_cents || steps.approval.outcome")
		assert.Equal(t, 4, finding.Count,
			"the three `if:` gates the manual reading found, plus the `outputs.debit_reference` copy it did not")
		assert.Equal(t, []int{159, 184, 210, 281}, siteLines(finding))

		gates := []string{}
		for _, site := range finding.Sites {
			if site.Field == "if" {
				gates = append(gates, site.Step)
			}
		}
		assert.Equal(t, []string{"debit", "credit", "notify_settlement"}, gates)
	})

}

// TestAuditOnboardingAndAccessReviewSweptClean is the negative half of the comment
// above [TestAuditReproducesTheManualAudit]: both files used to appear in that
// list, and this asserts the two collapses actually happened rather than trusting
// the doc comment. A regression here means the sweep's shaping (the wait's own
// `outputs:` on onboarding, the `http` task's own `outputs:` on access-review) came
// back apart, not that the corpus grew a new repetition — the assertion is scoped
// to the exact expressions the manual audit named, not "no findings at all".
func TestAuditOnboardingAndAccessReviewSweptClean(t *testing.T) {
	report := auditJSON(t, corpus)

	for _, path := range []string{
		corpus + "enterprise-customer-onboarding/workflow.yaml",
		corpus + "enterprise-access-review/workflow.yaml",
	} {
		for _, file := range report.Files {
			if file.Path != path {
				continue
			}
			for _, finding := range file.Findings {
				assert.NotContains(t, finding.Expr, "steps.activation_confirmation.payload.confirmed",
					"%s: onboarding's wait shaping should have collapsed this", path)
				assert.NotContains(t, finding.Expr, "steps.gather_evidence.results.filter(r, has(r.last_used.grantee))",
					"%s: access-review's http shaping should have collapsed this", path)
			}
		}
	}
}

// TestAuditIsNotALinter is the boundary the command's whole design rests on: a
// finding is a measurement, so it must not change what the process reports to
// whatever ran it.
func TestAuditIsNotALinter(t *testing.T) {
	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"audit", corpus})

	require.NoError(t, root.Execute(),
		"a corpus full of findings must still exit zero; nonzero is reserved for a file that could not be read")
	require.Contains(t, out.String(), "None of this is a defect",
		"the human output must say what it is not, since every other verb reading a Flowfile reports defects")

	// A path that does not exist is the failure this verb does have.
	broken := newRootCommand()
	broken.SetOut(&strings.Builder{})
	broken.SetErr(&strings.Builder{})
	broken.SetArgs([]string{"audit", corpus + "no-such-workflow-here.yaml"})
	require.Error(t, broken.Execute(), "an unreadable path is a real error and must not exit zero")
}

// TestAuditSkipsTrivialRepetition keeps the noise floor where the design put it.
// A corpus repeating a name or a literal is a language working; only a repeated
// computation is the friction #411 is about.
func TestAuditSkipsTrivialRepetition(t *testing.T) {
	path := writeWorkflow(t, "workflow.yaml", `edition: v2026.2
name: trivial
vars:
  a: ${true}
  b: ${true}
steps:
  - id: one
    if: ${vars.a}
    log:
      message: ${vars.a}
  - id: two
    if: ${vars.a}
    log:
      message: ${vars.a}
`)

	report := auditJSON(t, path)
	assert.Empty(t, report.Files,
		"a repeated literal and a repeated name are not findings: %+v", report.Files)
	assert.Equal(t, 1, report.Totals.Files)
}

// TestAuditCountsTheLargerExpressionOnce checks the subsumption rule: a
// sub-expression that never occurs outside a larger repeated one, and exactly as
// often, is the same friction counted twice.
func TestAuditCountsTheLargerExpressionOnce(t *testing.T) {
	path := writeWorkflow(t, "workflow.yaml", `edition: v2026.2
name: nested
steps:
  - id: one
    if: ${has(inputs.x) && size(inputs.x) > 0}
    log:
      message: one
  - id: two
    if: ${has(inputs.x) && size(inputs.x) > 0}
    log:
      message: two
`)

	report := auditJSON(t, path)
	require.Len(t, report.Files, 1)
	require.Len(t, report.Files[0].Findings, 1,
		"`has(inputs.x)` and `size(inputs.x) > 0` occur twice each, only inside the conjunction that also occurs twice: %+v",
		report.Files[0].Findings)
	assert.Equal(t, "has(inputs.x) && size(inputs.x) > 0", report.Files[0].Findings[0].Expr)
	assert.Equal(t, 2, report.Files[0].Findings[0].Count)
}
