package flowtest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A `trigger:` stanza that states a context rather than replaying a delivery.
//
// This is what makes a trigger-guarded branch exercisable at all: without it,
// `if: ${trigger.kind == "manual"}` is conditional behaviour that only manifests
// in production. The cases below cover both what it does and — at greater length —
// what it refuses, because every refusal here is one that would otherwise produce a
// *passing* test asserting something nobody wrote.

// theTriggerAwareWorkflow branches on the kind and reports every field.
const theTriggerAwareWorkflow = `edition: v2026.3
name: trigger-aware
steps:
  - id: notify
    if: ${trigger.kind != "schedule"}
    log:
      message: paging
  - id: correlate
    log:
      message: ${trigger.kind + "/" + trigger.name + "/" + trigger.principal}
outputs:
  started_by:
    value: ${trigger.kind}
  source:
    value: ${trigger.name}
`

// TestAStatedTriggerExercisesBothSidesOfABranch is the whole point of the stanza,
// in the one form that proves it: the same workflow, two cases, opposite verdicts
// about the same step, and no trigger anywhere near either of them.
func TestAStatedTriggerExercisesBothSidesOfABranch(t *testing.T) {
	t.Parallel()

	report := flowtest.RunSource("trigger-context", []byte(theTriggerAwareWorkflow), []byte(`edition: v2026.3
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: a manual start pages
    trigger:
      kind: manual
      principal: alice@example.com
    expect:
      ran: [notify, correlate]
      outputs:
        started_by: manual
        source: ""

  - name: a scheduled run does not page
    trigger:
      kind: schedule
      name: nightly
      principal: ops@example.com
    expect:
      ran: [correlate]
      skipped: [notify]
      outputs:
        started_by: schedule
        source: nightly

  - name: a run with no stated trigger is a manual start
    expect:
      ran: [notify, correlate]
      outputs:
        started_by: manual
        source: ""
`))

	require.Empty(t, report.GetRefused(), "the file was refused: %s", report.GetRefused())
	require.Len(t, report.GetCases(), 3)
	for _, c := range report.GetCases() {
		assert.Truef(t, c.GetPassed(), "case %q failed: %v", c.GetName(), c.GetFailures())
	}
}

// TestAStatedTriggerIsRefusedWhereItCannotMeanWhatItSays covers every refusal,
// because each one would otherwise be a green case certifying a belief.
//
// The unknown kind is the sharpest: a case stating `kind: schedual` against
// `if: ${trigger.kind == "schedule"}` asserts the branch is *not* taken, and it
// would pass, forever, for a workflow that behaves correctly.
func TestAStatedTriggerIsRefusedWhereItCannotMeanWhatItSays(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		tests string
		want  string
	}{
		{
			name: "a kind nothing can produce",
			tests: `  - name: misspelled
    trigger: { kind: schedual }
`,
			want: "is not a kind Flowstate starts runs with",
		},
		{
			name: "a stanza saying nothing at all",
			tests: `  - name: empty
    trigger: { principal: alice@example.com }
`,
			want: "says neither how the run started nor what delivery started it",
		},
		{
			name: "a stated context that also replays",
			tests: `  - name: both
    trigger: { kind: manual, webhook: payments, payload: ./x.json }
`,
			want: "names both a webhook",
		},
		{
			name: "a stated context carrying a delivery's fields",
			tests: `  - name: half-replay
    trigger: { kind: webhook, payload: ./x.json }
`,
			want: "states a context",
		},
		{
			name: "a stated context expecting a refusal",
			tests: `  - name: refusing-nothing
    trigger: { kind: manual }
    expect:
      refused: true
`,
			want: "expects a refusal",
		},
		{
			name: "a stated context asserting a mapping that never happened",
			tests: `  - name: mapping-nothing
    trigger: { kind: manual }
    expect:
      inputs: { order_id: ord_1 }
`,
			want: "expects mapped inputs",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			report := flowtest.RunSource("trigger-context",
				[]byte(theTriggerAwareWorkflow), []byte("edition: v2026.3\ntests:\n"+test.tests))

			require.NotEmpty(t, report.GetRefused(),
				"the file was accepted, so a case that cannot mean what it says would have passed")
			assert.Truef(t, strings.Contains(report.GetRefused(), test.want),
				"the refusal did not say %q; it said: %s", test.want, report.GetRefused())
		})
	}
}
