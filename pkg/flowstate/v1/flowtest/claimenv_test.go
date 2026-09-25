package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A `check:` claim is read in the vocabulary the run evaluates it in.
//
// Claims were parsed against the library-less base environment and evaluated
// against the workflow's profile, so `flow test` refused at load what it would
// have accepted at run time — including `steps.m.value.?a.orValue(0)`, which
// STYLE.md names as the canonical presence spelling and which the shipped
// `optional-dispatch` example uses. A test could not assert the idiom the style
// guide teaches (#1512).
//
// A macro was the quieter half: `steps.l.value.sum()` *parses* against the base
// environment, because a receiver-style macro reads as an ordinary call, so the
// load-time walkers saw a tree the run never evaluated.

// claimWorkflow gives each claim below something real to read: a map for
// presence, a list for the comprehension and binding macros.
const claimWorkflow = `edition: v2026.3
name: claims
steps:
  - id: m
    value: '${ {"a": 1} }'
  - id: l
    value: ${[1, 2, 3]}
outputs:
  m:
    value: ${steps.m.value}
`

// TestAClaimInProfileVocabularyLoadsAndRuns walks one claim per distinctive
// syntax or macro a profile library brings.
//
// Each is written the way an author would write it in the workflow itself, and
// each must both load and hold: a claim refused at load never reaches the run
// that would have accepted it, and a claim that loads but cannot evaluate is the
// same disagreement pointing the other way.
func TestAClaimInProfileVocabularyLoadsAndRuns(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		claim string
		from  string
	}{
		{
			name:  "optional syntax",
			claim: `steps.m.value.?a.orValue(0) == 1`,
			from:  "the optional library, and STYLE.md's canonical presence spelling",
		},
		{
			name:  "a comprehension macro",
			claim: `steps.l.value.sum() == 6`,
			from:  "the comprehensions library",
		},
		{
			name:  "another comprehension macro",
			claim: `steps.l.value.reduce(a, b, 0, a + b) == 6`,
			from:  "the comprehensions library's fold",
		},
		{
			name:  "a binding macro",
			claim: `cel.bind(x, steps.l.value, x.size()) == 3`,
			from:  "the bindings library",
		},
		{
			name:  "a list transform",
			claim: `steps.l.value.transformList(i, v, v * 2) == [2, 4, 6]`,
			from:  "the comprehensions library's two-variable transform",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			report := flowtest.RunFile(claimSuite(t, test.claim))
			require.Len(t, report.GetCases(), 1,
				"%s: the file was refused at load rather than run", test.from)

			assert.True(t, report.GetCases()[0].GetPassed(),
				"%s: the claim loaded but did not hold: %s",
				test.from, fieldsOf(report.GetCases()[0].GetFailures()))
		})
	}
}

// TestAClaimNoProfileCanParseIsStillRefusedAtLoad is the direction the union
// must not lose.
//
// Reading a claim in every profile's vocabulary widens what loads; it does not
// stop load from refusing. Nonsense no profile could parse is still a load-time
// diagnostic with a position, which is the contract an editor underlines.
func TestAClaimNoProfileCanParseIsStillRefusedAtLoad(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(claimSuite(t, `steps.m.value ==== 1`))
	require.Error(t, err, "a claim no profile can parse must still be refused at load")

	assert.Contains(t, err.Error(), "expect.check[0]",
		"the refusal must name the claim it is about")
}

// claimSuite writes a workflow and a one-case suite asserting the claim, and
// returns the suite's path.
func claimSuite(t *testing.T, claim string) string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), claimWorkflow)

	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `edition: v2026.3
tests:
  - name: c
    workflow: ./workflow.yaml
    expect:
      check:
        - `+claim+"\n")

	return path
}
