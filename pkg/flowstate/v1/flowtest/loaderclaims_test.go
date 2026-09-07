package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestAClaimFreeExpectationIsRefused pins the first of #1669's five: a case
// whose `expect:` claims nothing is refused when the file loads, at the
// `expect:` it wrote, naming `failed: false` as the spelling for a case that
// only proves the run completes. Before this the case was green whatever the
// run produced, and `--fail-on-warning` had nothing to promote.
func TestAClaimFreeExpectationIsRefused(t *testing.T) {
	t.Parallel()

	t.Run("an empty expect", func(t *testing.T) {
		t.Parallel()

		source := `
tests:
  - name: nothing claimed
    workflow: ./workflow.yaml
    expect: {}
`
		problems, _ := refuse(t, source)
		d := only(t, problems)
		line, _ := spot(t, source, "expect: {}")
		assert.Equal(t, line, d.Line, "the refusal points at the expect the case wrote")
		assert.Contains(t, d.Message, `test "nothing claimed" expect: claims nothing`)
		assert.Contains(t, d.Message, "`failed: false`", "the remedy names the spelling for a run that only has to complete")
	})

	t.Run("no expect at all", func(t *testing.T) {
		t.Parallel()

		problems, _ := refuse(t, `
tests:
  - name: nothing claimed
    workflow: ./workflow.yaml
`)
		d := only(t, problems)
		assert.Contains(t, d.Message, "claims nothing")
	})

	// The written-empty collections are claims — no outputs, nothing ran —
	// and the completion claim is the one the refusal names; none is refused.
	for name, expect := range map[string]string{
		"failed false":  "expect: {failed: false}",
		"empty outputs": "expect: {outputs: {}}",
		"empty ran":     "expect: {ran: []}",
		"a check":       "expect: {check: [run.failed == false]}",
	} {
		t.Run(name+" is a claim", func(t *testing.T) {
			t.Parallel()

			_, err := flowtest.LoadSource([]byte("tests:\n  - name: claimed\n    " + expect + "\n"))
			require.NoError(t, err, expect)
		})
	}
}

// TestAnUnknownKeyIsAnsweredWithTheNearestLegalOne pins the fifth: the
// decoder's `unknown field` refusal carries the did-you-mean every name in the
// format already gets, over the keys legal where the key was written, or that
// list when nothing is near. The three positions cover a case key, a key under
// `expect:`, and a key on a stub reached through a sequence index, which is
// the walk's each kind of step.
func TestAnUnknownKeyIsAnsweredWithTheNearestLegalOne(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		want   string
	}{
		{
			"a case key one letter off",
			"tests:\n  - name: x\n    workflow: ./workflow.yaml\n    expct:\n      ran: [a]\n",
			`unknown field "expct"; did you mean "expect"?`,
		},
		{
			"an expect key one letter off",
			"tests:\n  - name: x\n    workflow: ./workflow.yaml\n    expect:\n      output: {a: 1}\n",
			`unknown field "output"; did you mean "outputs"?`,
		},
		{
			"a stub key one letter off, reached through the list",
			"tests:\n  - name: x\n    workflow: ./workflow.yaml\n    expect: {ran: [a]}\n    stubs:\n      - task: log\n        retruns: {}\n",
			`unknown field "retruns"; did you mean "returns"?`,
		},
		{
			"a case key with nothing near",
			"tests:\n  - name: x\n    workflow: ./workflow.yaml\n    outputs: {a: 1}\n",
			`unknown field "outputs"; the keys legal here are: `,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			problems, _ := refuse(t, test.source)
			d := only(t, problems)
			assert.Contains(t, d.Message, test.want)
		})
	}

	// The list, when it is the answer, is the case's own keys: `expect:` and
	// `stubs:` are on it, and nothing from another level is.
	problems, _ := refuse(t, "tests:\n  - name: x\n    workflow: ./workflow.yaml\n    outputs: {a: 1}\n")
	d := only(t, problems)
	assert.Contains(t, d.Message, "expect")
	assert.Contains(t, d.Message, "stubs")
	assert.NotContains(t, d.Message, "returns", "a stub's keys are not legal on a case")
}

// TestAMismatchNamesEachSidesType pins the fourth: `expected "1", got 1` read
// as a quoting difference, and the difference was the type. Each side leads
// with its type in the spelling a Flowfile declares one with.
func TestAMismatchNamesEachSidesType(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `edition: v2026.3
name: counted
steps:
  - id: count
    value: ${1}
outputs:
  iterations:
    value: ${steps.count.value}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: a string where the workflow produced an int
    workflow: ./workflow.yaml
    expect:
      outputs:
        iterations: "1"
`)

	report := flowtest.RunFile(path)
	require.Len(t, report.GetCases(), 1)
	require.False(t, report.GetCases()[0].GetPassed(), "the case must fail: the types differ")
	assert.Contains(t, failureText(report.GetCases()[0].GetFailures()),
		`output "iterations": expected string "1", got int 1`)
}

// TestAMissingWorkflowNamesTheNearestSibling pins the second of #1669's five,
// where the loader can answer it: a `workflow:` that names no file fails the
// case with the Flowfile beside it that it nearly spelled. The candidates are
// the directory's Flowfiles and not its suites, so `workflow.test.yaml` is
// never the answer for `workflow.yaml`.
func TestAMissingWorkflowNamesTheNearestSibling(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `edition: v2026.3
name: present
steps:
  - id: a
    value: ${1}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the path is one letter off
    workflow: ./workflwo.yaml
    expect: {failed: false}
`)

	report := flowtest.RunFile(path)
	require.Len(t, report.GetCases(), 1)
	got := report.GetCases()[0].GetError()
	assert.Contains(t, got, `loading workflow "./workflwo.yaml"`)
	assert.Contains(t, got, `did you mean "workflow.yaml"?`)
	assert.NotContains(t, got, "workflow.test.yaml", "a suite is never what workflow: means")
}
