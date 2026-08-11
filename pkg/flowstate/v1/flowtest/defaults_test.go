package flowtest_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// defaultsWorkflow has two http steps that share the `http` task and a log
// step, plus one branch skipped per run. It is what the merge, step-form, and
// `others:` tests all run against: `small` and `large` differ only by which
// branch the amount takes, so telling them apart proves a step-form stub scopes
// to a step id rather than to the task both share.
const defaultsWorkflow = `
edition: v2026.3
name: defaults-fixture
inputs:
  amount:
    type: int
    required: true
    example: 1
steps:
  - id: announce
    log:
      message: ${"amount is %d".format([inputs.amount])}
  - id: small
    if: ${inputs.amount < 100}
    http:
      method: GET
      url: https://example.invalid/small
      parse_json: true
      outputs: '${ {"tag": response.json.tag} }'
  - id: large
    if: ${inputs.amount >= 100}
    http:
      method: GET
      url: https://example.invalid/large
      parse_json: true
      outputs: '${ {"tag": response.json.tag} }'
outputs:
  tag:
    value: '${inputs.amount < 100 ? steps.small.tag : steps.large.tag}'
`

func writeDefaultsWorkflow(t *testing.T, dir string) {
	t.Helper()
	writeFile(t, dir+"/workflow.yaml", defaultsWorkflow)
}

func runOneFile(t *testing.T, dir, contents string) *flowtest.File {
	t.Helper()
	writeFile(t, dir+"/x.test.yaml", contents)
	file, err := flowtest.Load(dir + "/x.test.yaml")
	require.NoError(t, err)
	return file
}

// TestDefaultsInputsMergeOneLevel pins two of the merge rules at once: a case's
// scalar wins over the default's, and a key the case does not name keeps the
// default's value rather than being wiped by the case setting a different key
// (issue #416). Asserting only the first would pass a merge that replaced the
// whole map, so both directions are checked in one case.
func TestDefaultsInputsMergeOneLevel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	file := runOneFile(t, dir, `
defaults:
  inputs: {amount: 1, environment: production}
tests:
  - name: a case replaces one key and inherits the rest
    workflow: ./workflow.yaml
    inputs: {amount: 500}
    expect: {}
`)

	got := file.Tests[0].Inputs
	require.EqualValues(t, 500, got["amount"], "the case's scalar must win over the default's")
	require.Equal(t, "production", got["environment"], "a default key the case did not name must remain")
}

// TestDefaultsStubsAppendUnlessTargetingTheSame pins the stub rule in both
// directions: a default stub the case does not touch is kept and appended, and
// a case stub aimed at the same task replaces the default rather than doubling
// it.
func TestDefaultsStubsAppendUnlessTargetingTheSame(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)

	// A run below 100 reaches `small`; the default log stub is inherited, and
	// the case adds an http stub. Both must be present, or the run fails on an
	// unstubbed task.
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: the default log stub is inherited and the case http stub appends
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    stubs:
      - task: http
        returns: {tag: small}
    expect:
      ran: [announce, small]
      others: skipped
      outputs: {tag: small}

  - name: a case log stub replaces the default rather than adding a second
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    stubs:
      - task: log
        returns: {}
      - task: http
        returns: {tag: small}
    expect:
      ran: [announce, small]
      others: skipped
      outputs: {tag: small}
`))
	require.Empty(t, report.GetRefused())
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: %v", c.GetName(), c.GetFailures())
	}

	// The replacement is observable in the effective stub count: the second
	// case declares two stubs of its own, and the default log stub is dropped
	// rather than appended, so the merged list is two and not three.
	file := runOneFile(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: replace
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
      - task: http
        returns: {tag: small}
    expect: {}
`)
	require.Len(t, file.Tests[0].Stubs, 2, "a case stub targeting the same task replaces the default, not appends")
}

// TestDefaultsSenderOnlyFillsOmittedSignals pins the resolved open question: a
// default sender reaches only a signal that declared none. A signal that named
// its own sender keeps it (explicit beats inherited).
func TestDefaultsSenderOnlyFillsOmittedSignals(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	file := runOneFile(t, dir, `
defaults:
  sender: {subject: default@example.com, issuer: https://sso.example.com}
tests:
  - name: one signal omits its sender, one names its own
    workflow: ./workflow.yaml
    signals:
      - name: a
      - name: b
        sender: {subject: explicit@example.com, issuer: https://sso.example.com}
    expect: {}
`)

	signals := file.Tests[0].Signals
	require.Len(t, signals, 2)
	require.NotNil(t, signals[0].Sender)
	require.Equal(t, "default@example.com", signals[0].Sender.Subject, "an omitted sender inherits the default")
	require.Equal(t, "explicit@example.com", signals[1].Sender.Subject, "an explicit sender is kept over the default")
}

// TestDefaultsRefuseExpressions checks the fixture rule: an expression anywhere
// in `defaults:` is refused when the file loads, named by its position
// (CLAUDE.md, "diagnostics are a feature").
func TestDefaultsRefuseExpressions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		defaults string
		position string
	}{
		{
			name:     "in an input",
			defaults: "  inputs: {version: '${inputs.x}'}",
			position: "defaults.inputs.version",
		},
		{
			name:     "nested inside an input map",
			defaults: "  inputs: {meta: {tag: '${steps.x.y}'}}",
			position: "defaults.inputs.meta.tag",
		},
		{
			name:     "in a stub returns",
			defaults: "  stubs:\n    - task: http\n      returns: {tag: '${service.name}'}",
			position: "defaults.stubs[0].returns.tag",
		},
		{
			name:     "in a sender claim",
			defaults: "  sender: {subject: a@b.com, issuer: https://i, claims: {team: '${inputs.t}'}}",
			position: "defaults.sender.claims.team",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeDefaultsWorkflow(t, dir)
			path := dir + "/x.test.yaml"
			writeFile(t, path, fmt.Sprintf(`
defaults:
%s
tests:
  - name: a case
    workflow: ./workflow.yaml
    expect: {}
`, tc.defaults))

			_, err := flowtest.Load(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.position, "the refusal must name the position of the expression")
			require.Contains(t, err.Error(), "fixture")
		})
	}
}

// TestDefaultsStubBound pins the size bound on the defaults stub list, the
// house rule that author-controlled but still-parsed input gets an explicit
// bound (CLAUDE.md).
func TestDefaultsStubBound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	var b strings.Builder
	b.WriteString("defaults:\n  stubs:\n")
	for i := 0; i < flowtest.MaxDefaultStubs+1; i++ {
		b.WriteString("    - task: log\n      returns: {}\n")
	}
	b.WriteString("tests:\n  - name: a case\n    workflow: ./workflow.yaml\n    expect: {}\n")
	path := dir + "/x.test.yaml"
	writeFile(t, path, b.String())

	_, err := flowtest.Load(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "more than the limit")
}

// writeInline writes a *.test.yaml and returns its path, for the tests that run
// a file rather than only load it.
func writeInline(t *testing.T, dir, contents string) string {
	t.Helper()
	path := dir + "/inline.test.yaml"
	writeFile(t, path, contents)
	return path
}
