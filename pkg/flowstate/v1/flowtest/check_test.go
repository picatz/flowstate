package flowtest_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestCheckBounds pins both sides of the check amplification bound: defaults
// are refused before they can be copied into every case, and the effective
// list is refused when individually valid defaults and case claims combine.
func TestCheckBounds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		defaultChecks int
		caseChecks    int
		want          string
	}{
		{name: "defaults before multiplication", defaultChecks: flowtest.MaxChecksPerTest + 1, want: "defaults"},
		{name: "effective case after merge", defaultChecks: flowtest.MaxChecksPerTest, caseChecks: 1, want: `test "a case"`},
		{name: "case claims", caseChecks: flowtest.MaxChecksPerTest + 1, want: `test "a case"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var b strings.Builder
			if tc.defaultChecks > 0 {
				b.WriteString("defaults:\n  check:\n")
				for range tc.defaultChecks {
					b.WriteString("    - true\n")
				}
			}
			b.WriteString("tests:\n  - name: a case\n    workflow: ./workflow.yaml\n    expect:\n")
			if tc.caseChecks == 0 {
				b.WriteString("      {}\n")
			} else {
				b.WriteString("      check:\n")
				for range tc.caseChecks {
					b.WriteString("        - true\n")
				}
			}

			_, err := flowtest.LoadSource([]byte(b.String()))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
			require.Contains(t, err.Error(), fmt.Sprintf("%d checks", flowtest.MaxChecksPerTest+1))
			require.Contains(t, err.Error(), fmt.Sprintf("limit of %d", flowtest.MaxChecksPerTest))
		})
	}
}

func TestTableCheckBoundsRunBeforeRowsAreExpanded(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		entryChecks int
		rowChecks   int
		rows        int
		want        string
	}{
		{
			name:        "entry before row multiplication",
			entryChecks: flowtest.MaxChecksPerTest + 1,
			rows:        flowtest.MaxTestsPerFile,
			want:        "table entry",
		},
		{
			name:        "effective row after entry merge",
			entryChecks: flowtest.MaxChecksPerTest,
			rowChecks:   1,
			rows:        1,
			want:        "after its table entry is applied",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var b strings.Builder
			b.WriteString("tests:\n  - name: a table\n    workflow: ./workflow.yaml\n    expect:\n      check:\n")
			for range tc.entryChecks {
				b.WriteString("        - true\n")
			}
			b.WriteString("    cases:\n")
			for i := range tc.rows {
				fmt.Fprintf(&b, "      - name: row-%d\n", i)
				if tc.rowChecks > 0 {
					b.WriteString("        expect:\n          check:\n")
					for range tc.rowChecks {
						b.WriteString("            - true\n")
					}
				}
			}

			_, err := flowtest.LoadSource([]byte(b.String()))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
			require.Contains(t, err.Error(), fmt.Sprintf("limit of %d", flowtest.MaxChecksPerTest))
		})
	}
}

// `expect.check:` (#1072): CEL claims over the finished run, witnessed on
// failure — the values a red claim read, printed beside it, redacted through
// the one spelling every other surface uses.

// writeCheckWorkflow computes something worth asserting about: a joined
// value, a list, and a declared output.
func writeCheckWorkflow(t *testing.T, dir string) {
	t.Helper()

	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: computed
inputs:
  region:
    type: string
steps:
  - id: plan
    value: "${ {'regions': [inputs.region, 'us-east-1'], 'count': 2} }"
  - id: joined
    value: ${inputs.region + ':' + string(steps.plan.value.count)}
outputs:
  said:
    value: ${steps.joined.value}
`)
}

// TestChecksHoldOverTheFinishedRun is the green direction: claims over step
// values, inputs, list indexing, and the run root, all holding at once.
func TestChecksHoldOverTheFinishedRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the plan holds its shape
    workflow: ./workflow.yaml
    inputs: {region: eu-west-1}
    expect:
      ran: [plan, joined]
      check:
        - size(steps.plan.value.regions) == 2
        - steps.plan.value.regions[0] == inputs.region
        - "${steps.joined.value == 'eu-west-1:2'}"
        - that: "!run.failed && run.local"
          because: a green local run says both
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestAFailedCheckArrivesWithItsWitnesses: the claim, the author's sentence,
// and the values the expression read — `path = value` — so a red check is
// evidence, not homework.
func TestAFailedCheckArrivesWithItsWitnesses(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: claims the wrong region
    workflow: ./workflow.yaml
    inputs: {region: eu-west-1}
    expect:
      check:
        - that: steps.plan.value.regions[0] == 'us-east-1'
          because: the fleet default must win
`)

	report := flowtest.RunFile(path)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Len(t, c.GetFailures(), 1)

	f := c.GetFailures()[0]
	assert.Equal(t, "expect.check[0]", f.GetField())
	assert.Contains(t, f.GetMessage(), "check failed: steps.plan.value.regions[0] == 'us-east-1'")
	assert.Contains(t, f.GetMessage(), "because: the fleet default must win")
	assert.Contains(t, f.GetMessage(), `steps.plan.value.regions[0] = "eu-west-1"`,
		"the witness is the value the claim actually read")
}

// TestACheckOverTheRunError is what the run root exists for: an error claim
// asserts against a failed run, where the named fields stop at a substring.
func TestACheckOverTheRunError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: bounded
inputs:
  replicas:
    type: int
    must: this <= 10
steps:
  - id: plan
    value: ${inputs.replicas}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the refusal names the bound and the step never runs
    workflow: ./workflow.yaml
    inputs: {replicas: 99}
    expect:
      failed: true
      check:
        - run.failed && run.error.contains('must satisfy')
        - "!('plan' in steps)"
`)

	report := flowtest.RunFile(path)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestACheckThatErrorsFailsClosed: a claim the run cannot answer did not
// hold. The failure carries the evaluator's own words.
func TestACheckThatErrorsFailsClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: reaches a step that does not exist
    workflow: ./workflow.yaml
    inputs: {region: eu-west-1}
    expect:
      check:
        - steps.missing.value == 1
`)

	report := flowtest.RunFile(path)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Len(t, c.GetFailures(), 1)
	assert.Contains(t, c.GetFailures()[0].GetMessage(), "check errored")
}

// TestANonBooleanCheckIsRefused the way `if:` refuses one: named, never
// coerced.
func TestANonBooleanCheckIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: a claim that is a value
    workflow: ./workflow.yaml
    inputs: {region: eu-west-1}
    expect:
      check:
        - steps.joined.value
`)

	report := flowtest.RunFile(path)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	assert.Contains(t, c.GetFailures()[0].GetMessage(), "must evaluate to a boolean")
}

// TestAMalformedCheckIsRefusedAtLoad, position named — syntax is a property
// of the file, so the author is told while they are still there.
func TestAMalformedCheckIsRefusedAtLoad(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
tests:
  - name: unbalanced
    workflow: ./workflow.yaml
    expect:
      check:
        - "steps.a.value =="
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), `test "unbalanced" expect.check[0]`)
}

// TestACheckEntryRefusesUnknownKeys: the loader's strictness holds inside the
// mapping form too.
func TestACheckEntryRefusesUnknownKeys(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
tests:
  - name: misspelled
    workflow: ./workflow.yaml
    expect:
      check:
        - that: "true"
          becuase: typo
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), `"becuase"`)
}

// TestChecksAccumulateAcrossAllThreeLevels: defaults, entry, row — every
// level's claims all hold, proven by a row that fails only the file-level
// one.
func TestChecksAccumulateAcrossAllThreeLevels(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
  check:
    - steps.plan.value.count == 2
tests:
  - name: regions
    inputs: {region: eu-west-1}
    expect:
      check:
        - size(steps.plan.value.regions) == 2
    cases:
      - name: holds everywhere
      - name: adds its own claim
        expect:
          check:
            - steps.plan.value.regions[0] == 'eu-west-1'
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// TestAFileLevelCheckFailsEveryCaseThatBreaksIt: the accumulate direction's
// teeth — the defaults claim reaches a case that never wrote it.
func TestAFileLevelCheckFailsEveryCaseThatBreaksIt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
  check:
    - steps.plan.value.count == 3
tests:
  - name: states no check of its own
    inputs: {region: eu-west-1}
    expect:
      ran: [plan, joined]
`)

	report := flowtest.RunFile(path)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Len(t, c.GetFailures(), 1)
	assert.Contains(t, c.GetFailures()[0].GetMessage(), "steps.plan.value.count = 2",
		"the witness shows what the file-level claim actually saw")
}

// TestACheckWitnessIsRedacted: a sensitive input a claim references reaches
// the report as [redacted], by the same set the transcript and the stub
// diagnostics share — a new surface must not be a new leak (#1052's rule).
func TestACheckWitnessIsRedacted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: guarded
inputs:
  token:
    type: string
    sensitive: true
steps:
  - id: echo
    value: ${inputs.token}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: claims the wrong token
    workflow: ./workflow.yaml
    inputs: {token: super-secret-value}
    expect:
      check:
        - steps.echo.value == 'expected-something-else'
`)

	report := flowtest.RunFile(path)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())

	message := c.GetFailures()[0].GetMessage()
	assert.NotContains(t, message, "super-secret-value")
	assert.Contains(t, message, "[redacted]")
}

// TestManyWitnessesAreBounded at [flowtest.MaxCheckWitnesses], the residual
// named rather than silently dropped.
func TestManyWitnessesAreBounded(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckWorkflow(t, dir)

	terms := make([]string, 0, flowtest.MaxCheckWitnesses+3)
	for i := 0; i < flowtest.MaxCheckWitnesses+3; i++ {
		terms = append(terms, "steps.plan.value.regions["+strings.Repeat("0+", i)+"0] == 'nope'")
	}
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: a very wide claim
    workflow: ./workflow.yaml
    inputs: {region: eu-west-1}
    expect:
      check:
        - "`+strings.Join(terms, " && ")+`"
`)

	report := flowtest.RunFile(path)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	message := c.GetFailures()[0].GetMessage()
	assert.LessOrEqual(t, strings.Count(message, " = "), flowtest.MaxCheckWitnesses,
		"witness lines stay bounded")
}
