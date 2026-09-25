package flowtest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// Three probes from #1668 against fixtures under testdata/stubs: two identical
// stubs, a case stub that shadows a filtered default, and the catch-all shape
// that must stay quiet.

// TestTwoIdenticalStubsAreRefusedAtLoadNamingBoth: the loader has both stubs in
// hand, so the copy-paste is refused before anything runs, at both positions,
// each naming the other. Before this the second stub loaded and ran, and was a
// "never answered an invocation" warning after the fact.
func TestTwoIdenticalStubsAreRefusedAtLoadNamingBoth(t *testing.T) {
	t.Parallel()

	report := flowtest.RunFile("testdata/stubs/duplicate.test.yaml")
	refused := report.GetRefused()
	require.NotEmpty(t, refused, "two identical stubs must not load")

	// Stub 1 is written at line 7, stub 3 at line 12; the unfiltered stub 2
	// between them is a different selection and is not named.
	assert.Contains(t, refused, "duplicate.test.yaml:7:9:")
	assert.Contains(t, refused, `stub 1 (task "log", where: inputs.message == "hello") is selected again, the same way, by test "a copy-paste leaves two identical stubs" stub 3 below`)
	assert.Contains(t, refused, "duplicate.test.yaml:12:9:")
	assert.Contains(t, refused, `stub 3 (task "log", where: inputs.message == "hello") selects the same call the same way as test "a copy-paste leaves two identical stubs" stub 1 above`)
	assert.NotContains(t, refused, "stub 2", "the unfiltered stub selects differently and is no twin")
	assert.Empty(t, report.GetCases(), "a refused file runs nothing")
}

// TestABoundedStubFollowedByItsTwinIsTheDrainShape is the one deliberate
// shape the refusal must leave alone: a stub with `times:` drains, and the
// next stub for the same call answers afterwards, which is how
// examples/conditional-and-retry fails a step once and then lets it succeed.
func TestABoundedStubFollowedByItsTwinIsTheDrainShape(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: drained then answered
    workflow: ./workflow.yaml
    stubs:
      - task: log
        times: 1
        returns: {}
      - task: log
        returns: {}
    expect:
      ran: [greet]
`))
	require.Empty(t, report.GetRefused(), "a times: stub ahead of its twin is the drain, not a duplicate")
}

// TestTwoIdenticalDefaultStubsAreRefusedAtLoad: the block every case inherits
// from is held to the same rule, judged once where it is written.
func TestTwoIdenticalDefaultStubsAreRefusedAtLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
    - task: log
      returns: {}
tests:
  - name: inherits both
    workflow: ./workflow.yaml
    expect:
      ran: [greet]
`))
	refused := report.GetRefused()
	require.NotEmpty(t, refused)
	assert.Contains(t, refused, `defaults.stubs[0] (task "log", no where:) is selected again, the same way, by defaults.stubs[1] below`)
	assert.Contains(t, refused, `defaults.stubs[1] (task "log", no where:) selects the same call the same way as defaults.stubs[0] above`)
}

// TestACaseStubBesideAFilteredDefaultIsWarnedAndTheTranscriptSaysWhoAnswered:
// the case stub is prepended, not substituted, so both are live; the warning
// says so and names the byte-identical filter that would replace the default,
// and the transcript attributes each invocation to the stub that answered it,
// marking the inherited one.
func TestACaseStubBesideAFilteredDefaultIsWarnedAndTheTranscriptSaysWhoAnswered(t *testing.T) {
	t.Parallel()

	file, err := flowtest.Load("testdata/stubs/shadow.test.yaml")
	require.NoError(t, err)
	run := flowtest.Run(t.Context(), file, "testdata/stubs", flowtest.RunOptions{})
	report := run.Report
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	var warnings []string
	for _, w := range c.GetWarnings() {
		warnings = append(warnings, w.GetMessage())
	}
	joined := strings.Join(warnings, "\n")
	assert.Contains(t, joined, `stub 1 (task "log") does not replace the default stub for the same task "log"`)
	assert.Contains(t, joined, `their where: clauses differ`)
	assert.NotContains(t, joined, `inputs.message ==`, "the warning must not copy source-sized expressions")
	assert.Contains(t, joined, "write the default's where: byte for byte")
	assert.Equal(t, 1, strings.Count(joined, "does not replace"), "one warning per case stub, not one per default:\n%s", joined)

	require.Len(t, run.Transcripts, 1)
	var lines []string
	for _, l := range run.Transcripts[0] {
		lines = append(lines, l.Text)
	}
	transcript := strings.Join(lines, "\n")
	assert.Contains(t, transcript, `stub 1 (task "log")`, "the case's own stub answered greet:\n%s", transcript)
	assert.Contains(t, transcript, `stub 2 (task "log", from defaults)`, "the inherited filtered default answered part:\n%s", transcript)
}

// TestAFilteredCaseStubAheadOfACatchAllDefaultIsNotAWarning is the negative
// direction and the corpus's shape: an unfiltered default is the fallthrough
// the merge rules promise, and warning on it would fire across examples/.
func TestAFilteredCaseStubAheadOfACatchAllDefaultIsNotAWarning(t *testing.T) {
	t.Parallel()

	report := flowtest.RunFile("testdata/stubs/catchall.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
	assert.Empty(t, c.GetWarnings(), "a catch-all default is a fallthrough, not a shadow")
}
