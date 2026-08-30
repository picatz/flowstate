package flowtest_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// byFileIn indexes a refusal's problems by the document each is about, so a test
// asserting about two documents at once can name them rather than counting on an
// order.
func byFileIn(t *testing.T, problems *flowtest.Diagnostics) map[string]flowtest.Diagnostic {
	t.Helper()

	out := make(map[string]flowtest.Diagnostic, len(problems.Problems))
	for _, d := range problems.Problems {
		_, seen := out[d.File]
		require.False(t, seen, "two problems about %s, so this test cannot name either: %v", d.File, problems)
		out[d.File] = d
	}

	return out
}

// TestAnInheritedStubIsNumberedTheWayItsOwnFileNumbersIt (Codex, #1185): the
// directory's stubs are *appended*, so the index a stub lands on in the combined
// list is the suite's numbering rather than the directory's. Attributing a
// diagnostic by that index sent a reader to `defaults.stubs[1]` of a file whose
// only entry is `defaults.stubs[0]`.
//
// Both files hold a malformed stub in one load, because the collision is
// precisely their interaction: each entry is `[0]` in the document that wrote
// it, and `[0]` and `[1]` in the list the run reads.
func TestAnInheritedStubIsNumberedTheWayItsOwnFileNumbersIt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sibling := filepath.Join(dir, "testdefaults.yaml")
	writeFile(t, sibling, `
defaults:
  stubs:
    - task: fromDir
      returns: {}
      fails:
        message: nope
`)
	suite := `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: fromSuite
      returns: {}
      fails:
        message: nope
tests:
  - name: the case
    expect:
      ran: [a]
`
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, suite)

	_, err := flowtest.Load(path)
	require.Error(t, err)
	problems, refused := errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)
	require.Equal(t, 2, problems.Total, "expected one problem per document: %v", problems)

	byFile := byFileIn(t, problems)
	require.Contains(t, byFile, sibling, "the directory's stub was not reported against its own file")
	require.Contains(t, byFile, path)

	// The directory's: named after that file, numbered and positioned the way
	// that file writes it.
	assert.Equal(t, "defaults.stubs[0]", byFile[sibling].Field,
		"the directory's only stub was addressed at an index it does not have")
	assert.Contains(t, byFile[sibling].Message, `defaults.stubs[0] for task "fromDir"`)
	assert.Equal(t, 4, byFile[sibling].Line)

	// The suite's own: its index, and its exact position.
	line, column := spot(t, suite, "task: fromSuite")
	assert.Equal(t, "defaults.stubs[0]", byFile[path].Field)
	assert.Contains(t, byFile[path].Message, `defaults.stubs[0] for task "fromSuite"`)
	assert.Equal(t, line, byFile[path].Line, "the suite's own stub lost the position it has")
	assert.Equal(t, column, byFile[path].Column)
}

// TestADefaultStubIsJudgedWhereItIsWritten: [mergeDefaults] copies a `defaults:`
// stub into every case, so judging the copies reported one mistake once per
// case — and against a merged index, which for a directory-written stub
// addresses neither document. It is judged at the block instead, the rule the
// same block's claims and its sender already follow.
func TestADefaultStubIsJudgedWhereItIsWritten(t *testing.T) {
	t.Parallel()

	source := `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
      fails:
        message: nope
tests:
  - name: one
    expect:
      ran: [a]
  - name: two
    expect:
      ran: [a]
  - name: three
    expect:
      ran: [a]
`
	problems, _ := refuse(t, source)
	d := only(t, problems)

	line, column := spot(t, source, "task: log")
	assert.Contains(t, d.Message, "declares both returns and fails")
	assert.Contains(t, d.Message, "defaults.stubs[0]",
		"the mistake was named after a case rather than after the block that holds it")
	assert.Equal(t, line, d.Line)
	assert.Equal(t, column, d.Column)

	// The skip is on the copies the merge marked, not on stubs in general: a
	// case's own stub with the very same mistake is still judged, once per case
	// that writes one.
	own := `
tests:
  - name: one
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
        fails:
          message: nope
  - name: two
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
        fails:
          message: nope
`
	problems, _ = refuse(t, own)
	assert.Equal(t, 2, problems.Total, "a case's own stub stopped being judged: %v", problems)
	assert.Contains(t, problems.Error(), `test "one" stub 1 for task "log" declares both returns and fails`)
	assert.Contains(t, problems.Error(), `test "two" stub 1`)
}

// TestATableEntrysStubIsStillJudged is the fail-open the skip above could have
// been, pinned. [mergeRow] folds a table entry through [mergeDefaults] with the
// entry standing in as the block, so an entry's stubs carry `fromDefaults` too —
// and a skip written on that mark leaves an entry's malformed stub judged by
// nobody at all, since nothing judges a table entry the way [checkDefaults]
// judges a block.
//
// So the skip asks whether the `defaults:` block holds this stub, not whether
// something inherited it.
func TestATableEntrysStubIsStillJudged(t *testing.T) {
	t.Parallel()

	source := `
defaults:
  workflow: ./workflow.yaml
tests:
  - name: table
    stubs:
      - task: fromEntry
        returns: {}
        fails:
          message: nope
    cases:
      - name: one
      - name: two
`
	problems, _ := refuse(t, source)

	require.Equal(t, 2, problems.Total, "a table entry's stub went unjudged: %v", problems)
	rendered := problems.Error()
	assert.Contains(t, rendered, `test "table/one" stub 1 for task "fromEntry" declares both returns and fails`)
	assert.Contains(t, rendered, `test "table/two" stub 1`)
	for _, d := range problems.Problems {
		assert.Empty(t, d.Field,
			"a stub written in a table entry was addressed at a path the entry does not hold")
	}
}

// TestAnEntryStubIdenticalToADefaultIsAnsweredAtTheBlock is the other side of
// the same search, and the behaviour that makes it a value comparison rather
// than a provenance one: two stubs that are the same value say the same thing
// about the same shape, so the block's answer is the one an author can act on
// and the case does not repeat it per row.
func TestAnEntryStubIdenticalToADefaultIsAnsweredAtTheBlock(t *testing.T) {
	t.Parallel()

	source := `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: shared
      returns: {}
      fails:
        message: nope
tests:
  - name: table
    stubs:
      - task: shared
        returns: {}
        fails:
          message: nope
    cases:
      - name: one
      - name: two
`
	problems, _ := refuse(t, source)
	d := only(t, problems)

	line, column := spot(t, source, "task: shared")
	assert.Contains(t, d.Message, "defaults.stubs[0] for task \"shared\"")
	assert.Equal(t, line, d.Line, "the one answer was not given at the block")
	assert.Equal(t, column, d.Column)
}

// TestATruncationSummaryLooksPastTheProblemsItKept (Codex, #1193) is the corner
// the fix below did not reach on its first pass: it decided "is there one file
// to name" by reading the problems that were *kept*, which is a set that by
// construction excludes the ones the line is about.
//
// Twenty problems in the sibling and ten in the suite is the shape that
// separates the two answers, because the bound then keeps twenty problems that
// really are all one document's — and drops ten that are all the other's. So the
// provenance is decided where a problem is found, over everything found.
func TestATruncationSummaryLooksPastTheProblemsItKept(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sibling := filepath.Join(dir, "testdefaults.yaml")
	path := filepath.Join(dir, "workflow.test.yaml")

	inherited := func(n int) string {
		var b strings.Builder
		b.WriteString("defaults:\n  inputs:\n")
		for i := range n {
			fmt.Fprintf(&b, "    a%02d: \"${nope.x}\"\n", i)
		}

		return b.String()
	}
	suite := func(n int) string {
		var b strings.Builder
		b.WriteString("defaults:\n  workflow: ./workflow.yaml\n  inputs:\n")
		for i := range n {
			fmt.Fprintf(&b, "    b%02d: \"${nope.x}\"\n", i)
		}
		b.WriteString("tests:\n  - name: the case\n    expect:\n      ran: [a]\n")

		return b.String()
	}
	load := func(t *testing.T) *flowtest.Diagnostics {
		t.Helper()

		_, err := flowtest.Load(path)
		require.Error(t, err)
		problems, refused := errors.AsType[*flowtest.Diagnostics](err)
		require.True(t, refused)
		require.Equal(t, 30, problems.Total, "the fixture stopped producing 30 problems: %v", problems)
		require.Len(t, problems.Problems, flowtest.MaxLoadProblems)

		return problems
	}

	writeFile(t, sibling, inherited(20))
	writeFile(t, path, suite(10))
	problems := load(t)

	// Asserted, not assumed: if the kept twenty ever stop being one document's,
	// this fixture has stopped exercising the corner and the claim below would
	// pass for the wrong reason.
	for _, d := range problems.Problems {
		require.Equal(t, sibling, d.File,
			"the fixture no longer keeps twenty problems from one file, so the corner is untested")
	}
	assert.Equal(t, "10 more problems were found and 20 are shown", lastLine(problems.Error()),
		"the dropped problems are the suite's, and the line named the sibling every kept problem is in")

	// The control, in the same shape: thirty problems, twenty kept and ten
	// dropped, all of them one document's. A fix that simply stopped naming a
	// file under truncation would pass the assertion above and fail this one.
	writeFile(t, sibling, inherited(30))
	writeFile(t, path, "defaults:\n  workflow: ./workflow.yaml\ntests:\n  - name: the case\n    expect:\n      ran: [a]\n")
	problems = load(t)

	assert.Equal(t, sibling+": 10 more problems were found and 20 are shown", lastLine(problems.Error()),
		"a truncated report about one document stopped naming it")
}

// TestATruncationSummaryNamesAFileOnlyWhenThereIsOneToName (Codex, #1185): the
// tail line prefixed the first *kept* problem's file, and what was dropped is
// not necessarily about that file — a suite and the directory's defaults are
// refused together. The count is the honest part.
func TestATruncationSummaryNamesAFileOnlyWhenThereIsOneToName(t *testing.T) {
	t.Parallel()

	// Fifteen problems written in each document, so the twenty that are kept
	// span both and the ten that are dropped are the suite's.
	dir := t.TempDir()
	sibling := filepath.Join(dir, "testdefaults.yaml")
	var inherited strings.Builder
	inherited.WriteString("defaults:\n  inputs:\n")
	for i := range 15 {
		fmt.Fprintf(&inherited, "    a%02d: \"${nope.x}\"\n", i)
	}
	writeFile(t, sibling, inherited.String())

	var own strings.Builder
	own.WriteString("defaults:\n  workflow: ./workflow.yaml\n  inputs:\n")
	for i := range 15 {
		fmt.Fprintf(&own, "    b%02d: \"${nope.x}\"\n", i)
	}
	own.WriteString("tests:\n  - name: the case\n    expect:\n      ran: [a]\n")
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, own.String())

	_, err := flowtest.Load(path)
	require.Error(t, err)
	problems, refused := errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)
	require.Equal(t, 30, problems.Total, "the fixture stopped producing problems in both files: %v", problems)
	require.Len(t, problems.Problems, flowtest.MaxLoadProblems)

	tail := lastLine(problems.Error())
	assert.Equal(t, "10 more problems were found and 20 are shown", tail,
		"a report spanning two documents told a reader which one the dropped problems were in")

	// The positive control, because a line asserted to be missing a file name is
	// worth nothing until the same line is seen carrying one: with every problem
	// in the suite, the file is named exactly as it always was.
	writeFile(t, sibling, "vars:\n  unused: 1\n")
	var alone strings.Builder
	alone.WriteString("defaults:\n  workflow: ./workflow.yaml\n  inputs:\n")
	for i := range 30 {
		fmt.Fprintf(&alone, "    c%02d: \"${nope.x}\"\n", i)
	}
	alone.WriteString("tests:\n  - name: the case\n    expect:\n      ran: [a]\n")
	writeFile(t, path, alone.String())

	_, err = flowtest.Load(path)
	require.Error(t, err)
	problems, refused = errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)
	assert.Equal(t, path+": 10 more problems were found and 20 are shown", lastLine(problems.Error()),
		"one document's report stopped naming the document")
}

// lastLine is the tail of a rendered refusal, which is where a bounded report
// says how much of itself is missing.
func lastLine(rendered string) string {
	lines := strings.Split(rendered, "\n")

	return lines[len(lines)-1]
}
