package flowtest_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The position-carrying loader (#923 step 1). Two claims run through every test
// here, and they are the two things the loader could not do before: a refusal
// names the line and column of the value it refuses, and a document that has
// several problems reports all of them.
//
// The third claim is the one that keeps the first honest, and it is the reason
// half of these tests exist: a position is claimed only where *this* document
// wrote the thing being refused. A test file inherits — a case's stubs from
// `defaults:`, a row's trigger from its entry, a suite's defaults from a
// sibling `testdefaults.yaml` — and a diagnostic that borrowed the inheriting
// case's position would underline text that is correct, which CLAUDE.md ranks
// worse than not underlining anything.

// refuse loads a suite written to a temporary directory and returns the
// problems it was refused with.
func refuse(t *testing.T, source string) (*flowtest.Diagnostics, string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, source)

	_, err := flowtest.Load(path)
	require.Error(t, err, "this suite was expected to be refused")

	problems, refused := errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused, "a load refusal must be *flowtest.Diagnostics, or nothing can read its positions: %v", err)

	return problems, path
}

// spot is the 1-based line and column of needle's first occurrence in source,
// so a test states the position it expects as the text it expects it on rather
// than as two numbers that have to be recounted whenever the fixture moves.
func spot(t *testing.T, source, needle string) (line, column int) {
	t.Helper()

	for i, text := range strings.Split(source, "\n") {
		if at := strings.Index(text, needle); at >= 0 {
			return i + 1, len([]rune(text[:at])) + 1
		}
	}
	t.Fatalf("the fixture does not contain %q, so this test is asserting about nothing", needle)

	return 0, 0
}

// only returns the single problem a suite was refused with, failing when there
// is more than one: a test asserting about "the diagnostic" has to know there
// is exactly one.
func only(t *testing.T, problems *flowtest.Diagnostics) flowtest.Diagnostic {
	t.Helper()

	require.Equal(t, problems.Total, len(problems.Problems), "some problems were dropped by the bound")
	require.Len(t, problems.Problems, 1, "expected one problem, got: %v", problems)

	return problems.Problems[0]
}

// TestALoadReportsEveryProblemAtOnce is the collection half of #923 step 1:
// before it, every check returned at the first failure, so a suite with four
// mistakes took four runs to fix. Independent problems are independent — a
// malformed stub in one case says nothing about another case's trigger — so
// they all travel in one refusal.
func TestALoadReportsEveryProblemAtOnce(t *testing.T) {
	t.Parallel()

	source := `
tests:
  - name: first
    workflow: ./workflow.yaml
    stubs:
      - returns: {}
    expect:
      ran: [a]
  - name: second
    workflow: ./workflow.yaml
    expect:
      others: sometimes
  - name: third
    expect:
      ran: [a]
`
	problems, _ := refuse(t, source)

	require.Equal(t, 3, problems.Total, "one refusal per problem: %v", problems)
	require.Len(t, problems.Problems, 3)

	rendered := problems.Error()
	assert.Contains(t, rendered, `test "first" stub 1 names neither a task nor a step`)
	assert.Contains(t, rendered, `test "second" expect.others: "sometimes" is not a value it accepts`)
	assert.Contains(t, rendered, `test "third" names no workflow`)

	// In source order, which is what makes a report readable next to the file
	// it is about — and what makes it the same report every time.
	for i := 1; i < len(problems.Problems); i++ {
		assert.LessOrEqual(t, problems.Problems[i-1].Line, problems.Problems[i].Line,
			"the problems are not in source order")
	}
}

// TestADiagnosticNamesThePositionOfTheValueItRefuses is the position half. Each
// case here refuses a different kind of value — a scalar, a key, a whole
// mapping — and each asserts the exact line and column, because "has some
// position" is satisfied by a position pointing at the wrong thing.
func TestADiagnosticNamesThePositionOfTheValueItRefuses(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		source string
		// on is the text the diagnostic must land on.
		on   string
		says string
	}{
		{
			name: "a scalar the grammar does not accept",
			source: `
tests:
  - name: the case
    workflow: ./workflow.yaml
    expect:
      others: sometimes
`,
			on:   "sometimes",
			says: "is not a value it accepts",
		},
		{
			name: "a trigger's signature",
			source: `
tests:
  - name: the case
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./delivery.json
      signature: maybe
`,
			on:   "maybe",
			says: `signature: "maybe" is not a value it accepts`,
		},
		{
			name: "a stub that targets nothing, positioned at the stub's first key",
			source: `
tests:
  - name: the case
    workflow: ./workflow.yaml
    stubs:
      - returns: {}
      - task: log
        returns: {}
`,
			on:   "returns: {}",
			says: "stub 1 names neither a task nor a step",
		},
		{
			name: "a case with a key missing, positioned at the case",
			source: `
tests:
  - name: the case
    expect:
      ran: [a]
`,
			on:   "name: the case",
			says: "names no workflow",
		},
		{
			name: "a var whose name CEL could never read back",
			source: `
vars:
  2fast: yes
tests:
  - name: the case
    workflow: ./workflow.yaml
    expect:
      ran: [a]
`,
			on:   "2fast",
			says: "must be a CEL identifier",
		},
		{
			name: "a coverage entry with no reason",
			source: `
coverage:
  allow_unreached:
    never-runs: ""
tests:
  - name: the case
    workflow: ./workflow.yaml
    expect:
      ran: [a]
`,
			on:   "never-runs",
			says: "has no reason",
		},
		{
			name: "an expression written into the defaults block",
			source: `
defaults:
  inputs:
    region: ${inputs.region}
tests:
  - name: the case
    workflow: ./workflow.yaml
    expect:
      ran: [a]
`,
			on:   "${inputs.region}",
			says: "may not hold an expression",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			problems, path := refuse(t, tc.source)
			d := only(t, problems)

			line, column := spot(t, tc.source, tc.on)
			assert.Equal(t, line, d.Line, "the diagnostic is not on the line holding the mistake: %v", problems)
			assert.Equal(t, column, d.Column, "the diagnostic is on the right line and the wrong column: %v", problems)
			assert.Contains(t, d.Message, tc.says)
			assert.Equal(t, path, d.File, "a problem must be attributable to the file it is about")
		})
	}
}

// TestARefusalRendersFileLineAndColumn pins the rendering, because it is what a
// terminal reader and a CI log see: `file:line:column: message`, the one join
// every positioned diagnostic in this repository goes through (#384).
func TestARefusalRendersFileLineAndColumn(t *testing.T) {
	t.Parallel()

	source := `
tests:
  - name: the case
    workflow: ./workflow.yaml
    expect:
      others: sometimes
`
	problems, path := refuse(t, source)
	line, column := spot(t, source, "sometimes")
	d := only(t, problems)

	assert.Equal(t, path+":"+strconv.Itoa(line)+":"+strconv.Itoa(column)+": "+d.Message, problems.Error(),
		"the rendered refusal is not `file:line:column: message`")
	assert.Contains(t, d.Message, `test "the case" expect.others:`,
		"the message must still name the case a reader matches back to the file")
}

// TestAnUnpositionedProblemStillReadsAsItAlwaysHas: a problem with the document
// as a whole has no line, and its rendering is byte for byte the `file: message`
// this loader has always produced — the honest end of the rule rather than an
// exception to it.
func TestAnUnpositionedProblemStillReadsAsItAlwaysHas(t *testing.T) {
	t.Parallel()

	problems, path := refuse(t, "defaults:\n  workflow: ./workflow.yaml\n")
	d := only(t, problems)

	assert.Zero(t, d.Line, "a file that declares no `tests:` has no line to point at")
	assert.Equal(t, path+": declares no tests", problems.Error())
}

// TestAnInheritedValueIsRefusedWithoutBorrowingACasesPosition is the rule that
// keeps a position honest, in the direction that is easy to get wrong: the
// sender under test is written once in `defaults:` and reaches the case by
// merge, so the case's own lines say nothing about it. Pointing at the case
// would underline correct text and send an author to fix it.
func TestAnInheritedValueIsRefusedWithoutBorrowingACasesPosition(t *testing.T) {
	t.Parallel()

	source := `
defaults:
  workflow: ./workflow.yaml
  sender:
    subject: approver@example.com
tests:
  - name: the case
    signals:
      - name: approve
        payload: {}
    expect:
      ran: [a]
`
	problems, _ := refuse(t, source)
	d := only(t, problems)

	assert.Contains(t, d.Message, `test "the case" signal 1 ("approve") sender:`,
		"the by-name prose is what still identifies an inherited value")
	assert.Zero(t, d.Line,
		"a value this document did not write at the case must not be positioned at the case, "+
			"nor at any node enclosing it")
	assert.Zero(t, d.Column)
	assert.Equal(t, "the case", d.Test, "the case is still named, structurally")

	// The positive direction, because an absence assertion is worth nothing
	// until the thing could have been present: the identical mistake written
	// on the signal itself is positioned exactly.
	written := strings.Replace(source, "  sender:\n    subject: approver@example.com\n", "", 1)
	written = strings.Replace(written, "        payload: {}",
		"        sender:\n          subject: approver@example.com", 1)

	problems, _ = refuse(t, written)
	line, column := spot(t, written, "subject: approver@example.com")
	positioned := only(t, problems)
	assert.Equal(t, line, positioned.Line, "the same mistake, written in the case, must be positioned")
	assert.Equal(t, column, positioned.Column)
}

// TestADefaultFromTheSiblingFileBorrowsNoPositionFromTheSuite is the same rule
// one level further out, and the level #1109 was about: the value is written in
// the directory's `testdefaults.yaml`, so nothing in this buffer describes it.
//
// The suite here states a `defaults.inputs` of its own, so there is an
// enclosing node to borrow — which is the whole point. A fallback would
// underline the suite's own `version:` to report the sibling's `region:`.
func TestADefaultFromTheSiblingFileBorrowsNoPositionFromTheSuite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), `
defaults:
  inputs:
    region: ${inputs.region}
`)
	source := `
defaults:
  workflow: ./workflow.yaml
  inputs:
    version: "1"
tests:
  - name: the case
    expect:
      ran: [a]
`
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, source)

	_, err := flowtest.Load(path)
	require.Error(t, err)
	problems, refused := errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)

	d := only(t, problems)
	assert.Contains(t, d.Message, "defaults.inputs.region holds the expression")
	assert.Zero(t, d.Line, "a value written in the sibling file must not be positioned in this one")
	assert.Zero(t, d.Column)
	assert.Equal(t, path, d.File,
		"the problem is still attributed to the suite that was loaded, which is the document that was refused")
}

// TestAProblemInsideTheDefaultsBlockIsPositionedThere is the other half of the
// same rule, and the reason the merged stub carries provenance at all: the stub
// really is written in this file, in `defaults:`, so the refusal lands on it
// rather than on the case that inherited it.
func TestAProblemInsideTheDefaultsBlockIsPositionedThere(t *testing.T) {
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
  - name: the case
    expect:
      ran: [a]
`
	problems, _ := refuse(t, source)
	d := only(t, problems)

	line, column := spot(t, source, "task: log")
	assert.Contains(t, d.Message, "declares both returns and fails")
	assert.Equal(t, line, d.Line, "the refusal did not land on the default that carries the mistake")
	assert.Equal(t, column, d.Column)
}

// TestATableRowIsPositionedAtTheRowRatherThanTheEntry: a table expands into the
// flat list every check reads, so case i is no longer `tests[i]`. Recomputing a
// position from the index would land on whatever entry happened to sit there.
func TestATableRowIsPositionedAtTheRowRatherThanTheEntry(t *testing.T) {
	t.Parallel()

	source := `
tests:
  - name: ordinary
    workflow: ./workflow.yaml
    expect:
      ran: [a]
  - name: tabled
    workflow: ./workflow.yaml
    cases:
      - name: fine
        expect:
          ran: [a]
      - name: broken
        expect:
          others: sometimes
`
	problems, _ := refuse(t, source)
	d := only(t, problems)

	line, column := spot(t, source, "sometimes")
	assert.Equal(t, line, d.Line, "the refusal did not land on the row that carries the mistake")
	assert.Equal(t, column, d.Column)
	assert.Equal(t, "tabled/broken", d.Test)
}

// TestTheProblemsAreBoundedAndTheTotalTravels: the resource is the refusal and
// the ratio belongs to whoever wrote the file, so the report is capped — and
// says how much it is not showing, because a bounded report that stayed silent
// about the bound would read as a complete one.
func TestTheProblemsAreBoundedAndTheTotalTravels(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("tests:\n")
	broken := flowtest.MaxLoadProblems + 5
	for range broken {
		b.WriteString("  - name: unnamed workflow\n    expect:\n      ran: [a]\n")
	}

	problems, path := refuse(t, b.String())

	assert.Equal(t, broken, problems.Total, "every problem is counted even where it is not shown")
	assert.Len(t, problems.Problems, flowtest.MaxLoadProblems, "the report is not bounded")
	assert.Contains(t, problems.Error(), path+": 5 more problems were found and 20 are shown")
}

// TestTheProblemsAreBoundedByBytesAsWellAsByCount: counting problems does not
// bound them, because the document chooses how large one is. Twenty diagnostics
// each quoting a value an anchor supplied is a megabyte of file answered with
// twenty megabytes of refusal, and that multiplier is what collecting adds.
//
// The first problem is kept whatever its size, because a refusal that explains
// nothing is the worse failure; the rest are kept while they fit.
func TestTheProblemsAreBoundedByBytesAsWellAsByCount(t *testing.T) {
	t.Parallel()

	// One anchored value, aliased into ten `defaults.inputs` keys: ten
	// diagnostics, each quoting the whole of it.
	big := "${" + strings.Repeat("a", 32<<10) + "}"
	var b strings.Builder
	b.WriteString("defaults:\n  workflow: ./workflow.yaml\n  inputs:\n    first: &big \"" + big + "\"\n")
	for i := 1; i < 10; i++ {
		fmt.Fprintf(&b, "    key%d: *big\n", i)
	}
	b.WriteString("tests:\n  - name: the case\n    expect:\n      ran: [a]\n")

	problems, _ := refuse(t, b.String())

	require.Equal(t, 10, problems.Total, "every problem is counted, however few are kept")
	assert.Less(t, len(problems.Problems), 10, "the byte bound was never reached")
	assert.Greater(t, len(problems.Problems), 0, "a refusal that explains nothing is the worse failure")
	assert.Less(t, len(problems.Error()), 2*flowtest.MaxLoadProblemBytes,
		"a megabyte of document must not answer with an unbounded refusal")
	assert.Contains(t, problems.Error(), "more problems were found")
}

// TestAStrictKeyRefusalKeepsItsPositionAndItsCause: the decoder's own refusals
// — a misspelled key, a syntax error — arrive as one positioned problem like
// everything else, and the decoder's error travels underneath so a caller that
// wants the exact token (the language server underlines it) still reaches it.
func TestAStrictKeyRefusalKeepsItsPositionAndItsCause(t *testing.T) {
	t.Parallel()

	source := `
tests:
  - name: the case
    workflow: ./workflow.yaml
    expct:
      ran: [a]
`
	problems, _ := refuse(t, source)
	d := only(t, problems)

	line, _ := spot(t, source, "expct:")
	assert.Equal(t, line, d.Line, "an unknown key must report on the line holding it")
	assert.Contains(t, d.Message, `unknown field "expct"`)
	assert.NotContains(t, d.Message, ">", "the parser's rendered source excerpt does not belong in a diagnostic")

	var yamlErr yaml.Error
	require.True(t, errors.As(error(problems), &yamlErr),
		"the decoder's error must stay reachable: an editor reads its token to underline the mistake")
}

// TestTheSameDocumentReportsTheSameWayEveryTime is the determinism claim, and
// it is not decorative: half the checks here walk maps — `vars:`, `secrets:`,
// `claims:`, `coverage.allow_unreached` — whose iteration order Go randomizes
// per run. A report that shuffles is one nobody can write a test against, this
// package's own included.
func TestTheSameDocumentReportsTheSameWayEveryTime(t *testing.T) {
	t.Parallel()

	source := `
vars:
  1bad: a
  2bad: b
  3bad: c
coverage:
  allow_unreached:
    alpha: ""
    beta: ""
    gamma: ""
tests:
  - name: the case
    workflow: ./workflow.yaml
    secrets:
      "no-scheme": one
      "also-bad": two
    starter:
      claims:
        "": nope
    expect:
      ran: [a]
`
	// One file, loaded over and over: a fresh temporary directory per load
	// would differ in the path alone and hide what this is asking about.
	path := filepath.Join(t.TempDir(), "workflow.test.yaml")
	writeFile(t, path, source)

	load := func() *flowtest.Diagnostics {
		t.Helper()

		_, err := flowtest.Load(path)
		require.Error(t, err)
		problems, refused := errors.AsType[*flowtest.Diagnostics](err)
		require.True(t, refused)

		return problems
	}

	first := load()
	rendered := first.Error()
	for range 20 {
		require.Equal(t, rendered, load().Error(), "the same bytes reported two different ways")
	}
	assert.Greater(t, first.Total, 6, "this fixture is meant to produce many problems at once")
}

// TestAValueReachedThroughAnAliasIsPositionedWhereItWasWritten: a suite may
// share fixture text with `&anchor` and `*alias`, and the value the decoder
// read is the anchor's. The position follows the value, which is the text an
// author has to edit — the alias site holds nothing to fix.
func TestAValueReachedThroughAnAliasIsPositionedWhereItWasWritten(t *testing.T) {
	t.Parallel()

	source := `
tests:
  - name: the anchoring case
    workflow: ./workflow.yaml
    expect: &shared
      others: sometimes
  - name: the aliasing case
    workflow: ./workflow.yaml
    expect: *shared
`
	problems, _ := refuse(t, source)

	require.Equal(t, 2, problems.Total, "both cases carry the shared mistake: %v", problems)
	line, column := spot(t, source, "sometimes")
	for _, d := range problems.Problems {
		assert.Equal(t, line, d.Line, "an aliased value must be positioned where the anchor wrote it")
		assert.Equal(t, column, d.Column)
	}
}

// TestAMergedKeyIsPositionedInsideTheAnchorItCameFrom: `<<:` is how a suite
// shares a case's boilerplate, and the merged value lives in the anchor. A
// position at the merge site would point at `<<: *base`, which is not where the
// mistake is.
func TestAMergedKeyIsPositionedInsideTheAnchorItCameFrom(t *testing.T) {
	t.Parallel()

	source := `
tests:
  - name: the anchoring case
    <<: &base
      workflow: ./workflow.yaml
      expect:
        others: sometimes
  - name: the merging case
    <<: *base
`
	problems, _ := refuse(t, source)

	require.Equal(t, 2, problems.Total, "both cases carry the merged mistake: %v", problems)
	line, column := spot(t, source, "sometimes")
	for _, d := range problems.Problems {
		assert.Equal(t, line, d.Line, "a merged value must be positioned in the anchor it was written in")
		assert.Equal(t, column, d.Column)
	}
}

// TestTheBytesDoorReportsWithoutAFileName: [flowtest.LoadSource] is given bytes
// and no path — the MCP tool's door — so its problems carry positions and no
// file. Inventing a name would be a fact about nothing.
func TestTheBytesDoorReportsWithoutAFileName(t *testing.T) {
	t.Parallel()

	source := "tests:\n  - name: the case\n    expect:\n      others: sometimes\n"

	_, err := flowtest.LoadSource([]byte(source))
	require.Error(t, err)

	problems, refused := errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)

	d := only(t, problems)
	assert.Empty(t, d.File)
	line, column := spot(t, source, "sometimes")
	assert.Equal(t, line, d.Line)
	assert.Equal(t, column, d.Column)
	assert.Equal(t, strconv.Itoa(line)+":"+strconv.Itoa(column)+": "+d.Message, problems.Error())
}
