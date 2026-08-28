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
// `others:` under test is written once on the table entry and reaches each row
// by merge, so the row's own lines say nothing about it. Pointing at the row
// would underline correct text and send an author to fix it.
//
// A table entry is the fixture because it is the inheritance this loader still
// judges per case. The `defaults:` block's own values — a sender, a check
// claim — are judged where they are written now, which is the stronger answer
// and the one #1185 landed; an entry's are not, because the row is the only
// thing that survives expansion. That gap is filed rather than hidden: it is
// the open thread on this PR.
func TestAnInheritedValueIsRefusedWithoutBorrowingACasesPosition(t *testing.T) {
	t.Parallel()

	source := `
tests:
  - name: tabled
    workflow: ./workflow.yaml
    expect:
      others: sometimes
    cases:
      - name: the row
        expect:
          ran: [a]
`
	problems, _ := refuse(t, source)
	d := only(t, problems)

	assert.Contains(t, d.Message, `test "tabled/the row" expect.others:`,
		"the by-name prose is what still identifies an inherited value")
	assert.Zero(t, d.Line,
		"a value this document did not write at the case must not be positioned at the case, "+
			"nor at any node enclosing it")
	assert.Zero(t, d.Column)
	assert.Equal(t, "tabled/the row", d.Test, "the case is still named, structurally")

	// The positive direction, because an absence assertion is worth nothing
	// until the thing could have been present: the identical mistake written on
	// the row itself is positioned exactly.
	written := strings.Replace(source, "    expect:\n      others: sometimes\n", "", 1)
	written = strings.Replace(written, "          ran: [a]", "          others: sometimes", 1)

	problems, _ = refuse(t, written)
	// The value, which is what the refusal is about — the key is not what an
	// author has to change.
	line, column := spot(t, written, "sometimes")
	positioned := only(t, problems)
	assert.Equal(t, line, positioned.Line, "the same mistake, written in the case, must be positioned")
	assert.Equal(t, column, positioned.Column)
}

// TestADefaultsSenderIsJudgedWhereItIsWritten (Codex, #1185): one identity in
// `defaults:` is installed on every signal that omits its own, so judging it
// only on those signals reported one mistake once per inheriting signal — and
// against `tests[i].signals[j].sender`, a path no document holds, which cost a
// sibling-file default its own name.
//
// It is judged at the block instead, which answers both halves at once: once,
// and named after whichever file wrote it.
func TestADefaultsSenderIsJudgedWhereItIsWritten(t *testing.T) {
	t.Parallel()

	// Two signals inherit it, so a per-signal check would report twice.
	const suite = `
defaults:
  workflow: ./workflow.yaml
tests:
  - name: the case
    signals:
      - name: approve
      - name: reject
`
	dir := t.TempDir()
	sibling := filepath.Join(dir, "testdefaults.yaml")
	writeFile(t, sibling, `
defaults:
  sender:
    subject: approver@example.com
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, suite)

	_, err := flowtest.Load(path)
	require.Error(t, err)
	problems, refused := errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)

	d := only(t, problems)
	assert.Contains(t, d.Message, "defaults.sender: names a subject or an issuer without the other")
	assert.Equal(t, sibling, d.File, "the identity is written in the directory's file, not the suite")
	assert.Zero(t, d.Line, "a path into another document is not a line in this one")

	// Written in the suite instead: same single refusal, named and positioned
	// here.
	own := strings.Replace(suite, "defaults:\n  workflow: ./workflow.yaml\n",
		"defaults:\n  workflow: ./workflow.yaml\n  sender:\n    subject: approver@example.com\n", 1)
	second := t.TempDir()
	path = filepath.Join(second, "workflow.test.yaml")
	writeFile(t, path, own)

	_, err = flowtest.Load(path)
	require.Error(t, err)
	problems, refused = errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)

	d = only(t, problems)
	line, column := spot(t, own, "subject: approver@example.com")
	assert.Equal(t, path, d.File)
	assert.Equal(t, line, d.Line, "a default this suite wrote must be positioned in it")
	assert.Equal(t, column, d.Column)

	// And the positive control the skip must not swallow: a signal that writes
	// its OWN malformed sender is still judged, at its own position.
	mine := strings.Replace(own, "      - name: reject",
		"      - name: reject\n        sender:\n          issuer: https://issuer.example.com", 1)
	third := t.TempDir()
	path = filepath.Join(third, "workflow.test.yaml")
	writeFile(t, path, mine)

	_, err = flowtest.Load(path)
	require.Error(t, err)
	problems, refused = errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)

	require.Equal(t, 2, problems.Total, "expected the block's own and the signal's own: %v", problems)
	var signalProblem flowtest.Diagnostic
	for _, problem := range problems.Problems {
		if strings.Contains(problem.Message, `signal 2 ("reject")`) {
			signalProblem = problem
		}
	}
	require.NotZero(t, signalProblem.Line, "the signal's own sender was swallowed by the skip: %v", problems)
	line, column = spot(t, mine, "issuer: https://issuer.example.com")
	assert.Equal(t, line, signalProblem.Line)
	assert.Equal(t, column, signalProblem.Column)
	assert.Equal(t, "the case", signalProblem.Test)
}

// TestADefaultFromTheSiblingFileIsNamedAfterThatFile is the same rule one level
// further out, and the level #1109 was about: the value is written in the
// directory's `testdefaults.yaml`, so nothing in this buffer describes it — not
// a line, and (Codex, #1179) not the file name either.
//
// The suite here states a `defaults.inputs` of its own, so there is an
// enclosing node to borrow and a plausible wrong file to name. A position
// fallback would underline the suite's own `version:` to report the sibling's
// `region:`, and a file stamped by the door alone would send an editor to a
// document that does not contain `region:` at all.
func TestADefaultFromTheSiblingFileIsNamedAfterThatFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sibling := filepath.Join(dir, "testdefaults.yaml")
	writeFile(t, sibling, `
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
	assert.Equal(t, sibling, d.File,
		"the problem names the suite, which does not contain the value being refused")
	assert.Contains(t, problems.Error(), sibling+": ",
		"the rendered refusal must name the file a reader has to open")

	// The other direction in the same load, which is what makes the
	// attribution a claim rather than a coincidence: the suite's own default,
	// at the same depth under the same block, keeps the suite's name and gains
	// a position. One refusal, two files, each named where its text is.
	own := strings.Replace(source, `    version: "1"`, "    version: ${inputs.version}", 1)
	writeFile(t, path, own)

	_, err = flowtest.Load(path)
	require.Error(t, err)
	problems, refused = errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)
	require.Len(t, problems.Problems, 2, "expected one problem per file: %v", problems)

	byFile := map[string]flowtest.Diagnostic{}
	for _, problem := range problems.Problems {
		byFile[problem.File] = problem
	}
	require.Contains(t, byFile, sibling)
	require.Contains(t, byFile, path)

	assert.Contains(t, byFile[sibling].Message, "defaults.inputs.region")
	assert.Zero(t, byFile[sibling].Line)

	line, column := spot(t, own, "${inputs.version}")
	assert.Contains(t, byFile[path].Message, "defaults.inputs.version")
	assert.Equal(t, line, byFile[path].Line, "the suite's own default must still be positioned in the suite")
	assert.Equal(t, column, byFile[path].Column)
}

// TestAnInheritedCheckClaimIsNamedAfterTheFileThatWroteIt is the shape the
// attribution above is hardest for, because a merged `check:` list is the one
// place where the directory's entries are *prepended*: index 0 of what the
// loader validates is the sibling's claim, while index 0 of the suite's own
// list is a different claim entirely. The claim is addressed by the index it
// has in the file that wrote it.
func TestAnInheritedCheckClaimIsNamedAfterTheFileThatWroteIt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sibling := filepath.Join(dir, "testdefaults.yaml")
	writeFile(t, sibling, `
defaults:
  check:
    - "steps.a.value ==="
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
  check:
    - "1 == 1"
tests:
  - name: the case
    expect:
      ran: [a]
`)

	_, err := flowtest.Load(path)
	require.Error(t, err)
	problems, refused := errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused)

	d := only(t, problems)
	assert.Contains(t, d.Message, "defaults.check[0]")
	assert.Equal(t, sibling, d.File,
		"a claim the directory file wrote was reported against the suite that inherited it")
	assert.Zero(t, d.Line, "an index into another document's list is not a line in this one")
}

// TestAWhollyInheritedCollectionIsNamedAfterTheFileThatHoldsIt (Codex, #1185):
// a bound on how many entries a collection may hold reports at the
// *collection's* path, which is an ancestor of every leaf the fold recorded and
// therefore matches none of them — so a suite that states no `vars:` of its own
// was told off for two hundred and one vars it does not contain.
//
// A collection the suite states no part of is recorded as itself, which is the
// rule already applied to a `defaults:` block the suite states nothing of.
func TestAWhollyInheritedCollectionIsNamedAfterTheFileThatHoldsIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		sibling  func() string
		suite    string
		says     string
		takeOver string // what the suite writes when it shares the collection
	}{
		{
			name: "vars",
			sibling: func() string {
				var b strings.Builder
				b.WriteString("vars:\n")
				for i := range flowtest.MaxVarsPerFile + 1 {
					fmt.Fprintf(&b, "  v%d: x\n", i)
				}

				return b.String()
			},
			suite: "tests:\n  - name: the case\n    workflow: ./workflow.yaml\n    expect:\n      ran: [a]\n",
			says:  "declares 201 vars",
			takeOver: "vars:\n  mine: x\n" +
				"tests:\n  - name: the case\n    workflow: ./workflow.yaml\n    expect:\n      ran: [a]\n",
		},
		{
			name: "defaults.stubs",
			sibling: func() string {
				var b strings.Builder
				b.WriteString("defaults:\n  stubs:\n")
				for i := range flowtest.MaxDefaultStubs + 1 {
					fmt.Fprintf(&b, "    - task: t%d\n      returns: {}\n", i)
				}

				return b.String()
			},
			suite:    "defaults:\n  workflow: ./workflow.yaml\ntests:\n  - name: the case\n    expect:\n      ran: [a]\n",
			says:     "defaults declares 101 stubs",
			takeOver: "defaults:\n  workflow: ./workflow.yaml\n  stubs:\n    - task: mine\n      returns: {}\ntests:\n  - name: the case\n    expect:\n      ran: [a]\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			sibling := filepath.Join(dir, "testdefaults.yaml")
			writeFile(t, sibling, tc.sibling())
			path := filepath.Join(dir, "workflow.test.yaml")
			writeFile(t, path, tc.suite)

			_, err := flowtest.Load(path)
			require.Error(t, err)
			problems, refused := errors.AsType[*flowtest.Diagnostics](err)
			require.True(t, refused)

			d := only(t, problems)
			assert.Contains(t, d.Message, tc.says)
			assert.Equal(t, sibling, d.File,
				"a collection wholly inherited was reported against the file that inherited it")

			// The other direction: once the suite writes into the collection
			// too, its size is a joint property and the suite is the document
			// that can stop inheriting, so the count stays its own.
			writeFile(t, path, tc.takeOver)

			_, err = flowtest.Load(path)
			require.Error(t, err)
			problems, refused = errors.AsType[*flowtest.Diagnostics](err)
			require.True(t, refused)

			shared := only(t, problems)
			assert.Contains(t, shared.Message, "more than the limit")
			assert.Equal(t, path, shared.File,
				"a collection both files write into is the suite's to answer for")
		})
	}
}

// TestTwoDocumentsDoNotShareOneIndexNamespace (Codex, #1185) is the collision
// the round before this one shipped: the directory's `check:` claims are
// *prepended*, so `defaults.check[0]` named the sibling's first claim and the
// suite's first claim on the same string. A set keyed on that string could only
// answer one way, and it answered wrongly for the suite — the worse direction,
// because a correct position was replaced by a wrong file.
//
// Both claims are malformed in one load, because the collision is precisely
// their interaction.
func TestTwoDocumentsDoNotShareOneIndexNamespace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sibling := filepath.Join(dir, "testdefaults.yaml")
	writeFile(t, sibling, `
defaults:
  check:
    - "steps.a.value ==="
`)
	suite := `
defaults:
  workflow: ./workflow.yaml
  check:
    - "steps.b.value !=="
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
	require.Equal(t, 2, problems.Total, "expected one problem per claim: %v", problems)

	byFile := map[string]flowtest.Diagnostic{}
	for _, problem := range problems.Problems {
		byFile[problem.File] = problem
	}
	require.Contains(t, byFile, sibling, "the directory's claim lost its file: %v", problems)
	require.Contains(t, byFile, path, "the suite's own claim was attributed elsewhere: %v", problems)

	// The directory's: named after it, and positioned in neither, since an
	// index into another document is not a line in this one.
	assert.Zero(t, byFile[sibling].Line)
	assert.Contains(t, byFile[sibling].Message, "steps.a.value")

	// The suite's own: its exact line and column, which the shared namespace
	// took away.
	line, column := spot(t, suite, `"steps.b.value !=="`)
	assert.Equal(t, line, byFile[path].Line, "the suite's own claim lost the position it has")
	assert.Equal(t, column, byFile[path].Column)
	assert.Contains(t, byFile[path].Message, "steps.b.value")
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

// TestACountBoundStopsTheWorkItBounds (Codex, #1179): collecting every problem
// is about a file whose size is legal, and a count bound is not a note — it is
// a refusal, because the work it bounds is what runs immediately after it.
//
// Each fixture below is over one bound *and* carries an unrelated mistake that
// the pass would report if it kept going, so `Total == 1` is a count of work
// performed rather than a count of what happened to be wrong. Removing any one
// of the four returns fails the subtest that names it.
func TestACountBoundStopsTheWorkItBounds(t *testing.T) {
	t.Parallel()

	// A case with no `workflow:`, which every subtest appends so that there is
	// always a second problem available to find.
	const alsoBroken = "tests:\n  - name: unrelated and also broken\n    expect:\n      ran: [a]\n"

	for _, tc := range []struct {
		name   string
		source func() string
		says   string
	}{
		{
			name: "more tests than the file may declare",
			source: func() string {
				var b strings.Builder
				b.WriteString("tests:\n")
				for range flowtest.MaxTestsPerFile + 1 {
					b.WriteString("  - name: over the limit\n    expect:\n      ran: [a]\n")
				}

				return b.String()
			},
			says: "more than the limit of 500",
		},
		{
			// The same bound one level down, and the one a count of written
			// entries cannot see: three legal entries whose rows sum past it.
			name: "more cases than the file may run once its rows are counted",
			source: func() string {
				var b strings.Builder
				b.WriteString("tests:\n")
				for entry := range 3 {
					fmt.Fprintf(&b, "  - name: table%d\n    workflow: ./workflow.yaml\n    cases:\n", entry)
					for row := range flowtest.MaxTestsPerFile / 2 {
						fmt.Fprintf(&b, "      - name: row%d\n        expect:\n          ran: [a]\n", row)
					}
				}
				b.WriteString("  - name: unrelated and also broken\n    expect:\n      ran: [a]\n")

				return b.String()
			},
			says: "once its `cases:` rows are counted",
		},
		{
			name: "more coverage entries than the stanza may hold",
			source: func() string {
				var b strings.Builder
				b.WriteString("coverage:\n  allow_unreached:\n")
				for i := range flowtest.MaxAllowUnreachedPerFile + 1 {
					fmt.Fprintf(&b, "    step%d: \"\"\n", i)
				}
				b.WriteString(alsoBroken)

				return b.String()
			},
			says: "coverage.allow_unreached declares",
		},
		{
			name: "more vars than the file may declare",
			source: func() string {
				var b strings.Builder
				b.WriteString("vars:\n")
				for i := range flowtest.MaxVarsPerFile + 1 {
					fmt.Fprintf(&b, "  1bad%d: x\n", i)
				}
				b.WriteString(alsoBroken)

				return b.String()
			},
			says: "vars, more than the limit of 200",
		},
		{
			name: "more default stubs than the block may hold",
			source: func() string {
				var b strings.Builder
				b.WriteString("defaults:\n  workflow: ./workflow.yaml\n  stubs:\n")
				for range flowtest.MaxDefaultStubs + 1 {
					b.WriteString("    - task: log\n      returns:\n        text: ${steps.a.value}\n")
				}
				b.WriteString(alsoBroken)

				return b.String()
			},
			says: "defaults declares",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			problems, _ := refuse(t, tc.source())

			require.Equal(t, 1, problems.Total,
				"the pass carried on past a count bound and did the work the bound exists to stop: %v", problems)
			assert.Contains(t, problems.Problems[0].Message, tc.says)
		})
	}
}

// TestAPerCaseCountBoundStopsThatCaseAndNotTheFile is the other half of the
// rule: the per-case counts bound work inside one case, so they skip that case
// rather than refusing the document — the loop around them is already bounded
// by [flowtest.MaxTestsPerFile].
func TestAPerCaseCountBoundStopsThatCaseAndNotTheFile(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("tests:\n  - name: too many stubs\n    workflow: ./workflow.yaml\n    stubs:\n")
	b.WriteString("      - returns: {}\n") // targetless, and never reached
	for range flowtest.MaxStubsPerTest {
		b.WriteString("      - task: log\n        returns: {}\n")
	}
	b.WriteString("  - name: a later case with its own mistake\n    expect:\n      ran: [a]\n")

	problems, _ := refuse(t, b.String())

	require.Equal(t, 2, problems.Total, "expected the count and the later case's own problem: %v", problems)
	rendered := problems.Error()
	assert.Contains(t, rendered, `test "too many stubs" declares 201 stubs`)
	assert.Contains(t, rendered, `test "a later case with its own mistake" names no workflow`,
		"a case skipped for its own count bound must not stop the cases after it")
	assert.NotContains(t, rendered, "names neither a task nor a step",
		"the per-stub checks the count bound exists to stop were run anyway")
}

// TestAStubWithNoTargetIsJudgedOnce (Codex, #1179): the target decides what a
// stub is, so a stub that names none is not judged further. Every later check
// quotes [stubTarget], which for a targetless stub reads `task ""` — a second
// diagnostic about a stub the first one already refused, spending the report's
// bound on a cascade.
func TestAStubWithNoTargetIsJudgedOnce(t *testing.T) {
	t.Parallel()

	source := `
tests:
  - name: the case
    workflow: ./workflow.yaml
    stubs:
      - returns: {}
        fails:
          message: nope
`
	problems, _ := refuse(t, source)
	d := only(t, problems)

	assert.Contains(t, d.Message, "names neither a task nor a step")
	assert.NotContains(t, problems.Error(), `task ""`,
		"a stub that identifies nothing was described by what it does not identify")

	// The positive direction: with a target, the very same `returns:` and
	// `fails:` pair is the diagnostic that gets reported.
	targeted := strings.Replace(source, "      - returns: {}", "      - task: log\n        returns: {}", 1)
	problems, _ = refuse(t, targeted)
	assert.Contains(t, only(t, problems).Message, "declares both returns and fails")
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

// TestADocumentTheDecoderCannotReadIsRefusedRatherThanFatal pins the crasher
// `FuzzLoadSource` found in CI: goccy v1.19.2 dereferences a nil ArrayNode on a
// sequence nested inside structs inside a sequence when the inner one is an
// empty tagged node, so thirty-seven bytes took the process down — which for
// `flow test` is the run and for `flow mcp` is every session that server holds.
//
// The corpus entry beside this package pins the input; this pins what the
// loader now *says* about it, which the fuzz target cannot: a refusal a reader
// can act on, in the same shape as every other refusal here.
func TestADocumentTheDecoderCannotReadIsRefusedRatherThanFatal(t *testing.T) {
	t.Parallel()

	_, err := flowtest.LoadSource([]byte("tests:\n  - expect:\n      ran: !!seq\n"))
	require.Error(t, err, "the decoder's panic was swallowed into a successful load")

	problems, refused := errors.AsType[*flowtest.Diagnostics](err)
	require.True(t, refused, "a decoder failure must arrive as this loader's own refusal: %v", err)

	d := only(t, problems)
	assert.Contains(t, d.Message, "the YAML decoder stopped on this document")
	assert.Contains(t, d.Message, "`ran: []`",
		"a refusal an author cannot act on is half a diagnostic")

	// The sibling file goes through the same contained decode, and a panic
	// there would be the same process for the same reason.
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), "defaults:\n  stubs: !!seq\n")
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, "tests:\n  - name: the case\n    workflow: ./workflow.yaml\n    expect:\n      ran: [a]\n")

	_, err = flowtest.Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "testdefaults.yaml", "the refusal must name the file that could not be read")
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
