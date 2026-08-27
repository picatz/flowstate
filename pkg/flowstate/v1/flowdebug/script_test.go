package flowdebug_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A recorded script is what a session read, written down. The tests here are
// the two directions that matters in: what a file is admitted as
// ([flowdebug.ReadScript]), and what a session will disagree with before it is
// run ([flowdebug.CheckScript]).

// debugSteps is the inventory a checked script is checked against — the ids a
// caller reads off the workflow, the same list the prompt completes over.
var debugSteps = []string{"build", "test", "deploy"}

// TestReadScriptKeepsTheStreamAsItIs is the property the whole design rests on:
// the file is the lines a session reads, with nothing removed and nothing
// added, so a replay and `< script` cannot be two different sessions.
func TestReadScriptKeepsTheStreamAsItIs(t *testing.T) {
	t.Parallel()

	lines, err := flowdebug.ReadScript(strings.NewReader(
		"# why the build step is wrong\nbreak build\ncontinue\ninspect steps.build.ok\n"))
	require.NoError(t, err)

	assert.Equal(t, []string{
		"# why the build step is wrong",
		"break build",
		"continue",
		"inspect steps.build.ok",
	}, lines, "the lines a session would read are not the lines that came back")
}

// TestReadScriptCutsLinesTheWayASessionsScannerDoes is the same property at the
// two edges where a difference would be silent: a final newline ends the last
// line rather than starting an empty one, and a CRLF file — which is what a
// script pasted out of an issue on Windows is — loses its carriage returns.
//
// The first is not cosmetic. An extra empty line at the end would be an extra
// command, and [flowdebug.CheckScript] would then refuse a file that is
// perfectly well formed.
func TestReadScriptCutsLinesTheWayASessionsScannerDoes(t *testing.T) {
	t.Parallel()

	trailing, err := flowdebug.ReadScript(strings.NewReader("step\ncontinue\n"))
	require.NoError(t, err)
	assert.Equal(t, []string{"step", "continue"}, trailing,
		"the newline ending the last line was read as a line of its own")

	unterminated, err := flowdebug.ReadScript(strings.NewReader("step\ncontinue"))
	require.NoError(t, err)
	assert.Equal(t, []string{"step", "continue"}, unterminated,
		"a script whose last line has no newline lost its last command")

	windows, err := flowdebug.ReadScript(strings.NewReader("step\r\ncontinue\r\n"))
	require.NoError(t, err)
	assert.Equal(t, []string{"step", "continue"}, windows,
		"a carriage return survived, so every command would be spelled with one on the end")
}

// TestReadScriptBoundsBytesAtWhatASessionCanRecord asserts the bound is reached
// as well as not exceeded — a reader that refused everything would satisfy the
// second half on its own, which is the failure CLAUDE.md's "assert the bound was
// reached" rule is about.
func TestReadScriptBoundsBytesAtWhatASessionCanRecord(t *testing.T) {
	t.Parallel()

	// One line, so that the *byte* bound is the one being tested: a file made
	// of short lines would hit the count bound first and this would be a test
	// of that instead.
	atTheBound := strings.Repeat("x", flowdebug.MaxScriptBytes-1) + "\n"
	require.Len(t, atTheBound, flowdebug.MaxScriptBytes)

	lines, err := flowdebug.ReadScript(strings.NewReader(atTheBound))
	require.NoError(t, err, "a script of exactly the bound was refused, so the bound is never reached")
	assert.Len(t, lines, 1)

	_, err = flowdebug.ReadScript(strings.NewReader(atTheBound + "x"))
	require.Error(t, err, "a script one byte over the bound was accepted")
	assert.Contains(t, err.Error(), "1048576", "the refusal does not name the bound it enforced")
}

// TestReadScriptBoundsLinesSeparatelyFromBytes is the reason there are two
// bounds rather than one.
//
// The file here is half a megabyte — comfortably inside [flowdebug.MaxScriptBytes]
// — and holds one more command than a session will ever record. Bounding one
// resource does not bound another whose ratio the writer chooses (CLAUDE.md),
// and a blank line is the cheapest command there is: a megabyte of newlines is
// ten times this count.
func TestReadScriptBoundsLinesSeparatelyFromBytes(t *testing.T) {
	t.Parallel()

	atTheBound := strings.Repeat("step\n", flowdebug.MaxScriptCommands)
	require.Less(t, len(atTheBound), flowdebug.MaxScriptBytes,
		"the fixture is over the byte bound, so this would not be a test of the line bound")

	lines, err := flowdebug.ReadScript(strings.NewReader(atTheBound))
	require.NoError(t, err, "a script of exactly the bound was refused, so the bound is never reached")
	assert.Len(t, lines, flowdebug.MaxScriptCommands)

	_, err = flowdebug.ReadScript(strings.NewReader(atTheBound + "step\n"))
	require.Error(t, err, "a script one command over the bound was accepted")
	assert.Contains(t, err.Error(), "100000", "the refusal does not name the bound it enforced")
}

// TestCheckScriptNamesAMisspelledCommand: the position, what is wrong, and what
// to do instead — the standard `flowfile/validate.go` sets.
//
// A prompt answers a typo and asks again, because there is a next line coming
// from the person who typed it. A file has no next line, so the same typo is a
// defect in an artifact and is reported before the run rather than during it.
func TestCheckScriptNamesAMisspelledCommand(t *testing.T) {
	t.Parallel()

	problems, total := flowdebug.CheckScript([]string{"break build", "  contnue"}, debugSteps)

	require.Len(t, problems, 1)
	assert.Equal(t, 1, total)
	assert.Equal(t, 2, problems[0].Line)
	assert.Equal(t, 3, problems[0].Column, "the column does not point at the word that is wrong")
	assert.Contains(t, problems[0].Message, `unknown command "contnue"`)
	assert.Contains(t, problems[0].Message, `"continue"`, "no suggestion, and the nearest name is one edit away")
}

// TestCheckScriptNamesAVerbItCannotSuggestFor keeps the diagnostic useful where
// there is nothing close: the vocabulary itself, so a reader learns what a
// session understands rather than only that this was not it.
func TestCheckScriptNamesAVerbItCannotSuggestFor(t *testing.T) {
	t.Parallel()

	problems, _ := flowdebug.CheckScript([]string{"frobnicate everything"}, debugSteps)

	require.Len(t, problems, 1)
	assert.Contains(t, problems[0].Message, "a session understands")
	assert.Contains(t, problems[0].Message, "continue")
	assert.NotContains(t, problems[0].Message, "did you mean",
		"a suggestion was offered for a word nothing resembles")
}

// TestCheckScriptNamesAStepTheWorkflowDoesNotDeclare is the check that makes
// this worth running at all: a breakpoint on a step id the workflow no longer
// has is a session that never stops, and a run that finishes unattended looks
// exactly like one that was watched.
func TestCheckScriptNamesAStepTheWorkflowDoesNotDeclare(t *testing.T) {
	t.Parallel()

	problems, _ := flowdebug.CheckScript([]string{"break buld", "until shipp"}, debugSteps)

	require.Len(t, problems, 2)

	assert.Equal(t, 1, problems[0].Line)
	assert.Equal(t, 7, problems[0].Column, "the column does not point at the step id")
	assert.Contains(t, problems[0].Message, `no step named "buld"`)
	assert.Contains(t, problems[0].Message, `"build"`, "the nearest declared step was not suggested")

	assert.Equal(t, 2, problems[1].Line)
	assert.Equal(t, 7, problems[1].Column)
	assert.Contains(t, problems[1].Message, `no step named "shipp"`)
	assert.Contains(t, problems[1].Message, `"build", "deploy", "test"`,
		"nothing resembles it, so the declared steps should be listed")
}

// TestCheckScriptSaysNothingAboutStepsItWasNotTold is the negative direction,
// and the one a false diagnostic would live in.
//
// A workflow that does not parse gives its caller no inventory, and answering
// "no step named build" on the strength of an empty list would tell an author
// their script is wrong about a file nobody read. False diagnostics are worse
// than missing ones (CLAUDE.md).
func TestCheckScriptSaysNothingAboutStepsItWasNotTold(t *testing.T) {
	t.Parallel()

	for _, inventory := range [][]string{nil, {}} {
		problems, total := flowdebug.CheckScript([]string{"break anything", "until whatever"}, inventory)

		assert.Empty(t, problems, "a step id was judged against an inventory nobody supplied")
		assert.Zero(t, total)
	}
}

// TestCheckScriptRefusesABlankLine is the one place a file and a prompt read
// the same bytes differently, and it is refused rather than reinterpreted.
//
// Refusing costs a real recording nothing: a session answers an empty line by
// setting the verb to `step` and recording that word, so a script it produced
// never holds one — which
// [TestARecordedScriptIsAScriptThisWillAccept] asserts rather than assumes.
func TestCheckScriptRefusesABlankLine(t *testing.T) {
	t.Parallel()

	problems, total := flowdebug.CheckScript([]string{"break build", "", "continue"}, debugSteps)

	require.Len(t, problems, 1)
	assert.Equal(t, 1, total)
	assert.Equal(t, 2, problems[0].Line)
	assert.Equal(t, 1, problems[0].Column)
	assert.Contains(t, problems[0].Message, "blank line")
	assert.Contains(t, problems[0].Message, "`step`", "the refusal does not say what to write instead")
}

// TestCheckScriptAcceptsACommentedScript: `#` is a comment at the prompt too,
// so a reproduction may carry the sentence saying what it reproduces.
func TestCheckScriptAcceptsACommentedScript(t *testing.T) {
	t.Parallel()

	problems, total := flowdebug.CheckScript([]string{
		"# what this reproduces: deploy reads the wrong artifact",
		"   # indented, and still a comment",
		"break deploy",
		"continue",
	}, debugSteps)

	assert.Empty(t, problems)
	assert.Zero(t, total)
}

// TestCheckScriptNamesALineNoReaderCouldGetPast.
//
// The bound is the reader's own ([flowdebug.MaxCommandBytes], which sizes the
// session's scanner), so a line past it does not merely fail — it stops the
// stream where it sits, and every command after it is never read. Reported with
// a position so the answer is "line 2", not "this file".
func TestCheckScriptNamesALineNoReaderCouldGetPast(t *testing.T) {
	t.Parallel()

	long := "inspect " + strings.Repeat("x", flowdebug.MaxCommandBytes)
	problems, _ := flowdebug.CheckScript([]string{"break build", long}, debugSteps)

	require.Len(t, problems, 1)
	assert.Equal(t, 2, problems[0].Line)
	assert.Contains(t, problems[0].Message, "65536", "the refusal does not name the bound it enforced")

	// And the bound is reached rather than only not exceeded: a line of
	// exactly the length a session accepts is accepted here.
	atTheBound := "inspect " + strings.Repeat("x", flowdebug.MaxCommandBytes-len("inspect "))
	require.Len(t, atTheBound, flowdebug.MaxCommandBytes)

	problems, _ = flowdebug.CheckScript([]string{atTheBound}, debugSteps)
	assert.Empty(t, problems, "a command of exactly the length the reader admits was refused")
}

// TestCheckScriptNamesTheThreeVerbsMissingTheirArgument, in the same words the
// prompt uses — the strings are constants shared with [flowdebug.Session]'s own
// dispatch, so a reader who meets one at a prompt and one in a file is given
// the same advice.
func TestCheckScriptNamesTheThreeVerbsMissingTheirArgument(t *testing.T) {
	t.Parallel()

	problems, total := flowdebug.CheckScript([]string{"until", "break", "inspect", "break build if"}, debugSteps)

	require.Len(t, problems, 4)
	assert.Equal(t, 4, total)
	assert.Contains(t, problems[0].Message, "until <step-id>")
	assert.Contains(t, problems[1].Message, "break <step-id>")
	assert.Contains(t, problems[2].Message, "inspect steps.")
	assert.Contains(t, problems[3].Message, "if", "a condition with no expression was accepted")
}

// TestCheckScriptRefusesAMalformedBreakCondition: `break body iff n == 7` arms
// an unconditional breakpoint at the prompt with a warning, which in a file
// nobody is watching would be a stop on every iteration of the loop somebody
// wrote a condition to escape (Codex, #1116).
func TestCheckScriptRefusesAMalformedBreakCondition(t *testing.T) {
	t.Parallel()

	problems, _ := flowdebug.CheckScript([]string{"break build iff 1 == 1"}, debugSteps)

	require.Len(t, problems, 1)
	assert.Contains(t, problems[0].Message, "break:")
	assert.Contains(t, problems[0].Message, "`if`")
}

// TestCheckScriptBoundsWhatItReports, and says how many there were.
//
// The resource is the refusal: a file at the line bound whose every line is
// wrong is a hundred thousand diagnostics, sized by whoever wrote the file. A
// bounded report that did not carry the total would read exactly like a short
// one, which is the failure this whole file exists to prevent one layer down.
func TestCheckScriptBoundsWhatItReports(t *testing.T) {
	t.Parallel()

	const wrong = flowdebug.MaxScriptProblems * 3

	lines := make([]string, 0, wrong)
	for range wrong {
		lines = append(lines, "frobnicate")
	}

	problems, total := flowdebug.CheckScript(lines, debugSteps)

	assert.Len(t, problems, flowdebug.MaxScriptProblems, "the report is not bounded at the bound")
	assert.Equal(t, wrong, total, "the count of what was found does not survive the bound")
}

// TestARecordedScriptIsAScriptThisWillAccept is the round trip, and the claim
// the verb rests on: what a session records is a file the reader admits and the
// checker passes.
//
// Driven through a real run rather than by building the list in Go, because the
// recording is a property of what [flowdebug.Session] does with commands — the
// empty line recorded as the word `step` is exactly the case a hand-built list
// would have got wrong.
func TestARecordedScriptIsAScriptThisWillAccept(t *testing.T) {
	t.Parallel()

	session, out, ran := recordSession(t,
		"# a comment, which is not a command\n"+
			"break deploy\n"+
			"\n"+
			"inspect 6 * 7\n"+
			"continue\n")

	require.Equal(t, []string{"build", "test", "deploy"}, ran)
	assert.Contains(t, out, "42")
	assert.NotContains(t, out, "unknown command", "the comment was answered as a mistyped command")

	recorded := session.Script()
	require.NotEmpty(t, recorded, "the session recorded nothing, so this proves nothing")
	assert.Equal(t, []string{"break deploy", "step", "inspect 6 * 7", "continue"}, recorded,
		"the recording is not the commands the session accepted")

	lines, err := flowdebug.ReadScript(strings.NewReader(strings.Join(recorded, "\n") + "\n"))
	require.NoError(t, err, "a session's own recording was refused by the reader")
	assert.Equal(t, recorded, lines)

	problems, total := flowdebug.CheckScript(lines, []string{"build", "test", "deploy"})
	assert.Empty(t, problems, "a session's own recording did not pass the checker")
	assert.Zero(t, total)
}

// TestReplayingARecordedScriptReachesTheSameDecisions is the other half of the
// round trip: the recording, fed back, runs the same steps and stops in the
// same places as the session that produced it.
func TestReplayingARecordedScriptReachesTheSameDecisions(t *testing.T) {
	t.Parallel()

	first, firstOut, firstRan := recordSession(t, "break deploy\ncontinue\ninspect steps.build.ok\ncontinue\n")
	require.NotEmpty(t, firstRan)

	_, replayOut, replayRan := recordSession(t, strings.Join(first.Script(), "\n")+"\n")

	assert.Equal(t, firstRan, replayRan, "the replay ran a different set of steps")
	assert.Equal(t, firstOut, replayOut, "the replay's account of the run differs from the session's")
	assert.Contains(t, replayOut, "break at deploy")
}

// TestACommentIsACommentAtTheAutopsyToo: the autopsy is a second prompt with a
// vocabulary of its own, and a comment has to mean the same nothing there.
//
// The cost of getting it wrong is not symmetrical with the breakpoint prompt's,
// which is why this is asserted rather than assumed: an empty line and every
// movement verb *leave* the autopsy, and an unknown command is answered with a
// warning — so a script carrying the sentence that says what it reproduces
// would have narrated a complaint per line, at the one prompt where the
// bindings a failed case was judged under still exist.
func TestACommentIsACommentAtTheAutopsyToo(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("# why this case failed\ninspect 6 * 7\nquit\n"),
		Out: &out,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	session.Autopsy(t.Context(), v1.NewScope(v1.CurrentProfile, nil), nil, []string{"a failure"})

	assert.Contains(t, out.String(), "42", "the question after the comment was never reached")
	assert.NotContains(t, out.String(), "unknown command",
		"the comment was answered as a mistyped command")
	assert.Equal(t, []string{"inspect 6 * 7", "quit"}, session.Script(),
		"the comment was recorded as something the session did")
}

// recordSession drives one run under a scripted session and hands the session
// back, so a test can ask what it recorded.
//
// [runDebugged]'s shape, with the session returned rather than discarded: the
// recording is the subject here, and nothing else in this package's tests needs
// it.
func recordSession(t *testing.T, script string) (*flowdebug.Session, string, []string) {
	t.Helper()

	var console strings.Builder

	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader(script),
		Out: &console,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ran := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, ran))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, err = v1.Run(ctx, &v1.Workflow{Name: "debugged", Steps: []*v1.Node{
		markStep("build"), markStep("test"), markStep("deploy"),
	}})
	require.NoError(t, err)

	return session, console.String(), ran.ids
}
