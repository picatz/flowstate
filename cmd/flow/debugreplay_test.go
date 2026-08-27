package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow debug replay`: the play-back half of the debugger's record-and-replay
// (#1111, item 3).
//
// The wiring under test is this command's own — the pre-flight over the script,
// the stream handed to the shared local-run path, and the account of what the
// script did afterward. What the session does with a command is
// `pkg/flowstate/v1/flowdebug`'s, and is not re-asserted here.

// writeDebugScript writes a script beside the test and hands back its path.
func writeDebugScript(t *testing.T, text string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(path, []byte(text), 0o600))

	return path
}

// TestDebugReplayIsTheSameSessionAsTheScriptOnStdin is the claim the verb makes
// and the reason the file is the command stream rather than a format wrapped
// around one.
//
// Redirecting a script into `flow run local --debug` already worked; this verb
// exists to name it, bound it and check it, and the moment it *transformed* the
// file it would be a second answer to a question the session already answers.
// So the two are compared whole rather than by sampling a line: same run, same
// stops, same account, byte for byte.
func TestDebugReplayIsTheSameSessionAsTheScriptOnStdin(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	const script = "break second\ncontinue\ninspect steps.first\ncontinue\n"

	redirected := runFlowStdin(t, script, "run", "local", path, "--debug")
	require.NoError(t, redirected.Err)

	replayed := runFlow(t, "debug", "replay", writeDebugScript(t, script), path)
	require.NoError(t, replayed.Err)

	require.NotEmpty(t, redirected.Stderr, "the recorded session narrated nothing, so this compares nothing")
	assert.Equal(t, redirected.Stderr, replayed.Stderr,
		"the replay reached different decisions than the same script redirected into `flow run local --debug`")
	assert.Equal(t, redirected.Stdout, replayed.Stdout,
		"the replay answered with a different document than the run it reproduces")
}

// TestDebugReplayRunsTheScriptShippedBesideAnExample is the capability reaching
// the corpus CI runs: a real workflow from `examples/`, a script beside it, and
// the answer the recorded session was asked for.
//
// The subject is `loop-accumulate`, whose own `final_sum` description ends by
// admitting the thing the script goes and looks at — the fold adds the counter
// *before* `update:` advances it. A conditional breakpoint on the third pass
// and an inspection of the carried value is what turns that sentence into
// something a reader can watch happen.
func TestDebugReplayRunsTheScriptShippedBesideAnExample(t *testing.T) {
	res := runFlow(t, "debug", "replay",
		filepath.Join("..", "..", "examples", "loop-accumulate", "debug.script"),
		filepath.Join("..", "..", "examples", "loop-accumulate", "workflow.yaml"))
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stderr, "break at term", "the conditional breakpoint never fired")
	assert.Contains(t, res.Stderr, `{"n":3,"sum":3}`,
		"the inspection did not answer with the carried value at the third pass")
	assert.Contains(t, res.Stderr, "true", "the arithmetic the example's own output describes did not hold")

	assert.NotContains(t, res.Stderr, "unread", "the script and the run disagreed about how far to go")
	assert.NotContains(t, res.Stderr, "ran out", "the script left the run to finish unattended")

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &document),
		"stdout is not the run document: %q", res.Stdout)
	assert.Contains(t, document, "runOutputs", "the answer lost the run's outputs")
}

// TestDebugReplayRefusesABreakOnAStepTheWorkflowDoesNotHave.
//
// The silent version of this is the one worth refusing: a breakpoint on an id
// nothing declares never fires, so the session resumes at the first boundary
// and the run finishes unattended — which reads exactly like a run somebody
// watched.
func TestDebugReplayRefusesABreakOnAStepTheWorkflowDoesNotHave(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	script := writeDebugScript(t, "# the reproduction\nbreak frist\ncontinue\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.Error(t, res.Err)

	assert.Contains(t, res.Err.Error(), script+":2:7:",
		"the refusal does not name the file, line and column a terminal makes clickable")
	assert.Contains(t, res.Err.Error(), `no step named "frist"`)
	assert.Contains(t, res.Err.Error(), `"first"`, "the nearest declared step was not suggested")

	assert.Empty(t, res.Stdout, "the workflow ran anyway, before the script was refused")
	assert.Equal(t, exitCodeFailure, res.ExitCode,
		"a refusal of the file named on the command line is a finding, not an invocation mistake")
}

// TestDebugReplayRefusesAMisspelledCommandBeforeRunningAnything.
//
// A prompt answers a typo and asks again, deliberately — ending someone's run
// over one is the worst reading of an ambiguous line. A file has no next line,
// so the same typo is a defect in an artifact, and the run does not start.
func TestDebugReplayRefusesAMisspelledCommandBeforeRunningAnything(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	script := writeDebugScript(t, "contnue\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.Error(t, res.Err)

	assert.Contains(t, res.Err.Error(), script+":1:1:")
	assert.Contains(t, res.Err.Error(), `unknown command "contnue"`)
	assert.Contains(t, res.Err.Error(), `"continue"`)

	assert.Empty(t, res.Stdout, "the workflow ran anyway")
	assert.NotContains(t, res.Stderr, "break at", "the run reached a step boundary before the script was checked")
}

// TestDebugReplayRefusesABlankLine: the one place a file and a prompt read the
// same bytes differently, refused rather than reinterpreted.
func TestDebugReplayRefusesABlankLine(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	script := writeDebugScript(t, "break second\n\ncontinue\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.Error(t, res.Err)

	assert.Contains(t, res.Err.Error(), script+":2:1:")
	assert.Contains(t, res.Err.Error(), "blank line")
	assert.Empty(t, res.Stdout, "the workflow ran anyway")
}

// TestDebugReplayRefusesASensitiveWorkflowWithoutReveal is design call 2, and
// the reason it is not a decision of this command's own: a replay narrates
// exactly the values `flow run local --debug` narrates, so it takes exactly the
// refusal that verb takes, from [decideCarriedValues] rather than from a second
// rule beside it (Codex, #1109).
func TestDebugReplayRefusesASensitiveWorkflowWithoutReveal(t *testing.T) {
	path := writeSensitiveDebugFixture(t)
	script := writeDebugScript(t, "continue\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.Error(t, res.Err)

	assert.Contains(t, res.Err.Error(), "--reveal-sensitive")
	assert.Contains(t, res.Err.Error(), "secretive")
	assert.NotContains(t, res.Err.Error(), "drop --debug",
		"the remedy names a flag this verb does not have")
	assert.NotContains(t, res.Stderr, "sk-live-", "the value the refusal exists to withhold was narrated")
	assert.Empty(t, res.Stdout)
}

// TestDebugReplayNarratesASensitiveWorkflowWhenAskedTo is the other half: the
// escape hatch is the same one, typed the same way, and it works — a refusal
// with no way past it would be a capability nobody can use.
func TestDebugReplayNarratesASensitiveWorkflowWhenAskedTo(t *testing.T) {
	path := writeSensitiveDebugFixture(t)
	script := writeDebugScript(t, "continue\n")

	res := runFlow(t, "debug", "replay", script, path, "--reveal-sensitive")
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stderr, "debugging secretive")
	assert.Contains(t, res.Stderr, "reveal", "the invocation did not record that the hatch was used")
}

// writeSensitiveDebugFixture is a workflow whose declarations withhold its
// transcript, so the reveal question has something to be about.
func writeSensitiveDebugFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: secretive
steps:
  - id: mint
    value: ${"sk-live-0123456789"}
outputs:
  token:
    value: ${steps.mint.value}
    sensitive: true
`), 0o600))

	return path
}

// TestDebugReplaySaysWhenTheScriptRanOutWithTheRunStillHeld.
//
// The session already says the run continued unattended, which is a fact about
// the run. This says what is wrong with the *script* and what to write instead,
// which is a different sentence for a different reader — the one who has the
// file open.
func TestDebugReplaySaysWhenTheScriptRanOutWithTheRunStillHeld(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	script := writeDebugScript(t, "step\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.NoError(t, res.Err, "a short script is not a failed run")

	assert.Contains(t, res.Stderr, "ran out while the run was still held")
	assert.Contains(t, res.Stderr, "`continue`", "the account does not say what to write instead")
	assert.Contains(t, res.Stderr, "`quit`")
}

// TestDebugReplaySaysWhenTheRunEndedWithCommandsUnread is the failure with no
// other symptom at all: the session simply never asks for the rest of the file,
// and a reproduction that stopped short looks exactly like one that finished.
func TestDebugReplaySaysWhenTheRunEndedWithCommandsUnread(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	script := writeDebugScript(t, "step\nstep\ninspect 'ans' + 'wered'\ncontinue\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stderr, "2 of "+script+"'s commands unread")
	assert.Contains(t, res.Stderr, "from line 3", "the account does not name where the unread commands start")
	assert.Contains(t, res.Stderr, `"inspect 'ans' + 'wered'"`)
	// The needle is the *answer*, which the quoted command does not contain —
	// and which cannot arrive from anywhere else in the account. `6 * 7` was
	// the obvious spelling and the wrong one: `42` collides with the digits
	// `t.TempDir()` puts in the path this very message prints, which failed
	// about one run in three.
	assert.NotContains(t, res.Stderr, "answered", "the unread inspection was answered")
}

// TestDebugReplaySaysWhenTheRunOfferedNoBoundaryAtAll is the loudest case, and
// it was the silent one.
//
// A workflow whose top-level steps are all skipped by their `if:` completes
// successfully and offers the debugger nothing: the boundary sits *below* the
// condition, so `eval.go` reports a skipped step through StepSkipped without
// asking. Not one line of the script is read, the run succeeds, and the answer
// is real — so there is no other symptom anywhere.
//
// The report was suppressed by a check that read only how many commands had
// been delivered, which conflated this with a run that failed before reaching a
// boundary (Codex, #1145). Those are opposite situations: one is a script with
// nothing to say about a failure, the other a script that went silently unused.
func TestDebugReplaySaysWhenTheRunOfferedNoBoundaryAtAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte("edition: v2026.3\nname: all-skipped\nsteps:\n"+
		"  - id: never\n    if: ${false}\n    log:\n      message: unreachable\n"), 0o600))

	script := writeDebugScript(t, "step\ninspect 'ans' + 'wered'\ncontinue\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.NoError(t, res.Err, "a run that skipped every step still succeeded")

	assert.Contains(t, res.Stderr, "without stopping at a single step boundary",
		"the whole script went unused and nothing said so")
	assert.Contains(t, res.Stderr, "skipped by its `if:`",
		"the account does not name why no boundary was offered")
	// See the sibling test for why the needle is a word rather than `42`.
	assert.NotContains(t, res.Stderr, "answered",
		"an inspection was answered by a run that never paused")
}

// TestDebugReplaySaysNothingWhenTheRunFailedBeforeAnyBoundary is the other side
// of that condition, and the reason it cannot simply be deleted.
//
// A run that fails before reaching a boundary has delivered nothing either, and
// "none of your script ran" there is a diagnostic about the script for a failure
// that has nothing to do with it. The run's own error is the answer; a false
// diagnostic beside it is worse than a missing one.
func TestDebugReplaySaysNothingWhenTheRunFailedBeforeAnyBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte("edition: v2026.3\nname: fails-first\ninputs:\n"+
		"  required_thing:\n    type: string\n    required: true\nsteps:\n"+
		"  - id: never\n    log:\n      message: unreachable\n"), 0o600))

	script := writeDebugScript(t, "step\ncontinue\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.Error(t, res.Err, "a run missing a required input was expected to fail")

	assert.NotContains(t, res.Stderr, "without stopping at a single step boundary",
		"a failed run was reported as a script that went unused")
	assert.NotContains(t, res.Stderr, "commands unread",
		"a failed run was reported as a script that went unread")
}

// TestDebugReplaySaysNothingAboutTrailingComments: a comment is not a command,
// so a script that ends in the sentence explaining itself has nothing left
// undone. The count above is of commands, and a false diagnostic is worse than
// a missing one.
func TestDebugReplaySaysNothingAboutTrailingComments(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	script := writeDebugScript(t, "continue\n# and that is the whole reproduction\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.NoError(t, res.Err)

	assert.NotContains(t, res.Stderr, "unread", "a trailing comment was counted as an unread command")
	assert.NotContains(t, res.Stderr, "ran out")
}

// TestDebugReplaySaysNothingAboutAScriptNothingRead keeps the account from
// blaming the script for a failure that has nothing to do with it: a run that
// never reached a boundary read no commands, and "every command is unread" is
// true and useless.
func TestDebugReplaySaysNothingAboutAScriptNothingRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: broken
steps:
  - id: nope
    task_that_does_not_exist: {}
`), 0o600))

	script := writeDebugScript(t, "step\nstep\ncontinue\n")

	res := runFlow(t, "debug", "replay", script, path)
	require.Error(t, res.Err, "a workflow with an unknown task was accepted")

	assert.NotContains(t, res.Stderr, "unread",
		"the script was blamed for a run that never reached a step boundary")
}

// TestDebugReplayRefusesAnEmptyScript. An empty one would run the workflow
// unattended, which is what `flow run local` already does — and a verb whose
// argument can be empty is a verb that quietly becomes another one.
func TestDebugReplayRefusesAnEmptyScript(t *testing.T) {
	path := writeRunLocalDebugFixture(t)

	for name, text := range map[string]string{"nothing at all": "", "only a comment": "# nothing here\n"} {
		t.Run(name, func(t *testing.T) {
			script := writeDebugScript(t, text)

			res := runFlow(t, "debug", "replay", script, path)

			if text == "" {
				require.Error(t, res.Err)
				assert.Contains(t, res.Err.Error(), "no commands")

				return
			}

			// A script of comments alone reaches the run, holds it at the
			// first boundary, and runs out — which is the account above
			// rather than a refusal, because the file does have lines in it.
			require.NoError(t, res.Err)
			assert.Contains(t, res.Stderr, "ran out while the run was still held")
		})
	}
}

// TestDebugReplayRefusesAScriptThatIsNotThere, in the words the operating
// system used, rather than a run that starts and then cannot be driven.
func TestDebugReplayRefusesAScriptThatIsNotThere(t *testing.T) {
	path := writeRunLocalDebugFixture(t)

	res := runFlow(t, "debug", "replay", filepath.Join(t.TempDir(), "absent.script"), path)
	require.Error(t, res.Err)

	assert.Contains(t, res.Err.Error(), "absent.script")
	assert.Empty(t, res.Stdout)
}

// TestDebugReplayKeepsStdoutTheDocumentUnderJSON: the console shares stderr
// with the run's own account, so a machine format and a debugging session
// compose — the same property `flow run local --debug` has, inherited rather
// than re-implemented.
func TestDebugReplayKeepsStdoutTheDocumentUnderJSON(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	script := writeDebugScript(t, "continue\n")

	res := runFlow(t, "debug", "replay", script, path, "-o", "json")
	require.NoError(t, res.Err)

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &document),
		"stdout under -o json is not a parseable document: %q", res.Stdout)
	assert.Equal(t, "STATUS_COMPLETED", document["status"])

	assert.NotContains(t, res.Stdout, "debug>", "the console leaked onto the answer's stream")
}

// TestDebugReplayTakesEveryFlagALocalRunTakes is the property that keeps this
// verb a local run rather than a narrower one.
//
// A replay reproduces a recorded session, and a run started with different
// inputs, a different egress policy or a different plugin directory is a
// different run. So the two flag sets are compared rather than sampled: a flag
// added to `flow run local` and not to this is a rehearsal this cannot
// reproduce, and the symptom is an unknown-flag error somebody meets while
// trying to reproduce something.
func TestDebugReplayTakesEveryFlagALocalRunTakes(t *testing.T) {
	t.Parallel()

	local := localFlagNames(flowCommand(t, "run", "local"))
	replay := localFlagNames(flowCommand(t, "debug", "replay"))

	require.NotEmpty(t, local, "`flow run local` declares no flags, so this compares nothing")
	assert.Equal(t, local, replay,
		"`flow debug replay` and `flow run local` no longer take the same flags")
}

// localFlagNames is the sorted set of flags one command declares itself.
//
// Sorted so the comparison is of the set rather than of the order two
// registrations happen to run in, and local rather than inherited because a
// persistent flag belongs to the root and is on both by construction.
func localFlagNames(cmd *cobra.Command) []string {
	var names []string
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		names = append(names, f.Name)
	})
	slices.Sort(names)

	return names
}

// TestDebugReplayHoldsItsDebuggerOn: `--debug` is how this command tells the
// shared local-run path what kind of run this is, not a choice, so typing it
// off changes nothing.
func TestDebugReplayHoldsItsDebuggerOn(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	script := writeDebugScript(t, "continue\n")

	res := runFlow(t, "debug", "replay", script, path, "--debug=false")
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stderr, "debugging debugged",
		"--debug=false turned a replay into a plain run, so the script drove nothing")
}

// TestDebugReplayIsFoundWhereSomebodyWouldLookForIt: registered, grouped, and
// reachable by the words a person types — the same claim `commands_test.go`
// makes of the tree as a whole, asked here because a verb nothing can reach is
// the failure this whole PR is about.
func TestDebugReplayIsFoundWhereSomebodyWouldLookForIt(t *testing.T) {
	t.Parallel()

	replay := flowCommand(t, "debug", "replay")

	assert.Equal(t, "replay", replay.Name())
	assert.True(t, strings.Contains(replay.Long, "script"), "the help does not say what a script is")

	debug := flowCommand(t, "debug")
	assert.Equal(t, "development", debug.GroupID)
}
