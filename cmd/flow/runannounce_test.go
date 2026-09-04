package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/watch"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a durable run tells a person about itself, and how much of that is
// identifier.
//
// picatz/flowstate#544's complaint was not that any one of these lines was
// wrong. It was arithmetic: a 45-character workflow id twice on the start line
// and again on every line of the follow, beside a 36-character run id repeated
// just as often, to carry a signal that amounts to "it is on step two" and
// "it finished". The tests here assert the *structure* that fixes it rather
// than searching for a substring, because "contains the step name" is
// satisfied by every one of the shapes this was rejected for.
//
// Three claims, and they are separable on purpose:
//
//  1. Each identifier is said once. The workflow id belongs to the `flow watch`
//     hint, where a reader does something with it; the run id belongs to the
//     first line about a run and to a handover, where it is news.
//  2. A run is narrated by the name its author gave it, which is the same noun
//     `flow run local` already uses — so the two drivers describe themselves
//     the same way.
//  3. None of this reaches a program. `-o json` and `-o jsonl` are byte-identical
//     to what they were, and the prose does not appear on either stream.

// runIDPattern matches a bare run id, for counting how many a narrated line names.
var runIDPattern = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// ansi matches the escape sequences a styled surface writes, so a test can
// assert what a line *says* independently of how it is painted.
var ansi = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// narrate follows a scripted run to its end and returns the lines a person was
// shown, unstyled.
func narrate(t *testing.T, answers []pollAnswer, options ...watchOption) []string {
	t.Helper()

	surface, _, errOut := plainSurface()

	err := followPlainly(t.Context(), surface, renderingOf(FormatText),
		&scriptedPoller{answers: answers}, time.Millisecond,
		"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil, options...)
	require.NoError(t, err, "the scripted run ended badly")

	return splitLines(errOut.String())
}

// words is each line reduced to its words, so two renderings of one sentence
// compare equal despite the padding a styled pill carries.
func words(lines []string) [][]string {
	reduced := make([][]string, 0, len(lines))
	for _, line := range lines {
		reduced = append(reduced, strings.Fields(line))
	}

	return reduced
}

// splitLines drops the trailing empty element a final newline leaves behind.
func splitLines(text string) []string {
	return strings.Split(strings.TrimRight(ansi.ReplaceAllString(text, ""), "\n"), "\n")
}

// TestAFollowNamesTheWorkflowByTheNameItsAuthorGaveIt is claim 2, and the
// reason the option exists at all: `flow run` has parsed the file, so it knows
// the workflow is called `computed-outputs`, and that is what `flow run local`
// calls the same workload on the same stream.
func TestAFollowNamesTheWorkflowByTheNameItsAuthorGaveIt(t *testing.T) {
	lines := narrate(t, []pollAnswer{runningAt("roll_out"), finishedPoll("report", "roll_out")},
		namedRun("computed-outputs"))

	require.Len(t, lines, 2, "one line per change, and there were two changes")

	assert.Equal(t,
		"RUNNING workflow computed-outputs run 0198f1e2-0000-7000-8000-000000000000 on roll_out",
		lines[0])
	assert.Equal(t, "COMPLETED workflow computed-outputs after report, roll_out", lines[1])

	for _, line := range lines {
		assert.NotContains(t, line, "flowstate-workflow-",
			"the workflow id is on the start line and in the `flow watch` hint; "+
				"a follow that repeats it per change is what #544 was filed about")
	}
}

// TestAFollowWithNoNameStillNamesTheRun is the fallback: `flow watch <id>` has
// no file to read a name out of, so the id it was handed is what it says — the
// same word the reader typed, which is the point.
func TestAFollowWithNoNameStillNamesTheRun(t *testing.T) {
	lines := narrate(t, []pollAnswer{finishedPoll("report")})

	require.Len(t, lines, 1)
	assert.Equal(t,
		"COMPLETED workflow flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2 "+
			"run 0198f1e2-0000-7000-8000-000000000000 after report",
		lines[0],
		"a walk that was told no name has to fall back to the id rather than to nothing")
}

// TestTheRunIdIsSaidOnceAndAgainWhenItChanges is claim 1 for the run id, in the
// direction that actually matters: it is not that the id is absent, it is that
// it appears exactly where it is news.
//
// The negative half is the load-bearing one. A test asserting "the first line
// carries the run id" passes just as well against the old shape, which carried
// it on every line; only counting the appearances across the whole walk can
// tell the two apart.
func TestTheRunIdIsSaidOnceAndAgainWhenItChanges(t *testing.T) {
	// A continue-as-new handover: the workflow id is unchanged and the run id is
	// not, and every answer after the handover carries the new one — which is why
	// the last line, whose run id is the same as the line before it, says nothing
	// about it.
	handover := runningAt("deploy")
	handover.response.RunId = "0198f1e2-0000-7000-8000-000000000001"

	done := finishedPoll("checkout", "build", "deploy")
	done.response.RunId = "0198f1e2-0000-7000-8000-000000000001"

	lines := narrate(t,
		[]pollAnswer{runningAt("checkout"), runningAt("build"), handover, done},
		namedRun("release"))

	require.Len(t, lines, 4)

	said := 0
	for _, line := range lines {
		if strings.Contains(line, "run 0198f1e2-") {
			said++
		}
	}

	assert.Equal(t, 2, said,
		"the run id belongs on the first line about a run and on the line where it "+
			"changed underneath the reader (a continue-as-new handover), and nowhere else")

	assert.Contains(t, lines[0], "run 0198f1e2-0000-7000-8000-000000000000")
	assert.NotContains(t, lines[1], "run 0198f1e2-",
		"the second line restated a run id that had not moved")
	assert.Contains(t, lines[2], "run 0198f1e2-0000-7000-8000-000000000001",
		"a run id that changed is news and has to be said")
	assert.NotContains(t, lines[3], "run 0198f1e2-")
}

// TestAFailedFollowNamesTheWorkflowAndTheReason is #544's "the failure output
// deserves the same pass": the same identifier discipline, and the reason kept.
func TestAFailedFollowNamesTheWorkflowAndTheReason(t *testing.T) {
	surface, _, errOut := plainSurface()

	err := followPlainly(t.Context(), surface, renderingOf(FormatText),
		&scriptedPoller{answers: []pollAnswer{{
			response: failedResponse(v1.RunResponse_STATUS_FAILED, "connection reset by peer"),
		}}}, time.Millisecond,
		"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil, namedRun("release"))

	// The exit status keeps the id, deliberately: an error has to stand on its
	// own — it is what a machine format's caller gets instead of the narration —
	// and the id is the word `flow get` and `flow watch` take.
	require.ErrorContains(t, err, `run "flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2" failed`)
	require.ErrorContains(t, err, "connection reset by peer")

	lines := splitLines(errOut.String())
	require.Len(t, lines, 1)
	assert.Equal(t,
		"FAILED workflow release run 0198f1e2-0000-7000-8000-000000000000: connection reset by peer",
		lines[0],
		"a failed run's line has to say what failed and why, by name")
}

// TestTheNarrationSaysTheSameThingStyledAndUnstyled is the no-color half.
//
// Styling is a rendering of the same sentence, not a different sentence, so a
// colour-capable surface has to produce exactly the plain text once the escape
// sequences are removed. Without this, a change that put an identifier inside a
// style only the styled path takes would pass every other test here.
func TestTheNarrationSaysTheSameThingStyledAndUnstyled(t *testing.T) {
	answers := []pollAnswer{runningAt("roll_out"), finishedPoll("report", "roll_out")}

	plain := narrate(t, answers, namedRun("computed-outputs"))

	styled, _, styledErr := terminalSurface(80, 24, colorprofile.TrueColor)
	require.NoError(t, followPlainly(t.Context(), styled, renderingOf(FormatText),
		&scriptedPoller{answers: answers}, time.Millisecond,
		"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil,
		namedRun("computed-outputs")))

	require.Contains(t, styledErr.String(), "\x1b[",
		"the styled surface produced no escape sequences at all, so this test is "+
			"comparing two copies of the plain shape and proves nothing")

	// Compared word by word rather than byte by byte: a styled status pill is
	// padded with a space either side and a plain one is not, which is the one
	// difference between the two renderings that is deliberate. Everything else —
	// every word, in order, including every identifier — has to match.
	assert.Equal(t, words(plain), words(splitLines(styledErr.String())),
		"the styled and unstyled narrations have to be the same sentence")
}

// TestTheMachineShapesAreUntouchedByTheNarration is claim 3.
//
// The narration lives on stderr and the document on stdout, and a program asked
// for `-o json`/`-o jsonl` gets no narration at all. Compared against
// [v1.MarshalRunDocument] over the same message rather than against a literal, so
// this asserts "the bytes are the run document, unchanged" rather than
// re-encoding a second opinion about what that document should be.
func TestTheMachineShapesAreUntouchedByTheNarration(t *testing.T) {
	final := response(v1.RunResponse_STATUS_COMPLETED, "report", "roll_out")

	for _, format := range []OutputFormat{FormatJSON, FormatJSONL} {
		t.Run(string(format), func(t *testing.T) {
			surface, out, errOut := plainSurface()

			require.NoError(t, followPlainly(t.Context(), surface, renderingOf(format),
				&scriptedPoller{answers: []pollAnswer{{response: final}}}, time.Millisecond,
				"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil,
				namedRun("computed-outputs")))

			expected, err := v1.MarshalRunDocument(final, format == FormatJSON, false)
			require.NoError(t, err)

			assert.Equal(t, string(expected)+"\n", out.String(),
				"a machine format's bytes are the run document and nothing else")
			assert.Empty(t, errOut.String(),
				"the prose is for a person; a program that asked for a document got narration too")
		})
	}
}

// TestRunSubjectPrefersTheWorkflowsOwnName pins the fallback rule the started
// line and the follow both read.
func TestRunSubjectPrefersTheWorkflowsOwnName(t *testing.T) {
	id := "flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2"

	assert.Equal(t, "computed-outputs",
		runSubject(&v1.Workflow{Name: "computed-outputs"}, id))

	// A workflow that reached the server without a usable name — `flow validate`
	// refuses one, so this is the shape of a document that arrived some other
	// way. Naming nothing would be the one unreadable answer.
	assert.Equal(t, id, runSubject(&v1.Workflow{Name: "   "}, id))
	assert.Equal(t, id, runSubject(nil, id))
}

// TestALiveViewLeavesASentenceBehind is the other half of "say what happened in
// a sentence", and the one nothing could see before.
//
// bubbletea erases its last frame on the way out, so everything the live view
// drew — the status, the steps, the run id — is gone the moment the command
// returns. A durable run watched on a terminal therefore ended with the
// `outputs` block and no statement that it had ended at all, while the same file
// through `flow run local` said `COMPLETED workflow <name>` and stopped. The two
// drivers have to describe themselves the same way.
//
// Asserted through [watchEnding] rather than through a recorded terminal,
// because what is at stake is a line written after the program has torn its
// screen down: a golden of the frame cannot see it, and neither can a substring
// search of a transcript that still holds the frame.
func TestALiveViewLeavesASentenceBehind(t *testing.T) {
	t.Run("a finished run says so", func(t *testing.T) {
		surface, _, errOut := plainSurface()
		model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second,
			"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil, namedRun("computed-outputs"))
		folded := fold(t, model, watch.StateMsg{
			Response: response(v1.RunResponse_STATUS_COMPLETED, "report", "roll_out"),
		})

		require.NoError(t, watchEnding(surface, renderingOf(FormatText), folded))

		lines := splitLines(errOut.String())
		require.NotEmpty(t, lines)
		assert.Equal(t,
			"COMPLETED workflow computed-outputs run 0198f1e2-0000-7000-8000-000000000000 "+
				"after report, roll_out",
			lines[0],
			"a live view that has erased itself has to leave the same sentence the plain "+
				"shape writes — and it is the first thing this shape writes that stays, so "+
				"the run id `flow get --run-id` takes comes due here")
	})

	t.Run("a document asked for gets no prose", func(t *testing.T) {
		surface, _, errOut := plainSurface()
		model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second,
			"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil, namedRun("computed-outputs"))
		folded := fold(t, model, watch.StateMsg{
			Response: response(v1.RunResponse_STATUS_COMPLETED, "report"),
		})

		require.NoError(t, watchEnding(surface, renderingOf(FormatJSON), folded))
		assert.Empty(t, errOut.String(),
			"a caller that asked for a document was narrated at anyway")
	})

	t.Run("a walk that gave up claims nothing", func(t *testing.T) {
		surface, _, errOut := plainSurface()
		model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second,
			"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil, namedRun("computed-outputs"))
		folded := fold(t, model,
			watch.StateMsg{At: observed, Err: transientRefusal()},
			watch.StateMsg{At: observed.Add(outageAllowance), Err: transientRefusal()})

		require.True(t, folded.State().GaveUp())
		require.ErrorContains(t, watchEnding(surface, renderingOf(FormatText), folded), "gave up")
		assert.Empty(t, errOut.String(),
			"a watch that lost the server stated an outcome it does not know")
	})
}

// TestALiveViewAccountsForEveryAttemptItWatched is picatz/flowstate#836's
// finding, in the shape that found it.
//
// The live view draws the run id in its header and bubbletea erases the frame
// on the way out, so nothing it showed survives the command. A first attempt at
// #544 read those frames as having told somebody, which left a completed
// terminal run carrying no run id at all — and across a continue-as-new
// handover the erased frames had been the only place the earlier attempts were
// ever named. `flow get --run-id` and `flow watch --run-id` both take one, so
// an attempt whose id never reached the reader is an attempt they cannot ask
// about.
//
// The rule the fix states: a run id is owed until a line that *stays* has
// carried it. Nothing the live view draws counts.
func TestALiveViewAccountsForEveryAttemptItWatched(t *testing.T) {
	surface, _, errOut := plainSurface()
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second,
		"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil, namedRun("renewal-reminder"))

	first := response(v1.RunResponse_STATUS_RUNNING, "poll")

	// The handover. Same workflow id, a new run id, and the frame that drew it is
	// about to be thrown away.
	second := response(v1.RunResponse_STATUS_RUNNING, "poll")
	second.RunId = "0198f1e2-0000-7000-8000-000000000001"

	third := response(v1.RunResponse_STATUS_COMPLETED, "poll", "remind")
	third.RunId = "0198f1e2-0000-7000-8000-000000000002"

	folded := fold(t, model,
		watch.StateMsg{Response: first},
		watch.StateMsg{Response: second},
		watch.StateMsg{Response: third})

	require.NoError(t, watchEnding(surface, renderingOf(FormatText), folded))

	lines := splitLines(errOut.String())
	require.Len(t, lines, 1)
	assert.Equal(t,
		"COMPLETED workflow renewal-reminder runs "+
			"0198f1e2-0000-7000-8000-000000000000, "+
			"0198f1e2-0000-7000-8000-000000000001, "+
			"0198f1e2-0000-7000-8000-000000000002 after poll, remind",
		lines[0],
		"every attempt this walk watched has to be nameable afterwards")
}

// TestEveryRunIdReachesAPersistentLineExactlyOnce is the invariant both shapes
// hold, stated once and checked against both.
//
// Counting is the point. "The transcript mentions the run id" passes against
// the shape #544 rejected, which said it on every line; "the transcript omits
// it" passes against the shape #836 rejected, which said it nowhere. Exactly
// once, per attempt, is the only claim that fails both.
func TestEveryRunIdReachesAPersistentLineExactlyOnce(t *testing.T) {
	attempts := []string{
		"0198f1e2-0000-7000-8000-000000000000",
		"0198f1e2-0000-7000-8000-000000000001",
	}

	walk := func() []pollAnswer {
		running := runningAt("poll")
		handover := runningAt("remind")
		handover.response.RunId = attempts[1]
		done := finishedPoll("poll", "remind")
		done.response.RunId = attempts[1]

		return []pollAnswer{running, handover, done}
	}

	t.Run("the plain shape", func(t *testing.T) {
		transcript := strings.Join(narrate(t, walk(), namedRun("renewal-reminder")), "\n")

		for _, attempt := range attempts {
			assert.Equal(t, 1, strings.Count(transcript, attempt),
				"attempt %s did not reach the transcript exactly once:\n%s", attempt, transcript)
		}
	})

	t.Run("the live shape", func(t *testing.T) {
		surface, _, errOut := plainSurface()
		model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second,
			"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil, namedRun("renewal-reminder"))

		msgs := make([]tea.Msg, 0, len(walk()))
		for _, answer := range walk() {
			msgs = append(msgs, watch.StateMsg{Response: answer.response})
		}

		require.NoError(t, watchEnding(surface, renderingOf(FormatText), fold(t, model, msgs...)))

		transcript := errOut.String()
		for _, attempt := range attempts {
			assert.Equal(t, 1, strings.Count(transcript, attempt),
				"attempt %s did not reach the transcript exactly once:\n%s", attempt, transcript)
		}
	})
}

// TestALiveViewBoundsAnUnboundedHandoverChain is picatz/flowstate#836's second
// finding: the live view accumulates a run id per continue-as-new handover and
// drains the ledger only at the end, so a workload that hands over without
// bound — a nested loop against a small step budget does exactly this — made the
// watcher retain an unbounded slice and then join it into an unbounded string.
//
// The bound keeps the retained tail and counts the rest, so the parting line
// stays a line however long the chain. The current attempt is always named,
// because it is the one `flow get --run-id` is actually reached for.
func TestALiveViewBoundsAnUnboundedHandoverChain(t *testing.T) {
	surface, _, errOut := plainSurface()
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second,
		"flowstate-workflow-01b09563-6f8a-4ab1-a1d0-67896e7b8da2", nil, namedRun("renewal-reminder"))

	// A hundred handovers, each a fresh run id, none of them ever written to a
	// stream that stays until the terminal line below.
	const handovers = 100
	msgs := make([]tea.Msg, 0, handovers+1)
	for i := 0; i < handovers; i++ {
		running := response(v1.RunResponse_STATUS_RUNNING, "poll")
		running.RunId = fmt.Sprintf("0198f1e2-0000-7000-8000-%012d", i)
		msgs = append(msgs, watch.StateMsg{Response: running})
	}
	last := response(v1.RunResponse_STATUS_COMPLETED, "poll", "remind")
	last.RunId = fmt.Sprintf("0198f1e2-0000-7000-8000-%012d", handovers)
	msgs = append(msgs, watch.StateMsg{Response: last})

	require.NoError(t, watchEnding(surface, renderingOf(FormatText), fold(t, model, msgs...)))

	lines := splitLines(errOut.String())
	require.Len(t, lines, 1)
	line := lines[0]

	// Bounded: the earlier attempts are a count, not a hundred ids.
	assert.Contains(t, line, "earlier attempts),",
		"a long handover chain has to collapse to a count rather than name every attempt:\n%s", line)

	ids := runIDPattern.FindAllString(line, -1)
	assert.LessOrEqual(t, len(ids), 8,
		"the narrated line named %d run ids; the ledger is unbounded:\n%s", len(ids), line)

	// The current attempt is always named — that is the one a reader acts on.
	assert.Contains(t, line, fmt.Sprintf("0198f1e2-0000-7000-8000-%012d", handovers),
		"the final attempt, which is the one flow get --run-id reaches for, was dropped:\n%s", line)
}
