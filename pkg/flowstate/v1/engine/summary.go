package engine

import (
	"strings"

	"go.temporal.io/sdk/workflow"
)

// The summaries a durable run writes into its own history, so that every
// command the engine issues says which part of the file it came from.
//
// # The gap this closes
//
// Every step of every workflow reaches Temporal as an activity typed `Task`,
// `TaskInScope`, `TaskAuthorized` or `TaskInScopeAuthorized` — see
// [executor.dispatch], where the step id is an *argument* rather than the
// activity's name, because one interpreter runs every workflow and the
// activity type is a property of the interpreter. That is right, and it makes
// a run illegible in the two tools an operator already has: a hundred-step
// workflow renders in Temporal Web as a hundred identical `TaskInScope`
// rows, and `temporal workflow show` prints the same wall. The information
// needed to tell them apart was in the history all along, inside each
// activity's input payload.
//
// Which is exactly where it must not be read from. That payload is the
// *resolved* task — inputs with `${secret(...)}` references already turned
// into references the activity resolves, and everything else an author wrote
// spelled out. A reader that decoded activity inputs to label a timeline
// would be reading workflow history for values this repository spends a whole
// section of CLAUDE.md keeping out of it. A summary is the opposite: a
// separate, deliberately tiny field carrying one value that is already
// constrained to a grammar, chosen by the engine rather than by an author's
// data.
//
// # Why this is safe to write
//
// Both inputs are grammar-constrained by the schema, which is what makes
// backtick delimiting sound — the same argument [server.runStaticSummary]
// makes for the workflow-level summary this mirrors one level down.
// [v1.Node].id is `^[A-Za-z0-9-_]+$` with a 128-byte ceiling
// (`proto/flowstate/v1/workflow.proto`), so a step id can hold no backtick,
// no newline and no unbounded length — the three ways a single-line Markdown
// field gets broken by its own content.
//
// Neither value is secret and neither is derived from anything the history
// does not already carry: a step id appears in every `ActivityTaskScheduled`
// input on the same run. This is Temporal visibility data, exactly as broadly
// readable as the memo and the search attributes beside it.
//
// A summary travels as a payload (`dataConverter.ToPayload`, the SDK's
// `buildUserMetadata`), so a deployment running a payload codec encrypts these
// along with everything else and reads them back through its codec server
// rather than in raw history. That is the same property the workflow-level
// summary already has, and it is the right one: a label is data, and a
// deployment that has decided its history is ciphertext does not get an
// exception carved out by the interpreter.
//
// # Why this is safe to add to an engine with runs in flight
//
// A summary travels as `UserMetadata` on the command, and the SDK's replay
// matcher does not look at it: `SCHEDULE_ACTIVITY_TASK` is matched on
// `ActivityId` and `ActivityType.Name`, and `START_TIMER` on `TimerId`
// (`internal/internal_task_handlers.go`). So a history recorded by an engine
// that wrote no summaries replays against one that does. That is not a claim
// resting on reading the SDK: `TestReplayCorpus` replays ten real histories
// recorded from earlier engines — none of which carry any of this — on every
// run of this package's tests, which is the negative direction the claim
// needs.
//
// # The shape
//
// The position first, because the step is what a reader is looking for, then
// what the command is where the command's own name does not already say. Same
// `·`-separated shape as the workflow-level summary, and the same
// `> `-separated position [progress.currentDetailsMarkdown] renders into the
// SDK's own Details view — this is one function rather than a rendering at each
// site for that comment's reason: two independent spellings of "which step"
// drift.
//
// The position and not only the id, because an id is unique within a
// *visibility domain* rather than within a file: two sibling `loop:` blocks may
// each legally declare a body step called `page` (see `refScope` in
// `flowfile/validate.go`), and labelling both `page` would leave a reader
// exactly where an unlabelled row does — unable to tell which of two things
// produced it (Codex, #1118). [v1.RunProgress].path answers the same question
// live, and cannot answer it about a history: it is a query against a running
// workflow, and by the time anybody reads the row the run has moved on.
//
// One command shape is still labelled by id alone, and it is a limit rather
// than a choice: a compensation is dispatched from the run-level undo stack,
// whose entries are [v1.PendingUndo] and carry a `step_id` and no position. Two
// sibling loops each registering an undo for a body step of the same name are
// therefore indistinguishable in history. Fixing it is a schema field and
// belongs with its own compatibility argument, not here.

// maxSummaryLabel bounds a rendered label, in bytes.
//
// A position is as many step ids as the specification nests, each of which the
// schema allows 128 bytes, so a deeply nested one has no bound of its own —
// while Temporal's summary payload is limited to 400 bytes by default and a
// label that fails to convert would fail the step it labels. Bounding here
// rather than trusting the nesting is the rule this repository applies to
// anything whose size somebody else decides.
//
// Elided from the *left*, keeping the innermost entries: the step that actually
// ran is the answer, and its enclosing blocks are context a reader can find
// elsewhere in the specification. The elision is always *stated*, with a
// leading `… > `, even when nothing of the position survived — a bare `page`
// reads as a top-level step, which is a different fact and a wrong one.
const maxSummaryLabel = 256

// label renders one command's summary: where the step is, then what the command
// is where the command's own name does not say.
func label(path []string, stepID, kind string) string {
	var suffix string
	if kind != "" {
		suffix = " · " + kind
	}

	step := "`" + stepID + "`"
	for i := len(path); i >= 0; i-- {
		var b strings.Builder
		if i < len(path) {
			b.WriteString("… > ")
		}
		for _, enclosing := range path[len(path)-i:] {
			b.WriteString("`")
			b.WriteString(enclosing)
			b.WriteString("` > ")
		}
		b.WriteString(step)
		b.WriteString(suffix)

		if rendered := b.String(); len(rendered) <= maxSummaryLabel || i == 0 {
			return rendered
		}
	}

	// Unreachable: the loop returns at i == 0. Present because a bound that
	// falls through to nothing is worse than one that says what it kept.
	return step + suffix
}

// stepSummary labels the activity one task step schedules.
//
// The step id alone: the activity type beside it already says this is a task,
// and the four spellings of that type differ by how the activity is
// authorized, which is a fact about the deployment rather than about the file.
//
// A loop body's iterations share a summary, because they share a step. That is
// honest rather than lossy — the position *within* a loop is what
// [v1.RunProgress].path answers, and it is answered by a query against the
// running workflow rather than from history, because history records what was
// scheduled and a path is where the run is now.
func stepSummary(path []string, stepID string) string {
	return label(path, stepID, "")
}

// undoStepSummary labels a compensation's activity.
//
// Named apart from the forward step it undoes because the two are otherwise
// indistinguishable in history: a compensation is dispatched through the same
// [executor.dispatch] with the same activity types, and a saga unwinding six
// steps would otherwise read as six more steps running.
func undoStepSummary(stepID string) string {
	return label(nil, stepID, "undo")
}

// sleepSummary labels the durable timer a `sleep:` step parks on.
//
// The SDK's own [workflow.Sleep] already writes the summary `Sleep`, which
// says what the command is and not which step issued it — so a run asleep for
// six hours shows a timer that could have come from any wait in the file. This
// is why [executor.waitFor] calls `NewTimerWithOptions` rather than `Sleep`:
// that is the identical command (`Sleep` is `NewTimerWithOptions` with a fixed
// summary, `internal/workflow.go`), with the label carrying the one fact the
// SDK cannot know.
func sleepSummary(path []string, stepID string) string {
	return label(path, stepID, "sleep")
}

// waitTimeoutSummary labels the timer bounding a `wait_for_signal:` step.
//
// Distinguished from [sleepSummary] because the two answer different
// questions for a reader: a fired sleep timer is a run doing what it was told,
// and a fired wait timer is a gate nobody answered.
func waitTimeoutSummary(path []string, stepID string) string {
	return label(path, stepID, "wait timeout")
}

// callVarsSummary labels the activity a `call:` step's callee `vars:` are
// evaluated in.
//
// The calling step rather than the callee's name, because the step is what a
// reader has in hand: a workflow called from three places produces three of
// these, and which one is stuck is the question.
func callVarsSummary(path []string, stepID string) string {
	return label(path, stepID, "call vars")
}

// runVarsSummary labels the activity a run's own top-level `vars:` are
// evaluated in. Not step-scoped, because they are not a step: they are
// evaluated once for the run, before any step runs.
//
// No backticks, because there is no value in it to delimit. The two other
// unstepped labels are the same shape for the same reason.
const runVarsSummary = "run vars"

// pluginAdmissionSummary labels the activity that checks a run's pinned plugins
// against what the worker actually has.
const pluginAdmissionSummary = "plugin admission"

// withSummary returns ctx with the activity summary set, leaving every other
// activity option exactly as it was.
//
// For the dispatches that are not a step's work — the two `vars:` activities and
// plugin admission — where the options already on the context are the right ones
// and only the label is missing. A step's own activity does not come through
// here: it builds its options from its policy, which is where its summary is set
// too (see [activityOptionsFor]).
//
// Every one of them is labelled rather than only the interesting ones, because
// that is what makes the claim checkable:
// `TestEveryCommandInHistoryNamesItsStep` asserts that *no* activity or timer in
// a run's history is unlabelled, which is a claim a reader can trust; a test
// listing the labelled ones is a claim about the list.
func withSummary(ctx workflow.Context, summary string) workflow.Context {
	opts := workflow.GetActivityOptions(ctx)
	opts.Summary = summary

	return workflow.WithActivityOptions(ctx, opts)
}
