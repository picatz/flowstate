package flowstatev1

import (
	"context"
	"unicode/utf8"
)

// A task's own account of its work, for whoever is watching the run.
//
// A custom task is opaque to a debugger: the run stops before it and after it,
// and what happens between — the pages a sync fetched, the retries a client
// made inside one attempt — is invisible. [NoteTask] is the seam a task author
// uses to say so, bounded and optional: a note reaches a [TaskNoter] only when
// one is watching the run, and costs a context lookup otherwise.
//
// A note is presentation, not data. It never enters the step's outputs or the
// run's history, and it is rendered through the watching session's own
// redaction like everything else a session prints — which is also why a task
// must not write a secret into one: redaction there is a transcript control,
// not a boundary.

// MaxTaskNoteBytes bounds one note.
const MaxTaskNoteBytes = 1024

// TaskNoter is a [RunObserver] that also wants a task's own account of its
// work.
type TaskNoter interface {
	// TaskNoted is called with the step the task is running for and the note.
	TaskNoted(step, text string)
}

type taskStepKey struct{}

// contextWithTaskStep records which step a task runs for, when something is
// listening for notes, and returns ctx unchanged otherwise.
func contextWithTaskStep(ctx context.Context, step string) context.Context {
	if _, ok := RunObserverFromContext(ctx).(TaskNoter); !ok {
		return ctx
	}

	return context.WithValue(ctx, taskStepKey{}, step)
}

// NoteTask reports a task's progress to whoever is debugging the run, cut to
// [MaxTaskNoteBytes]. It is a no-op when nobody is, and never fails the task.
func NoteTask(ctx context.Context, text string) {
	noter, ok := RunObserverFromContext(ctx).(TaskNoter)
	if !ok {
		return
	}
	step, _ := ctx.Value(taskStepKey{}).(string)

	if len(text) > MaxTaskNoteBytes {
		// The elision counts against the bound, so a cut note is never
		// longer than an uncut one may be.
		const elision = "…"
		cut := MaxTaskNoteBytes - len(elision)
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		text = text[:cut] + elision
	}

	observeSafely(func() { noter.TaskNoted(step, text) })
}
