package flowstatev1

import (
	"errors"
	"fmt"
)

// A step tolerated by `continue_on_error:` records its failure as
// `${steps.<id>.error}`, and that string is a value like any other: authors
// write `if:` conditions against it. A value an expression compares must be the
// same sentence wherever the step runs — across drivers, across attempts, and
// across versions of the substrate that carried the failure.
//
// It was not. The local driver recorded the raw Go error, while the durable
// driver recorded the failure as Temporal handed it back: wrapped in an activity
// envelope with scheduled event ids and a worker identity, with the classified
// error's type and retryability restated at every level of the unwrap chain. So
// the same tolerated step recorded
//
//	task "http": GET http://…/nope returned status 404
//
// locally and
//
//	engine: flowstate run failed: step "flaky": activity error (type: TaskInScope,
//	scheduledEventID: 8, startedEventID: 9, identity: 51@host): task "http": …
//	(type: InvalidInput, retryable: false): … (type: TaskError, retryable: true): …
//
// durably. Against a real server the event ids and identity vary per run, so the
// durable value was not merely different but unstable — in the one value whose
// whole purpose is being compared by an author's `if:`.
//
// So the recorded text is one value with one renderer, in the package both
// drivers already import — the same shape as the retry defaults in
// retrydefaults.go, and for the same reason: one function cannot disagree with
// itself. The durable driver additionally has to carry this text across the
// activity boundary, which it does by making it the application error's message
// and reading exactly that message back where the step's outputs are recorded.

// StepErrorOutput is the name a tolerated step failure is recorded under, making
// it readable as `${steps.<id>.error}`. Its absence means the step succeeded.
//
// The flowfile validator knows this name too — it is the one output that comes
// from a step's *policy* rather than from its task, so it is spelled here rather
// than in each place that needs it.
const StepErrorOutput = "error"

// StepErrorText renders a step failure into the string recorded under
// [StepErrorOutput].
//
// Built from the classified failure — the task's name, the [ErrorKind], and the
// cause — and never from whatever transport carried it, so nothing about a
// particular driver, attempt, or Temporal version can reach a value an author's
// expression compares against. Everything carrying meaning is kept; only the
// carrying is stripped.
//
// A failure that is not a classified [TaskError] is recorded as its own words,
// which both drivers hold identically before any wrapping is applied.
func StepErrorText(err error) string {
	if err == nil {
		return ""
	}

	var taskErr *TaskError
	if !errors.As(err, &taskErr) || taskErr.Task == "" || taskErr.Err == nil {
		return err.Error()
	}

	if taskErr.Kind == "" {
		return fmt.Sprintf("task %q failed: %v", taskErr.Task, taskErr.Err)
	}

	return fmt.Sprintf("task %q failed (%s): %v", taskErr.Task, taskErr.Kind, taskErr.Err)
}

// FailedStepOutputs records a tolerated failure as a step's outputs, under
// [StepErrorOutput].
//
// It takes the already-rendered text rather than the error, because the two
// drivers hold the failure in different shapes at the moment of recording: the
// local driver still has the task's own error and renders it with
// [StepErrorText], while the durable driver has Temporal's envelope around it
// and extracts the same text from the application error inside. One builder for
// the recorded shape keeps the output's name from being spelled per driver.
func FailedStepOutputs(text string) *Node_Outputs {
	return &Node_Outputs{
		NamedValues: map[string]*Value{
			StepErrorOutput: NewLiteral(text),
		},
	}
}
