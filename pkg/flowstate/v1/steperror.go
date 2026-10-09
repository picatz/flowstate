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

// StepErrorItemOutput is the name a tolerated failure inside a loop iteration
// records the iteration's own binding under — the `as:` value in scope when the
// step failed, readable as `${steps.<loop id>.results[i].<step id>.item}`.
//
// The information was always in scope at the failure: a `for_each` body runs
// with its item bound, a `loop:` body with its carried state. It used to be
// dropped, so "which records failed" had to be reconstructed downstream by set
// subtraction — `inputs.records` minus the ids that succeeded — recomputing
// from the complement a value the engine held at the moment it recorded the
// failure (#157). Attaching it makes the failure entry name its own item.
//
// One fixed name rather than the author's `as:` name, deliberately: the `as:`
// name is bound *inside* the loop and nowhere else (the same reason a loop's
// final state is read as `state`, not as the `as:` name), and renaming a
// binding must not change the shape downstream expressions read. `item` is
// also [DefaultIterator], the name a `for_each` binds when the author writes
// none — the reading it already teaches.
//
// It is attached by [AttachIterationBinding], and only to steps the driver's
// own node walk recorded as failed-and-tolerated — a fact each driver marks at
// the moment it records the failure, never an inference from the outputs' own
// names. A step that *succeeds* while declaring an output literally named
// `error` (or `item`) keeps its declared shape untouched: the marker, not the
// name, is what decides.
const StepErrorItemOutput = "item"

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
//
// A cancellation cause [withCancellationCause] appended is read out and
// reattached explicitly, rather than left to whatever errors.As happens to
// find. A task classifies its own error into a [TaskError] before
// [withCancellationCause] ever sees it — the http task returns one wrapping
// context.DeadlineExceeded, and the cause naming *why* the step's deadline
// was reached (a schedule-to-close budget, an undo budget) is appended
// outside that TaskError, not inside it. errors.As walks past the wrapper to
// find the TaskError underneath regardless, so building the rendered text
// from taskErr.Task/Kind/Err alone silently drops that outer suffix — leaving
// schedule-to-close expiry indistinguishable from an ordinary per-attempt
// timeout in exactly the outputs (`continue_on_error:`, a compensation
// summary) this feature exists to make distinguishable. Keeping the cause
// here, rather than folding it into TaskError.Err at the point it is
// attached, is the choice that keeps [withCancellationCause] free of any
// TaskError-specific knowledge: it enriches whatever error it is given, and
// this is the one place that already has to know how a [TaskError] renders.
func StepErrorText(err error) string {
	if err == nil {
		return ""
	}

	// Like ClassifyError, prefer the overall budget over the last attempt
	// preserved beneath it. Rendering the nested TaskError directly would erase
	// the configured bound that ended the step and make ordinary retry exhaustion
	// and total_timeout: expiry read the same way.
	if overall, ok := errors.AsType[*scheduleToCloseTimeoutError](err); ok {
		return overall.Error()
	}

	var cause string
	if enriched, ok := errors.AsType[*causeEnrichedError](err); ok {
		cause = ": " + enriched.cause.Error()
	}

	var taskErr *TaskError
	if !errors.As(err, &taskErr) || taskErr.Task == "" || taskErr.Err == nil {
		return err.Error()
	}

	// A cause that already names the task in its own words — a task-shape policy
	// denial — is rendered as itself rather than under a `task %q failed (%s):`
	// frame that would name the task a second time (#184, #899). The kind still
	// travels structurally (recordedStepKind), and the denial's own sentence is
	// self-describing, so nothing a reader needs is lost by dropping the frame.
	if selfNamesTask(taskErr.Err) {
		return taskErr.Err.Error() + cause
	}

	if taskErr.Kind == "" {
		return fmt.Sprintf("task %q failed: %v%s", taskErr.Task, taskErr.Err, cause)
	}

	return fmt.Sprintf("task %q failed (%s): %v%s", taskErr.Task, taskErr.Kind, taskErr.Err, cause)
}

// StepFailureOutput is the name a tolerated step's typed failure is recorded
// under, beside [StepErrorOutput]: `${steps.<id>.failure}` is a map with
// [FailureKindField], [FailureMessageField] and [FailureRetryableField].
//
// `error` stays the one sentence it always was, so every `has(...error)` and
// `!= ”` an author wrote keeps its meaning; `failure` is what an author reads
// when the question is *which* failure rather than *whether* (#1905). Its
// `message` is the same text by construction: both are built from one
// [StepFailure].
const StepFailureOutput = "failure"

// The fields of the map recorded under [StepFailureOutput].
const (
	// FailureKindField is the [ErrorKind] the failure was classified as.
	FailureKindField = "kind"
	// FailureMessageField is the same sentence [StepErrorOutput] holds.
	FailureMessageField = "message"
	// FailureRetryableField is the kind's retry default, [ErrorKind.Retryable].
	// It states what the classification permits, not whether a particular
	// attempt was retried: an attempt-level narrowing (an unknown outcome) is a
	// property of the attempt, not of the recorded failure.
	FailureRetryableField = "retryable"
)

// A StepFailure is what a driver knows of a tolerated failure at the moment it
// records it: the rendered sentence and the classification. Each driver builds
// one from the shape its failure is in (a raw error locally, Temporal's
// application error durably), and [FailedStepOutputs] is the one place it
// becomes outputs, so the two cannot spell the recorded shape differently.
type StepFailure struct {
	// Text is the [StepErrorText] sentence, recorded as `error` and `failure.message`.
	Text string

	// Kind is the failure's classification, recorded as `failure.kind`.
	Kind ErrorKind
}

// NewStepFailure builds the [StepFailure] for an error a driver holds in its
// own bare shape.
func NewStepFailure(err error) StepFailure {
	return StepFailure{Text: StepErrorText(err), Kind: ClassifyError(err)}
}

// FailedStepOutputs records a tolerated failure as a step's outputs, under
// [StepErrorOutput] and [StepFailureOutput].
//
// It takes the already-rendered text rather than the error, because the two
// drivers hold the failure in different shapes at the moment of recording: the
// local driver still has the task's own error and renders it with
// [StepErrorText], while the durable driver has Temporal's envelope around it
// and extracts the same text from the application error inside. One builder for
// the recorded shape keeps the output's name from being spelled per driver.
func FailedStepOutputs(f StepFailure) *Node_Outputs {
	return &Node_Outputs{
		NamedValues: map[string]*Value{
			StepErrorOutput: NewLiteral(f.Text),
			StepFailureOutput: NewLiteralMap(map[string]any{
				FailureKindField:      f.Kind.String(),
				FailureMessageField:   f.Text,
				FailureRetryableField: f.Kind.Retryable(),
			}),
		},
	}
}

// A StepError is a failure positioned at the step it happened in. It renders
// the sentence the local driver has always produced (`step "id": cause`) and
// unwraps to the cause, so every message and every errors.Is/As is unchanged;
// what it adds is that the step id is a field instead of a prefix a reader has
// to parse back out of text.
type StepError struct {
	Step string
	Err  error
}

func (e *StepError) Error() string { return fmt.Sprintf("step %q: %v", e.Step, e.Err) }

func (e *StepError) Unwrap() error { return e.Err }

// FailedStepOf names the step a run's failure happened in: the innermost step
// the failure passes through before it reaches the task that raised it, so a
// failure inside a block names the step in the block, and one a `call:`
// brought back from its callee names the `call:` step, whose id is the only
// one this workflow's author wrote. It is empty when the failure carries no
// step, as a run's own timeout does.
func FailedStepOf(err error) string {
	var step string
	for ; err != nil; err = errors.Unwrap(err) {
		switch e := err.(type) {
		case *StepError:
			step = e.Step
		case *TaskError:
			return step
		}
	}

	return step
}
