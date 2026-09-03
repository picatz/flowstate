package conformance

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ErrorKindCase pairs a workflow that fails outright — not a step
// [continue_on_error] tolerates, but a run that never completes — with the
// [v1.ErrorKind] both drivers must classify the failure as.
//
// #241's P2 puts ErrorKind on the wire (RunResponse.Error.kind), which is only
// worth having if both drivers agree on it — invariant 3, restated for a value
// that previously stopped at ClassifyError and never had to survive a second
// driver's own wrapping. See CLAUDE.md's account of the default-attempt-count
// and marshalJSON disagreements: a value with one meaning, computed twice, is
// how the two drivers came to disagree before, and this is the shared case set
// that catches the same shape here.
type ErrorKindCase struct {
	// Name of the case, used for test identification.
	Name string
	// Workflow is the workflow whose run must fail outright.
	Workflow *v1.Workflow
	// ExpectedKind is the classification both drivers must agree on.
	ExpectedKind v1.ErrorKind
	// TaskDef is a fixture this case needs registered. Nil means it uses a
	// task already present in the build or the shared timeout fixture.
	TaskDef *v1.TaskDef
	// Attempts reads how often TaskDef ran when retry behavior is part of the
	// case. ExpectedAttempts is the count both drivers must agree on.
	Attempts         func() int32
	ExpectedAttempts int32
}

// ErrorKindCases returns workflows engineered to fail with a known,
// deterministic [v1.ErrorKind]. Most isolate classification; a case that also
// supplies Attempts deliberately offers retries so both drivers must prove a
// permanent kind stops after one execution. The httpBaseURL should come from
// [NewHTTPServer], which also registers the loopback-permitting http task the
// HTTP case below needs.
func ErrorKindCases(httpBaseURL string) []ErrorKindCase {
	const unknownOutcomeTaskName = "test.error_kind_upstream_unknown"

	var unknownOutcomeAttempts atomic.Int32
	unknownOutcomeTask := v1.TaskDef{
		Name: unknownOutcomeTaskName,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			unknownOutcomeAttempts.Add(1)
			return nil, v1.NewTaskError(unknownOutcomeTaskName, v1.ErrorKindUpstreamUnknown,
				errors.New("the task may have completed before its response was lost"))
		},
	}

	return []ErrorKindCase{
		{
			// Permanent because the specification names a task no worker
			// provides — retrying evaluates the same specification against the
			// same registry and fails the same way.
			Name: "unknown task is UnknownTask",
			Workflow: &v1.Workflow{
				Name: "error-kind-unknown-task",
				Steps: []*v1.Node{{
					Id:   "bad",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "nosuchtask"}},
				}},
			},
			ExpectedKind: v1.ErrorKindUnknownTask,
		},
		{
			// A 404 is the endpoint rejecting this exact request, which is the
			// http task's own InvalidInput rule (see httpExpectationMet) — the
			// same answer for a 4xx with no `expect:` written, on both drivers.
			Name: "http 4xx with no continue_on_error is InvalidInput",
			Workflow: &v1.Workflow{
				Name: "error-kind-invalid-input",
				Steps: []*v1.Node{{
					Id: "bad",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url":    v1.NewLiteral(httpBaseURL + "/status/404"),
							"method": v1.NewLiteral(http.MethodGet),
						},
					}},
				}},
			},
			ExpectedKind: v1.ErrorKindInvalidInput,
		},
		{
			// The verdict the plugin host builds for sdk.OutcomeUnknown and
			// therefore for a recovered SDK task panic. Three attempts are
			// available, but an outcome that may already have committed is
			// permanent: retrying could duplicate its side effects.
			Name: "an unknown upstream outcome is permanent",
			Workflow: &v1.Workflow{
				Name: "error-kind-upstream-unknown",
				Steps: []*v1.Node{{
					Id:   "uncertain",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: unknownOutcomeTask.Name}},
					Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
						MaxAttempts:        3,
						InitialInterval:    durationpb.New(time.Millisecond),
						BackoffCoefficient: 1,
						MaxInterval:        durationpb.New(time.Millisecond),
					}},
				}},
			},
			ExpectedKind:     v1.ErrorKindUpstreamUnknown,
			TaskDef:          &unknownOutcomeTask,
			Attempts:         unknownOutcomeAttempts.Load,
			ExpectedAttempts: 1,
		},
		{
			// The failure that belongs to no task, and the one this set was
			// blind to until #184 traced it. An expression the *engine*
			// evaluates — a step's input here, a `vars:`, a loop's `items:` —
			// never reaches a task, so nothing built a [v1.TaskError] and
			// [v1.ClassifyError] fell through to its default: both drivers
			// answered `Internal`, which errors.go defines as "a defect in
			// Flowstate itself" and which [v1.ErrorKind.Retryable] reports
			// true for. Agreeing on the wrong answer is still agreeing, which
			// is exactly why a conformance set can miss it — so the case has
			// to name the kind it wants rather than only that the two match.
			//
			// `['a'][5]` compiles and fails when evaluated, which is what
			// keeps this a *runtime* classification rather than something
			// `flow validate` would have refused first.
			Name: "an input expression that fails to evaluate is Expression",
			Workflow: &v1.Workflow{
				Name: "error-kind-input-expression",
				Steps: []*v1.Node{{
					Id: "bad",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "log",
						Inputs: map[string]*v1.Value{"message": v1.NewExpr("['a'][5]")},
					}},
				}},
			},
			ExpectedKind: v1.ErrorKindExpression,
		},
		{
			// The same classification down a different path, and the path an
			// author is likeliest to take: a step's `if:` is evaluated by
			// runNodes before the step's own `vars:` are in scope and before
			// any task is dispatched, so a failure here is even further from a
			// task than the input case above.
			Name: "a condition that fails to evaluate is Expression",
			Workflow: &v1.Workflow{
				Name: "error-kind-condition-expression",
				Steps: []*v1.Node{
					guarded("bad", "['a'][5] == 'never'", "unreachable"),
				},
			},
			ExpectedKind: v1.ErrorKindExpression,
		},
		{
			// #915: the failure neither driver had a word for. A step cut off
			// by its own `timeout:` reached every operator surface as
			// `Internal` — "a defect in Flowstate itself" — because nothing
			// built a [v1.TaskError] for a bound the *engine* imposed and
			// [v1.ClassifyError] fell through to its default, exactly as the
			// two expression cases above did before #899.
			//
			// The two drivers arrive at it down entirely different roads, which
			// is why this belongs here rather than in either driver's package.
			// Locally the deadline is `runStepAttempt`'s own
			// [context.WithTimeout] and [ErrorKindTimeoutTaskDef] returns the
			// bare `context.DeadlineExceeded` it sees. Durably the same budget
			// is the activity's StartToClose, and whichever of the two ends the
			// attempt first — Temporal's timer raising a *temporal.TimeoutError
			// at the workflow, or the task's own return crossing the activity
			// boundary as an ApplicationError — has to produce the same word,
			// because which one wins is a race no author can see and neither
			// answer is more true than the other.
			Name: "a step cut off by its own timeout: is Timeout",
			Workflow: &v1.Workflow{
				Name: "error-kind-step-timeout",
				Steps: []*v1.Node{{
					Id:   "slow",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: ErrorKindTimeoutTaskName}},
					Policy: &v1.StepPolicy{
						Timeout: durationpb.New(ErrorKindTimeoutBudget),

						// One attempt, so the case costs one budget rather
						// than five plus the backoff between them. Not a claim
						// about retryability: [v1.ErrorKindTimeout] is
						// retryable on both drivers, and a case that let the
						// default five run would assert the same kind five
						// times as slowly.
						Retry: &v1.RetryPolicy{MaxAttempts: 1},
					},
				}},
			},
			ExpectedKind: v1.ErrorKindTimeout,
		},
	}
}

// ErrorKindTimeoutTaskName is the name [ErrorKindTimeoutTaskDef] registers
// under, for the timeout case above. Both drivers' callers of
// [ErrorKindCases] register it before running the corpus.
const ErrorKindTimeoutTaskName = "test.error_kind_timeout"

// ErrorKindTimeoutBudget is the `timeout:` the timeout case declares.
//
// Short enough that a driver spends it rather than being slowed by it, and
// long enough to be reached by dispatch on a loaded machine rather than
// racing the work of getting the task started. It is a whole step budget on
// both drivers, so nothing else in the run waits on it.
const ErrorKindTimeoutBudget = 250 * time.Millisecond

// ErrorKindTimeoutTaskDef is a [v1.TaskDef] that runs until its context ends
// and then reports that, unclassified.
//
// Unclassified is the point, and it is what makes this a test of the *engine's*
// classification rather than of a task's. A task that observes its own deadline
// and builds a [v1.TaskError] has said something [v1.ClassifyError] keeps ahead
// of anything it would infer — the http task does exactly that — so a fixture
// that classified itself would assert its own opinion travels, which was never
// in doubt. Returning the bare `context.DeadlineExceeded` is what a task
// written without any thought about classification returns, and that is the
// case #915 is about.
//
// It honours the context rather than blocking outright, because a task that
// ignored it would hang the local driver forever: only the durable driver can
// end an attempt whose task never looks. That asymmetry is real and is
// [v1.DefaultStartToCloseTimeout]'s own subject; it is not this case's, and a
// corpus entry that could hang one driver is not a corpus entry.
func ErrorKindTimeoutTaskDef() v1.TaskDef {
	return v1.TaskDef{
		Name:    ErrorKindTimeoutTaskName,
		Summary: "test fixture that runs until its step's timeout: ends it",
		Fn: func(ctx context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			<-ctx.Done()

			return nil, ctx.Err()
		},
	}
}
