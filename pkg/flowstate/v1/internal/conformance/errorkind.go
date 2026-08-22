package conformance

import (
	"net/http"

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
}

// ErrorKindCases returns workflows engineered to fail with a known,
// deterministic [v1.ErrorKind] — no retries, no network flakiness, nothing
// that depends on timing — so the assertion is about classification and
// nothing else. The httpBaseURL should come from [NewHTTPServer], which also
// registers the loopback-permitting http task both cases below need.
func ErrorKindCases(httpBaseURL string) []ErrorKindCase {
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
	}
}
