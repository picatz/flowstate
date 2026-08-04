package flowstatev1_test

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestRunWorkflowHasGuardOnToleratedSuccess covers
// [tests.ToleratedSuccessHasGuardCases] in the local driver — the value
// `has(steps.<id>.error)` must read once a `continue_on_error` step has actually
// succeeded. The same cases run against the durable driver in the engine
// package's TestRunWorkflowHasGuardOnToleratedSuccess, which is what keeps the
// two from disagreeing about a distinction (has() false vs. a hard "no such key"
// error) that only the durable driver's Continue-As-New compaction can get
// wrong. See #176.
//
// Deliberately its own file rather than an addition to eval_test.go: that file
// is in another agent's blast radius (a clock is being threaded through eval.go
// concurrently), so this caller registration lives beside it instead of inside
// it, avoiding a mid-flight collision on a file this change does not otherwise
// need to touch. runWorkflow itself is defined in eval_test.go and reused here
// unchanged — same package, same test binary, one helper.
func TestRunWorkflowHasGuardOnToleratedSuccess(t *testing.T) {
	for _, test := range tests.ToleratedSuccessHasGuardCases() {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}
