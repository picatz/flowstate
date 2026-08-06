package flowstatev1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// taskOutputManyItems returns a []any of n small ints, mirroring
// constraints_test.go's manyItems — duplicated rather than shared because
// that helper lives in the external flowstatev1_test package and this file
// needs package-internal access to [checkTaskOutputElementBound].
func taskOutputManyItems(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = i
	}
	return items
}

// taskOutputNestedStruct mirrors constraints_test.go's nestedStruct, for the
// same reason [taskOutputManyItems] does.
func taskOutputNestedStruct(depth int, leaf any) any {
	v := leaf
	for i := 0; i < depth; i++ {
		v = map[string]any{"child": v}
	}
	return v
}

// TestCheckTaskOutputElementBoundElementCountReached pins the same shape
// TestBindRunInputsEnforcesStandardRules (constraints_test.go) pins for the
// input side: a list at the bound passes, one element past it is refused,
// naming both the task and the bound.
func TestCheckTaskOutputElementBoundElementCountReached(t *testing.T) {
	atBound := &Node_Outputs{NamedValues: map[string]*Value{
		"items": NewLiteralList(taskOutputManyItems(maxListElements)...),
	}}
	require.NoError(t, checkTaskOutputElementBound("fetch", atBound))

	pastBound := &Node_Outputs{NamedValues: map[string]*Value{
		"items": NewLiteralList(taskOutputManyItems(maxListElements + 1)...),
	}}
	err := checkTaskOutputElementBound("fetch", pastBound)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"fetch"`, "the refusal must name the task")
	assert.Contains(t, err.Error(), "10000", "the refusal must name the bound reached")
}

// TestCheckTaskOutputElementBoundSumsAcrossNamedValues pins the "total across
// the whole value" accounting [maxListElements]'s own doc comment describes:
// no single named value trips the bound alone, but their sum does.
func TestCheckTaskOutputElementBoundSumsAcrossNamedValues(t *testing.T) {
	half := maxListElements / 2
	out := &Node_Outputs{NamedValues: map[string]*Value{
		"a": NewLiteralList(taskOutputManyItems(half)...),
		"b": NewLiteralList(taskOutputManyItems(half + 1)...),
	}}
	err := checkTaskOutputElementBound("fetch", out)
	require.Error(t, err, "a bound that is only checked per-name lets this through")
}

// TestCheckTaskOutputElementBoundNestedInStruct pins that a list nested
// inside a struct-shaped output value is counted exactly as a top-level one
// is — the bug [checkConstraintValueBound]'s own doc comment describes for
// the input side applies identically here, since the walker is the same one.
func TestCheckTaskOutputElementBoundNestedInStruct(t *testing.T) {
	nested := map[string]any{"records": taskOutputManyItems(maxListElements + 1)}
	out := &Node_Outputs{NamedValues: map[string]*Value{"result": NewValue(nested)}}
	err := checkTaskOutputElementBound("fetch", out)
	require.Error(t, err, "a list nested inside a struct output must still be counted")
}

// TestCheckTaskOutputElementBoundDepthReached pins the independent depth
// bound: a value that never trips the element count can still exhaust the
// walker's own recursion.
func TestCheckTaskOutputElementBoundDepthReached(t *testing.T) {
	out := &Node_Outputs{NamedValues: map[string]*Value{
		"result": NewValue(taskOutputNestedStruct(maxConstraintValueDepth+1, "leaf")),
	}}
	err := checkTaskOutputElementBound("fetch", out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "levels deep")
}

// TestCheckTaskOutputElementBoundNilAndEmpty pins that a nil or empty output
// passes without a walk, matching every other constraint entry point's
// nil-is-a-no-op convention.
func TestCheckTaskOutputElementBoundNilAndEmpty(t *testing.T) {
	require.NoError(t, checkTaskOutputElementBound("fetch", nil))
	require.NoError(t, checkTaskOutputElementBound("fetch", &Node_Outputs{}))
}

// TestEvalInScopeRefusesOversizedTaskOutput is the wiring test: a task
// registered directly (not through http) whose Fn returns a Node_Outputs past
// the element bound must have EvalInScope refuse it, classified
// non-retryable — the size of a task's result cannot change on a retry, so
// retrying only spends a worker's time confirming the same refusal.
func TestEvalInScopeRefusesOversizedTaskOutput(t *testing.T) {
	registry := NewRegistry()
	require.NoError(t, registry.Register(TaskDef{
		Name: "oversized-stub",
		Fn: func(ctx context.Context, inputs map[string]*Value, scope *Scope) (*Node_Outputs, error) {
			return &Node_Outputs{NamedValues: map[string]*Value{
				"items": NewLiteralList(taskOutputManyItems(maxListElements + 1)...),
			}}, nil
		},
	}))
	ctx := NewContextWithRegistry(context.Background(), registry)

	task := &Task{Name: "oversized-stub"}
	out, err := task.EvalInScope(ctx, NewScope("", &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}))
	require.Error(t, err, "a task result past the element bound must be refused")
	require.Nil(t, out)
	assert.Contains(t, err.Error(), "oversized-stub")

	var taskErr *TaskError
	require.ErrorAs(t, err, &taskErr)
	assert.Equal(t, ErrorKindLimitExceeded, taskErr.Kind)
	assert.False(t, ClassifyError(err).Retryable(), "an oversized task result must not be retried")
}

// TestEvalInScopeAllowsTaskOutputAtBound pins the other direction: a task
// result exactly at the bound is not refused, matching
// [TestCheckTaskOutputElementBoundElementCountReached]'s boundary.
func TestEvalInScopeAllowsTaskOutputAtBound(t *testing.T) {
	registry := NewRegistry()
	require.NoError(t, registry.Register(TaskDef{
		Name: "at-bound-stub",
		Fn: func(ctx context.Context, inputs map[string]*Value, scope *Scope) (*Node_Outputs, error) {
			return &Node_Outputs{NamedValues: map[string]*Value{
				"items": NewLiteralList(taskOutputManyItems(maxListElements)...),
			}}, nil
		},
	}))
	ctx := NewContextWithRegistry(context.Background(), registry)

	task := &Task{Name: "at-bound-stub"}
	out, err := task.EvalInScope(ctx, NewScope("", &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}))
	require.NoError(t, err)
	require.NotNil(t, out)
}
