package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
)

func TestResolveTaskInputs_PreResolveValueExprs(t *testing.T) {
	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
	}}

	tests := []struct {
		name   string
		expr   string
		expect string
	}{
		{name: "direct select", expr: "a.result", expect: "hi"},
		{name: "call on select", expr: "string(a.result)", expect: "hi"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr(tc.expr),
			}}
			resolved, err := v1.ResolveTaskInputs(t.Context(), task, v1.NewScope(prev))
			require.NoError(t, err)
			got := resolved.Inputs["message"].GetLiteral().GetStringValue()

			// Resolution returns a copy; the original must be untouched so a
			// loop body's task can be resolved again for the next iteration.
			require.NotNil(t, task.Inputs["message"].GetExpr(),
				"resolution must not mutate the task it was given")
			require.Equal(t, tc.expect, got)
		})
	}
}

func TestResolveTaskInputs_MixedTypes_Table(t *testing.T) {
	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"s": {NamedValues: map[string]*v1.Value{
			"str": v1.NewLiteral("ok"),
		}},
		"n": {NamedValues: map[string]*v1.Value{
			"num": v1.NewLiteral(int64(2)),
		}},
		"b": {NamedValues: map[string]*v1.Value{
			"flag": v1.NewLiteral(true),
		}},
	}}

	tests := []struct {
		name      string
		expr      string
		wantStr   *string
		wantInt64 *int64
		wantBool  *bool
	}{
		{name: "string select", expr: "s.str", wantStr: strp("ok")},
		{name: "string call", expr: "string(n.num)", wantStr: strp("2")},
		{name: "int math", expr: "n.num + 3", wantInt64: intp(5)},
		{name: "bool select", expr: "b.flag", wantBool: boolp(true)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr(tc.expr),
			}}
			resolved, err := v1.ResolveTaskInputs(t.Context(), task, v1.NewScope(prev))
			require.NoError(t, err)
			lit := resolved.Inputs["message"].GetLiteral()
			if tc.wantStr != nil {
				require.Equal(t, *tc.wantStr, lit.GetStringValue())
			}
			if tc.wantInt64 != nil {
				require.Equal(t, *tc.wantInt64, lit.GetInt64Value())
			}
			if tc.wantBool != nil {
				require.Equal(t, *tc.wantBool, lit.GetBoolValue())
			}
		})
	}
}

func strp(s string) *string { return &s }
func intp(i int64) *int64   { return &i }
func boolp(b bool) *bool    { return &b }

func TestCompactPrevOutputsForTask_MinimalSubset(t *testing.T) {
	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": {NamedValues: map[string]*v1.Value{
			"result": v1.NewLiteral("hi"),
			"other":  v1.NewLiteral("nope"),
		}},
		"b": {NamedValues: map[string]*v1.Value{
			"foo": v1.NewLiteral(int64(42)),
		}},
	}}

	tests := []struct {
		name string
		task *v1.Task
		want map[string][]string // step -> fields (empty list => whole step)
	}{
		{
			name: "cel task string expr references a.result",
			task: &v1.Task{Name: "cel", Inputs: map[string]*v1.Value{
				"expr": v1.NewLiteral("a.result + '!'")}},
			want: map[string][]string{"a": {"result"}},
		},
		{
			name: "non-cel expr inputs reference a.result and b.foo",
			task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("a.result + string(b.foo)")}},
			want: map[string][]string{"a": {"result"}, "b": {"foo"}},
		},
		{
			name: "reference whole step ident",
			task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("a")}},
			want: map[string][]string{"a": {}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trimmed := compactPrevOutputsForTask(tc.task, prev)
			require.NotNil(t, trimmed)
			// Validate steps present
			for step, fields := range tc.want {
				outs, ok := trimmed.StepValues[step]
				require.True(t, ok, "missing step %s", step)
				if len(fields) == 0 {
					// whole step expected
					require.Contains(t, outs.NamedValues, "result")
				} else {
					// only requested fields
					require.Len(t, outs.NamedValues, len(fields))
					for _, f := range fields {
						require.Contains(t, outs.NamedValues, f)
					}
				}
			}
		})
	}
}

func TestCompactPrevOutputsForTask_MissingRefs(t *testing.T) {
	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
	}}
	// Reference non-existent step "x"; expect empty subset
	task := &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
		"message": v1.NewExpr("x.result"),
	}}
	trimmed := compactPrevOutputsForTask(task, prev)
	require.NotNil(t, trimmed)
	require.Empty(t, trimmed.StepValues)
}

func TestCompactOutputsForRemainingSteps_Table(t *testing.T) {
	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("a")}},
		"b": {NamedValues: map[string]*v1.Value{"foo": v1.NewLiteral(int64(1))}},
		"c": {NamedValues: map[string]*v1.Value{"bar": v1.NewLiteral("c")}},
	}}

	steps := []*v1.Node{
		{Id: "s1", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
			"message": v1.NewExpr("a.result")}}}},
		{Id: "s2", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
			"message": v1.NewExpr("string(b.foo)")}}}},
		{Id: "s3", Kind: &v1.Node_Task{Task: &v1.Task{Name: "cel", Inputs: map[string]*v1.Value{
			"expr": v1.NewLiteral("c.bar + '!'")}}}},
	}

	tests := []struct {
		name   string
		from   int
		expect map[string][]string
	}{
		{name: "from 0 includes a.result,b.foo,c.bar", from: 0, expect: map[string][]string{"a": {"result"}, "b": {"foo"}, "c": {"bar"}}},
		{name: "from 1 excludes a, includes b.foo,c.bar", from: 1, expect: map[string][]string{"b": {"foo"}, "c": {"bar"}}},
		{name: "from 2 includes only c.bar", from: 2, expect: map[string][]string{"c": {"bar"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trimmed := compactOutputsForRemainingSteps(steps, tc.from, prev)
			require.NotNil(t, trimmed)
			require.Equal(t, len(tc.expect), len(trimmed.StepValues))
			for step, fields := range tc.expect {
				outs, ok := trimmed.StepValues[step]
				require.True(t, ok, "missing step %s", step)
				require.Len(t, outs.NamedValues, len(fields))
				for _, f := range fields {
					require.Contains(t, outs.NamedValues, f)
				}
			}
		})
	}
}

// Test_activityError_retryAfter covers carrying a server's Retry-After to the
// substrate, and the two ways that could go wrong.
//
// The delay belongs on the retryable path and only there: a non-retryable
// application error has no next attempt to delay, so setting it would be inert while
// looking implemented.
func Test_activityError_retryAfter(t *testing.T) {
	t.Run("a retryable failure with a delay carries it", func(t *testing.T) {
		err := activityError("http", &v1.TaskError{
			Task:       "http",
			Kind:       v1.ErrorKindUpstream,
			Err:        errors.New("429 Too Many Requests"),
			RetryAfter: 30 * time.Second,
		})

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, 30*time.Second, appErr.NextRetryDelay())
		require.False(t, appErr.NonRetryable(), "carrying a delay must not make it permanent")
	})

	t.Run("a retryable failure with no delay takes exactly its old path", func(t *testing.T) {
		// The plugin host depends on this: a retryable kind with no delay must be
		// returned unchanged, since an unwrapped error is retryable by default.
		original := &v1.TaskError{Task: "http", Kind: v1.ErrorKindUpstream, Err: errors.New("boom")}

		err := activityError("http", original)
		require.Same(t, original, err, "the zero case must stay on the path it takes today")
	})

	t.Run("a permanent failure ignores a delay", func(t *testing.T) {
		err := activityError("http", &v1.TaskError{
			Task:       "http",
			Kind:       v1.ErrorKindInvalidInput,
			Err:        errors.New("bad request"),
			RetryAfter: 30 * time.Second,
		})

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.True(t, appErr.NonRetryable())
	})

	t.Run("a delay is found through wrapping", func(t *testing.T) {
		// A plugin's failure arrives inside fmt.Errorf("plugin %q: %w", ...), so a
		// type assertion would miss every one of them.
		wrapped := fmt.Errorf("plugin %q: %w", "example", &v1.TaskError{
			Task:       "http",
			Kind:       v1.ErrorKindUpstream,
			Err:        errors.New("503"),
			RetryAfter: 5 * time.Second,
		})

		err := activityError("http", wrapped)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, 5*time.Second, appErr.NextRetryDelay())
	})

	t.Run("a non-task error is unaffected", func(t *testing.T) {
		require.Zero(t, v1.RetryAfter(errors.New("plain")))
		require.Zero(t, v1.RetryAfter(nil))
	})
}
