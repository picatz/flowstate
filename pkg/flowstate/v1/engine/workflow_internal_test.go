package engine

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/assert"
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
			task := &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr(tc.expr),
			}}
			resolved, err := v1.ResolveTaskInputs(t.Context(), task, v1.NewScope(v1.CurrentProfile, prev))
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
			task := &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr(tc.expr),
			}}
			resolved, err := v1.ResolveTaskInputs(t.Context(), task, v1.NewScope(v1.CurrentProfile, prev))
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
			// A literal carries no references, whatever it happens to spell.
			//
			// This case used to be the opposite claim: the retired `cel` task took its
			// expression as a literal string, so compaction special-cased that one task
			// name and parsed the string looking for step references. `cel` retired at
			// edition v2026.2 and the special case went with it, which makes the rule
			// uniform — an expression is a `Value_Expr` and nothing else is one.
			//
			// Worth a case rather than a deletion, because the failure it guards is
			// silent and expensive in the other direction: something that reads like an
			// expression being *treated* as one would drag steps into the carryover that
			// nothing names, and the cost is history size on exactly the long runs this
			// engine exists for.
			name: "a literal that looks like an expression references nothing",
			task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewLiteral("a.result + '!'")}},
			want: map[string][]string{},
		},
		{
			name: "non-cel expr inputs reference a.result and b.foo",
			task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("a.result + string(b.foo)")}},
			want: map[string][]string{"a": {"result"}, "b": {"foo"}},
		},
		{
			name: "reference whole step ident",
			task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("a")}},
			want: map[string][]string{"a": {}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trimmed := compactPrevOutputsForTask(tc.task, prev)
			require.NotNil(t, trimmed)
			// Nothing beyond what is referenced, as well as nothing missing. Checking
			// only the steps named below is satisfied by a compaction that trims
			// nothing at all, which is the direction that costs history rather than
			// the run — and so the direction nobody notices.
			require.Len(t, trimmed.GetStepValues(), len(tc.want),
				"kept the wrong number of steps: %v", stepIDsOf(trimmed))
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
	task := &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
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
		{Id: "s1", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
			"message": v1.NewExpr("a.result")}}}},
		{Id: "s2", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
			"message": v1.NewExpr("string(b.foo)")}}}},
		// Was a `cel` step, whose expression arrived as a literal string and needed a
		// special case in collectValueRefs to be seen at all. Both retired at edition
		// v2026.2; an expression input is now the only thing that carries a reference,
		// and what this step is here for — a third distinct step, referenced only from
		// the tail of the walk — is unchanged.
		{Id: "s3", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
			"message": v1.NewExpr("c.bar + '!'")}}}},
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
			trimmed := compactOutputsForRemainingSteps(steps, tc.from, prev, nil)
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

// TestCompactOutputsForRemainingSteps_UndoInputs is the neighbouring hole behind
// #418 slice 1's compensation-join finding, in the caller that suffers from it
// worst.
//
// A compensation's inputs are resolved at the instant its step *succeeds*, so an
// output only they name has to survive the handover exactly as one a task's own
// inputs name does. Missing it prunes the output at Continue-As-New and fails the
// run in the next segment at the moment it registers the compensation — after the
// step's effect has already happened, which is the one outcome the undo invariant
// forbids. It is also precisely the failure the schema comment on `PendingUndo`
// predicts for "a fifth reference site that walk does not know about".
func TestCompactOutputsForRemainingSteps_UndoInputs(t *testing.T) {
	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("a")}},
	}}

	// The remaining step's own task names nothing; only its `undo:` does.
	steps := []*v1.Node{
		{Id: "s0", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}},
		{
			Id:   "s1",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")}}},
			Undo: &v1.Compensation{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("a.result"),
			}}},
		},
	}

	trimmed := compactOutputsForRemainingSteps(steps, 1, prev, nil)
	require.NotNil(t, trimmed)

	outs, kept := trimmed.StepValues["a"]
	require.True(t, kept, "an output only a remaining step's `undo:` names was pruned at the handover")
	require.Contains(t, outs.GetNamedValues(), "result")
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
		}, false)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, 30*time.Second, appErr.NextRetryDelay())
		require.False(t, appErr.NonRetryable(), "carrying a delay must not make it permanent")
	})

	t.Run("a retryable failure with no delay stays retryable and says the same thing", func(t *testing.T) {
		// This used to assert the error was returned *unchanged*, on the reasoning
		// that an unwrapped error is retryable by default. It is wrapped now, so
		// that the application error's message carries the text a tolerated failure
		// records — which is how `${steps.<id>.error}` reads the same under either
		// driver.
		//
		// So what is pinned is the property that assertion was standing in for,
		// rather than the identity that happened to deliver it: still retryable,
		// still typed by kind, with the cause reachable for anything that classifies
		// on it.
		original := &v1.TaskError{Task: "http", Kind: v1.ErrorKindUpstream, Err: errors.New("boom")}

		err := activityError("http", original, false)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.False(t, appErr.NonRetryable(), "a retryable kind must not become permanent")
		require.Zero(t, appErr.NextRetryDelay(), "no delay was asked for, so none may be imposed")
		require.Equal(t, v1.ErrorKindUpstream.String(), appErr.Type())
		require.Equal(t, v1.StepErrorText(original), appErr.Message(),
			"the message is how the recorded text crosses the activity boundary")

		var taskErr *v1.TaskError
		require.ErrorAs(t, err, &taskErr, "the classified cause must stay reachable")
		require.Same(t, original, taskErr)
	})

	t.Run("a permanent failure ignores a delay", func(t *testing.T) {
		err := activityError("http", &v1.TaskError{
			Task:       "http",
			Kind:       v1.ErrorKindInvalidInput,
			Err:        errors.New("bad request"),
			RetryAfter: 30 * time.Second,
		}, false)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.True(t, appErr.NonRetryable())
	})

	t.Run("a rate-limited failure with a delay carries it (#912)", func(t *testing.T) {
		// ErrorKindRateLimited alongside the Upstream case above: the gate this
		// function applies is kind.Retryable(), not a specific kind, but #912's
		// defect was precisely a kind that should have gated true and did not
		// (ErrorKindInvalidInput, permanent, classifying 429). This pins the
		// kind that replaced it the same way, rather than trusting that a
		// generic gate covers a member nothing named explicitly.
		err := activityError("http", &v1.TaskError{
			Task:       "http",
			Kind:       v1.ErrorKindRateLimited,
			Err:        errors.New("429 Too Many Requests"),
			RetryAfter: 30 * time.Second,
		}, false)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, 30*time.Second, appErr.NextRetryDelay())
		require.False(t, appErr.NonRetryable(), "RateLimited must be retryable, or its Retry-After stays inert")
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

		err := activityError("http", wrapped, false)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, 5*time.Second, appErr.NextRetryDelay())
	})

	t.Run("a non-task error is unaffected", func(t *testing.T) {
		require.Zero(t, v1.RetryAfter(errors.New("plain")))
		require.Zero(t, v1.RetryAfter(nil))
	})
}

// Test_activityError_category covers #750: a step whose policy tolerates its
// task's failure (`continue_on_error: true`) must produce a Temporal
// ApplicationError categorized [temporal.ApplicationErrorCategoryBenign], and
// an untolerated failure of the identical shape must not — on every branch
// [activityError] can take, since the bug this closes was that none of them
// ever set Category at all.
func Test_activityError_category(t *testing.T) {
	t.Run("a tolerated retryable failure with a delay is benign", func(t *testing.T) {
		err := activityError("http", &v1.TaskError{
			Task:       "http",
			Kind:       v1.ErrorKindUpstream,
			Err:        errors.New("429 Too Many Requests"),
			RetryAfter: 30 * time.Second,
		}, true)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, temporal.ApplicationErrorCategoryBenign, appErr.Category())
	})

	t.Run("an untolerated retryable failure with a delay is not benign", func(t *testing.T) {
		err := activityError("http", &v1.TaskError{
			Task:       "http",
			Kind:       v1.ErrorKindUpstream,
			Err:        errors.New("429 Too Many Requests"),
			RetryAfter: 30 * time.Second,
		}, false)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, temporal.ApplicationErrorCategoryUnspecified, appErr.Category())
	})

	t.Run("a tolerated retryable failure with no delay is benign", func(t *testing.T) {
		err := activityError("http",
			&v1.TaskError{Task: "http", Kind: v1.ErrorKindUpstream, Err: errors.New("boom")}, true)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, temporal.ApplicationErrorCategoryBenign, appErr.Category())
	})

	t.Run("an untolerated retryable failure with no delay is not benign", func(t *testing.T) {
		err := activityError("http",
			&v1.TaskError{Task: "http", Kind: v1.ErrorKindUpstream, Err: errors.New("boom")}, false)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, temporal.ApplicationErrorCategoryUnspecified, appErr.Category())
	})

	t.Run("a tolerated non-retryable failure is benign and still permanent", func(t *testing.T) {
		err := activityError("http", &v1.TaskError{
			Task: "http",
			Kind: v1.ErrorKindInvalidInput,
			Err:  errors.New("bad request"),
		}, true)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.True(t, appErr.NonRetryable(), "tolerating a failure must not make it retry")
		require.Equal(t, temporal.ApplicationErrorCategoryBenign, appErr.Category())
	})

	t.Run("an untolerated non-retryable failure is not benign", func(t *testing.T) {
		err := activityError("http", &v1.TaskError{
			Task: "http",
			Kind: v1.ErrorKindInvalidInput,
			Err:  errors.New("bad request"),
		}, false)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.True(t, appErr.NonRetryable())
		require.Equal(t, temporal.ApplicationErrorCategoryUnspecified, appErr.Category())
	})

	t.Run("a tolerated policy denial is not benign", func(t *testing.T) {
		err := activityError("http", &v1.TaskError{
			Task: "http",
			Kind: v1.ErrorKindPolicyDenied,
			Err:  errors.New("egress denied"),
		}, true)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.True(t, appErr.NonRetryable())
		require.Equal(t, v1.ErrorKindPolicyDenied.String(), appErr.Type())
		require.Equal(t, temporal.ApplicationErrorCategoryUnspecified, appErr.Category())
	})

	t.Run("a nil error stays nil regardless of tolerance", func(t *testing.T) {
		require.NoError(t, activityError("http", nil, true))
		require.NoError(t, activityError("http", nil, false))
	})
}

// TestCompactPrevOutputsForTask_RootedReferences is the test that would have
// caught this walker being left behind.
//
// It is a second, independent implementation of a job flowfile's validator also
// does — reading which steps an expression names — and only one of the two has a
// compiler watching it. When references gained a `steps.` root, this one saw
// `Ident("steps")`, matched no step of that name, recorded nothing, and the
// caller pruned every output to an empty map. No error at compile time, none at
// submit; the run would simply find nothing where its inputs used to be, and only
// after a Continue-As-New, which is to say only on the long runs this engine
// exists for.
func TestCompactPrevOutputsForTask_RootedReferences(t *testing.T) {
	prev := &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"a": {NamedValues: map[string]*v1.Value{
				"result": v1.NewLiteral("hello"),
				"other":  v1.NewLiteral("unused"),
			}},
			"b": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("bye")}},
			"c": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("unused")}},
		},
	}

	tests := []struct {
		name string
		expr string
		// want maps each step kept to the outputs kept on it. A step absent from
		// want must be pruned; an empty set means the whole step is kept.
		want map[string][]string
	}{
		{
			name: "a rooted reference keeps exactly its own output",
			expr: "steps.a.result",
			want: map[string][]string{"a": {"result"}},
		},
		{
			name: "the bare form still works, for a spec compiled before the root",
			expr: "a.result",
			want: map[string][]string{"a": {"result"}},
		},
		{
			// The shape a fixed-depth match misses. `steps.a.result.code` is three
			// selects over the root, and reaching only two of them leaves the
			// reference unrecognised — which prunes everything rather than one thing.
			name: "selecting into an output keeps the output",
			expr: "steps.a.result.startsWith('h')",
			want: map[string][]string{"a": {"result"}},
		},
		{
			name: "naming a step keeps all of it",
			expr: "steps.a",
			want: map[string][]string{"a": {}},
		},
		{
			name: "two rooted references keep both steps and neither of the third",
			expr: "steps.a.result + steps.b.result",
			want: map[string][]string{"a": {"result"}, "b": {"result"}},
		},
		{
			// Nothing here can narrow it, so nothing is dropped. Being wrong this way
			// costs history size; being wrong the other way costs the run.
			name: "the root named on its own keeps everything",
			expr: "size(steps)",
			want: map[string][]string{"a": {}, "b": {}, "c": {}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewExpr(tc.expr)},
			}

			trimmed := compactPrevOutputsForTask(task, prev)

			require.Len(t, trimmed.GetStepValues(), len(tc.want),
				"kept the wrong number of steps: %v", stepIDsOf(trimmed))
			for id, outputs := range tc.want {
				kept, ok := trimmed.GetStepValues()[id]
				require.True(t, ok, "step %q was pruned but is referenced", id)
				if len(outputs) == 0 {
					continue
				}
				for _, name := range outputs {
					assert.Contains(t, kept.GetNamedValues(), name,
						"step %q lost output %q", id, name)
				}
				assert.Len(t, kept.GetNamedValues(), len(outputs),
					"step %q kept outputs nothing referenced", id)
			}
		})
	}
}

func stepIDsOf(o *v1.Workflow_StepOutputs) []string {
	ids := make([]string, 0, len(o.GetStepValues()))
	for id := range o.GetStepValues() {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
