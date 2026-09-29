package flowstatev1_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestAFailureCarriesWhatEveryCalleeOnItsWayWithheld: each call boundary a
// failure leaves wraps it with that callee's set, and the outermost carrier
// answers for the whole chain, so a middle workflow's own set does not hide
// what its callee's failure carried (#2210).
func TestAFailureCarriesWhatEveryCalleeOnItsWayWithheld(t *testing.T) {
	t.Parallel()

	sensitive := func(value string) v1.SensitiveValues {
		return v1.SensitiveInputValues(map[string]*v1.Value{"x": v1.NewLiteral(value)}, map[string]bool{"x": true})
	}
	cause := errors.New("no such key: leaf-secret")
	leaf := fmt.Errorf("workflow %q: %w", "leaf", v1.WithFailureSensitiveValues(cause, sensitive("leaf-secret")))
	middle := fmt.Errorf("workflow %q: %w", "middle", v1.WithFailureSensitiveValues(fmt.Errorf("step %q: %w", "inner", leaf), sensitive("middle-secret")))

	carried := v1.FailureSensitiveValues(middle)
	assert.True(t, carried.IsSensitive("leaf-secret"), "the leaf's set was lost behind the middle's")
	assert.True(t, carried.IsSensitive("middle-secret"), "the middle's set was not carried")

	// The failure itself is unchanged: its text, and what errors.Is finds.
	assert.Equal(t, `workflow "middle": step "inner": workflow "leaf": no such key: leaf-secret`, middle.Error())
	assert.ErrorIs(t, middle, cause)
}

// TestAFailureCarryingNothingIsItself: an empty set, which is every run with
// no debugger, returns the failure as it was, and a failure that was never
// wrapped carries nothing.
func TestAFailureCarryingNothingIsItself(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	require.Same(t, cause, v1.WithFailureSensitiveValues(cause, v1.SensitiveValues{}))
	assert.True(t, v1.FailureSensitiveValues(cause).Empty())
	assert.True(t, v1.FailureSensitiveValues(nil).Empty())
	assert.NoError(t, v1.WithFailureSensitiveValues(nil, v1.WithheldSensitiveValues()))
}

type plainObserver struct{}

func (plainObserver) StepFinished(string, *v1.Node_Outputs, error, bool) {}
func (plainObserver) StepSkipped(string)                                 {}
func (plainObserver) WaitStarted(string, string, time.Duration, bool)    {}

type withholdingObserver struct {
	plainObserver
	withheld []v1.SensitiveValues
}

func (o *withholdingObserver) StepFinishedWithholding(_ string, _ *v1.Node_Outputs, _ error, _ bool, withhold v1.SensitiveValues) {
	o.withheld = append(o.withheld, withhold)
}

// TestTheSetsAreComputedOnlyForAReader: a run computes what a position
// withholds only while something reads it — a debugger, or an observer that
// renders with it, as `flow test`'s transcript does (#2211). An ordinary
// observer costs a run nothing, and the failure it returns carries nothing.
func TestTheSetsAreComputedOnlyForAReader(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	spec := &v1.Workflow{Name: "parent", Profile: v1.CurrentProfile, Steps: []*v1.Node{{
		Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
			Workflow: &v1.Workflow{
				Name:           "child",
				Profile:        v1.CurrentProfile,
				DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
				Steps:          []*v1.Node{{Id: "boom", Kind: &v1.Node_Value{Value: v1.NewExpr(`{"a": 1}[inputs.api_key]`)}}},
			},
			Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
		}},
	}}}

	_, err := v1.RunWithInputs(v1.NewContextWithRunObserver(t.Context(), plainObserver{}), spec, nil)
	require.ErrorContains(t, err, secret)
	assert.True(t, v1.FailureSensitiveValues(err).Empty(), "a run with no reader carried a set")

	reader := &withholdingObserver{}
	_, err = v1.RunWithInputs(v1.NewContextWithRunObserver(t.Context(), reader), spec, nil)
	require.ErrorContains(t, err, secret, "reading the sets changed the failure's text")
	assert.True(t, v1.FailureSensitiveValues(err).IsSensitive(secret), "the failure did not carry the callee's set")
	require.NotEmpty(t, reader.withheld)
	assert.True(t, reader.withheld[0].IsSensitive(secret), "the callee's step was not told its set")
}

// TestAFailedCompensationCarriesWhatItsRegistrationWithheld: a compensation is
// registered with inputs taken where its step ran, a callee's sensitive input
// among them, and its failure quotes them in the run's final message. The
// failure that set off the unwind came from the caller and carries none of it,
// so the run's error carries what each registering position withheld (#2213).
func TestAFailedCompensationCarriesWhatItsRegistrationWithheld(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "make", Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
		return &v1.Node_Outputs{}, nil
	}}))
	require.NoError(t, registry.Register(v1.TaskDef{Name: "release", Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
		// Not retryable, so the compensation fails once rather than until its
		// retries run out.
		return nil, v1.NewTaskError("release", v1.ErrorKindInvalidInput,
			fmt.Errorf("refused to release %s", inputs["key"].GetLiteral().GetStringValue()))
	}}))
	nested := &v1.Node{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
		Workflow: &v1.Workflow{
			Name:           "child",
			Profile:        v1.CurrentProfile,
			DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
			Steps: []*v1.Node{{
				Id:   "made",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "make"}},
				Undo: &v1.Compensation{Task: &v1.Task{Name: "release", Inputs: map[string]*v1.Value{"key": v1.NewExpr("inputs.api_key")}}},
			}},
		},
		Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
	}}}
	boom := &v1.Node{Id: "boom", Kind: &v1.Node_Value{Value: v1.NewExpr(`{"a": 1}["b"]`)}}
	ctx := v1.NewContextWithRegistry(t.Context(), registry)

	// In order, and inside a parallel branch, whose compensations its parent
	// takes over when the branch joins.
	for name, steps := range map[string][]*v1.Node{
		"in order": {nested, boom},
		"in a branch": {{Id: "both", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
			{Steps: []*v1.Node{nested}},
		}}}}, boom},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			spec := &v1.Workflow{Name: "parent", Profile: v1.CurrentProfile, Steps: steps}
			_, err := v1.RunWithInputs(v1.NewContextWithRunObserver(ctx, &withholdingObserver{}), spec, nil)
			require.ErrorContains(t, err, "could not undo", "no compensation failed, so this proves nothing")
			require.ErrorContains(t, err, secret, "the compensation's failure does not quote its input, so this proves nothing")
			carried := v1.FailureSensitiveValues(err)
			assert.NotContains(t, carried.RedactText(err.Error(), "[withheld]"), secret, "the run's final message showed the callee's sensitive input")

			_, err = v1.RunWithInputs(v1.NewContextWithRunObserver(ctx, plainObserver{}), spec, nil)
			require.ErrorContains(t, err, secret)
			assert.True(t, v1.FailureSensitiveValues(err).Empty(), "a run with no reader carried a set")
		})
	}
}
