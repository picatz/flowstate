package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const deterministicEvaluationRuns = 100

func TestTaskInputFailureOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	task := &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
		"zzz": v1.NewExpr("['z'][9]"),
		"aaa": v1.NewExpr("['a'][5]"),
	}}
	scope := v1.NewScope(v1.CurrentProfile, nil)

	errorText := repeatedError(t, func() error {
		_, err := v1.ResolveTaskInputs(t.Context(), task, scope)
		return err
	})

	require.Contains(t, errorText, `input "aaa": evaluate expression`)
}

func TestCallArgumentFailureOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	arguments := map[string]*v1.Value{
		"zzz": v1.NewExpr("['z'][9]"),
		"aaa": v1.NewExpr("['a'][5]"),
	}
	scope := v1.NewScope(v1.CurrentProfile, nil)

	errorText := repeatedError(t, func() error {
		_, err := v1.ResolveCallArguments(t.Context(), arguments, scope)
		return err
	})

	require.Contains(t, errorText, `argument "aaa": evaluate expression`)
}

func TestWaitOutputBindingFailureOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	invalid := func() *v1.Value {
		return &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{}}}
	}
	raw := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"zzz": invalid(),
		"aaa": invalid(),
	}}
	signal := &v1.Signal{Outputs: map[string]*v1.Value{"result": v1.NewExpr("true")}}
	scope := v1.NewScope(v1.CurrentProfile, nil)

	errorText := repeatedError(t, func() error {
		_, err := v1.ShapeSignalOutputs(t.Context(), signal, raw, scope, time.Time{})
		return err
	})

	require.Contains(t, errorText, `binding "aaa" for outputs shaping`)
}

func repeatedError(t *testing.T, run func() error) string {
	t.Helper()

	errors := make(map[string]struct{})
	for range deterministicEvaluationRuns {
		err := run()
		require.Error(t, err)
		errors[err.Error()] = struct{}{}
	}
	require.Len(t, errors, 1, "one input set reported more than one first failure")
	for errorText := range errors {
		return errorText
	}
	return ""
}
