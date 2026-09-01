package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// theCredential is what a hand-built specification would otherwise carry into
// workflow history, and what a refusal must not repeat back.
const theCredential = "postgres://app:hunter2@db.example:5432/app?sslmode=verify-full"

// registerRequiredSecretTask installs a task requiring its dsn input to be a
// whole secret reference, the shape `plugins/sql` declares.
func registerRequiredSecretTask(t *testing.T) string {
	t.Helper()

	const name = "test-required-secret-input.probe"
	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:                 name,
		SecretInputs:         []string{"dsn"},
		RequiredSecretInputs: []string{"dsn"},
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(name) })

	return name
}

func literalDSN() map[string]*v1.Value {
	return map[string]*v1.Value{"dsn": v1.NewLiteral(theCredential)}
}

func secretRefDSN() map[string]*v1.Value {
	return map[string]*v1.Value{"dsn": {Kind: &v1.Value_SecretRef{
		SecretRef: &v1.SecretRef{Scheme: "env", Name: "APP_DSN"},
	}}}
}

// TestValidateSpecificationRefusesLiteralInRequiredSecretInput is the
// regression: without the admission check the literal is admitted, this
// submission becomes a run, and the credential is durable before the plugin
// host's dispatch-time refusal gets a turn.
func TestValidateSpecificationRefusesLiteralInRequiredSecretInput(t *testing.T) {
	name := registerRequiredSecretTask(t)
	s := &FlowstateServer{}

	wf := &v1.Workflow{Name: "hand-built", Steps: []*v1.Node{{
		Id:   "probe",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: name, Inputs: literalDSN()}},
	}}}

	err := s.validateSpecification(wf)
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.ErrorContains(t, err, `step "probe"`)
	require.ErrorContains(t, err, `input "dsn"`)
	require.NotContains(t, err.Error(), theCredential,
		"the refusal echoed the credential it exists to keep out of durable state")
	require.NotContains(t, err.Error(), "hunter2",
		"the refusal echoed the credential it exists to keep out of durable state")
}

func TestValidateSpecificationRefusesExpressionInRequiredSecretInput(t *testing.T) {
	name := registerRequiredSecretTask(t)
	s := &FlowstateServer{}

	wf := &v1.Workflow{Name: "hand-built-expression", Steps: []*v1.Node{{
		Id: "probe",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: name, Inputs: map[string]*v1.Value{
			"dsn": {Kind: &v1.Value_Expr{Expr: &expr.ParsedExpr{Expr: &expr.Expr{
				ExprKind: &expr.Expr_ConstExpr{ConstExpr: &expr.Constant{
					ConstantKind: &expr.Constant_StringValue{StringValue: theCredential},
				}},
			}}}},
		}}},
	}}}

	err := s.validateSpecification(wf)
	require.ErrorContains(t, err, `input "dsn"`)
	require.NotContains(t, err.Error(), "hunter2")
}

func TestValidateSpecificationRefusesLiteralInUndoAndInsideACallee(t *testing.T) {
	name := registerRequiredSecretTask(t)
	s := &FlowstateServer{}

	t.Run("undo", func(t *testing.T) {
		wf := &v1.Workflow{Name: "hand-built-undo", Steps: []*v1.Node{{
			Id:   "probe",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: name, Inputs: secretRefDSN()}},
			Undo: &v1.Compensation{Task: &v1.Task{Name: name, Inputs: literalDSN()}},
		}}}

		err := s.validateSpecification(wf)
		require.ErrorContains(t, err, `step "probe" undo`)
		require.ErrorContains(t, err, `input "dsn"`)
		require.NotContains(t, err.Error(), "hunter2")
	})

	t.Run("callee", func(t *testing.T) {
		callee := &v1.Workflow{Name: "callee", Steps: []*v1.Node{{
			Id:   "inner",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: name, Inputs: literalDSN()}},
		}}}
		wf := &v1.Workflow{Name: "hand-built-call", Steps: []*v1.Node{{
			Id:   "outer",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
		}}}

		err := s.validateSpecification(wf)
		require.ErrorContains(t, err, `step "inner"`)
		require.ErrorContains(t, err, `input "dsn"`)
		require.NotContains(t, err.Error(), "hunter2")
	})
}

func TestValidateSpecificationAdmitsSecretReferenceUnchanged(t *testing.T) {
	name := registerRequiredSecretTask(t)
	s := &FlowstateServer{}

	wf := &v1.Workflow{Name: "referenced", Steps: []*v1.Node{{
		Id:   "probe",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: name, Inputs: secretRefDSN()}},
		Undo: &v1.Compensation{Task: &v1.Task{Name: name, Inputs: secretRefDSN()}},
	}}}
	before := proto.Clone(wf).(*v1.Workflow)

	require.NoError(t, s.validateSpecification(wf))
	require.True(t, proto.Equal(before.GetSteps()[0], wf.GetSteps()[0]),
		"admission rewrote a step whose secret inputs were already whole references")
}
