package flowtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestDurableIneligibleNamesWhyACaseStaysLocal(t *testing.T) {
	logTask := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}
	}
	plain := &v1.Workflow{Name: "plain", Steps: []*v1.Node{logTask("a")}}
	readsLocal := &v1.Workflow{Name: "local", Steps: []*v1.Node{{
		Id: "a",
		Condition: &v1.Value{Kind: &v1.Value_Expr{Expr: &expr.ParsedExpr{Expr: &expr.Expr{ExprKind: &expr.Expr_SelectExpr{SelectExpr: &expr.Expr_Select{
			Operand: &expr.Expr{ExprKind: &expr.Expr_IdentExpr{IdentExpr: &expr.Expr_Ident{Name: "run"}}},
			Field:   "local",
		}}}}}},
		Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
	}}}
	runIdent := func() *expr.Expr {
		return &expr.Expr{ExprKind: &expr.Expr_IdentExpr{IdentExpr: &expr.Expr_Ident{Name: "run"}}}
	}
	conditioned := func(e *expr.Expr) *v1.Workflow {
		return &v1.Workflow{Name: "cond", Steps: []*v1.Node{{
			Id:        "a",
			Condition: &v1.Value{Kind: &v1.Value_Expr{Expr: &expr.ParsedExpr{Expr: e}}},
			Kind:      &v1.Node_Task{Task: &v1.Task{Name: "log"}},
		}}}
	}
	// run["local"]: an index, not a selection, which names the same fact.
	readsIndexed := conditioned(&expr.Expr{ExprKind: &expr.Expr_CallExpr{CallExpr: &expr.Expr_Call{
		Function: "_[_]",
		Args: []*expr.Expr{runIdent(), {ExprKind: &expr.Expr_ConstExpr{ConstExpr: &expr.Constant{
			ConstantKind: &expr.Constant_StringValue{StringValue: "local"},
		}}}},
	}}})
	readsStartedAt := conditioned(&expr.Expr{ExprKind: &expr.Expr_SelectExpr{SelectExpr: &expr.Expr_Select{
		Operand: runIdent(),
		Field:   "started_at",
	}}})
	compensated := &v1.Workflow{Name: "saga", Steps: []*v1.Node{{
		Id:   "a",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
		Undo: &v1.Compensation{},
	}}}

	for name, tc := range map[string]struct {
		test         *Test
		workflow     *v1.Workflow
		compiled     []compiledStub
		unregistered []string
		want         string
	}{
		"nothing in the way":     {test: &Test{}, workflow: plain},
		"reads run indexed":      {test: &Test{}, workflow: readsIndexed, want: "run.local"},
		"reads run.started_at":   {test: &Test{}, workflow: readsStartedAt},
		"signals":                {test: &Test{Signals: []SignalScript{{}}}, workflow: plain},
		"reads run.local":        {test: &Test{}, workflow: readsLocal, want: "run.local"},
		"faults":                 {test: &Test{Faults: []Fault{{}}}, workflow: plain, want: "injects faults"},
		"unregistered stub task": {test: &Test{}, workflow: plain, unregistered: []string{"nope.task"}, want: "nope.task"},
		"step stub, plain":       {test: &Test{}, workflow: plain, compiled: []compiledStub{{step: "a"}}},
		"step stub, compensated": {test: &Test{}, workflow: compensated, compiled: []compiledStub{{step: "a"}}, want: "stubs a step by id"},
		"task stub, compensated": {test: &Test{}, workflow: compensated, compiled: []compiledStub{{task: "log"}}},
	} {
		t.Run(name, func(t *testing.T) {
			got := durableIneligible(tc.test, tc.workflow, tc.compiled, tc.unregistered)
			if tc.want == "" {
				assert.Empty(t, got)

				return
			}
			assert.Contains(t, got, tc.want)
		})
	}
}

func TestUnregisteredTasksFiltersToWhatThisBuildLacks(t *testing.T) {
	assert.Equal(t, []string{"nope.task"}, unregisteredTasks([]string{"log", "nope.task"}))
	assert.Empty(t, unregisteredTasks([]string{"log"}))
}
