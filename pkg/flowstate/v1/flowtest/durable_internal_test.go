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
