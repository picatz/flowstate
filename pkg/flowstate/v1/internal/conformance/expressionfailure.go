package conformance

import (
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ExpressionFailureCase pairs a workflow whose run fails on an expression with
// the structured account (`RunResponse.error.expression`, #1551 and #1568) both
// drivers must report for it, and the kind they must classify it as.
//
// The account is what a program reads instead of the failure's words, so it is
// held to agree field for field: the same operator and operand types for the
// failures cel-go explains, the same step and location for every failure a step
// claimed, and no account at all for one no step claimed.
type ExpressionFailureCase struct {
	// Name of the case, used for test identification.
	Name string
	// Workflow is the workflow whose run must fail on an expression.
	Workflow *v1.Workflow
	// Want is the account both drivers must report, or nil for none.
	Want *v1.ExpressionFailure
	// WantKind is the classification both drivers must agree on.
	WantKind v1.ErrorKind
}

// ExpressionFailureCases returns the shared cases. The local caller reads the
// account off the error chain through [v1.ExpressionFailureOf] and
// [v1.ExpressionFailure.Reportable]; the durable caller reads it off the run
// failure's details, the way the server does.
func ExpressionFailureCases() []ExpressionFailureCase {
	where := func(line, column int32) *v1.SourceLocation {
		return &v1.SourceLocation{File: "w.yaml", Line: line, Column: column}
	}
	step := func(id string, source *v1.SourceLocation, expression string) *v1.Node {
		return &v1.Node{Id: id, Source: source, Kind: &v1.Node_Value{Value: v1.NewExpr(expression)}}
	}
	bare := func(id string, source *v1.SourceLocation) *v1.ExpressionFailure {
		return &v1.ExpressionFailure{Step: id, Location: source}
	}

	const overload = `steps.n.value + "x"`
	offset := int32(len(`steps.n.value `))
	overloadWant := &v1.ExpressionFailure{
		Operator:      "+",
		OperandTypes:  []string{"int", "string"},
		Subexpression: overload,
		Offset:        proto.Int32(offset),
		Caret:         proto.Int32(offset),
		Step:          "bad",
		Location:      where(6, 5),
	}
	overloadCallee := &v1.Workflow{
		Name:  "callee",
		Steps: []*v1.Node{step("n", nil, "2"), step("bad", where(9, 3), overload)},
	}
	qualified := proto.CloneOf(overloadWant)
	qualified.Step = "callee/bad"
	qualified.Location = nil

	// A callee whose `vars:` fail: evaluated before any of its steps, and by
	// the durable driver in an activity of its own.
	varsCallee := &v1.Workflow{
		Name:  "vars-callee",
		Vars:  map[string]*v1.Value{"q": v1.NewExpr(`[1][size([1]) + 4]`)},
		Steps: []*v1.Node{step("never", nil, "1")},
	}

	return []ExpressionFailureCase{
		{
			Name: "an operation with no overload reports its operator, operands and step",
			Workflow: &v1.Workflow{
				Name:  "failure-overload",
				Steps: []*v1.Node{step("n", nil, "2"), step("bad", where(6, 5), overload)},
			},
			Want:     overloadWant,
			WantKind: v1.ErrorKindExpression,
		},
		{
			Name: "an index out of bounds reports its step and location only",
			Workflow: &v1.Workflow{
				Name:  "failure-index",
				Steps: []*v1.Node{step("bad", where(4, 7), `[1][5]`)},
			},
			Want:     bare("bad", where(4, 7)),
			WantKind: v1.ErrorKindExpression,
		},
		{
			Name: "a division by zero reports its step and location only",
			Workflow: &v1.Workflow{
				Name:  "failure-divide",
				Steps: []*v1.Node{step("bad", where(4, 7), `1 / 0`)},
			},
			Want:     bare("bad", where(4, 7)),
			WantKind: v1.ErrorKindExpression,
		},
		{
			// A step id means something only in the file that declares it, so a
			// failure that crossed a call names the callee and drops the line.
			Name: "a failure inside a callee is qualified by it",
			Workflow: &v1.Workflow{
				Name:  "failure-caller",
				Steps: []*v1.Node{{Id: "called", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: overloadCallee}}}},
			},
			Want:     qualified,
			WantKind: v1.ErrorKindExpression,
		},
		{
			// Nothing claims it: no step is running when a declared output is
			// evaluated, so there is no step to name and no account to report.
			Name: "a failure outside any step reports no account",
			Workflow: &v1.Workflow{
				Name:  "failure-outside-step",
				Steps: []*v1.Node{step("fine", where(4, 7), "1")},
				DeclaredOutputs: []*v1.OutputDeclaration{
					{Name: "r", Value: v1.NewExpr(`1 / size([])`)},
				},
			},
			Want:     nil,
			WantKind: v1.ErrorKindExpression,
		},
		{
			// The run's own `vars:` are evaluated before any step exists.
			Name: "a run whose vars fail reports no account",
			Workflow: &v1.Workflow{
				Name:  "failure-run-vars",
				Vars:  map[string]*v1.Value{"q": v1.NewExpr(`[1][size([1]) + 4]`)},
				Steps: []*v1.Node{step("never", where(4, 7), "1")},
			},
			Want:     nil,
			WantKind: v1.ErrorKindExpression,
		},
		{
			// The call step claims a callee's failing `vars:`, which the durable
			// driver evaluates in the WorkflowVars activity.
			Name: "a callee whose vars fail reports the call step",
			Workflow: &v1.Workflow{
				Name:  "failure-callee-vars",
				Steps: []*v1.Node{{Id: "called", Source: where(5, 3), Kind: &v1.Node_Call{Call: &v1.Call{Workflow: varsCallee}}}},
			},
			Want:     bare("called", where(5, 3)),
			WantKind: v1.ErrorKindExpression,
		},
	}
}
