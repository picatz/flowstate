package flowstatev1

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/interpreter"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// TypeAdapter is the default type adapter used for CEL evaluation in Flowstate.
//
// In the future this might not just be the default one from the CEL library, but
// a custom one that provides additional functionality or type handling specific to Flowstate
// in the future.
var TypeAdapter = types.DefaultTypeAdapter

// Ensure StepsOutputActivation implements the interpreter.Activation interface.
var _ interpreter.Activation = (*StepsOutputActivation)(nil)

// StepsOutputActivation is an interpreter activation that provides access to the
// outputs of the previous steps in a workflow. It allows resolving values by their names
// and supports nested field access using dot notation (e.g., "stepName.fieldName").
type StepsOutputActivation struct {
	Prev *Workflow_StepOutputs
}

func (e *StepsOutputActivation) ResolveName(name string) (any, bool) {
	if e.Prev == nil {
		return nil, false
	}
	if e.Prev.StepValues == nil {
		return nil, false
	}

	selectorFields := strings.Split(name, ".")
	if len(selectorFields) == 0 {
		return nil, false
	}
	stepName := selectorFields[0]

	outputs, hasVal := e.Prev.StepValues[stepName]
	if !hasVal {
		return nil, false
	}

	if len(selectorFields) == 1 {
		return outputs, true
	}
	currentVal := outputs
	for _, field := range selectorFields[1:] {
		if currentVal == nil || currentVal.NamedValues == nil {
			return nil, false
		}
		nextVal, hasField := currentVal.NamedValues[field]
		if !hasField {
			return nil, false
		}
		currentVal = &Node_Outputs{
			NamedValues: map[string]*Value{
				field: nextVal,
			},
		}
	}

	v := currentVal.NamedValues[selectorFields[len(selectorFields)-1]]
	switch v.GetKind().(type) {
	case *Value_Expr:
		ast := cel.ParsedExprToAst(v.GetExpr())
		env, err := cel.NewEnv()
		if err != nil {
			return nil, false
		}
		prg, err := env.Program(ast)
		if err != nil {
			return nil, false
		}
		out, _, err := prg.Eval(cel.Activation(e))
		if err != nil {
			return nil, false
		}
		return out, true
	case *Value_Literal:
		rv, err := cel.ValueToRefValue(TypeAdapter, v.GetLiteral())
		if err != nil {
			return nil, false
		}
		return rv, true
	default:
		return nil, false
	}
}

func (e *StepsOutputActivation) Parent() interpreter.Activation {
	return nil
}

func NewLiteralList(vals ...any) *Value {
	literals := make([]*expr.Value, 0, len(vals))
	for _, v := range vals {
		literals = append(literals, NewLiteral(v).GetLiteral())
	}
	return &Value{
		Kind: &Value_Literal{
			Literal: &expr.Value{
				Kind: &expr.Value_ListValue{
					ListValue: &expr.ListValue{
						Values: literals,
					},
				},
			},
		},
	}
}

func NewLiteral(val any) *Value {
	switch v := val.(type) {
	case string:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_StringValue{
						StringValue: v,
					},
				},
			},
		}
	case int:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_Int64Value{
						Int64Value: int64(v),
					},
				},
			},
		}
	case float64:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_DoubleValue{
						DoubleValue: v,
					},
				},
			},
		}
	case float32:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_DoubleValue{
						DoubleValue: float64(v),
					},
				},
			},
		}
	case int64:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_Int64Value{
						Int64Value: v,
					},
				},
			},
		}
	case bool:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_BoolValue{
						BoolValue: v,
					},
				},
			},
		}
	case *expr.Value:
		return &Value{
			Kind: &Value_Literal{
				Literal: v,
			},
		}
	case error:
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Errorf("flowstatev1: error value: %w", v).Error(),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	default:
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Sprintf("flowstatev1: unsupported type for new value: %T", v),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	}
}

func NewExpr(exprStr string) *Value {
	v, err := newValueExprWithErr(exprStr)
	if err != nil {
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Errorf("failed to create CEL expression: %w", err).Error(),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	}
	return v
}

func newValueExprWithErr(exprStr string) (*Value, error) {
	env, err := cel.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	ast, issues := env.Parse(exprStr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to parse CEL expression: %w", issues.Err())
	}

	parsedExpr, err := cel.AstToParsedExpr(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to convert AST to parsed expression: %w", err)
	}

	return &Value{
		Kind: &Value_Expr{
			Expr: parsedExpr,
		},
	}, nil
}

func NewValue(v any) *Value {
	if v == nil {
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_NullValue{},
				},
			},
		}
	}

	switch val := v.(type) {
	case *Value:
		return val
	case string, int, float64, float32, int64, bool, *expr.Value:
		return NewLiteral(val)
	case []any:
		return NewLiteralList(val...)
	case error:
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Errorf("flowstatev1: error value: %w", val).Error(),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	default:
		// Handle other slice types using reflection
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			// Convert slice to []any
			slice := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				slice[i] = rv.Index(i).Interface()
			}
			return NewLiteralList(slice...)
		}

		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Sprintf("flowstatev1: unsupported type for new value: %T", val),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	}
}

func NewNamedValues(inputValues map[string]any) map[string]*Value {
	if inputValues == nil {
		return nil
	}

	outputValues := make(map[string]*Value, len(inputValues))
	for name, val := range inputValues {
		outputValues[name] = NewValue(val)
	}
	return outputValues
}

func (v *Value) Error() error {
	if errKind := v.GetError(); errKind != nil {
		return fmt.Errorf("flowstatev1: value error: %s (code: %s)", errKind.Message, errKind.Code.String())
	}
	return nil
}

func Run(ctx context.Context, w *Workflow) (*Workflow_StepOutputs, error) {
	return eval(ctx, w)
}

func eval(ctx context.Context, w *Workflow) (*Workflow_StepOutputs, error) {
	if w == nil || len(w.Steps) == 0 {
		return nil, fmt.Errorf("workflow cannot be nil or empty")
	}

	stepOutputs := &Workflow_StepOutputs{
		StepValues: make(map[string]*Node_Outputs),
	}

	for _, node := range w.Steps {
		switch n := node.Kind.(type) {
		case *Node_Task:
			taskResult, err := n.Task.Eval(ctx, stepOutputs)
			if err != nil {
				return nil, err
			}
			stepOutputs.StepValues[node.Id] = taskResult
		default:
			return nil, fmt.Errorf("unsupported node kind: %T", n)
		}
	}

	return stepOutputs, nil
}

func (t *Task) Eval(ctx context.Context, prevStepOutputs *Workflow_StepOutputs) (*Node_Outputs, error) {
	if t == nil {
		return nil, fmt.Errorf("task cannot be nil")
	}
	f, ok := taskLibrary[t.Name]
	if !ok {
		return nil, fmt.Errorf("unsupported task name: %s", t.Name)
	}
	if f == nil {
		return nil, fmt.Errorf("task function for %s is nil", t.Name)
	}
	return f(ctx, t.Inputs, prevStepOutputs)
}
