package flowfile

import (
	"fmt"
	"regexp"

	yaml "github.com/goccy/go-yaml"
	"github.com/google/cel-go/cel"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/proto"
)

// Internal intermediary types for unmarshaling/marshaling the Flowfile DSL
// to and from the flowstatev1.Workflow proto representation. These types
// correspond to the structure of the Flowfile YAML-based DSL document, which itself
// is a simplified representation of the flowstatev1.Workflow proto for ease of
// human consumption and authoring.
type (
	flowfile struct {
		Name        string         `yaml:"name"`
		Description string         `yaml:"description,omitempty"`
		Steps       []flowfileStep `yaml:"steps"`
	}

	flowfileStep struct {
		ID   string        `yaml:"id"`
		Task *flowfileTask `yaml:"task"`
	}

	flowfileTask struct {
		Name        string         `yaml:"name"`
		Description string         `yaml:"description,omitempty"`
		Inputs      map[string]any `yaml:"inputs"`
	}
)

// flowfileExprPattern matches strings of the form ${...} for CEL expressions
// within a Flowfile document.
var flowfileExprPattern = regexp.MustCompile(`^\$\{(.+)\}$`)

// toProto converts a Flowfile DSL to a flowstatev1.Workflow proto
func (f *flowfile) toProto() (*v1.Workflow, error) {
	steps := make([]*v1.Node, len(f.Steps))
	for i, s := range f.Steps {
		if s.Task == nil {
			return nil, fmt.Errorf("step %q missing task", s.ID)
		}

		task := &v1.Task{
			Name:   s.Task.Name,
			Inputs: map[string]*v1.Value{},
		}
		for k, v := range s.Task.Inputs {
			if k == "vars" {
				varsMap, ok := v.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("step %s vars must be a map", s.ID)
				}
				for vk, vv := range varsMap {
					val, err := toProtoValue(vv)
					if err != nil {
						return nil, fmt.Errorf("step %s var %s: %w", s.ID, vk, err)
					}
					task.Inputs[vk] = val
				}
				continue
			}
			val, err := toProtoValue(v)
			if err != nil {
				return nil, fmt.Errorf("step %s input %s: %w", s.ID, k, err)
			}
			task.Inputs[k] = val
		}
		steps[i] = &v1.Node{
			Id:   s.ID,
			Kind: &v1.Node_Task{Task: task},
		}
	}
	return &v1.Workflow{
		Name:        f.Name,
		Description: proto.String(f.Description),
		Steps:       steps,
	}, nil
}

// toProtoValue converts a value from the Flowfile DSL to a flowstatev1.Value
// proto, representing either a literal or an expression.
func toProtoValue(v any) (*v1.Value, error) {
	switch val := v.(type) {
	case string:
		// Expression string
		if m := flowfileExprPattern.FindStringSubmatch(val); m != nil {
			valueExpr := v1.NewExpr(m[1])
			if valueExpr.GetError() != nil {
				return nil, fmt.Errorf("invalid expression %q: %w", m[1], valueExpr.Error())
			}
			return valueExpr, nil
		}
		// Literal string
		return v1.NewValue(val), nil
	case int, int64, float64, bool, map[string]any, []any:
		// Other supported literal types
		return v1.NewValue(val), nil
	default:
		return nil, fmt.Errorf("unsupported input type: %T", v)
	}
}

// toInt64 converts int or int64 to int64, otherwise returns 0.
func toInt64(v any) int64 {
	switch i := v.(type) {
	case int:
		return int64(i)
	case int64:
		return i
	}
	return 0
}

// Unmarshal parses a Flowfile YAML-based DSL representation into a
// flowstatev1.Workflow proto representation that can be evaluated.
func Unmarshal(data []byte) (*v1.Workflow, error) {
	var f flowfile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	return f.toProto()
}

// Marshal writes a flowstatev1.Workflow proto to a Flowfile
// YAML-based DSL representation that is more human-friendly.
func Marshal(wf *v1.Workflow) ([]byte, error) {
	f := &flowfile{
		Name:        wf.GetName(),
		Description: wf.GetDescription(),
		Steps:       make([]flowfileStep, len(wf.Steps)),
	}
	for i, step := range wf.Steps {
		task := step.GetTask()
		inputs := make(map[string]any)
		for k, v := range task.Inputs {
			switch v.GetKind().(type) {
			case *v1.Value_Expr:
				exprStr, err := cel.AstToString(cel.ParsedExprToAst(v.GetExpr()))
				if err != nil {
					return nil, fmt.Errorf("step %q input %q: %w", step.Id, k, err)
				}

				inputs[k] = fmt.Sprintf("${%s}", exprStr)
			case *v1.Value_Literal:
				refVal, err := cel.ValueToRefValue(v1.TypeAdapter, v.GetLiteral())
				if err != nil {
					return nil, fmt.Errorf("step %q input %q: %w", step.Id, k, err)
				}
				inputs[k] = refVal.Value()
			default:
				return nil, fmt.Errorf("step %q input %q: unsupported value type %T", step.Id, k, v.GetKind())
			}
		}
		f.Steps[i] = flowfileStep{
			ID:   step.Id,
			Task: &flowfileTask{Name: task.Name, Inputs: inputs},
		}
	}
	return yaml.Marshal(f)
}
