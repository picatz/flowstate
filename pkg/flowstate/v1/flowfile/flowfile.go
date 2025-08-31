package flowfile

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

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
	case int, int64, float64, bool:
		// Other supported primitive literal types
		return v1.NewValue(val), nil
	case map[string]any:
		// If any nested value contains an expression, convert the entire map
		// to a CEL map expression to allow per-key expressions.
		if needsCEL(val) {
			celExpr, err := toCELExprFromAny(val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert map to CEL expr: %w", err)
			}
			return v1.NewExpr(celExpr), nil
		}
		return v1.NewValue(val), nil
	case []any:
		if needsCEL(val) {
			celExpr, err := toCELExprFromAny(val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert list to CEL expr: %w", err)
			}
			return v1.NewExpr(celExpr), nil
		}
		return v1.NewValue(val), nil
	default:
		return nil, fmt.Errorf("unsupported input type: %T", v)
	}
}

// needsCEL returns true if the provided value (map/list) contains any nested
// string of the form ${...} that should be converted to a CEL expression.
func needsCEL(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		for _, vv := range x {
			if containsExpr(vv) {
				return true
			}
		}
		return false
	case []any:
		for _, vv := range x {
			if containsExpr(vv) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func containsExpr(v any) bool {
	switch t := v.(type) {
	case string:
		return flowfileExprPattern.MatchString(t)
	case map[string]any, []any:
		return needsCEL(t)
	default:
		return false
	}
}

// toCELExprFromAny builds a CEL expression string from a Go literal that may
// contain nested ${...} expression strings. String literals are quoted; ${...}
// strings are included as raw CEL expressions.
func toCELExprFromAny(v any) (string, error) {
	switch x := v.(type) {
	case string:
		if m := flowfileExprPattern.FindStringSubmatch(x); m != nil {
			return m[1], nil
		}
		return quoteCELString(x), nil
	case int:
		return fmt.Sprintf("%d", x), nil
	case int64:
		return fmt.Sprintf("%d", x), nil
	case uint:
		return fmt.Sprintf("%d", x), nil
	case uint64:
		return fmt.Sprintf("%d", x), nil
	case uint32:
		return fmt.Sprintf("%d", x), nil
	case float64:
		return fmt.Sprintf("%g", x), nil
	case float32:
		return fmt.Sprintf("%g", x), nil
	case bool:
		if x {
			return "true", nil
		}
		return "false", nil
	case map[string]any:
		// Use stable key order for test determinism
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(x))
		for _, k := range keys {
			valExpr, err := toCELExprFromAny(x[k])
			if err != nil {
				return "", err
			}
			parts = append(parts, fmt.Sprintf("%s: %s", quoteCELString(k), valExpr))
		}
		return fmt.Sprintf("{%s}", strings.Join(parts, ", ")), nil
	case []any:
		elems := make([]string, 0, len(x))
		for _, e := range x {
			valExpr, err := toCELExprFromAny(e)
			if err != nil {
				return "", err
			}
			elems = append(elems, valExpr)
		}
		return fmt.Sprintf("[%s]", strings.Join(elems, ", ")), nil
	default:
		return "", fmt.Errorf("unsupported type in CEL expr conversion: %T", v)
	}
}

func quoteCELString(s string) string {
	// Use single quotes and escape existing single quotes/backslashes.
	esc := strings.ReplaceAll(s, "\\", "\\\\")
	esc = strings.ReplaceAll(esc, "'", "\\'")
	return "'" + esc + "'"
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
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
