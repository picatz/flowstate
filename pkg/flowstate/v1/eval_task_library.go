package flowstatev1

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/cel-go/cel"
	ref "github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"github.com/google/cel-go/interpreter"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var taskLibrary = map[string]taskFunc{
	"echo":   taskFuncEcho,
	"printf": taskFuncPrintf,
	"http":   taskFuncHTTP(http.DefaultClient),
	"cel":    taskFuncCEL,
}

type taskFunc func(ctx context.Context, input map[string]*Value, prevStepOutputs *Workflow_StepOutputs) (*Node_Outputs, error)

func taskFuncEcho(ctx context.Context, input map[string]*Value, prevStepOutputs *Workflow_StepOutputs) (*Node_Outputs, error) {
	taskInputs := &Task_Echo_Inputs{}
	if err := populateProtoMessageFromValueMap(input, taskInputs, prevStepOutputs); err != nil {
		return nil, fmt.Errorf("failed to parse echo task inputs: %w", err)
	}

	return nodeOutputsFromProtoMessage(&Task_Echo_Outputs{
		Result: taskInputs.Message,
	})
}

func taskFuncPrintf(ctx context.Context, input map[string]*Value, prevStepOutputs *Workflow_StepOutputs) (*Node_Outputs, error) {
	taskInputs := &Task_Printf_Inputs{}
	if err := populateProtoMessageFromValueMap(input, taskInputs, prevStepOutputs); err != nil {
		return nil, fmt.Errorf("failed to parse printf task inputs: %w", err)
	}

	args := make([]any, len(taskInputs.Args))
	for i, arg := range taskInputs.Args {
		switch kind := arg.GetKind().(type) {
		case *Value_Expr:
			ast := cel.ParsedExprToAst(kind.Expr)
			env, err := cel.NewEnv()
			if err != nil {
				return nil, fmt.Errorf("failed to create CEL environment: %w", err)
			}
			prg, err := env.Program(ast)
			if err != nil {
				return nil, fmt.Errorf("failed to compile CEL expression: %w", err)
			}
			out, _, err := prg.Eval(cel.Activation(&StepsOutputActivation{Prev: prevStepOutputs}))
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate CEL expression: %w", err)
			}
			value, err := cel.RefValueToValue(out)
			if err != nil {
				return nil, fmt.Errorf("failed to convert CEL reference to value: %w", err)
			}
			switch v := value.GetKind().(type) {
			case *expr.Value_StringValue:
				args[i] = v.StringValue
			case *expr.Value_Int64Value:
				args[i] = v.Int64Value
			case *expr.Value_BoolValue:
				args[i] = v.BoolValue
			default:
				return nil, fmt.Errorf("unsupported printf argument type: %T", v)
			}
		case *Value_Literal:
			lit := kind.Literal
			switch v := lit.GetKind().(type) {
			case *expr.Value_StringValue:
				args[i] = v.StringValue
			case *expr.Value_Int64Value:
				args[i] = v.Int64Value
			case *expr.Value_BoolValue:
				args[i] = v.BoolValue
			default:
				return nil, fmt.Errorf("unsupported printf argument type: %T", v)
			}
		default:
			return nil, fmt.Errorf("unsupported value type: %T", arg)
		}
	}

	return nodeOutputsFromProtoMessage(&Task_Printf_Outputs{
		Result: fmt.Sprintf(taskInputs.Format, args...),
	})
}

func taskFuncHTTP(httpClient *http.Client) taskFunc {
	return func(ctx context.Context, input map[string]*Value, prevStepOutputs *Workflow_StepOutputs) (*Node_Outputs, error) {

		taskInputs := &Task_HTTP_Inputs{
			Method: proto.String(http.MethodGet),
		}
		if err := populateProtoMessageFromValueMap(input, taskInputs, prevStepOutputs); err != nil {
			return nil, fmt.Errorf("failed to parse http task inputs: %w", err)
		}

		if err := taskInputs.ValidateAll(); err != nil {
			return nil, fmt.Errorf("invalid http task inputs: %w", err)
		}

		var body io.Reader
		if taskInputs.Body != nil {
			body = strings.NewReader(taskInputs.GetBody())
		}

		httpReq, err := http.NewRequestWithContext(ctx, taskInputs.GetMethod(), taskInputs.GetUrl(), body)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP request: %w", err)
		}

		httpResp, err := httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("failed to execute HTTP request: %w", err)
		}
		defer httpResp.Body.Close()
		if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
			return nil, fmt.Errorf("HTTP request failed with status code %d", httpResp.StatusCode)
		}
		respBody, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read HTTP response body: %w", err)
		}
		return nodeOutputsFromProtoMessage(&Task_HTTP_Outputs{
			StatusCode: int32(httpResp.StatusCode),
			Body:       string(respBody),
			// Headers:    fmt.Sprintf("%v", httpResp.Header),
		})
	}
}

func taskFuncCEL(ctx context.Context, input map[string]*Value, prevStepOutputs *Workflow_StepOutputs) (*Node_Outputs, error) {
	exprInput, ok := input["expr"]
	if !ok {
		return nil, fmt.Errorf("missing expr input")
	}

	var exprStr string
	if lit := exprInput.GetLiteral(); lit != nil {
		exprStr = lit.GetStringValue()
	} else {
		val, err := valueToCEL(exprInput, prevStepOutputs)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve expr input: %w", err)
		}
		if s, ok := val.Value().(string); ok {
			exprStr = s
		} else {
			return nil, fmt.Errorf("expr input must resolve to string, got %T", val.Value())
		}
	}

	varBindings := map[string]*Value{}
	var libs []string
	for k, v := range input {
		switch k {
		case "expr":
			continue
		case "libs":
			if lit := v.GetLiteral(); lit != nil {
				if lv := lit.GetListValue(); lv != nil {
					for _, elem := range lv.Values {
						if s := elem.GetStringValue(); s != "" {
							libs = append(libs, s)
						} else {
							return nil, fmt.Errorf("libs elements must be strings")
						}
					}
				} else if s := lit.GetStringValue(); s != "" {
					libs = append(libs, s)
				} else {
					return nil, fmt.Errorf("libs input must be a string or list of strings")
				}
			} else {
				return nil, fmt.Errorf("libs input must be literal")
			}
			continue
		case "vars":
			lit := v.GetLiteral()
			if lit == nil {
				return nil, fmt.Errorf("vars input must be literal")
			}
			mv, ok := lit.GetKind().(*expr.Value_MapValue)
			if !ok {
				return nil, fmt.Errorf("vars input must be a map")
			}
			for _, entry := range mv.MapValue.Entries {
				key := entry.GetKey().GetStringValue()
				varBindings[key] = &Value{Kind: &Value_Literal{Literal: entry.Value}}
			}
		default:
			varBindings[k] = v
		}
	}

	var envOpts []cel.EnvOption
	for _, name := range libs {
		switch strings.ToLower(name) {
		case "math":
			envOpts = append(envOpts, ext.Math())
		case "strings":
			envOpts = append(envOpts, ext.Strings())
		case "lists":
			envOpts = append(envOpts, ext.Lists())
		case "sets":
			envOpts = append(envOpts, ext.Sets())
		case "encoders":
			envOpts = append(envOpts, ext.Encoders())
		case "protos":
			envOpts = append(envOpts, ext.Protos())
		case "bindings":
			envOpts = append(envOpts, ext.Bindings())
		case "comprehensions":
			envOpts = append(envOpts, ext.TwoVarComprehensions())
		case "regex":
			envOpts = append(envOpts, cel.OptionalTypes(), ext.Regex())
		case "optional":
			envOpts = append(envOpts, cel.OptionalTypes())
		default:
			return nil, fmt.Errorf("unknown CEL extension library %q", name)
		}
	}

	env, err := cel.NewEnv(envOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	ast, issues := env.Parse(exprStr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to parse CEL expression: %w", issues.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to compile CEL expression: %w", err)
	}

	celVars := make(map[string]any, len(varBindings))
	for name, val := range varBindings {
		cval, err := valueToCEL(val, prevStepOutputs)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve var %s: %w", name, err)
		}
		celVars[name] = cval
	}
	bindings := map[string]any{"vars": celVars}

	// Create an activation containing the user-provided variables.
	varAct, err := interpreter.NewActivation(bindings)
	if err != nil {
		return nil, fmt.Errorf("failed to create activation: %w", err)
	}
	// Combine the step outputs with the variable activation so expressions
	// can reference both previous steps and local variables.
	act := interpreter.NewHierarchicalActivation(&StepsOutputActivation{Prev: prevStepOutputs}, varAct)

	out, _, err := prg.Eval(act)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate CEL expression: %w", err)
	}

	resultVal, err := cel.RefValueToValue(out)
	if err != nil {
		return nil, fmt.Errorf("failed to convert CEL value: %w", err)
	}

	return nodeOutputsFromProtoMessage(&Task_CEL_Outputs{Result: NewLiteral(resultVal)})
}

// valueToCEL resolves the given Value into a CEL value. Literals are converted
// directly, while expressions are evaluated using a minimal CEL environment that
// can reference previous step outputs.
func valueToCEL(v *Value, prevStepOutputs *Workflow_StepOutputs) (ref.Val, error) {
	switch kind := v.GetKind().(type) {
	case *Value_Literal:
		return cel.ValueToRefValue(TypeAdapter, kind.Literal)
	case *Value_Expr:
		ast := cel.ParsedExprToAst(kind.Expr)
		env, err := cel.NewEnv()
		if err != nil {
			return nil, fmt.Errorf("failed to create CEL environment: %w", err)
		}
		prg, err := env.Program(ast)
		if err != nil {
			return nil, fmt.Errorf("failed to compile CEL expression: %w", err)
		}
		out, _, err := prg.Eval(cel.Activation(&StepsOutputActivation{Prev: prevStepOutputs}))
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate CEL expression: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported value kind %T", kind)
	}
}

func nodeOutputsFromProtoMessage(msg proto.Message) (*Node_Outputs, error) {
	outputs := &Node_Outputs{
		NamedValues: map[string]*Value{},
	}
	msgFields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < msgFields.Len(); i++ {
		fieldDesc := msgFields.Get(i)
		fieldName := string(fieldDesc.Name())
		val := msg.ProtoReflect().Get(fieldDesc)
		if fieldDesc.IsList() {
			valList := val.List()
			var values []*expr.Value
			for j := 0; j < valList.Len(); j++ {
				elem := valList.Get(j)
				switch fieldDesc.Kind() {
				case protoreflect.StringKind:
					values = append(values, &expr.Value{Kind: &expr.Value_StringValue{StringValue: elem.String()}})
				case protoreflect.Int32Kind, protoreflect.Int64Kind:
					values = append(values, &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: elem.Int()}})
				case protoreflect.BoolKind:
					values = append(values, &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: elem.Bool()}})
				case protoreflect.MessageKind:
					if v, ok := elem.Message().Interface().(*Value); ok {
						if lit := v.GetLiteral(); lit != nil {
							values = append(values, lit)
						} else {
							// fallback: wrap as struct or skip
						}
					} else {
						return nil, fmt.Errorf("unsupported message type in list output for field %q", fieldName)
					}
				default:
					return nil, fmt.Errorf("unsupported list element type in output: %s", fieldDesc.Kind().String())
				}
			}
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_ListValue{
							ListValue: &expr.ListValue{Values: values},
						},
					},
				},
			}
			continue
		}
		if fieldDesc.IsMap() {
			// For now, skip map fields (could add support if needed)
			continue
		}
		val = msg.ProtoReflect().Get(fieldDesc)
		switch fieldDesc.Kind() {
		case protoreflect.StringKind:
			if strVal := val.String(); strVal != "" {
				outputs.NamedValues[fieldName] = &Value{
					Kind: &Value_Literal{
						Literal: &expr.Value{
							Kind: &expr.Value_StringValue{
								StringValue: strVal,
							},
						},
					},
				}
			}
		case protoreflect.Int32Kind, protoreflect.Int64Kind:
			if intVal := val.Int(); intVal != 0 {
				outputs.NamedValues[fieldName] = &Value{
					Kind: &Value_Literal{
						Literal: &expr.Value{
							Kind: &expr.Value_Int64Value{
								Int64Value: intVal,
							},
						},
					},
				}
			}
		case protoreflect.BoolKind:
			if boolVal := val.Bool(); boolVal {
				outputs.NamedValues[fieldName] = &Value{
					Kind: &Value_Literal{
						Literal: &expr.Value{
							Kind: &expr.Value_BoolValue{
								BoolValue: boolVal,
							},
						},
					},
				}
			}
		case protoreflect.MessageKind:
			msgType := fieldDesc.Message().FullName()
			switch msgType {
			case "google.api.expr.v1alpha1.Value":
				if v, ok := val.Message().Interface().(*expr.Value); ok {
					outputs.NamedValues[fieldName] = &Value{
						Kind: &Value_Literal{Literal: v},
					}
				}
			case "flowstate.v1.Value":
				if v, ok := val.Message().Interface().(*Value); ok {
					outputs.NamedValues[fieldName] = v
				}
			default:
				return nil, fmt.Errorf("unsupported message type in output: %s", msgType)
			}
		default:
			return nil, fmt.Errorf("unsupported field type: %s", fieldDesc.Kind().String())
		}
	}
	return outputs, nil
}

func populateProtoMessageFromValueMap(input map[string]*Value, msg proto.Message, prevStepOutputs *Workflow_StepOutputs) error {
	msgFields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < msgFields.Len(); i++ {
		fieldDesc := msgFields.Get(i)
		fieldName := string(fieldDesc.Name())
		val, ok := input[fieldName]
		if !ok {
			continue // Field not provided in input map
		}
		if fieldDesc.IsMap() {
			return fmt.Errorf("populateProtoMessageFromValueMap does not support map fields")
		}
		if fieldDesc.IsList() {
			// Expect input value to be a list of *Value
			var listField = msg.ProtoReflect().Mutable(fieldDesc).List()
			switch v := val.GetKind().(type) {
			case *Value_Literal:
				// Support google.api.expr.v1alpha1.Value with list kind
				if lv, ok := v.Literal.GetKind().(*expr.Value_ListValue); ok {
					for _, elem := range lv.ListValue.Values {
						switch fieldDesc.Kind() {
						case protoreflect.StringKind:
							listField.Append(protoreflect.ValueOfString(elem.GetStringValue()))
						case protoreflect.Int32Kind, protoreflect.Int64Kind:
							listField.Append(protoreflect.ValueOfInt64(elem.GetInt64Value()))
						case protoreflect.BoolKind:
							listField.Append(protoreflect.ValueOfBool(elem.GetBoolValue()))
						case protoreflect.MessageKind:
							msgType := fieldDesc.Message()
							msgTypeName := string(msgType.FullName())
							if msgTypeName == "flowstate.v1.Value" {
								// Directly wrap the expr.Value as a *Value
								// For printf.args, etc.
								v := &Value{Kind: &Value_Literal{Literal: elem}}
								listField.Append(protoreflect.ValueOfMessage(v.ProtoReflect()))
								continue
							}
							msgTypeInfo, err := protoregistry.GlobalTypes.FindMessageByName(msgType.FullName())
							if err != nil {
								return fmt.Errorf("could not find message type %q: %w", msgTypeName, err)
							}
							msgVal := msgTypeInfo.New().Interface()
							if structVal, ok := elem.Kind.(*expr.Value_MapValue); ok {
								inputMap := make(map[string]*Value)
								for _, e := range structVal.MapValue.Entries {
									k := e.Key.GetStringValue()
									v := e.Value
									inputMap[k] = &Value{Kind: &Value_Literal{Literal: v}}
								}
								err := populateProtoMessageFromValueMap(inputMap, msgVal, prevStepOutputs)
								if err != nil {
									return fmt.Errorf("failed to populate sub-message for list field %q: %w", fieldName, err)
								}
								listField.Append(protoreflect.ValueOfMessage(msgVal.ProtoReflect()))
							} else {
								return fmt.Errorf("expected struct value for message element in list field %q", fieldName)
							}
						default:
							return fmt.Errorf("unsupported list element type: %s", fieldDesc.Kind().String())
						}
					}
				} else {
					return fmt.Errorf("expected ListValue for list field %q", fieldName)
				}
			case *Value_Expr:
				// Evaluate CEL expression, expect result to be a slice
				ast := cel.ParsedExprToAst(v.Expr)
				env, err := cel.NewEnv()
				if err != nil {
					return fmt.Errorf("failed to create CEL environment: %w", err)
				}
				prg, err := env.Program(ast)
				if err != nil {
					return fmt.Errorf("failed to compile CEL expression: %w", err)
				}
				out, _, err := prg.Eval(cel.Activation(&StepsOutputActivation{Prev: prevStepOutputs}))
				if err != nil {
					return fmt.Errorf("failed to evaluate CEL expression: %w", err)
				}
				// Try to convert to []interface{}
				if arr, ok := out.Value().([]interface{}); ok {
					for _, elem := range arr {
						switch fieldDesc.Kind() {
						case protoreflect.StringKind:
							if s, ok := elem.(string); ok {
								listField.Append(protoreflect.ValueOfString(s))
							} else {
								return fmt.Errorf("expected string in list for field %q", fieldName)
							}
						case protoreflect.Int32Kind, protoreflect.Int64Kind:
							if i, ok := elem.(int64); ok {
								listField.Append(protoreflect.ValueOfInt64(i))
							} else {
								return fmt.Errorf("expected int64 in list for field %q", fieldName)
							}
						case protoreflect.BoolKind:
							if b, ok := elem.(bool); ok {
								listField.Append(protoreflect.ValueOfBool(b))
							} else {
								return fmt.Errorf("expected bool in list for field %q", fieldName)
							}
						default:
							return fmt.Errorf("unsupported list element type: %s", fieldDesc.Kind().String())
						}
					}
				} else {
					return fmt.Errorf("expected array result from CEL for list field %q", fieldName)
				}
			default:
				return fmt.Errorf("unsupported value type for list field %q: %T", fieldName, val)
			}
			continue
		}
		switch kind := val.GetKind().(type) {
		case *Value_Expr:
			ast := cel.ParsedExprToAst(kind.Expr)
			env, err := cel.NewEnv()
			if err != nil {
				return fmt.Errorf("failed to create CEL environment: %w", err)
			}
			prg, err := env.Program(ast)
			if err != nil {
				return fmt.Errorf("failed to compile CEL expression: %w", err)
			}
			out, _, err := prg.Eval(cel.Activation(&StepsOutputActivation{
				Prev: prevStepOutputs,
			}))
			if err != nil {
				return fmt.Errorf("failed to evaluate CEL expression: %w", err)
			}
			value, err := cel.RefValueToValue(out)
			if err != nil {
				return fmt.Errorf("failed to convert CEL reference to value: %w", err)
			}
			switch fieldDesc.Kind() {
			case protoreflect.StringKind:
				if strVal := value.GetStringValue(); strVal != "" {
					msg.ProtoReflect().Set(fieldDesc, protoreflect.ValueOfString(strVal))
				} else {
					return fmt.Errorf("expected string value for field %q, got %T", fieldName, value.GetKind())
				}
			case protoreflect.Int32Kind, protoreflect.Int64Kind:
				if intVal := value.GetInt64Value(); intVal != 0 {
					msg.ProtoReflect().Set(fieldDesc, protoreflect.ValueOfInt64(intVal))
				} else {
					return fmt.Errorf("expected int64 value for field %q, got %T", fieldName, value.GetKind())
				}
			case protoreflect.BoolKind:
				if boolVal := value.GetBoolValue(); boolVal {
					msg.ProtoReflect().Set(fieldDesc, protoreflect.ValueOfBool(boolVal))
				} else {
					return fmt.Errorf("expected bool value for field %q, got %T", fieldName, value.GetKind())
				}
			default:
				return fmt.Errorf("unsupported field type: %s", fieldDesc.Kind().String())
			}
		case *Value_Literal:
			lit := kind.Literal
			switch fieldDesc.Kind() {
			case protoreflect.StringKind:
				if strVal := lit.GetStringValue(); strVal != "" {
					msg.ProtoReflect().Set(fieldDesc, protoreflect.ValueOfString(strVal))
				} else {
					return fmt.Errorf("expected string value for field %q, got %T", fieldName, lit.GetKind())
				}
			case protoreflect.Int32Kind, protoreflect.Int64Kind:
				if intVal := lit.GetInt64Value(); intVal != 0 {
					msg.ProtoReflect().Set(fieldDesc, protoreflect.ValueOfInt64(intVal))
				} else {
					return fmt.Errorf("expected int64 value for field %q, got %T", fieldName, lit.GetKind())
				}
			case protoreflect.BoolKind:
				if boolVal := lit.GetBoolValue(); boolVal {
					msg.ProtoReflect().Set(fieldDesc, protoreflect.ValueOfBool(boolVal))
				} else {
					return fmt.Errorf("expected bool value for field %q, got %T", fieldName, lit.GetKind())
				}
			default:
				return fmt.Errorf("unsupported field type: %s", fieldDesc.Kind().String())
			}
		default:
			return fmt.Errorf("unsupported value type: %T", val)
		}
	}
	return nil
}
