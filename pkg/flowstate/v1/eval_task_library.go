package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	ref "github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// builtinTasks returns the definitions of the tasks Flowstate ships with.
//
// Each definition declares how the engine must treat the task's inputs, so no
// part of the engine needs to know a task's name to execute it correctly.
func builtinTasks() []TaskDef {
	return []TaskDef{
		{
			Name:    "log",
			Summary: "Emit a message for a person to read.",
			Inputs:  (&Task_Log_Inputs{}).ProtoReflect().Descriptor(),
			Outputs: (&Task_Log_Outputs{}).ProtoReflect().Descriptor(),
			Fn:      taskFuncLog,
		},
		// `outputs` expressions reference the response (status_code, body,
		// headers), which exists only after the request completes, so the http
		// task evaluates them itself rather than the workflow resolving them.
		HTTPTaskDef(defaultEgressPolicy()),
	}
}

// AllowLoopbackEgressEnv names the environment variable that permits the http
// task to reach loopback addresses.
//
// Developing against a service on localhost is ordinary and the default policy
// denies it, so there has to be a way to say yes. It is an explicit opt-in rather
// than a default because the same permission is what lets a workflow reach a
// worker's own internal endpoints in production.
const AllowLoopbackEgressEnv = "FLOWSTATE_ALLOW_LOOPBACK_EGRESS"

// defaultEgressPolicy is the egress policy the http task enforces.
//
// A workflow names the URL it fetches, which makes the http task a way to ask
// the worker to make a request on the author's behalf — including to addresses
// only the worker can reach, such as internal services and cloud instance
// metadata. The policy is what makes that safe: it denies internal address
// ranges, re-checks every redirect hop, and bounds the response, all by default.
//
// It is built once. A failure to build it is a defect in Flowstate rather than
// something a workflow can cause, so it panics rather than falling back to an
// ungoverned client — failing open here would undo the entire point.
var defaultEgressPolicy = sync.OnceValue(func() *netpolicy.Policy {
	var opts []netpolicy.Option
	if os.Getenv(AllowLoopbackEgressEnv) == "true" {
		opts = append(opts, netpolicy.WithAllowLoopback())
	}
	p, err := netpolicy.New(opts...)
	if err != nil {
		panic("flowstate: building the default egress policy: " + err.Error())
	}
	return p
})

// HTTPTaskDef returns the http task definition enforcing the given egress policy.
//
// Registering the result replaces the built-in http task, which is how a
// deployment applies its own egress rules — or how a test points the task at a
// local server the default policy would refuse to reach.
func HTTPTaskDef(policy *netpolicy.Policy) TaskDef {
	return TaskDef{
		Name:           "http",
		Summary:        "Perform an HTTP request and return the response.",
		Inputs:         (&Task_HTTP_Inputs{}).ProtoReflect().Descriptor(),
		Outputs:        (&Task_HTTP_Outputs{}).ProtoReflect().Descriptor(),
		DeferredInputs: []string{"outputs", "expect"},
		// `expect` and not `outputs`, and the difference is in the task rather than
		// in the grammar.
		//
		// `httpExpectSatisfied` reads `expectSpec.GetExpr()` and refuses anything
		// else, so a literal there is a run that fails on its first request — the
		// validator is moving that refusal to where the author can see it.
		//
		// `outputs` takes both. `taskFuncHTTP` handles `*Value_Literal` through
		// `literalToValueMap` and returns those names as the step's outputs, which
		// is a working form: a step that emits constants alongside a fetch. Listing
		// it here would have made `flow validate` refuse a workflow the engine runs,
		// which is this rule's own failure mode pointed the other way.
		ExpressionInputs: []string{"expect"},
		NeedsPrevOutputs: true,
		Fn:               taskFuncHTTP(policy),
	}
}

// taskFuncLog emits a message and returns nothing.
//
// Returning an empty outputs message rather than nil, because nil means "this step
// contributed no entry" to the executors and an empty one means "this step ran and
// produced nothing". A `log:` step did run, and a run record that cannot tell the two
// apart cannot show that it did.
func taskFuncLog(ctx context.Context, input map[string]*Value, scope *Scope) (*Node_Outputs, error) {
	taskInputs := &Task_Log_Inputs{}
	if err := populateProtoMessageFromValueMap(ctx, input, taskInputs, scope); err != nil {
		return nil, NewTaskError("log", ErrorKindInvalidInput, err)
	}

	// The declared bounds, enforced rather than decorative.
	//
	// A `log:` step's fields are chosen by the workflow — count, key length and value
	// length all — and they are written to a worker's logs and into durable history.
	// Bounding the resource an author controls is the rule this repo states; a bound
	// nothing checks is a comment. This is also where a level outside the enum is
	// caught for a specification that reached a worker without passing `flow validate`.
	if err := Validate(taskInputs); err != nil {
		return nil, NewTaskError("log", ErrorKindInvalidInput, err)
	}

	// Sorted, because a map's order is not one and a log line whose fields shuffle
	// between two runs of the same workflow is a diff nobody can read.
	attrs := make([]any, 0, len(taskInputs.GetFields()))
	for _, name := range slices.Sorted(maps.Keys(taskInputs.GetFields())) {
		attrs = append(attrs, slog.String(name, taskInputs.GetFields()[name]))
	}

	LoggerFrom(ctx).LogAttrs(ctx, slogLevel(taskInputs.GetLevel()), taskInputs.GetMessage(),
		attrsOf(attrs)...)

	return nodeOutputsFromProtoMessage(&Task_Log_Outputs{})
}

// attrsOf narrows a slice built as []any back to the attribute type LogAttrs wants.
//
// LogAttrs is the allocation-free entry point and takes []slog.Attr; the slice is
// assembled as []any only because that is what reads naturally above. Anything that is
// not an Attr is dropped rather than coerced — there is nothing else in it, and a
// silent drop beats a panic in the one task whose whole job is to be visible.
func attrsOf(values []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(values))
	for _, v := range values {
		if attr, ok := v.(slog.Attr); ok {
			attrs = append(attrs, attr)
		}
	}

	return attrs
}

// maxHTTPResponseBytes bounds how much of an HTTP response body the http task
// will read.
//
// The binding reason is memory safety: a worker must not let a remote endpoint
// decide how much it allocates. The value is also chosen to sit within
// Temporal's default per-payload limit, so a body that reads successfully can
// actually flow through a workflow.
//
// This is a default, not a ceiling on what the system can handle. Genuinely
// large payloads are a solved problem on this substrate — a custom payload
// codec can offload the blob to external storage and carry a reference through
// history (the claim-check pattern), which is the coherent way to raise this
// rather than simply buffering more in the worker. Until that codec exists,
// workflows needing only part of a large response should select fields with the
// outputs input.

// idempotentMethods are the methods RFC 9110 defines as idempotent: repeating one
// has the same effect on the server as making it once.
//
// POST and PATCH are absent deliberately. They are the methods for which a repeat
// is a second effect.
var idempotentMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodHead:    true,
	http.MethodPut:     true,
	http.MethodDelete:  true,
	http.MethodOptions: true,
	http.MethodTrace:   true,
}

// retriableTransportFailure reports whether a request that failed without a
// response can be attempted again.
//
// A transport failure is not one situation but two, and they differ in the only way
// that matters. If the connection was never established, the request cannot have
// taken effect, and retrying it is safe whatever the method. If the request was
// written and then the transport failed — a timeout awaiting the response, a reset
// mid-flight — the outcome is *unknown*: the server may have completed the work and
// failed only to tell us. Retrying that is not retrying a failure, it is performing
// the operation a second time.
//
// For an idempotent method that is fine by definition. For a POST or a PATCH it is
// how one charge becomes two, so an unknown outcome is reported as permanent and the
// author decides what to do about it. That matches what ErrorKind.Retryable already
// documents as the intent — "retrying a POST that already took effect is worse than
// surfacing a failure that might have resolved on its own" — which the transport
// path previously did not honor, because it classified by transport-versus-policy and
// never looked at the method.
func retriableTransportFailure(method string, err error) bool {
	if idempotentMethods[strings.ToUpper(method)] {
		return true
	}

	// A dial failure means no bytes reached the server: connection refused, no
	// route, DNS failure. Nothing can have happened, so this stays retriable even
	// for a non-idempotent method — which keeps the common "the server is not up
	// yet" case working.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}

	return false
}

// firstHeaderValues flattens response headers to one value per name, which is
// the shape the schema declares and the shape a workflow author expects when
// writing ${steps.<id>.headers['Content-Type']}.
//
// Repeated headers keep their first value. Expressions needing every value of a
// repeated header can reach them through the outputs input, where headers are
// exposed as lists.
func firstHeaderValues(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for name, values := range h {
		if len(values) > 0 {
			out[name] = values[0]
		}
	}
	return out
}

// httpOutputsEnv returns the CEL environment used to shape HTTP task outputs.
//
// It declares the response variables an expression may reference and enables the
// json library, so a workflow can pick fields out of a JSON body without a
// dedicated task per response shape. The environment is built once and shared.
var httpOutputsEnv = sync.OnceValues(func() (*cel.Env, error) {
	env, err := cel.NewEnv(
		// One variable, and everything about the response hangs from it.
		//
		// These names are *system-chosen* and injected into an author's namespace,
		// which is the shape the signal-payload fix already rooted under `payload.*`
		// — for two reasons that apply identically here. The set will grow: a future
		// `duration_ms` written bare would capture a binding somebody already had.
		// And the collision is representable today: `as: body` on a loop enclosing an
		// http step whose `expect:` says `body` reads the response, not the item, and
		// nothing in the file says so.
		//
		// Dynamically typed because a root answered whole cannot be given a field-wise
		// type here without restating the response's shape in a second place.
		cel.Variable(ResponseRoot, cel.DynType),
		jsonLibrary(),
	)
	if err != nil {
		return nil, fmt.Errorf("create HTTP outputs CEL environment: %w", err)
	}
	return env, nil
})

func taskFuncHTTP(policy *netpolicy.Policy) TaskFunc {
	return func(ctx context.Context, input map[string]*Value, scope *Scope) (*Node_Outputs, error) {
		taskInputs := &Task_HTTP_Inputs{
			Method: proto.String(http.MethodGet),
		}

		// `outputs` and `expect` are evaluated against the response rather than
		// against earlier steps, so they are held back from population: resolving
		// them here would fail on `status_code` and `body`, which do not exist yet.
		var outputsSpec, expectSpec *Value
		inputForPopulate := input
		if _, hasOutputs := input["outputs"]; hasOutputs || input["expect"] != nil {
			outputsSpec, expectSpec = input["outputs"], input["expect"]
			inputForPopulate = make(map[string]*Value, len(input))
			for k, v := range input {
				if k == "outputs" || k == "expect" {
					continue
				}
				inputForPopulate[k] = v
			}
		}

		if err := populateProtoMessageFromValueMap(ctx, inputForPopulate, taskInputs, scope); err != nil {
			return nil, NewTaskError("http", ErrorKindInvalidInput, err)
		}

		var outputsExpr *expr.ParsedExpr
		if outputsSpec != nil {
			switch kind := outputsSpec.GetKind().(type) {
			case *Value_Literal:
				converted, err := literalToValueMap(kind.Literal)
				if err != nil {
					return nil, fmt.Errorf("invalid outputs literal: %w", err)
				}
				if len(converted) > 0 {
					taskInputs.Outputs = converted
				}
			case *Value_Expr:
				outputsExpr = kind.Expr
			case *Value_Error_:
				return nil, fmt.Errorf("invalid outputs specification: %s", kind.Error.GetMessage())
			default:
				return nil, fmt.Errorf("unsupported outputs specification kind: %T", kind)
			}
		}

		// Enforce the constraints the schema declares (a valid URI, a known
		// method). This is real validation: the previously generated validator
		// was produced by a plugin that reads a different option set than the
		// schema uses, so every check in it was a no-op.
		if err := Validate(taskInputs); err != nil {
			return nil, NewTaskError("http", ErrorKindInvalidInput, err)
		}

		requestURL, err := applyQuery(taskInputs.GetUrl(), taskInputs.GetQuery())
		if err != nil {
			return nil, NewTaskError("http", ErrorKindInvalidInput, err)
		}

		bodyText, contentType, err := httpRequestBody(taskInputs)
		if err != nil {
			return nil, NewTaskError("http", ErrorKindInvalidInput, err)
		}

		var body io.Reader
		if bodyText != "" || taskInputs.Body != nil {
			body = strings.NewReader(bodyText)
		}

		httpReq, err := http.NewRequestWithContext(ctx, taskInputs.GetMethod(), requestURL, body)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP request: %w", err)
		}

		// Apply request headers if provided.
		if taskInputs.Headers != nil {
			for k, v := range taskInputs.Headers {
				httpReq.Header.Add(k, v)
			}
		}

		// A structured body implies the header describing it, but only when the author
		// did not say: someone sending a JSON variant like application/ld+json has
		// been more specific than we can be, and overwriting that would be wrong.
		if contentType != "" && httpReq.Header.Get("Content-Type") == "" {
			httpReq.Header.Set("Content-Type", contentType)
		}

		httpResp, err := policy.Client().Do(httpReq)
		if err != nil {
			// A policy denial is deliberate and will happen again; a connection
			// reset, DNS failure, or timeout may succeed later. Distinguishing
			// them is what stops a denied request from being retried.
			if errors.Is(err, netpolicy.ErrDenied) {
				return nil, NewTaskError("http", ErrorKindPolicyDenied, err)
			}

			if !taskInputs.GetRetryOnUnknownOutcome() && !retriableTransportFailure(taskInputs.GetMethod(), err) {
				return nil, NewTaskError("http", ErrorKindUpstreamUnknown, fmt.Errorf(
					"%s %s failed with no response, so whether it took effect is unknown: %w",
					taskInputs.GetMethod(), taskInputs.GetUrl(), err))
			}

			return nil, NewTaskError("http", ErrorKindUpstream, err)
		}
		defer httpResp.Body.Close()

		// The body is read before success is decided, because `expect` is an
		// expression over the response and the interesting cases are about its
		// content: a 200 carrying an error, or a 404 that means "not yet". Reading it
		// first costs nothing — the policy bounds the read either way.
		//
		// The policy bounds the read, so no endpoint a workflow names can decide
		// how much memory the worker allocates.
		respBody, err := policy.ReadResponseBody(httpResp)
		if err != nil {
			var tooLarge *netpolicy.BodyTooLargeError
			if errors.As(err, &tooLarge) {
				return nil, NewTaskError("http", ErrorKindLimitExceeded, fmt.Errorf(
					"response body from %s is too large: %w; use the outputs input to select only the fields this step needs",
					taskInputs.GetUrl(), err))
			}
			// The status said the request succeeded and only reading the reply
			// failed. For an idempotent method another attempt is free; for a POST
			// or a PATCH it would perform the operation a second time, and this
			// time we know the first one completed rather than merely suspecting
			// it. So the outcome is reported rather than retried.
			//
			// This path matters more than it looks. A failure after the headers is
			// the normal way a chunked or event-stream response breaks, so it stops
			// being an edge case as soon as a response is anything but one buffered
			// body.
			if !taskInputs.GetRetryOnUnknownOutcome() && !idempotentMethods[strings.ToUpper(taskInputs.GetMethod())] {
				return nil, NewTaskError("http", ErrorKindUpstreamUnknown, fmt.Errorf(
					"%s %s returned %d and then the response could not be read, so it took effect but its result is lost: %w",
					taskInputs.GetMethod(), taskInputs.GetUrl(), httpResp.StatusCode, err))
			}

			return nil, NewTaskError("http", ErrorKindUpstream,
				fmt.Errorf("failed to read HTTP response body: %w", err))
		}

		// Parsing is opt-in, so a body that is not JSON is a real error here rather
		// than a silently empty value: a step that asked for JSON and got HTML has a
		// problem worth naming.
		var parsedJSON *expr.Value
		if taskInputs.GetParseJson() {
			parsedJSON, err = parseJSONResponse(respBody)
			if err != nil {
				return nil, NewTaskError("http", ErrorKindInvalidInput, fmt.Errorf(
					"%s %s: %w", taskInputs.GetMethod(), taskInputs.GetUrl(), err))
			}
		}

		respVars := httpResponseVars(httpResp, respBody, parsedJSON)

		if err := httpExpectationMet(ctx, taskInputs, expectSpec, httpResp, respVars, scope); err != nil {
			return nil, err
		}

		// Default outputs mirror the response so a workflow can reference
		// ${steps.<id>.status_code}, ${steps.<id>.body}, and ${steps.<id>.headers} without
		// declaring an outputs expression.
		defaultOuts := &Task_HTTP_Outputs{
			StatusCode: int32(httpResp.StatusCode),
			Body:       string(respBody),
			Headers:    firstHeaderValues(httpResp.Header),
			Json:       parsedJSON,
		}

		// If typed outputs provided (either as explicit map entries or via a CEL map expression),
		// evaluate them using the response variables and return only those named values.
		if len(taskInputs.Outputs) > 0 || outputsExpr != nil {
			env, err := httpOutputsEnv()
			if err != nil {
				return nil, err
			}
			varAct, err := interpreter.NewActivation(respVars)
			if err != nil {
				return nil, fmt.Errorf("failed to create activation: %w", err)
			}
			// Ctx and Eval are set so that a stored expression resolved while
			// shaping these outputs is itself cancellable and cost-bounded.
			// Without them StepsOutputActivation falls back to context.Background.
			act := interpreter.NewHierarchicalActivation(
				&StepsOutputActivation{Prev: scope.StepOutputs(), Ctx: ctx, Eval: DefaultEvaluator()},
				varAct)

			if outputsExpr != nil {
				// Through the shared evaluator, which is what applies the cost
				// limit and makes the evaluation cancellable. Building a program
				// here by hand did neither: an author's `outputs:` expression is
				// the expression they most directly control, and it was the one
				// place in the engine that ran unbounded.
				out, err := DefaultEvaluator().EvalParsed(ctx, env, outputsExpr, act)
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate HTTP outputs expression: %w", err)
				}
				pv, err := cel.RefValueToValue(out)
				if err != nil {
					return nil, fmt.Errorf("failed to convert HTTP outputs expression result: %w", err)
				}
				mv, ok := pv.GetKind().(*expr.Value_MapValue)
				if !ok {
					return nil, fmt.Errorf("HTTP outputs expression must evaluate to a map")
				}
				outputs := &Node_Outputs{NamedValues: make(map[string]*Value, len(mv.MapValue.Entries))}
				for _, e := range mv.MapValue.Entries {
					outputs.NamedValues[e.Key.GetStringValue()] = &Value{
						Kind: &Value_Literal{Literal: e.Value},
					}
				}
				return outputs, nil
			}

			outputs := &Node_Outputs{NamedValues: map[string]*Value{}}
			for name, v := range taskInputs.Outputs {
				switch k := v.GetKind().(type) {
				case *Value_Expr:
					// Same path as the whole-block form above, for the same reason.
					out, err := DefaultEvaluator().EvalParsed(ctx, env, k.Expr, act)
					if err != nil {
						return nil, fmt.Errorf("failed to evaluate CEL outputs: %w", err)
					}
					pv, err := cel.RefValueToValue(out)
					if err != nil {
						return nil, fmt.Errorf("failed to convert outputs value: %w", err)
					}
					outputs.NamedValues[name] = &Value{Kind: &Value_Literal{Literal: pv}}
				case *Value_Literal:
					outputs.NamedValues[name] = &Value{Kind: &Value_Literal{Literal: k.Literal}}
				default:
					return nil, fmt.Errorf("unsupported outputs value kind for %q: %T", name, v.GetKind())
				}
			}
			return outputs, nil
		}

		return nodeOutputsFromProtoMessage(defaultOuts)
	}
}

func literalToValueMap(lit *expr.Value) (map[string]*Value, error) {
	if lit == nil {
		return nil, nil
	}
	mv, ok := lit.GetKind().(*expr.Value_MapValue)
	if !ok {
		return nil, fmt.Errorf("outputs literal must be a map")
	}
	if len(mv.MapValue.Entries) == 0 {
		return nil, nil
	}
	out := make(map[string]*Value, len(mv.MapValue.Entries))
	for _, entry := range mv.MapValue.Entries {
		key := entry.GetKey().GetStringValue()
		if key == "" {
			return nil, fmt.Errorf("outputs map keys must be strings")
		}
		out[key] = &Value{
			Kind: &Value_Literal{
				Literal: entry.Value,
			},
		}
	}
	return out, nil
}

// valueToCEL resolves the given Value into a CEL value. Literals are converted
// directly, while expressions are evaluated against previous step outputs under
// the shared evaluator's limits.
func valueToCEL(ctx context.Context, v *Value, scope *Scope) (ref.Val, error) {
	switch kind := v.GetKind().(type) {
	case *Value_Literal:
		return cel.ValueToRefValue(TypeAdapter, kind.Literal)
	case *Value_Expr:
		return DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, scope.Activation(ctx))
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
			// Convert proto map fields into a CEL MapValue literal.
			mv := msg.ProtoReflect().Get(fieldDesc).Map()
			entries := make([]*expr.MapValue_Entry, 0, mv.Len())
			mv.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				// Only string keys are supported in our protos.
				key := &expr.Value{Kind: &expr.Value_StringValue{StringValue: k.String()}}

				// Convert value based on value kind.
				var val *expr.Value
				switch fieldDesc.MapValue().Kind() {
				case protoreflect.StringKind:
					val = &expr.Value{Kind: &expr.Value_StringValue{StringValue: v.String()}}
				case protoreflect.Int32Kind, protoreflect.Int64Kind:
					val = &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: v.Int()}}
				case protoreflect.BoolKind:
					val = &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: v.Bool()}}
				case protoreflect.MessageKind:
					// If the value is a flowstate.v1.Value, unwrap its literal.
					if vv, ok := v.Message().Interface().(*Value); ok {
						if lit := vv.GetLiteral(); lit != nil {
							val = lit
							break
						}
					}
					// Fallback: unsupported message kind in map
					val = &expr.Value{Kind: &expr.Value_NullValue{}}
				default:
					// Fallback to null for unsupported kinds to avoid panic.
					val = &expr.Value{Kind: &expr.Value_NullValue{}}
				}
				entries = append(entries, &expr.MapValue_Entry{Key: key, Value: val})
				return true
			})
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{Literal: &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}}}},
			}
			continue
		}
		val = msg.ProtoReflect().Get(fieldDesc)

		// Emit the field unless the schema gives it explicit presence and it is
		// unset. Skipping any field that merely holds a zero value would drop a
		// legitimately empty result — an empty response body, a count of zero —
		// leaving downstream ${steps.<id>.field} references unresolvable.
		if fieldDesc.HasPresence() && !msg.ProtoReflect().Has(fieldDesc) {
			continue
		}

		switch fieldDesc.Kind() {
		case protoreflect.StringKind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_StringValue{StringValue: val.String()},
					},
				},
			}
		case protoreflect.Int32Kind, protoreflect.Int64Kind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_Int64Value{Int64Value: val.Int()},
					},
				},
			}
		case protoreflect.BoolKind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_BoolValue{BoolValue: val.Bool()},
					},
				},
			}
		case protoreflect.DoubleKind, protoreflect.FloatKind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_DoubleValue{DoubleValue: val.Float()},
					},
				},
			}
		case protoreflect.BytesKind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_BytesValue{BytesValue: val.Bytes()},
					},
				},
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
				// Generic nested message -> convert to a CEL map by reflecting fields.
				nested := val.Message()
				nd := nested.Descriptor().Fields()
				nestedEntries := make([]*expr.MapValue_Entry, 0, nd.Len())
				for i := 0; i < nd.Len(); i++ {
					f := nd.Get(i)
					fv := nested.Get(f)
					key := &expr.Value{Kind: &expr.Value_StringValue{StringValue: string(f.Name())}}
					var ev *expr.Value
					switch f.Kind() {
					case protoreflect.StringKind:
						ev = &expr.Value{Kind: &expr.Value_StringValue{StringValue: fv.String()}}
					case protoreflect.Int32Kind, protoreflect.Int64Kind:
						ev = &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: fv.Int()}}
					case protoreflect.BoolKind:
						ev = &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: fv.Bool()}}
					default:
						// For now, represent unsupported nested kinds as null.
						ev = &expr.Value{Kind: &expr.Value_NullValue{}}
					}
					nestedEntries = append(nestedEntries, &expr.MapValue_Entry{Key: key, Value: ev})
				}
				outputs.NamedValues[fieldName] = &Value{
					Kind: &Value_Literal{Literal: &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: nestedEntries}}}},
				}
			}
		default:
			return nil, fmt.Errorf("unsupported field type: %s", fieldDesc.Kind().String())
		}
	}
	return outputs, nil
}

// appendListElements converts CEL list elements into a repeated protobuf field.
//
// Both a literal list and the result of a list expression pass through here, so
// the two cannot disagree about what a list may contain. They previously did: the
// expression path inspected the evaluated value's native Go type and understood
// only strings, integers, and booleans, which rejected every list of
// message-typed elements — including printf's args, whose elements are
// flowstate.v1.Value. A list mixing a step reference with a literal therefore
// failed, while the same list written entirely as literals worked.
func appendListElements(
	ctx context.Context,
	elems []*expr.Value,
	fieldDesc protoreflect.FieldDescriptor,
	listField protoreflect.List,
	scope *Scope,
) error {
	for i, elem := range elems {
		if fieldDesc.Kind() == protoreflect.MessageKind {
			pv, err := listMessageElement(ctx, elem, fieldDesc, scope)
			if err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
			listField.Append(pv)
			continue
		}

		pv, err := scalarFromLiteral(elem, fieldDesc)
		if err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}
		listField.Append(pv)
	}
	return nil
}

// listMessageElement converts one CEL value into a message element of a repeated
// field.
func listMessageElement(
	ctx context.Context,
	elem *expr.Value,
	fieldDesc protoreflect.FieldDescriptor,
	scope *Scope,
) (protoreflect.Value, error) {
	msgType := fieldDesc.Message()

	// A flowstate.v1.Value carries the CEL value as-is, which is what lets a task
	// like printf accept arguments of mixed type.
	if msgType.FullName() == "flowstate.v1.Value" {
		wrapped := &Value{Kind: &Value_Literal{Literal: elem}}
		return protoreflect.ValueOfMessage(wrapped.ProtoReflect()), nil
	}

	mapVal, ok := elem.GetKind().(*expr.Value_MapValue)
	if !ok {
		return protoreflect.Value{}, fmt.Errorf("expected a map to build %s, got %s",
			msgType.FullName(), literalKindName(elem))
	}

	msgTypeInfo, err := protoregistry.GlobalTypes.FindMessageByName(msgType.FullName())
	if err != nil {
		return protoreflect.Value{}, fmt.Errorf("could not find message type %q: %w", msgType.FullName(), err)
	}

	nested := msgTypeInfo.New().Interface()
	inputMap := make(map[string]*Value, len(mapVal.MapValue.GetEntries()))
	for _, e := range mapVal.MapValue.GetEntries() {
		inputMap[e.GetKey().GetStringValue()] = &Value{Kind: &Value_Literal{Literal: e.GetValue()}}
	}
	if err := populateProtoMessageFromValueMap(ctx, inputMap, nested, scope); err != nil {
		return protoreflect.Value{}, err
	}
	return protoreflect.ValueOfMessage(nested.ProtoReflect()), nil
}

// scalarFromLiteral converts a CEL literal to a protobuf value for a scalar
// field.
//
// The conversion is driven by which kind the literal actually holds, not by
// whether the extracted value is non-zero. Testing the extracted value conflates
// "wrong type" with "legitimately empty", which rejected every zero value a
// workflow could supply: an empty string message, a count of 0, a flag set to
// false.
// setMapEntries writes a CEL map's entries into a protobuf map field.
//
// One implementation for the literal and the expression paths, and — more to the point
// — the *same* conversion a scalar field uses. Each map path used to have its own
// switch calling the typed getter for the field's kind directly, and a protobuf getter
// answers the zero value for a value of some other kind rather than failing. So
// `headers: {X-Count: 5}` sent the header as an empty string, and `fields: {code: 500}`
// logged `code=`: the wrong thing, silently, in a request and a durable record.
//
// A key that cannot hold its value is now an error naming both, which is what the
// equivalent scalar field has always done. There was never a reason for a value inside
// a mapping to follow looser rules than the same value written beside a key.
func setMapEntries(entries []*expr.MapValue_Entry, fieldDesc protoreflect.FieldDescriptor, m protoreflect.Map) error {
	valueDesc := fieldDesc.MapValue()

	for _, e := range entries {
		key := e.GetKey().GetStringValue()

		// A flowstate.v1.Value map carries whatever the author wrote, unconverted —
		// the shape is the point, so there is nothing to check it against. Handled
		// before the scalar path, which has no case for it.
		if valueDesc.Kind() == protoreflect.MessageKind {
			if e.GetValue().GetKind() == nil {
				continue
			}
			held := &Value{Kind: &Value_Literal{Literal: e.GetValue()}}
			m.Set(protoreflect.ValueOfString(key).MapKey(), protoreflect.ValueOfMessage(held.ProtoReflect()))

			continue
		}

		converted, err := scalarFromLiteral(e.GetValue(), valueDesc)
		if err != nil {
			return fmt.Errorf("key %q: %w", key, err)
		}
		m.Set(protoreflect.ValueOfString(key).MapKey(), converted)
	}

	return nil
}

func scalarFromLiteral(lit *expr.Value, fieldDesc protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	switch fieldDesc.Kind() {
	case protoreflect.StringKind:
		v, ok := lit.GetKind().(*expr.Value_StringValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected a string, got %s", literalKindName(lit))
		}
		return protoreflect.ValueOfString(v.StringValue), nil
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		switch v := lit.GetKind().(type) {
		case *expr.Value_Int64Value:
			if fieldDesc.Kind() == protoreflect.Int32Kind {
				return protoreflect.ValueOfInt32(int32(v.Int64Value)), nil
			}
			return protoreflect.ValueOfInt64(v.Int64Value), nil
		case *expr.Value_Uint64Value:
			if fieldDesc.Kind() == protoreflect.Int32Kind {
				return protoreflect.ValueOfInt32(int32(v.Uint64Value)), nil
			}
			return protoreflect.ValueOfInt64(int64(v.Uint64Value)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected an integer, got %s", literalKindName(lit))
		}
	case protoreflect.BoolKind:
		v, ok := lit.GetKind().(*expr.Value_BoolValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected a boolean, got %s", literalKindName(lit))
		}
		return protoreflect.ValueOfBool(v.BoolValue), nil
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		v, ok := lit.GetKind().(*expr.Value_DoubleValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected a number, got %s", literalKindName(lit))
		}
		if fieldDesc.Kind() == protoreflect.FloatKind {
			return protoreflect.ValueOfFloat32(float32(v.DoubleValue)), nil
		}
		return protoreflect.ValueOfFloat64(v.DoubleValue), nil
	case protoreflect.BytesKind:
		v, ok := lit.GetKind().(*expr.Value_BytesValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected bytes, got %s", literalKindName(lit))
		}
		return protoreflect.ValueOfBytes(v.BytesValue), nil
	case protoreflect.EnumKind:
		// Written as the choice, not as a number. `level: warn` is what a Flowfile
		// says; the number is storage, and a language whose author has to know the
		// storage has failed at being one.
		v, ok := lit.GetKind().(*expr.Value_StringValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected one of %s, got %s",
				strings.Join(EnumValueNames(fieldDesc.Enum()), ", "), literalKindName(lit))
		}
		number, known := EnumValueNumber(fieldDesc.Enum(), v.StringValue)
		if !known {
			return protoreflect.Value{}, fmt.Errorf("%q is not one of %s",
				v.StringValue, strings.Join(EnumValueNames(fieldDesc.Enum()), ", "))
		}
		return protoreflect.ValueOfEnum(number), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field type %s", fieldDesc.Kind())
	}
}

// literalKindName names the kind a CEL literal holds, for error messages that
// tell a workflow author what they actually supplied.
func literalKindName(lit *expr.Value) string {
	switch lit.GetKind().(type) {
	case *expr.Value_StringValue:
		return "a string"
	case *expr.Value_Int64Value:
		return "an integer"
	case *expr.Value_Uint64Value:
		return "an unsigned integer"
	case *expr.Value_DoubleValue:
		return "a number"
	case *expr.Value_BoolValue:
		return "a boolean"
	case *expr.Value_BytesValue:
		return "bytes"
	case *expr.Value_NullValue:
		return "null"
	case *expr.Value_ListValue:
		return "a list"
	case *expr.Value_MapValue:
		return "a map"
	case nil:
		return "nothing"
	default:
		return fmt.Sprintf("%T", lit.GetKind())
	}
}

func populateProtoMessageFromValueMap(ctx context.Context, input map[string]*Value, msg proto.Message, scope *Scope) error {
	msgFields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < msgFields.Len(); i++ {
		fieldDesc := msgFields.Get(i)
		fieldName := string(fieldDesc.Name())
		val, ok := input[fieldName]
		if !ok {
			continue // Field not provided in input map
		}
		if fieldDesc.IsMap() {
			// Support string-keyed maps with primitive values and flowstate.v1.Value messages.
			m := msg.ProtoReflect().Mutable(fieldDesc).Map()
			switch v := val.GetKind().(type) {
			case *Value_Literal:
				if mv, ok := v.Literal.GetKind().(*expr.Value_MapValue); ok {
					if err := setMapEntries(mv.MapValue.GetEntries(), fieldDesc, m); err != nil {
						return fmt.Errorf("field %q: %w", fieldName, err)
					}

					continue
				}
				return fmt.Errorf("expected map literal for field %q", fieldName)
			case *Value_Expr:
				// Evaluate the CEL expression and convert to a protobuf expr.Value.
				out, err := valueToCEL(ctx, val, scope)
				if err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
				pv, err := cel.RefValueToValue(out)
				if err != nil {
					return fmt.Errorf("failed to convert CEL value: %w", err)
				}
				if mv, ok := pv.GetKind().(*expr.Value_MapValue); ok {
					if err := setMapEntries(mv.MapValue.GetEntries(), fieldDesc, m); err != nil {
						return fmt.Errorf("field %q: %w", fieldName, err)
					}
					continue
				}
				return fmt.Errorf("expected map from CEL for field %q", fieldName)
			default:
				return fmt.Errorf("unsupported map input for field %q: %T", fieldName, val)
			}
		}
		if fieldDesc.IsList() {
			listField := msg.ProtoReflect().Mutable(fieldDesc).List()
			switch v := val.GetKind().(type) {
			case *Value_Literal:
				lv, ok := v.Literal.GetKind().(*expr.Value_ListValue)
				if !ok {
					return fmt.Errorf("field %q expects a list, but got %s",
						fieldName, literalKindName(v.Literal))
				}
				if err := appendListElements(ctx, lv.ListValue.GetValues(), fieldDesc, listField, scope); err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
			case *Value_Expr:
				// Evaluate the expression, then convert its result through the
				// same path a literal list takes. Inspecting the CEL value's
				// native Go type instead would diverge from the literal path —
				// which is how a list mixing a reference with a literal, such as
				// printf's args, came to be rejected.
				out, err := valueToCEL(ctx, val, scope)
				if err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
				converted, err := cel.RefValueToValue(out)
				if err != nil {
					return fmt.Errorf("field %q: converting expression result: %w", fieldName, err)
				}
				lv, ok := converted.GetKind().(*expr.Value_ListValue)
				if !ok {
					return fmt.Errorf("field %q expects a list, but the expression produced %s",
						fieldName, literalKindName(converted))
				}
				if err := appendListElements(ctx, lv.ListValue.GetValues(), fieldDesc, listField, scope); err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
			default:
				return fmt.Errorf("unsupported value type for list field %q: %T", fieldName, val)
			}
			continue
		}
		// A singular flowstate.v1.Value field carries whatever the author wrote,
		// unconverted: a literal of any shape, or a secret reference. The http task's
		// `json` body is one, since a request body can be an object of any shape and
		// flattening it to a scalar would lose it.
		if fieldDesc.Kind() == protoreflect.MessageKind &&
			fieldDesc.Message().FullName() == "flowstate.v1.Value" {
			resolved := val

			// An expression is evaluated first, so the field holds a value rather
			// than something still to be computed.
			if val.GetExpr() != nil {
				out, err := valueToCEL(ctx, val, scope)
				if err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
				literal, err := cel.RefValueToValue(out)
				if err != nil {
					return fmt.Errorf("field %q: converting expression result: %w", fieldName, err)
				}
				resolved = &Value{Kind: &Value_Literal{Literal: literal}}
			}

			msg.ProtoReflect().Set(fieldDesc, protoreflect.ValueOfMessage(resolved.ProtoReflect()))
			continue
		}

		switch kind := val.GetKind().(type) {
		case *Value_Expr:
			out, err := valueToCEL(ctx, val, scope)
			if err != nil {
				return fmt.Errorf("field %q: %w", fieldName, err)
			}
			value, err := cel.RefValueToValue(out)
			if err != nil {
				return fmt.Errorf("failed to convert CEL reference to value: %w", err)
			}
			pv, err := scalarFromLiteral(value, fieldDesc)
			if err != nil {
				return fmt.Errorf("field %q: %w", fieldName, err)
			}
			msg.ProtoReflect().Set(fieldDesc, pv)
		case *Value_Literal:
			pv, err := scalarFromLiteral(kind.Literal, fieldDesc)
			if err != nil {
				return fmt.Errorf("field %q: %w", fieldName, err)
			}
			msg.ProtoReflect().Set(fieldDesc, pv)
		default:
			return fmt.Errorf("unsupported value type: %T", val)
		}
	}
	return nil
}

// PopulateLiterals fills msg from the inputs an author wrote as literals, ignoring
// every other kind.
//
// It exists so a *compiler* can ask what the engine would say about the part of a
// step it can already see. `flow validate` used to check an input's type against the
// field and stop there, so a file declaring `method: FETCH` validated cleanly and
// then failed at run time with a Protobuf-flavoured message naming no line — the
// author learning about a rule the schema had stated all along, from the surface
// least able to point at it.
//
// Literals only, and that is the whole discipline. An expression's value depends on
// step outputs that do not exist yet, so a rule checked against it would be checked
// against nothing; a secret is resolved in the activity that needs it and is not a
// value here at all. Both are left out, which means the message this fills is
// deliberately *partial* — so a caller must ignore any violation about a field being
// absent, since absence here says nothing about the file.
//
// The context is unused and cannot be otherwise: resolving an expression is the one
// thing that would need one, and there are none.
func PopulateLiterals(msg proto.Message, inputs map[string]*Value) error {
	literals := make(map[string]*Value, len(inputs))
	for name, value := range inputs {
		if _, isLiteral := value.GetKind().(*Value_Literal); isLiteral {
			literals[name] = value
		}
	}

	return populateProtoMessageFromValueMap(context.Background(), literals, msg, nil)
}
