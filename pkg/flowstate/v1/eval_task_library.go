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
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
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

// DefaultEgressPolicy returns the egress policy the built-in http task
// enforces when nothing has replaced it: internal address ranges denied,
// loopback denied unless [AllowLoopbackEgressEnv] is set, every redirect hop
// re-checked, the response body bounded.
//
// This is the *constant* this build ships, not "whatever [DefaultRegistry]
// currently has registered for `http`" — those answer different questions.
// [DefaultRegistry]'s `http` entry is mutable process state: `flow run local
// --egress-policy` and any other caller of [HTTPTaskDef] replace it, and
// once replaced, a reader consulting the registry sees the replacement, not
// this. A caller that needs the *documented default itself* — regardless of
// what anything else in the process has done — calls this directly. See
// pkg/flowstate/embed's RunOptions.EgressPolicy for exactly this need: a nil
// policy there must be a fixed posture, not whatever the registry happens to
// hold at the moment a run starts.
func DefaultEgressPolicy() *netpolicy.Policy {
	return defaultEgressPolicy()
}

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
		// This task reads `outputs:` as a replacement for the outputs it
		// declares, which is the capability every other surface used to infer
		// from the input's name. Declared here so the inference can stop: see
		// [TaskDef.ShapesOutputs].
		ShapesOutputs:    true,
		NeedsPrevOutputs: true,
		AuthorityInputs:  []string{"bearer", "credential"},
		CredentialInputs: []string{"credential"},
		// The three inputs this task turns into request bytes itself, inside the
		// activity: a header it sets on the request, a form it encodes, a JSON
		// body it serializes. A reference nested in one is resolved at exactly
		// that moment and nowhere earlier.
		//
		// `query` is missing on purpose and not by oversight. It is the same kind
		// of map as `form` and is encoded a few lines away from it, so nothing
		// about the mechanism refuses it — what refuses it is the destination: a
		// query string is written to access logs, kept in browser history, and
		// forwarded in a Referer header by anything following a redirect. A
		// credential that reaches one is a credential published, so the position
		// stays refused both here and, for a specification that never met this
		// compiler, in `valueToQueryString`.
		NestedSecretInputs: []string{"form", "headers", "json"},
		// Takes no policy, deliberately. What it answers is what the *task* can
		// request, which is the same in every deployment — see the file it lives
		// in for why asking the policy instead would put DNS in an editor and
		// deployment configuration in a diagnostic.
		CheckLiteral: checkHTTPLiteral,
		Fn:           taskFuncHTTP(policy),
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

// httpResponseEnv returns the CEL environment the http task's own expressions —
// `outputs:` and `expect:` — are evaluated in.
//
// The profile's environment plus the response root, and the profile part is a fix
// rather than a flourish. This used to be `cel.NewEnv(response, jsonLibrary())`: the
// json library and nothing else. So `${vars.greeting.upperAscii()}` worked in a
// `vars:` binding, in an `if:`, in `items:`, in `wait_until:` and in every other task
// input, and failed inside `outputs:` — after the request had already been made,
// since that is when the expression runs.
//
// That is the exact defect `libs:` was retired to end. A step could once name its own
// extension libraries and nothing else in the file could, so one step spoke a richer
// dialect than the rest; the profile replaced it with a single membership. This was
// the same split from the other side — one position speaking a *poorer* dialect than
// the rest — and it survived the retirement because it is not spelled anywhere in the
// grammar. One dialect means these two positions as well.
//
// Found by a type checker disagreeing with the runtime: the validator judged these
// expressions against the profile, which was right, against a runtime that was not.
//
// # The example above needs both halves
//
// `${vars.greeting.upperAscii()}` names a function and a variable, and fixing the
// environment fixed only the function. The *activation* is where the variable comes
// from, and both callers built one by hand over the step outputs alone — so the same
// expression kept failing here, now saying `no such key: greeting` about a variable
// declared at the top of the file. Worse than the unbound name it replaced: an empty
// `vars` root answers by design, so that `vars.missing` reads as a missing key rather
// than as the namespace not existing, and with nothing in it every var read that way.
//
// One dialect is a claim about names as much as functions, and a position gets both
// from the scope or it is still a dialect of its own. See [Scope.Activation].
//
// # The profile is the run's, not the build's
//
// Taken from the scope rather than from [CurrentProfile], because this evaluates on
// whichever worker picked the activity up, and that worker's build may know a
// different set of libraries than the one that compiled the spec. The whole point of
// recording a profile is that the run's vocabulary travels with it — resolving it
// here from a package constant would be the bug invariant 10 exists to prevent, in
// the one place the scope is already carrying the answer.
func httpResponseEnv(profile string) (*cel.Env, error) {
	libs, err := ProfileLibraries(profile)
	if err != nil {
		return nil, err
	}

	base, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil, err
	}

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
	env, err := base.Extend(cel.Variable(ResponseRoot, cel.DynType))
	if err != nil {
		return nil, fmt.Errorf("create HTTP outputs CEL environment: %w", err)
	}

	return env, nil
}

// revealSecret returns the [revealFunc] the http task resolves references
// through: authorized and resolved from the activity's own execution context, and
// registered with the scrubber so a peer reflecting the value back cannot put it
// into an output.
//
// The context is captured rather than threaded through every converter, and that is
// deliberate. What travels is a function, so nothing between here and the header or
// the encoded body holds a resolved value in a field, a map, or a struct — the
// arrangement that survives `%+v` on whatever happens to hold it.
func revealSecret(ctx context.Context, scrubber *secrets.Scrubber) revealFunc {
	return func(ref *SecretRef) (string, error) {
		secret, err := ResolveSecret(ctx, ref)
		if err != nil {
			return "", &secretResolutionError{err: fmt.Errorf(
				"resolving reference %s: %w", secretRefText(ref), err)}
		}
		scrubber.Add(secret)

		return secret.Reveal(), nil
	}
}

// httpInputError classifies a failure to build the request from its inputs.
//
// A reference the policy refused is a denial and will be refused again; a
// malformed input is the file's mistake. Both used to be reported as invalid
// input, which made a policy decision look like a typo and — worse — made it
// retryable in exactly the cases retrying cannot help.
func httpInputError(err error) error {
	var resolution *secretResolutionError
	if errors.As(err, &resolution) {
		kind := ErrorKindPolicyDenied
		if secrets.Retryable(err) {
			kind = ErrorKindUpstream
		}

		return NewTaskError("http", kind, err)
	}

	return NewTaskError("http", ErrorKindInvalidInput, err)
}

func taskFuncHTTP(policy *netpolicy.Policy) TaskFunc {
	return func(ctx context.Context, input map[string]*Value, scope *Scope) (*Node_Outputs, error) {
		taskInputs := &Task_HTTP_Inputs{
			Method: proto.String(http.MethodGet),
		}

		// `outputs` and `expect` are evaluated against the response rather than
		// against earlier steps, so they are held back from population: resolving
		// them here would fail on `status_code` and `body`, which do not exist yet.
		//
		// `headers` is held back for a different reason and only when it is written
		// as a structure. The schema's field is map<string, string> and a reference
		// is not a string, so a structure carrying one has nowhere to be put down —
		// and putting the *resolved* value there would be worse than the error it
		// avoids, since a proto struct field is exactly what `fmt` reflects into.
		// It stays a Value until [httpRequestHeaders] hands it to the request.
		var outputsSpec, expectSpec, headersSpec *Value
		inputForPopulate := input
		if _, hasOutputs := input["outputs"]; hasOutputs || input["expect"] != nil ||
			input["headers"].GetStructure() != nil {
			outputsSpec, expectSpec = input["outputs"], input["expect"]
			if input["headers"].GetStructure() != nil {
				headersSpec = input["headers"]
			}
			inputForPopulate = make(map[string]*Value, len(input))
			for k, v := range input {
				if k == "outputs" || k == "expect" {
					continue
				}
				if k == "headers" && headersSpec != nil {
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
			case *Value_Structure_:
				// The mapping form, compiled entry by entry so its names survive
				// into the specification. Each entry lands in the same field a
				// literal map lands in and is evaluated by the same loop below —
				// the shape the schema always had for this input, now reachable
				// from a spelling that keeps its keys.
				mapped := kind.Structure.GetMap()
				if mapped == nil {
					return nil, fmt.Errorf("outputs must be a mapping of names to values, not a list")
				}
				taskInputs.Outputs = mapped.GetEntries()
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
			return nil, httpInputError(err)
		}

		// The scrubber exists before the first reference is resolved, because every
		// value resolved from here on is registered with it: a peer that reflects a
		// header or a body back is the path that turns a request credential into a
		// durable output.
		scrubber := secrets.NewScrubber()
		reveal := revealSecret(ctx, scrubber)

		bodyText, contentType, err := httpRequestBody(taskInputs, reveal)
		if err != nil {
			return nil, httpInputError(err)
		}

		var body io.Reader
		if bodyText != "" || taskInputs.Body != nil {
			body = strings.NewReader(bodyText)
		}

		// Carry the run's attested identity into the egress policy, so a rule can
		// scope this request by tenant (#240). It is rendered from the one
		// WorkloadIdentity the scope carries — the same source the secret-access and
		// task-shape policies read — rather than derived a second way, which is what
		// keeps the three surfaces agreeing about who is calling. A local run's scope
		// has no identity: that renders as the empty identity, which an
		// identity-scoped allow rule declines to match, exactly as the task-shape
		// surface behaves.
		if id := scope.GetIdentity(); id != nil {
			ctx = netpolicy.ContextWithIdentity(ctx, netpolicy.Identity{
				Subject:   id.GetSubject(),
				Issuer:    id.GetIssuer(),
				Namespace: id.GetNamespace(),
				Claims:    id.GetClaims(),
			})
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

		// And the ones written as a structure, whose references are resolved one
		// entry at a time, straight onto the request. Applied before the bearer and
		// credential blocks below so that an `Authorization` written here is the
		// header they refuse to overwrite — two ways of setting one header have to
		// meet whichever order an author writes them in.
		if headersSpec != nil {
			header, err := httpRequestHeaders(headersSpec, reveal)
			if err != nil {
				return nil, httpInputError(err)
			}
			for name, values := range header {
				for _, value := range values {
					httpReq.Header.Add(name, value)
				}
			}
		}

		// Resolve the reference only here, after the task has crossed into its
		// execution context. The workflow and the activity payload continue to hold
		// only the reference; the revealed value exists for the lifetime of this
		// request and is never added to an output.
		if taskInputs.GetBearer() != nil && taskInputs.GetCredential() != "" {
			return nil, NewTaskError("http", ErrorKindInvalidInput,
				fmt.Errorf("bearer and credential are mutually exclusive; use a static secret reference or a JIT federation target, not both"))
		}
		if bearer := taskInputs.GetBearer(); bearer != nil {
			ref := bearer.GetSecretRef()
			if ref == nil {
				return nil, NewTaskError("http", ErrorKindInvalidInput,
					fmt.Errorf("bearer must be a secret reference, such as env:API_TOKEN"))
			}
			if httpReq.Header.Get("Authorization") != "" {
				return nil, NewTaskError("http", ErrorKindInvalidInput,
					fmt.Errorf("bearer and an Authorization header cannot both be set"))
			}
			secret, err := ResolveSecret(ctx, ref)
			if err != nil {
				kind := ErrorKindPolicyDenied
				if secrets.Retryable(err) {
					kind = ErrorKindUpstream
				}
				return nil, NewTaskError("http", kind, fmt.Errorf("resolving bearer reference %s: %w", secretRefText(ref), err))
			}
			scrubber.Add(secret)
			httpReq.Header.Set("Authorization", "Bearer "+secret.Reveal())
		}
		if target := taskInputs.GetCredential(); target != "" {
			if httpReq.Header.Get("Authorization") != "" {
				return nil, NewTaskError("http", ErrorKindInvalidInput,
					fmt.Errorf("credential and an Authorization header cannot both be set"))
			}
			if err := AuthorizeCredential(ctx, httpReq, target); err != nil {
				kind := ErrorKindPolicyDenied
				if auth.Retryable(err) {
					kind = ErrorKindUpstream
				}
				return nil, NewTaskError("http", kind,
					fmt.Errorf("authorizing federation target %q: %w", target, err))
			}
			// The broker applies material directly to the request. Register both the
			// complete header and its bearer value so a peer reflecting either form
			// cannot put the credential into outputs or history.
			authorization := httpReq.Header.Get("Authorization")
			scrubber.AddValue(authorization)
			if token, found := strings.CutPrefix(authorization, "Bearer "); found {
				scrubber.AddValue(token)
			}
		}

		// A structured body implies the header describing it, but only when the author
		// did not say: someone sending a JSON variant like application/ld+json has
		// been more specific than we can be, and overwriting that would be wrong.
		if contentType != "" && httpReq.Header.Get("Content-Type") == "" {
			httpReq.Header.Set("Content-Type", contentType)
		}

		// Built, authorized, and about to leave the worker. A step that sits here is
		// waiting on somebody else, and until this said so a long request was
		// indistinguishable from a wedged worker.
		ReportProgress(ctx, PhaseRequesting)

		httpResp, err := policy.Client().Do(httpReq)
		if err != nil {
			err = scrubber.ScrubError(err)
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
		// The peer answered; what is left is however long it takes to say the rest.
		// Separate from the phase above because the two fail differently: stuck
		// requesting is a peer that has said nothing, stuck reading is a peer that
		// answered and then stopped talking.
		ReportProgress(ctx, PhaseReadingResponse)

		respBody, err := policy.ReadResponseBody(httpResp)
		if err != nil {
			err = scrubber.ScrubError(err)
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

		// A peer may reflect the Authorization header in its response body or
		// headers. Those values are about to become task outputs and workflow
		// history, so scrub them before parsing, expectation evaluation, logging or
		// output shaping can observe them. Scrubbing only transport errors protects
		// the exceptional path and leaves the successful echo path wide open.
		respBody = []byte(scrubber.Scrub(string(respBody)))
		for name, values := range httpResp.Header {
			for i := range values {
				values[i] = scrubber.Scrub(values[i])
			}
			httpResp.Header[name] = values
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

			// Bounded here, before anything below gets a chance to walk it: both
			// `expect:` (httpExpectationMet, below) and `outputs:` (the
			// evaluation further down) run a CEL expression directly against
			// `response.json` *inside this function*, so
			// [checkTaskOutputElementBound] at [Task.EvalInScope] — which only
			// sees what this function returns — is too late to stop a
			// comprehension over an oversized response from ever running. The
			// byte cap on the body (see [netpolicy.Policy.ReadResponseBody])
			// bounds bytes, not elements: a body well under it can still carry
			// tens of thousands of small elements, exactly the asymmetry #204
			// measured for the input side and #224 review found still open
			// here for a task's own evaluation of its result.
			if err := checkHTTPResponseElementBound(taskInputs.GetUrl(), parsedJSON); err != nil {
				return nil, err
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
			env, err := httpResponseEnv(scope.GetProfile())
			if err != nil {
				return nil, err
			}
			varAct, err := interpreter.NewActivation(respVars)
			if err != nil {
				return nil, fmt.Errorf("failed to create activation: %w", err)
			}
			// The scope's own activation, which is what carries `vars.<name>` and a
			// loop's iterator into here — and which sets Ctx, Eval and Profile, so a
			// stored expression resolved while shaping these outputs stays
			// cancellable, cost-bounded and pinned to the run's vocabulary. The
			// hand-built one this replaces set the first two and dropped the rest.
			//
			// The response is the child, so it is asked first; see the same note in
			// [httpExpectSatisfied].
			act := interpreter.NewHierarchicalActivation(scope.Activation(ctx), varAct)

			if outputsExpr != nil {
				// Through the shared evaluator, which is what applies the cost
				// limit and makes the evaluation cancellable. Building a program
				// here by hand did neither: an author's `outputs:` expression is
				// the expression they most directly control, and it was the one
				// place in the engine that ran unbounded.
				//
				// Every failure below is classified, and that is not tidiness. An
				// unwrapped error classifies as [ErrorKindInternal], which is
				// *retryable* — so a typo in `outputs:` re-ran the whole attempt,
				// and the whole attempt begins by sending the request again. A POST
				// that had already succeeded was sent five times for a mistake no
				// number of attempts could fix. [ErrorKind.Retryable] states the
				// rule this was breaking: "Retrying a POST that already took effect
				// is worse than surfacing a failure that might have resolved on its
				// own." `expect:`, one file over, has classified all along.
				out, err := DefaultEvaluator().EvalParsed(ctx, env, outputsExpr, act)
				if err != nil {
					return nil, NewTaskError("http", ErrorKindExpression,
						fmt.Errorf("evaluating outputs: %w", err))
				}
				pv, err := cel.RefValueToValue(out)
				if err != nil {
					return nil, NewTaskError("http", ErrorKindExpression,
						fmt.Errorf("converting the result of outputs: %w", err))
				}
				mv, ok := pv.GetKind().(*expr.Value_MapValue)
				if !ok {
					return nil, NewTaskError("http", ErrorKindExpression,
						fmt.Errorf("outputs must evaluate to a map of names to values, got %s",
							out.Type().TypeName()))
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
						return nil, NewTaskError("http", ErrorKindExpression,
							fmt.Errorf("evaluating output %q: %w", name, err))
					}
					pv, err := cel.RefValueToValue(out)
					if err != nil {
						return nil, NewTaskError("http", ErrorKindExpression,
							fmt.Errorf("converting output %q: %w", name, err))
					}
					outputs.NamedValues[name] = &Value{Kind: &Value_Literal{Literal: pv}}
				case *Value_Literal:
					outputs.NamedValues[name] = &Value{Kind: &Value_Literal{Literal: k.Literal}}
				default:
					return nil, NewTaskError("http", ErrorKindInvalidInput,
						fmt.Errorf("output %q is neither an expression nor a literal (%T)", name, v.GetKind()))
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

// nestedSecretHelp says where a reference nested in a list or a mapping can go,
// for a specification that reached a worker without passing `flow validate`.
//
// It names the http task the way the neighbouring message about `bearer:` does,
// rather than asking the registry which inputs accept one: this file builds the
// registry, so reading it from here is an initialization cycle. The author-facing
// answer — which inputs of *this* task accept a reference, named from the task's
// own definition — is the compiler's, in `flowfile`, where there is a line and a
// column to put it on.
const nestedSecretHelp = "; an input that accepts one applies its entries itself, inside the " +
	"activity, which is what lets the reference stay a reference — the http task's headers, " +
	"form and json are the ones built today"

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
			case *Value_Structure_:
				// A mapping whose entries are values in their own right, which is
				// the shape that can hold a secret reference. It is set entry by
				// entry and unconverted: the field's value type is
				// flowstate.v1.Value, so what the author wrote arrives at the task
				// exactly as written, reference included, and the task resolves it
				// where it uses it.
				entries, isMap := StructureMap(val)
				if !isMap {
					return fmt.Errorf("field %q expects a mapping, but a list was given", fieldName)
				}
				if fieldDesc.MapValue().Message() == nil ||
					fieldDesc.MapValue().Message().FullName() != "flowstate.v1.Value" {
					// map<string, string> and its like. The entries could be
					// flattened into strings, and a reference among them could
					// not — and this is the branch a reference would arrive by,
					// so flattening would resolve one into a field that a `%+v`
					// anywhere would print.
					return fmt.Errorf(
						"field %q holds plain values, so it cannot carry a secret reference%s",
						fieldName, nestedSecretHelp)
				}
				for name, entry := range entries {
					if name == "" {
						return fmt.Errorf("field %q has an entry with an empty name", fieldName)
					}
					m.Set(protoreflect.ValueOfString(name).MapKey(),
						protoreflect.ValueOfMessage(entry.ProtoReflect()))
				}
				continue
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
		case *Value_SecretRef:
			// The one kind that is deliberately inert everywhere except the activity
			// that uses it, and which no task input accepts yet.
			//
			// Named rather than left to the default below, which reported
			// `unsupported value type: *flowstatev1.Value` — a Go type, naming
			// neither the input nor the reference, for a spelling `flow validate`
			// had just accepted. An author who wrote `${secret(...)}` where it does
			// not go got no way to tell what they had written wrong.
			//
			// About the *field* and not the task, which is a distinction with a
			// caller: `plugin/sdk/values.go` takes a singular `flowstate.v1.Value`
			// field whole, secret reference included, and says so in the same words.
			// A task-wide claim would send an author away from another input on the
			// same task that would have worked.
			return fmt.Errorf(
				"field %q was given a secret reference (%s:%s), which this field's type "+
					"cannot hold; a field declared as flowstate.v1.Value receives one whole, "+
					"which is how a task takes a value it resolves itself — the http task's "+
					"bearer: input is the one built today",
				fieldName, kind.SecretRef.GetScheme(), kind.SecretRef.GetName())

		case *Value_Structure_:
			// A list or a mapping where the field holds one value. Named for the
			// same reason the reference above is: this is the shape a Flowfile
			// compiles a structure holding a reference into, so an author who put
			// one where a scalar belongs meets a sentence about what they wrote.
			return fmt.Errorf(
				"field %q was given a list or a mapping, which this field's type cannot hold%s",
				fieldName, nestedSecretHelp)

		default:
			return fmt.Errorf("field %q: unsupported value type: %T", fieldName, val)
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
