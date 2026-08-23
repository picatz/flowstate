package flowstatev1

import (
	"os"
	"sync"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// AllowLoopbackEgressEnv names the environment variable that permits the http
// task to reach loopback addresses.
//
// Developing against a service on localhost is ordinary and the default policy
// denies it, so there has to be a way to say yes. It is an explicit opt-in rather
// than a default because the same permission is what lets a workflow reach a
// worker's own internal endpoints in production.
const AllowLoopbackEgressEnv = "FLOWSTATE_ALLOW_LOOPBACK_EGRESS"

// AllowLoopbackEgressValue is the one value of [AllowLoopbackEgressEnv] that
// permits loopback. Anything else — including `1`, which reads as an opt-in to
// every person who has ever set a boolean environment variable — leaves the
// default policy denying loopback exactly as if the variable were unset.
//
// Exported so that a diagnostic offering the opt-in reads the value rather than
// spelling it: `flow run local`'s loopback denial suggested
// `FLOWSTATE_ALLOW_LOOPBACK_EGRESS=1` for as long as this check has said
// `"true"`, which is a remedy that does not work handed to the one author who
// most needs one that does. The check below is the only reader of the variable,
// so a suggestion built from this constant cannot disagree with it.
const AllowLoopbackEgressValue = "true"

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
	if os.Getenv(AllowLoopbackEgressEnv) == AllowLoopbackEgressValue {
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
		// A declared response runs the task's own deferred-input evaluation —
		// `expect:` and `outputs:` over the response a test supplies — so a
		// stub can exercise the shaping a `returns:` stub bypasses (#925).
		StubResponseFn: httpStubResponseFn,
		Fn:             taskFuncHTTP(policy),
	}
}
