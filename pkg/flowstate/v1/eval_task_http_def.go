package flowstatev1

import (
	"os"
	"slices"
	"sync"

	"github.com/goccy/go-yaml"

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
	if allowLoopbackEgress() {
		opts = append(opts, netpolicy.WithAllowLoopback())
	}
	p, err := netpolicy.New(opts...)
	if err != nil {
		panic("flowstate: building the default egress policy: " + err.Error())
	}
	return p
})

// allowLoopbackEgress reads the one lever the default policy has, once.
//
// Once, and shared, because two things are derived from it — the policy the
// built-in http task enforces and the document a worker grants every plugin it
// launches ([DefaultEgressPolicyDocument]) — and they are the same deployment's
// answer to one question. Reading the variable twice would let a process that
// changed it between the reads govern its own task under one posture and its
// plugins under the other.
var allowLoopbackEgress = sync.OnceValue(func() bool {
	return os.Getenv(AllowLoopbackEgressEnv) == AllowLoopbackEgressValue
})

// defaultEgressPolicyDocument writes the default down, once.
//
// Marshaling the [netpolicy.Config] rather than spelling the YAML is what makes
// the document and the policy one fact: the same struct that parses an
// operator's file produces this, so a field added to the config surface cannot
// leave the default saying something the parser no longer reads. Failing to
// marshal a struct with no dynamic content is a defect in Flowstate rather than
// anything a workload can cause, which is why it panics for the same reason
// [defaultEgressPolicy] does.
var defaultEgressPolicyDocument = sync.OnceValue(func() []byte {
	doc, err := yaml.Marshal(netpolicy.Config{
		DeploymentDefault: true,
		Egress:            netpolicy.EgressConfig{AllowLoopback: allowLoopbackEgress()},
	})
	if err != nil {
		panic("flowstate: writing the default egress policy document: " + err.Error())
	}
	return doc
})

// DefaultEgressPolicyDocument returns the deployment default written as a policy
// document: the same posture [DefaultEgressPolicy] builds, in the form an
// operator's --egress-policy file has, marked `deployment_default: true`.
//
// A worker launching a plugin grants this when no operator policy was
// configured. Serializing the default rather than leaving the grant out is what
// makes an absent grant mean only "launched by something other than a worker":
// a plugin under a default worker holds the policy that worker's own http task
// runs under, and can see that it is the default rather than an operator's file
// (see [netpolicy.Config.DeploymentDefault] and #1332).
//
// Each call returns a fresh copy, because the caller hands it to a plugin
// launch that clones and forwards it, and a shared slice reaching that far is a
// grant one plugin's launch could edit for the next.
func DefaultEgressPolicyDocument() []byte {
	return slices.Clone(defaultEgressPolicyDocument())
}

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
