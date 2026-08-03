package main

import (
	"context"
	"os"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// secretScheme is this plugin's own secret reference scheme: a Flowfile
// authenticates a private repository by writing
// `token: ${secret('vcs:some-name')}`, never a literal.
//
// # Why this plugin resolves its own secrets, instead of accepting any
// # scheme's reference
//
// The obvious-looking alternative - let a task's `token` input accept a
// reference to *any* configured secret backend (env, file, vault, another
// plugin's own scheme) - is not available to a plugin task today, and this
// is worth stating plainly rather than working around silently, since it
// shapes this plugin's whole authentication story.
//
// A task's inputs are resolved by the engine before the step runs, except for
// inputs a [flowstatev1.TaskDef] names in DeferredInputs - and a secret
// reference is never resolved during that pass regardless: eval.go's
// resolveValue refuses to read one in an expression, precisely so a
// reference stays a reference until the one activity that needs the value
// resolves it (invariant 7). For a built-in task like http, "the activity
// that needs the value" is code running inside the engine process, which is
// why taskFuncHTTP can call [flowstatev1.ResolveSecret] directly against the
// engine's own secrets.Store.
//
// A plugin task is different: the [flowstatev1.TaskDef.Fn] the engine calls
// for a plugin task is generated code in this repository's own
// pkg/flowstate/v1/plugin/task.go, and it does not call ResolveSecret - it
// forwards the step's inputs to this process over RPC exactly as given,
// [flowstatev1.Value_SecretRef] included, because a v1.SecretRef carries no
// material by construction and is safe to send as-is. There is no RPC this
// process can call on the host to resolve an *arbitrary* reference on its
// own behalf - [pluginv1.SecretService] runs the other direction, letting
// the host ask a plugin to resolve schemes *that plugin* advertises. So the
// only reference a plugin task can act on today is one whose scheme the same
// plugin binary itself resolves, because then "ask the host to resolve it"
// and "call the function that answers CAPABILITY_SECRETS requests" are the
// same code, called directly, with no RPC in between.
//
// This is reported in both plugins' READMEs as the most significant gap
// found while building them: a workflow cannot hand a plugin task a secret
// that lives in the engine's env/file/vault providers, or in a *different*
// plugin's scheme, without that other provider's owner also standing up a
// scheme this plugin (or one like it) resolves - which is exactly what this
// plugin and flowstate-plugin-github each do, independently, rather than
// being able to share one credential between them.
const secretScheme = "vcs"

// secretEnvPrefix is what this plugin's secrets are named with in the
// process environment, exactly the pattern flowstate-plugin-example uses.
const secretEnvPrefix = "VCS_SECRET_"

// resolveSecret answers a reference from the environment, scoped by
// namespace - the implementation [sdk.Secrets.Resolve] is registered with,
// called by the host over SecretService.Resolve, which is the one path that
// actually carries a namespace (see [SecretRequest.Namespace]).
func resolveSecret(_ context.Context, req sdk.SecretRequest) (sdk.SecretResponse, error) {
	name := envSegment(req.Name)
	if name == "" {
		return sdk.SecretResponse{}, sdk.InvalidInput("secret name is empty")
	}

	variable := secretEnvPrefix
	if namespace := envSegment(req.Namespace); namespace != "" {
		variable += namespace + "_"
	}
	variable += name

	value, ok := os.LookupEnv(variable)
	if !ok {
		return sdk.SecretResponse{}, sdk.NotFound("no secret %q in namespace %q (looked for %s)", req.Name, req.Namespace, variable)
	}
	if value == "" {
		return sdk.SecretResponse{}, sdk.NotFound("%s is set but empty", variable)
	}

	return sdk.SecretResponse{Value: []byte(value)}, nil
}

// envSegment renders part of a reference as part of an environment variable
// name, so that a name from a workflow cannot construct a variable name of
// its choosing.
func envSegment(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - ('a' - 'A'))
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// tokenFromValue extracts an HTTPS credential from a task's `token` input.
//
// A literal string is refused outright - CLAUDE.md is explicit that a
// credential must be a secret reference, never a literal in a Flowfile, and
// this is the one place that rule is enforced for every task in this plugin.
// An unset input is not an error: it means the repository is public.
//
// # The namespace this cannot apply
//
// Resolution happens by calling [resolveSecret] directly, in this same
// process, rather than through the SecretService RPC the host would use -
// see [secretScheme]'s doc comment for why that is the only path available
// to a plugin task at all today. The cost of that path is this function's
// own limitation, and it is a real one: [sdk.SecretRequest.Namespace] is
// what lets a multi-tenant deployment's secret backend scope a lookup to the
// calling workload's own tenant, and a plugin *task* - as opposed to a
// plugin's SecretService handler, which the host calls directly and does
// pass a namespace - has no access to the caller's namespace or identity at
// all. [pluginv1.ExecuteRequest] carries both, but sdk.Task.Fn's signature
// does not expose them (see sdk.go's TaskFunc and taskService.Execute).
//
// So this function resolves every reference in the empty (default) tenant's
// namespace, unconditionally. On a single-tenant deployment - which
// invariant 8, "self-hosted first," treats as the ordinary case - this is
// exactly correct. On a deployment serving several tenants from one worker
// pool, this is wrong in a way worth saying loudly rather than leaving
// implicit: every workload, whichever tenant it belongs to, would resolve
// `${secret('vcs:...')}` against the *same* default-namespace variable,
// which is a tenancy hole of the same shape CLAUDE.md's own env-provider
// story warns about, just with the ambiguity moved from the variable name
// to a namespace this function never received. See the README's "SDK gaps"
// section - fixing this needs sdk.TaskFunc's signature to carry the caller's
// namespace, which is a change to pkg/flowstate/v1/plugin, not to this
// plugin, and is out of scope here.
func tokenFromValue(ctx context.Context, v *flowstatev1.Value) (string, error) {
	if v == nil {
		return "", nil
	}

	switch kind := v.GetKind().(type) {
	case nil:
		return "", nil

	case *flowstatev1.Value_SecretRef:
		ref := kind.SecretRef
		if ref.GetScheme() != secretScheme {
			return "", sdk.InvalidInput(
				"token must be a %q secret reference; got scheme %q", secretScheme, ref.GetScheme())
		}
		resp, err := resolveSecret(ctx, sdk.SecretRequest{Scheme: ref.GetScheme(), Name: ref.GetName()})
		if err != nil {
			return "", err
		}
		return string(resp.Value), nil

	case *flowstatev1.Value_Literal:
		return "", sdk.InvalidInput(
			"token must be a secret reference (${secret('vcs:name')}), never a literal value; " +
				"a literal here would put a credential in the Flowfile and in workflow history")

	default:
		return "", sdk.InvalidInput("token cannot be a %T", kind)
	}
}
