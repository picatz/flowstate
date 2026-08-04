package main

import (
	"context"
	"os"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// secretScheme is this plugin's own secret reference scheme:
// `token: ${secret('git:some-name')}`, never a literal. See
// plugins/vcs/secrets.go's doc comment for the full argument on why a
// plugin task can only resolve its own scheme today - nothing about that
// gap is specific to this plugin, so it is not repeated here in full; the
// short version is that a plugin task's inputs reach it over RPC exactly as
// given, and there is no RPC a plugin can call to ask the host to resolve an
// arbitrary reference on its own behalf.
//
// Once PR #160 (TaskManifest.secret_inputs) merges, this plugin should
// declare its token input there instead and drop this plugin-local scheme -
// referenced here by number rather than built against, since that PR is not
// merged yet.
const secretScheme = "git"

// secretEnvPrefix is what this plugin's secrets are named with in the
// process environment.
const secretEnvPrefix = "GIT_SECRET_"

// resolveSecret answers a reference from the environment, scoped by
// namespace - see plugins/vcs/secrets.go's identical function for the one
// gap this shares with it: namespace is always the default (empty) one,
// which is correct for a single-tenant deployment and wrong, in the way
// documented there, for one serving several tenants from a shared worker
// pool.
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
// See plugins/vcs/secrets.go's identical function for the full argument: a
// literal is refused outright, an unset input means a public repository,
// and only this plugin's own scheme resolves.
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
			"token must be a secret reference (${secret('git:name')}), never a literal value; " +
				"a literal here would put a credential in the Flowfile and in workflow history")

	default:
		return "", sdk.InvalidInput("token cannot be a %T", kind)
	}
}
