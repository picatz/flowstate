package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// secretScheme is the compatibility secret provider for existing
// `${secret('git:...')}` references. Tasks no longer resolve it directly:
// token is declared in secret_inputs, so the host resolves any configured
// provider under the caller's namespace and scrubs the resulting value before
// the task sees it. Keeping this provider preserves existing Flowfiles while
// making it one backend behind the shared host-resolution contract.
const secretScheme = "git"

// secretEnvPrefix is what this plugin's secrets are named with in the
// process environment.
const secretEnvPrefix = "GIT_SECRET_"

// resolveSecret answers a reference from the environment, scoped by the
// namespace the host established for the caller.
func resolveSecret(_ context.Context, req sdk.SecretRequest) (sdk.SecretResponse, error) {
	name, err := envSegment(req.Name)
	if err != nil {
		return sdk.SecretResponse{}, sdk.InvalidInput("invalid secret name %q: %v", req.Name, err)
	}
	if name == "" {
		return sdk.SecretResponse{}, sdk.InvalidInput("secret name is empty")
	}

	namespace, err := envSegment(req.Namespace)
	if err != nil {
		return sdk.SecretResponse{}, sdk.InvalidInput("invalid secret namespace %q: %v", req.Namespace, err)
	}
	// The namespace length is part of the key even for the default namespace.
	// A delimiter alone is ambiguous because '_' can occur in either encoded
	// segment (it represents '-'). The length makes the segment boundary
	// explicit, so no namespace/name pair can alias another pair.
	variable := secretEnvPrefix + strconv.Itoa(len(namespace)) + "_" + namespace + "_" + name

	value, ok := os.LookupEnv(variable)
	if !ok {
		return sdk.SecretResponse{}, sdk.NotFound("no secret %q in namespace %q (looked for %s)", req.Name, req.Namespace, variable)
	}
	if value == "" {
		return sdk.SecretResponse{}, sdk.NotFound("%s is set but empty", variable)
	}

	return sdk.SecretResponse{Value: []byte(value)}, nil
}

// envSegment renders one reference segment as part of an environment variable
// name: lowercase ASCII letters and digits pass through upcased, a hyphen
// becomes an underscore, and everything else is refused. The empty string is a
// valid segment, and is what the default namespace renders as.
//
// Rejecting rather than replacing any other character keeps this mapping
// injective: authorization is performed against the original reference, so two
// references must never select the same variable. Injectivity of a segment is
// not by itself enough — see [resolveSecret] for how the pair of them is kept
// unambiguous — but without it nothing downstream can recover the difference.
//
// The refusal names the offending character and where it is, because the
// author of a Flowfile sees this through `sdk.InvalidInput` and "some character
// somewhere is wrong" is not something anyone can act on.
func envSegment(s string) (string, error) {
	var b strings.Builder
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-':
			b.WriteByte('_')
		default:
			return "", fmt.Errorf(
				"%q at offset %d is not allowed; use only lowercase ASCII letters, digits, and hyphens",
				r, i)
		}
	}

	return b.String(), nil
}

// tokenFromValue extracts the host-resolved HTTPS credential from token. The
// manifest requires any supplied token to be a whole secret reference, but by
// the time this function runs the host has replaced that reference with its
// string value. An unresolved reference is therefore a host/manifest defect,
// not an input this plugin may resolve in the default namespace.
func tokenFromValue(_ context.Context, v *flowstatev1.Value) (string, error) {
	if v == nil {
		return "", nil
	}

	switch kind := v.GetKind().(type) {
	case nil:
		return "", nil
	case *flowstatev1.Value_Literal:
		s, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok {
			return "", sdk.InvalidInput("token must resolve to a string")
		}
		return s.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed("token reached this task still holding a secret reference; the host must resolve declared secret_inputs before invoking the plugin")
	default:
		return "", sdk.InvalidInput("token cannot be a %T", kind)
	}
}
