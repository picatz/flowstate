package main

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// dsnFromValue extracts the resolved connection string from the dsn input.
//
// By the time a task's Fn runs, dsn holds either a literal string (an
// author wrote one directly - discouraged but not refused here, for the
// same reason plugins/codex's apiKeyFromValue is not: this task cannot tell
// that case apart from the one below, since both arrive as the same
// [flowstatev1.Value_Literal] shape) or the value the host resolved from a
// secret reference this task declared in secret_inputs (main.go). A
// [flowstatev1.Value_SecretRef] should never reach here at all - the host
// refuses to forward one for a declared input without resolving it first -
// and is refused defensively rather than trusted to already be impossible.
func dsnFromValue(v *flowstatev1.Value) (string, error) {
	if v == nil {
		return "", sdk.InvalidInput("dsn is required")
	}

	switch kind := v.GetKind().(type) {
	case nil:
		return "", sdk.InvalidInput("dsn is required")
	case *flowstatev1.Value_Literal:
		s, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok {
			return "", sdk.InvalidInput("dsn must be a string")
		}
		if s.StringValue == "" {
			return "", sdk.InvalidInput("dsn is required")
		}
		return s.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed(
			"dsn reached this task still holding a secret reference; the host is supposed to " +
				"resolve every declared secret_inputs entry before calling this task, so this is a " +
				"bug in the host or in this task's own manifest, not something a Flowfile author caused")
	default:
		return "", sdk.InvalidInput("dsn cannot be a %T", kind)
	}
}
