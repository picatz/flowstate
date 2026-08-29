package flowtest

import (
	"testing"

	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// scopeWithInput is the smallest scope [sensitiveNativeValues] reads: one
// input under one name. Internal because the value this test needs — a
// literal the converter refuses — cannot be written in a YAML fixture a
// reader could take in at a glance.
func scopeWithInput(name string, value *v1.Value) *v1.Scope {
	return &v1.Scope{Inputs: map[string]*v1.Value{name: value}}
}

// A sensitive input this cannot read must withhold the whole invocation, not
// drop out of the redaction set. Both shapes here used to `continue`, which
// left the diagnostic redacting *nothing* about that input while looking
// exactly like a run with no sensitive inputs at all — allow-on-error in the
// one function whose job is to deny (CLAUDE.md, "fail closed").
func TestASensitiveInputThatCannotBeReadWithholdsEveryInput(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value *v1.Value
	}{
		{
			// Not a literal at all: GetLiteral is nil, so there is no value
			// to compare anything against.
			name:  "an unresolved expression",
			value: &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "TOKEN"}}},
		},
		{
			// A literal [v1.LiteralToGo] refuses: a map keyed by an integer
			// has no Go map[string]any spelling, and it fails closed rather
			// than collapsing every entry into object[""].
			name: "a literal with a non-string map key",
			value: &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{
				Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{{
					Key:   &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 1}},
					Value: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "shh-secret-value"}},
				}}}},
			}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sensitive := sensitiveNativeValues(scopeWithInput("creds", tc.value), map[string]bool{"creds": true})
			require.True(t, sensitive.WithholdAll(), "an unreadable sensitive input must withhold, not be skipped")

			// And the refusal reaches the diagnostic: an ordinary,
			// non-sensitive input goes with it.
			msg := unmatchedStubError("http", 1,
				map[string]any{"url": "https://example.invalid/probe"}, nil, sensitive, nil).Error()
			require.NotContains(t, msg, "https://example.invalid/probe")
			require.Contains(t, msg, "[redacted: url]")
			require.Contains(t, msg, "could not be enumerated")
		})
	}
}
