package flowtest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// scopeWithInput is the smallest scope [sensitiveNativeValues] reads: one
// input under one name. Internal because the values these tests need — a
// literal the converter refuses, and a list wide enough to blow the walk's
// bound — cannot be written in a YAML fixture a reader could take in at a
// glance, and one of them cannot be written in one at all.
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
			require.True(t, sensitive.withholdAll, "an unreadable sensitive input must withhold, not be skipped")
			require.Empty(t, sensitive.values, "a partial set is what withholdAll exists to refuse")

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

// The walk's bound is exact on both sides: a sensitive input whose whole tree
// fits is enumerated normally, and one element more withholds everything
// rather than proceeding with the prefix the walk managed to collect.
func TestTheSensitiveDescendantBoundWithholdsRatherThanTruncates(t *testing.T) {
	t.Parallel()

	// The list itself counts as one of the values, so a list of
	// maxSensitiveDescendants-1 elements is the widest one that fits.
	fits := sensitiveNativeValues(
		scopeWithInput("bulk", literalStringList(maxSensitiveDescendants-1)),
		map[string]bool{"bulk": true},
	)
	require.False(t, fits.withholdAll)
	require.Len(t, fits.values, maxSensitiveDescendants, "the container and every element are in the set")

	over := sensitiveNativeValues(
		scopeWithInput("bulk", literalStringList(maxSensitiveDescendants)),
		map[string]bool{"bulk": true},
	)
	require.True(t, over.withholdAll, "one element past the bound must withhold, not truncate")
	require.Empty(t, over.values)
}

// A one-rune leaf is kept out of the textual backstop: replacing every `a` in
// the rendered line would destroy the diagnostic while protecting nothing the
// exact-value comparison has not already caught. The declared input's own
// value has no such floor, which the second half pins.
func TestOnlyTheDeclaredValueEscapesTheSubstringFloor(t *testing.T) {
	t.Parallel()

	nested := sensitiveNativeValues(
		scopeWithInput("creds", &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{
			Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{
				{
					Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: "initial"}},
					Value: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "a"}},
				},
				{
					Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: "token"}},
					Value: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "shh-secret-value"}},
				},
			}}},
		}}}),
		map[string]bool{"creds": true},
	)
	require.NotContains(t, nested.substrings, "a", "a one-rune leaf is a shredder, not a redaction")
	require.Contains(t, nested.substrings, "shh-secret-value")
	// It is still compared by value, so the leaf itself never prints.
	require.True(t, isSensitiveValue("a", nested.values))

	declared := sensitiveNativeValues(
		scopeWithInput("pin", &v1.Value{Kind: &v1.Value_Literal{
			Literal: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "a"}},
		}}),
		map[string]bool{"pin": true},
	)
	require.Contains(t, declared.substrings, "a",
		"the value `sensitive:` names is replaced textually whatever its length: it is what `\"Bearer \" + inputs.pin` needs")
}

// literalStringList builds a sensitive-input-shaped literal list of n
// distinct strings.
func literalStringList(n int) *v1.Value {
	values := make([]*expr.Value, 0, n)
	for i := range n {
		values = append(values, &expr.Value{Kind: &expr.Value_StringValue{StringValue: fmt.Sprintf("element-%d", i)}})
	}
	return &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{
		Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: values}},
	}}}
}
