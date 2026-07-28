package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestEveryTaskDescribesItself is what `flow tasks`, the editor, and the
// validator all rest on.
//
// Each of those used to answer "what does this task take" separately — two
// implementations of required-ness, and a type namer that existed only in the
// language server, which is why `flow tasks` could say a task exists and not what
// it needs. One answer now, derived from the descriptor.
func TestEveryTaskDescribesItself(t *testing.T) {
	t.Parallel()

	for _, def := range v1.DefaultRegistry().All() {
		t.Run(def.Name, func(t *testing.T) {
			t.Parallel()

			inputs := v1.Inputs(def)
			require.NotEmpty(t, inputs, "task %q describes no inputs", def.Name)

			for _, field := range inputs {
				require.NotEmpty(t, field.Name)
				require.NotEmpty(t, field.Type, "input %q has no type an author could read", field.Name)
				require.NotContains(t, field.Type, "TYPE_",
					"input %q reports a Protobuf type name rather than the DSL's: %s", field.Name, field.Type)
			}

			// Required first, so a reader meets what they cannot leave out before
			// what merely tunes it.
			seenOptional := false
			for _, field := range inputs {
				if !field.Required {
					seenOptional = true
					continue
				}
				require.False(t, seenOptional,
					"required input %q sorts after an optional one", field.Name)
			}
		})
	}
}

// TestRequiredIsReadFromTheSchema checks the rule against a task whose answer is
// known, in both directions.
//
// The http task declares url required and method not, so a helper that reported
// everything required — or nothing — would pass a weaker test.
func TestRequiredIsReadFromTheSchema(t *testing.T) {
	t.Parallel()

	def, found := v1.LookupTask("http")
	require.True(t, found)

	required := map[string]bool{}
	for _, field := range v1.Inputs(def) {
		required[field.Name] = field.Required
	}

	require.True(t, required["url"], "url is required by the schema and is not reported as such")
	require.False(t, required["method"], "method is optional and is reported as required")
}

// TestADeferredInputIsMarked covers the distinction an author most needs and
// cannot infer.
//
// The engine resolves an expression before scheduling a step. A deferred input is
// evaluated by the task instead, against a scope the workflow does not have —
// which is why http's `outputs` may name `status_code` and an ordinary input may
// not. Nothing in the type says so.
func TestADeferredInputIsMarked(t *testing.T) {
	t.Parallel()

	def, found := v1.LookupTask("http")
	require.True(t, found)

	deferred := map[string]bool{}
	for _, field := range v1.Inputs(def) {
		deferred[field.Name] = field.Deferred
	}

	require.True(t, deferred["outputs"], "http's outputs is evaluated by the task and is not marked")
	require.False(t, deferred["url"], "url is resolved by the engine and is marked deferred")
}
