package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const raisingSource = `edition: v2026.4
name: raising
inputs:
  tenant:
    type: string
    required: true
errors:
  QuotaExceeded:
    description: the tenant is over its quota
  Unauthorized: {}
steps:
  - id: refuse
    fail:
      error: QuotaExceeded
      message: ${"tenant " + inputs.tenant + " is over quota"}
`

// TestDeclaredErrorsAndFailRoundTrip pins that `errors:` and `fail:` are written
// back as read, so `flow fix` cannot delete either.
func TestDeclaredErrorsAndFailRoundTrip(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse([]byte(raisingSource))
	require.NoError(t, err)
	require.Len(t, workflow.GetDeclaredErrors(), 2)
	require.Equal(t, "QuotaExceeded", workflow.GetDeclaredErrors()[0].GetName())
	require.Equal(t, "QuotaExceeded", workflow.GetSteps()[0].GetFail().GetError())

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	assert.Contains(t, string(written), "errors:")
	assert.Contains(t, string(written), "fail:")

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	round, err := flowfile.Marshal(again)
	require.NoError(t, err)
	require.Equal(t, string(written), string(round))
}

// TestFailDiagnostics covers what the compiler and validator refuse.
func TestFailDiagnostics(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		src  string
		want string
	}{
		"valid": {src: raisingSource},
		"undeclared name suggests the nearest": {
			src:  strings.Replace(raisingSource, "error: QuotaExceeded", "error: QuotaExceded", 1),
			want: `did you mean "QuotaExceeded"`,
		},
		"a built-in kind cannot be declared": {
			src:  strings.Replace(raisingSource, "Unauthorized: {}", "Timeout: {}", 1),
			want: "built-in",
		},
		"a duplicate declaration": {
			src:  strings.Replace(raisingSource, "Unauthorized: {}", "QuotaExceeded: {}", 1),
			want: "already defined",
		},
		"a lower-case name": {
			src:  strings.Replace(raisingSource, "Unauthorized: {}", "unauthorized: {}", 1),
			want: "unauthorized",
		},
		"a message that reads an unknown input": {
			src:  strings.Replace(raisingSource, "inputs.tenant +", "inputs.tenat +", 1),
			want: "tenat",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			diagnostics := validateTriggerSource(t, tc.src)
			if tc.want == "" {
				require.Empty(t, diagnostics)

				return
			}
			var found bool
			for _, d := range diagnostics {
				found = found || strings.Contains(d.Message, tc.want)
			}
			require.True(t, found, "want %q in %v", tc.want, diagnostics)
		})
	}
}

// TestDeclaredKindIsAKnownFailureKind: a declared name passes a `failure.kind`
// comparison, a misspelling of it does not, and a call step's kind is never
// judged against the caller's own declarations.
func TestDeclaredKindIsAKnownFailureKind(t *testing.T) {
	t.Parallel()

	const source = `edition: v2026.4
name: catching
errors:
  QuotaExceeded: {}
steps:
  - id: fetch
    continue_on_error: true
    http:
      url: https://example.com/
  - id: react
    if: ${steps.fetch.failure.kind == "%s"}
    log:
      message: hi
`
	require.Empty(t, validateTriggerSource(t, strings.Replace(source, "%s", "QuotaExceeded", 1)))

	var found bool
	for _, d := range validateTriggerSource(t, strings.Replace(source, "%s", "QuotaExceded", 1)) {
		found = found || strings.Contains(d.Message, `did you mean "QuotaExceeded"`)
	}
	require.True(t, found)
}

// TestFailRunsAsItsDeclaredKind runs the raised failure on the local driver and
// reads back the kind and the sentence. The durable driver's half is the shared
// conformance case.
func TestFailRunsAsItsDeclaredKind(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse([]byte(raisingSource))
	require.NoError(t, err)

	_, err = v1.RunWithInputs(t.Context(), workflow, map[string]*v1.Value{"tenant": v1.NewLiteral("acme")})
	require.Error(t, err)
	require.Equal(t, v1.ErrorKind("QuotaExceeded"), v1.ClassifyError(err))
	require.Contains(t, err.Error(), "tenant acme is over quota")
}

// TestFailMessageReachingASensitiveInputIsRefused: the message is recorded in
// history, so the reach rule a gate prompt follows applies, under its own code.
func TestFailMessageReachingASensitiveInputIsRefused(t *testing.T) {
	t.Parallel()

	src := strings.Replace(raisingSource, "    required: true\n", "    required: true\n    sensitive: true\n", 1)

	var found bool
	for _, d := range validateTriggerSource(t, src) {
		if d.Code == v1.DiagnosticCodeSensitiveInFailMessage {
			found = true
			assert.Equal(t, "refuse", d.Step)
			assert.Equal(t, "fail.message", d.Field)
		}
		assert.NotEqual(t, v1.DiagnosticCodeSensitiveInPrompt, d.Code, "a fail message is not a gate prompt")
	}
	require.True(t, found)
}
