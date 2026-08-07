package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// #234's V5: an input declared `sensitive:` written straight into a `log:`
// message lands the value in run history and stdout in the clear, undoing the
// declaration. See sensitive_log.go for the boundary this draws — direct
// surfacing refused, a derived value left alone — and why the derived half
// matters as much as the caught half: a false positive on `${inputs.token !=
// ""}` would train an author to disable the lint.

// sensitiveLogSource wraps one `log:` message in a workflow that declares
// `token` sensitive and `username` not, so a case only has to supply the
// message expression it is testing. The message is emitted as a YAML
// double-quoted scalar so that inner CEL quotes are the expression's, not YAML
// punctuation the scanner trips over.
func sensitiveLogSource(message string) string {
	return strings.Join([]string{
		"edition: v2026.2",
		"name: leaky",
		"inputs:",
		"  token:",
		"    type: string",
		"    sensitive: true",
		"  username:",
		"    type: string",
		"steps:",
		"  - id: announce",
		"    log:",
		`      message: "` + strings.ReplaceAll(message, `"`, `\"`) + `"`,
		"",
	}, "\n")
}

// sensitiveLogDiagnostics returns the rendered diagnostics that came from the
// sensitive-in-log lint specifically, told apart from anything else Validate
// says about the same file by this lint's own sentence.
func sensitiveLogDiagnostics(t *testing.T, src string) []string {
	t.Helper()

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err, "the file does not compile, so this says nothing about the lint")

	var out []string
	for _, d := range ds {
		if strings.Contains(d.Error(), "is declared `sensitive:`") {
			out = append(out, d.Error())
		}
	}
	return out
}

func TestSensitiveLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// message is the `log:` message expression under test.
		message string
		// refused is whether the lint must fire; when false it must stay silent.
		refused bool
	}{
		{
			name:    "bare sensitive reference is refused",
			message: `${inputs.token}`,
			refused: true,
		},
		{
			name:    "sensitive reference concatenated into a message is refused",
			message: `${'token: ' + inputs.token}`,
			refused: true,
		},
		{
			name:    "sensitive reference deeper in a concatenation chain is refused",
			message: `${'a ' + inputs.username + ' has token ' + inputs.token}`,
			refused: true,
		},
		{
			name:    "sensitive reference by index is refused",
			message: `${inputs["token"]}`,
			refused: true,
		},
		{
			name:    "a non-sensitive input logged directly is fine",
			message: `${inputs.username}`,
			refused: false,
		},
		{
			// The false-positive guard the boundary exists for: a boolean derived
			// from the sensitive value says a token was supplied without saying
			// which. Refusing this is what would train an author to disable the
			// lint, so it must stay silent.
			name:    "a guard derived from a sensitive input is allowed",
			message: `${inputs.token != ""}`,
			refused: false,
		},
		{
			name:    "a length derived from a sensitive input is allowed",
			message: `${'token length: ' + string(inputs.token.size())}`,
			refused: false,
		},
		{
			name:    "an existence test on a sensitive input is allowed",
			message: `${has(inputs.token)}`,
			refused: false,
		},
		{
			name:    "a literal message is fine",
			message: "hello",
			refused: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sensitiveLogDiagnostics(t, sensitiveLogSource(tc.message))
			if tc.refused {
				require.Len(t, got, 1, "expected exactly one sensitive-in-log diagnostic")
				assert.Contains(t, got[0], `input "token"`)
			} else {
				assert.Empty(t, got, "the lint must stay silent on a derived or non-sensitive value")
			}
		})
	}
}

// TestSensitiveLogPositionAndCode pins that the diagnostic lands on the log
// step's message with the stable code an agent branches on, not merely that
// some sentence was produced.
func TestSensitiveLogPositionAndCode(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(sensitiveLogSource(`${inputs.token}`)))
	require.NoError(t, err)

	var found []flowfile.Diagnostic
	for _, d := range ds {
		if d.Code == v1.DiagnosticCodeSensitiveInLog {
			found = append(found, d)
		}
	}

	require.Len(t, found, 1)
	assert.Equal(t, "announce", found[0].Step)
	assert.Equal(t, "message", found[0].Field)
	assert.Positive(t, found[0].Line, "the diagnostic must carry a source position")
}

// TestSensitiveLogInsideBlocks pins that the walk reaches a `log:` nested in a
// loop body or a parallel branch, the same tree the negation lint covers, since
// `inputs.<name>` means the same workflow input at any depth.
func TestSensitiveLogInsideBlocks(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: nested
inputs:
  token:
    type: string
    sensitive: true
steps:
  - id: fan
    for_each:
      items: ${[1, 2, 3]}
      as: n
      steps:
        - id: shout
          log:
            message: ${inputs.token}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	var found int
	for _, d := range ds {
		if d.Code == v1.DiagnosticCodeSensitiveInLog {
			found++
			assert.Equal(t, "shout", d.Step)
		}
	}
	assert.Equal(t, 1, found, "a sensitive log inside a for_each body must be caught")
}
