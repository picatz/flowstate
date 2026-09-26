package flowdebug_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A session renders values as JSON, and JSON rewrites the very text a
// substring redactor looks for: `"`, `\`, `<`, `>`, `&` and control characters
// escape, and a byte string becomes base64. Redacting the rendered line alone
// therefore missed a secret holding any of them. These tests drive the
// production redaction set ([v1.SensitiveValues]) through every printing path
// a session has — the step narration, a typed `inspect`, and
// [flowdebug.Session.Evaluate]'s text and value — and assert neither the
// plaintext nor its encoded spelling survives.

// debugSetup is one session paused at an autopsy over scope, with the
// narration of outputs printed first, answering both the typed script and
// the probe.
type debugSetup struct {
	scope   *v1.Scope
	outputs map[string]*v1.Node_Outputs
	script  []string
	probe   func(*flowdebug.Session)
	install func(*flowdebug.Session)
}

// run drives setup and returns everything the session printed.
func (setup debugSetup) run(t *testing.T) string {
	t.Helper()

	var out strings.Builder

	done := make(chan struct{})
	console := &probing{steps: append(setup.script, "quit"), before: func(s *flowdebug.Session) {
		defer close(done)

		if setup.probe != nil {
			setup.probe(s)
		}
	}}

	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	setup.install(session)

	for id, outputs := range setup.outputs {
		session.StepFinished(id, outputs, nil, false)
	}

	session.Autopsy(t.Context(), setup.scope, nil, []string{"a failure"})
	<-done

	return out.String()
}

// withBothRedactors installs a set the way `flow test` does: equality over
// values, substrings over text.
func withBothRedactors(sensitive v1.SensitiveValues) func(*flowdebug.Session) {
	return func(s *flowdebug.Session) {
		s.SetRedactor(sensitive.RedactSubstrings)
		s.SetValueRedactor(sensitive.RedactTree)
	}
}

// withTextRedactor installs only the substring half, the fail-closed
// configuration a caller can reach by installing one redactor.
func withTextRedactor(sensitive v1.SensitiveValues) func(*flowdebug.Session) {
	return func(s *flowdebug.Session) {
		s.SetRedactor(sensitive.RedactSubstrings)
	}
}

// jsonSpelling is text as JSON writes it inside a string, without the quotes.
func jsonSpelling(t *testing.T, text string) string {
	t.Helper()

	encoded, err := json.Marshal(text)
	require.NoError(t, err)

	return strings.Trim(string(encoded), `"`)
}

// TestAnEscapedSecretIsWithheldFromEveryPrintedAnswer is a composed string —
// the shape the equality half cannot recognise, so the substring half is the
// only one that can — holding a secret JSON would escape.
func TestAnEscapedSecretIsWithheldFromEveryPrintedAnswer(t *testing.T) {
	t.Parallel()

	for _, secret := range []string{`pa"ss`, `a<b&c>d`, `pa\ss`, "pa\tss"} {
		t.Run(fmt.Sprintf("%q", secret), func(t *testing.T) {
			t.Parallel()

			header := "Bearer " + secret
			outputs := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"header": v1.NewLiteral(header)}}

			printed := debugSetup{
				scope: &v1.Scope{Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"deploy": outputs,
				}}},
				outputs: map[string]*v1.Node_Outputs{"deploy": outputs},
				script:  []string{"inspect steps.deploy.header"},
				install: withBothRedactors(v1.SensitiveValues{}.WithValues(secret)),
				probe: func(s *flowdebug.Session) {
					text, value, err := s.Evaluate(t.Context(), "steps.deploy.header")
					require.NoError(t, err)
					assert.Equal(t, `"Bearer [redacted]"`, text)
					require.NotNil(t, value)
					assert.Equal(t, "Bearer [redacted]", value.Value())
				},
			}.run(t)

			assert.NotContains(t, printed, secret)
			assert.NotContains(t, printed, jsonSpelling(t, secret),
				"the secret printed in its JSON-escaped spelling, which the substring redactor never searched for")
			assert.Contains(t, printed, `deploy -> header: "Bearer [redacted]"`,
				"the step narration withholds the composed value")
			assert.Contains(t, printed, "\n\"Bearer [redacted]\"\n",
				"the typed inspection withholds the composed value")
		})
	}
}

// TestABytesSecretIsWithheldFromEveryPrintedAnswer is the same miss in the
// other direction: a `bytes` leaf renders as base64, which no plaintext
// substring can match however it is escaped.
func TestABytesSecretIsWithheldFromEveryPrintedAnswer(t *testing.T) {
	t.Parallel()

	const secret = "hunter2"

	encoded := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"value": v1.NewLiteral(&expr.Value{Kind: &expr.Value_BytesValue{BytesValue: []byte(secret)}}),
	}}

	printed := debugSetup{
		scope: &v1.Scope{Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"encode": encoded,
		}}},
		outputs: map[string]*v1.Node_Outputs{"encode": encoded},
		script:  []string{"inspect steps.encode.value"},
		install: withBothRedactors(v1.SensitiveValues{}.WithValues(secret)),
		probe: func(s *flowdebug.Session) {
			text, value, err := s.Evaluate(t.Context(), "steps.encode.value")
			require.NoError(t, err)
			assert.NotContains(t, text, base64.StdEncoding.EncodeToString([]byte(secret)))
			require.NotNil(t, value)
			assert.Equal(t, []byte("[redacted]"), value.Value(),
				"the structured half stays bytes, with the secret withheld from them")
		},
	}.run(t)

	assert.NotContains(t, printed, base64.StdEncoding.EncodeToString([]byte(secret)),
		"the secret printed as base64, which is the secret")
	assert.NotContains(t, printed, secret)
}

// TestASecretMapKeyIsWithheldFromEveryPrintedAnswer covers a key: one equal
// to the secret and one containing it, under both redactor configurations.
// The equality half recognises only the first, so the second is left to the
// substring half — which, before rendering, is the only place it can see it.
func TestASecretMapKeyIsWithheldFromEveryPrintedAnswer(t *testing.T) {
	t.Parallel()

	const secret = `pa"ss`

	for name, install := range map[string]func(v1.SensitiveValues) func(*flowdebug.Session){
		"text redactor only": withTextRedactor,
		"both redactors":     withBothRedactors,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			lookup := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"ids": v1.NewLiteralMap(map[string]any{secret: int64(1), "Bearer " + secret: int64(2)}),
			}}

			printed := debugSetup{
				scope: &v1.Scope{Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"lookup": lookup,
				}}},
				outputs: map[string]*v1.Node_Outputs{"lookup": lookup},
				script:  []string{"inspect steps.lookup.ids"},
				install: install(v1.SensitiveValues{}.WithValues(secret)),
				probe: func(s *flowdebug.Session) {
					text, value, err := s.Evaluate(t.Context(), "steps.lookup.ids")
					require.NoError(t, err)
					assert.NotContains(t, text, jsonSpelling(t, secret))
					assert.Contains(t, text, `"Bearer [redacted]":2`)
					if value != nil {
						assert.NotContains(t, fmt.Sprintf("%v", value.Value()), secret)
					}
				},
			}.run(t)

			assert.NotContains(t, printed, secret)
			assert.NotContains(t, printed, jsonSpelling(t, secret),
				"a key holding the secret printed in its JSON-escaped spelling")
			assert.Contains(t, printed, `"Bearer [redacted]":2`)
		})
	}
}

// TestEvaluationSeesTheRealValueWhilePrintingWithholdsIt pins the documented
// policy: redaction is a transcript control. An expression is evaluated against
// the real binding, so a comparison against the secret answers truthfully,
// while the value itself never prints.
func TestEvaluationSeesTheRealValueWhilePrintingWithholdsIt(t *testing.T) {
	t.Parallel()

	const secret = "hunter2"

	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.Inputs = map[string]*v1.Value{"token": v1.NewLiteral(secret)}

	printed := debugSetup{
		scope:   scope,
		script:  []string{`inspect inputs.token == "` + secret + `"`, "inspect inputs.token"},
		install: withBothRedactors(v1.SensitiveValues{}.WithValues(secret)),
		probe: func(s *flowdebug.Session) {
			text, _, err := s.Evaluate(t.Context(), `inputs.token == "`+secret+`"`)
			require.NoError(t, err)
			assert.Equal(t, "true", text, "a comparison against the real value answers truthfully")

			text, _, err = s.Evaluate(t.Context(), `inputs.token == "guess"`)
			require.NoError(t, err)
			assert.Equal(t, "false", text)

			text, _, err = s.Evaluate(t.Context(), "inputs.token")
			require.NoError(t, err)
			assert.Equal(t, `"[redacted]"`, text)
		},
	}.run(t)

	assert.Contains(t, printed, "\ntrue\n")
	assert.Contains(t, printed, "\n\"[redacted]\"\n")
}
