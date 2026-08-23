package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `prompt:` as an author writes it: parsed, validated, positioned, and written
// back out again.
//
// The round trip is the one that has bitten before. `flow fix` and `flow fmt`
// rewrite a file through [flowfile.Marshal], so a key the writer does not know
// about is a key the command silently *deletes*; signals.go records that lesson
// and marshal.go's own comment repeats it. A test that only proves the parser
// accepts the key would go green through exactly that failure.

// promptSource is a workflow whose one gate asks a question, with whatever
// prompt line the case supplies.
func promptSource(promptLine string) string {
	return strings.Join([]string{
		"edition: v2026.3",
		"name: asking",
		"inputs:",
		"  version:",
		"    type: string",
		"    required: true",
		"steps:",
		"  - id: approval",
		"    wait_for_signal:",
		"      name: deploy-approved",
		"      " + promptLine,
		"      timeout: 24h",
		"",
	}, "\n")
}

// TestAPromptCompilesAndValidates is the reachability claim: a Flowfile can
// express this, and `flow validate` accepts it.
func TestAPromptCompilesAndValidates(t *testing.T) {
	t.Parallel()

	src := promptSource(`prompt: ${"deploy " + inputs.version + "?"}`)

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	prompt := wf.GetSteps()[0].GetWait().GetSignal().GetPrompt()
	require.NotNil(t, prompt, "the parser dropped `prompt:` on the floor")
	require.NotNil(t, prompt.GetExpr(), "a fenced prompt compiled to something other than an expression")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, ds, "a valid prompt was reported as a problem")
}

// TestAnUnfencedPromptStaysAPlainSentence pins that this position reads a fence
// the way every other input position does: a plain string is a plain string,
// not expression source. Getting this backwards is how `sleep: 30s` would have
// become the CEL expression `30s`.
func TestAnUnfencedPromptStaysAPlainSentence(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(promptSource("prompt: approve the deploy?")))
	require.NoError(t, err)

	prompt := wf.GetSteps()[0].GetWait().GetSignal().GetPrompt()
	require.NotNil(t, prompt)
	assert.Nil(t, prompt.GetExpr(),
		"an unfenced prompt was read as expression source, so a plain question would have to parse as CEL")
}

// TestAPromptSurvivesARoundTrip is the `flow fmt` claim, compared as bytes
// rather than by re-validating: a rewriter that dropped the key would produce a
// file that still validates perfectly and asks nobody anything.
func TestAPromptSurvivesARoundTrip(t *testing.T) {
	t.Parallel()

	for _, promptLine := range []string{
		`prompt: ${"deploy " + inputs.version + "?"}`,
		"prompt: approve the deploy?",
	} {
		t.Run(promptLine, func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(promptSource(promptLine)))
			require.NoError(t, err)

			written, err := flowfile.Marshal(wf)
			require.NoError(t, err)
			assert.Contains(t, string(written), "prompt:",
				"the writer dropped `prompt:`, so `flow fmt` deletes the question a gate asks")

			// And the re-parsed file asks the same question, which is the claim a
			// substring check alone does not make.
			again, _, err := flowfile.Parse(written)
			require.NoError(t, err)

			assert.Equal(t,
				wf.GetSteps()[0].GetWait().GetSignal().GetPrompt().String(),
				again.GetSteps()[0].GetWait().GetSignal().GetPrompt().String(),
				"the question changed on its way through the formatter")
		})
	}
}

// TestAPromptNamingSomethingUndeclaredIsReportedAtItsOwnLine is the diagnostic
// half. The position matters as much as the message: without a span recorded at
// this path the squiggle lands on `- id:` while the faulty expression is lines
// away, which is #318's finding repeated on a new key.
func TestAPromptNamingSomethingUndeclaredIsReportedAtItsOwnLine(t *testing.T) {
	t.Parallel()

	src := promptSource(`prompt: ${"deploy " + inputs.nosuchthing + "?"}`)

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, ds, "a prompt naming an input nobody declared was accepted")

	var found bool
	for _, d := range ds {
		if strings.Contains(d.Error(), "nosuchthing") {
			found = true
		}
	}
	assert.True(t, found, "the diagnostics never mentioned the name the prompt got wrong: %v", ds)
}

// TestAPromptSeesNoneOfTheWaitsOwnResult pins the asymmetry with `outputs:`,
// which is the reason `prompt:` is a sibling of that key rather than an entry in
// it: a prompt is evaluated when the wait *parks*, so the result does not exist
// yet and `${payload}` there really is naming a step.
func TestAPromptSeesNoneOfTheWaitsOwnResult(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(promptSource("prompt: ${payload.question}")))
	require.NoError(t, err)
	assert.NotEmpty(t, ds,
		"a prompt read `payload` as the wait's own result, which does not exist when the question "+
			"is asked, so the name silently means something no step produced")
}

// TestAPromptReachingASensitiveInputIsRefusedAtItsOwnLine is the lint, through a
// file, with a position, the compile layer of the rule sensitive_prompt.go
// documents. Derived rather than surfaced on purpose: this is the case the
// `log:` lint deliberately allows and this one deliberately does not.
func TestAPromptReachingASensitiveInputIsRefusedAtItsOwnLine(t *testing.T) {
	t.Parallel()

	src := strings.Join([]string{
		"edition: v2026.3",
		"name: asking",
		"inputs:",
		"  salary:",
		"    type: int",
		"    required: true",
		"    sensitive: true",
		"steps:",
		"  - id: approval",
		"    wait_for_signal:",
		"      name: raise-approved",
		// Quoted, because the `: ` inside the conditional is YAML mapping syntax.
		`      prompt: '${inputs.salary > 100000 ? "a large raise" : "a small raise"}'`,
		"",
	}, "\n")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	var refusal *flowfile.Diagnostic
	for i, d := range ds {
		if d.Code == v1.DiagnosticCodeSensitiveInPrompt {
			refusal = &ds[i]
		}
	}
	require.NotNil(t, refusal,
		"a prompt deriving its text from a `sensitive:` input was accepted: %v", ds)
	assert.Contains(t, refusal.Error(), "salary")
	assert.Equal(t, "approval", refusal.Step)
}

func TestAPromptReachingASensitiveInputThroughAStepVarIsRefused(t *testing.T) {
	t.Parallel()

	src := strings.Join([]string{
		"edition: v2026.3",
		"name: asking",
		"inputs:",
		"  token:",
		"    type: string",
		"    sensitive: true",
		"steps:",
		"  - id: approval",
		"    vars:",
		`      question: '${"approve " + inputs.token}'`,
		"    wait_for_signal:",
		"      name: deploy-approved",
		"      prompt: ${question}",
		"",
	}, "\n")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Condition(t, func() bool {
		for _, d := range ds {
			if d.Code == v1.DiagnosticCodeSensitiveInPrompt {
				return true
			}
		}
		return false
	}, "a step var hid a prompt's reach into a sensitive input: %v", ds)
}
