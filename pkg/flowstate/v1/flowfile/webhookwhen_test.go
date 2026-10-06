package flowfile_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The `when:` admission predicate, as a file says it: it compiles, is checked as
// a boolean over `event` and nothing else, refuses what could never decide, and
// is written back by the formatter.

const webhookWhenKey = "    idempotency_key: ${event.headers[\"stripe-signature\"]}\n"

// withWhenLine is webhookSource with a `when:` line ahead of the key.
func withWhenLine(line string) string {
	return strings.Replace(webhookSource, webhookWhenKey, "    when: "+line+"\n"+webhookWhenKey, 1)
}

func TestAWhenCompilesAndValidatesClean(t *testing.T) {
	t.Parallel()

	source := withWhenLine(`${event.body.type == "charge.captured"}`)

	workflow, positions, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)
	require.NotNil(t, workflow.GetTriggers().GetWebhooks()[0].GetWhen().GetExpr(), "`when:` is an expression")

	_, ok := positions.At("triggers[0].when")
	assert.True(t, ok, "`when:` has no recorded position")

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Empty(t, diagnostics)
}

func TestAWhenMustNameTheDelivery(t *testing.T) {
	t.Parallel()

	for name, line := range map[string]string{
		"a constant expression": `${true}`,
		"a bare literal":        `true`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			diagnostics, err := flowfile.ValidateSource([]byte(withWhenLine(line)))
			require.NoError(t, err)
			require.NotEmpty(t, diagnostics)
			assert.Contains(t, diagnostics[0].Message, "does not depend on the delivery")
			assert.Contains(t, diagnostics[0].Message, "when:")
			assert.Positive(t, diagnostics[0].Line, "a diagnostic names its position")
		})
	}
}

// TestAWhenIsCheckedAsABoolean is the same type check `if:` has: an expression
// that can never be a bool is refused where it is written, not at the first
// delivery.
func TestAWhenIsCheckedAsABoolean(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(withWhenLine(`${size(event.body.type)}`)))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)
	assert.Contains(t, diagnostics[0].Message, "`when:` is an admission predicate")
	assert.Contains(t, diagnostics[0].Message, "bool")
	assert.Positive(t, diagnostics[0].Line)
}

func TestAWhenReadsOnlyTheEvent(t *testing.T) {
	t.Parallel()

	for _, reference := range []string{"inputs.order_id", "steps.record.value", "vars.region"} {
		t.Run(reference, func(t *testing.T) {
			t.Parallel()

			diagnostics, err := flowfile.ValidateSource(
				[]byte(withWhenLine(`${event.body.type == ` + reference + `}`)))
			require.NoError(t, err)
			require.NotEmpty(t, diagnostics)
			assert.Contains(t, diagnostics[0].Message, `webhook "stripe" reads`)
			assert.Contains(t, diagnostics[0].Message, "only name in scope is `event`")
		})
	}
}

func TestAWhenMayNotReadASecret(t *testing.T) {
	t.Parallel()

	_, err := flowfile.Unmarshal([]byte(withWhenLine(`${event.body.token == secret('env:ORDER')}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a secret reference cannot appear in a `when:`")
}

// TestAWhenOnABridgeIsAccepted: the one `when:` covers a `signal:` webhook too,
// and the file does not need a second spelling inside the arm.
func TestAWhenOnABridgeIsAccepted(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.4
name: gate
signals:
  approved:
    allow: ${sender.identity.subject == "gate/slack"}
triggers:
  - webhook: slack
    verify:
      hmac_sha256: ${secret('env:SLACK_SIGNING_SECRET')}
    when: ${event.body.type == "block_actions"}
    idempotency_key: ${event.body.trigger_id}
    signal:
      name: approved
      correlate: ${event.body.actions[0].value}
steps:
  - id: gate
    wait_for_signal:
      name: approved
      timeout: 1h
`
	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err)
	assert.NotNil(t, workflow.GetTriggers().GetWebhooks()[0].GetWhen())

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	assert.Empty(t, diagnostics)

	// A `when:` inside the arm is not a second spelling: `signal:` has no such key.
	nested := strings.Replace(source, "      correlate:", "      when: ${event.body.type == \"x\"}\n      correlate:", 1)
	_, err = flowfile.Unmarshal([]byte(nested))
	require.Error(t, err, "a `when:` inside `signal:` was accepted as a second spelling")
}

func TestMarshalKeepsAWhen(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(withWhenLine(`${event.body.type == "charge.captured"}`)))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	assert.Contains(t, string(written), "when:", "the formatter dropped what an author wrote")

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.Empty(t, cmp.Diff(workflow, again, protocmp.Transform()))
}
