package flowfile_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The `respond_within:` bound, as a file says it: it compiles to a duration,
// keeps its position, is held to the schema's bounds with the fix beside the
// refusal, is refused where it cannot work, and is written back by the
// formatter.

const webhookRespondKey = "    with:\n"

// withRespond is webhookSource, with `outputs:` and a `respond_within:` line.
func withRespond(line string) string {
	source := strings.Replace(webhookSource, webhookRespondKey, "    respond_within: "+line+"\n"+webhookRespondKey, 1)

	return source + "outputs:\n  order:\n    value: ${inputs.order_id}\n"
}

func TestARespondWithinCompilesAndValidatesClean(t *testing.T) {
	t.Parallel()

	source := withRespond("5s")

	workflow, positions, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, workflow.GetTriggers().GetWebhooks()[0].GetRespondWithin().AsDuration())

	_, ok := positions.At("triggers[0].respond_within")
	assert.True(t, ok, "`respond_within:` has no recorded position")

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Empty(t, diagnostics)
}

// TestRespondWithinIsHeldToItsBoundsAtTheKey: the schema's 100ms..30s, said once
// and at the line, with the way out beside it. Both edges pass.
func TestRespondWithinIsHeldToItsBoundsAtTheKey(t *testing.T) {
	t.Parallel()

	for _, ok := range []string{"100ms", "30s", "1500ms"} {
		diagnostics, err := flowfile.ValidateSource([]byte(withRespond(ok)))
		require.NoError(t, err)
		assert.Empty(t, diagnostics, "%s is inside the bound", ok)
	}

	for _, refused := range []string{"99ms", "30001ms", "1m", "1h"} {
		diagnostics, err := flowfile.ValidateSource([]byte(withRespond(refused)))
		require.NoError(t, err)
		require.Len(t, diagnostics, 1, "%s: one fault, one voice", refused)
		assert.Contains(t, diagnostics[0].Message, "100ms to 30s")
		assert.Contains(t, diagnostics[0].Message, "`flow get`")
		assert.Equal(t, 11, diagnostics[0].Line, "a bound is reported at the key that broke it")
	}
}

func TestRespondWithinMustBeADuration(t *testing.T) {
	t.Parallel()

	for _, line := range []string{"soon", "5", "0s", "-1s"} {
		_, err := flowfile.Unmarshal([]byte(withRespond(line)))
		require.Error(t, err, "%q is not a bound", line)
	}
}

// TestRespondWithinIsRefusedWithASignalBridgeAndWithoutOutputs: the two
// compile-time refusals, each at the key an author edits.
func TestRespondWithinIsRefusedWithASignalBridgeAndWithoutOutputs(t *testing.T) {
	t.Parallel()

	t.Run("without outputs", func(t *testing.T) {
		t.Parallel()

		source := strings.TrimSuffix(withRespond("5s"), "outputs:\n  order:\n    value: ${inputs.order_id}\n")
		diagnostics, err := flowfile.ValidateSource([]byte(source))
		require.NoError(t, err)
		require.Len(t, diagnostics, 1)
		assert.Contains(t, diagnostics[0].Message, "declares no `outputs:`")
		assert.Positive(t, diagnostics[0].Line)
	})

	t.Run("with a signal", func(t *testing.T) {
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
    idempotency_key: ${event.body.trigger_id}
    respond_within: 2s
    signal:
      name: approved
      correlate: ${event.body.actions[0].value}
steps:
  - id: gate
    wait_for_signal:
      name: approved
      timeout: 1h
outputs:
  decided:
    value: ${steps.gate.timed_out}
`
		diagnostics, err := flowfile.ValidateSource([]byte(source))
		require.NoError(t, err)
		require.Len(t, diagnostics, 1)
		assert.Contains(t, diagnostics[0].Message, "both `respond_within:` and `signal:`")
		assert.Equal(t, 11, diagnostics[0].Line)
	})
}

func TestMarshalKeepsARespondWithin(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(withRespond("1500ms")))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	assert.Contains(t, string(written), "respond_within: 1.5s", "the formatter dropped what an author wrote")

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.Empty(t, cmp.Diff(workflow, again, protocmp.Transform()))
}
