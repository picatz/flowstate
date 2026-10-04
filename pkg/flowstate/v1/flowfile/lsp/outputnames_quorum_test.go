package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// waitQuorumFile is an unshaped quorum gate: its outputs are the batch's own
// three names and the three a quorum adds.
const waitQuorumFile = `edition: v2026.4
name: wait-quorum
steps:
  - id: gate
    wait_for_signals:
      name: release-approved
      timeout: 1h
      quorum:
        approve: 2
  - id: report
    log:
      message: ${steps.gate.decision}
`

func TestTheWaitQuorumFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(waitQuorumFile))
	require.NoError(t, err)
	assert.Empty(t, diags)
}

// TestCompletionAfterAnUnshapedQuorumOffersTheQuorumOutputs is the downstream
// half of authoring a quorum: `${steps.gate.}` offers what the wait produces,
// which includes `decision`, `approvals` and `vetoed_by`, not only the three a
// plain batch does.
func TestCompletionAfterAnUnshapedQuorumOffersTheQuorumOutputs(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///wait-quorum-completion.yaml"
	c.open(uri, waitQuorumFile)

	at := positionOf(t, waitQuorumFile, "message: ${steps.gate.decision}", len("message: ${steps.gate."))
	got := labels(c.complete(uri, at.Line, at.Character).Items)
	assert.ElementsMatch(t,
		[]string{"decision", "approvals", "vetoed_by", "timed_out", "deliveries", "count"}, got)
}

// TestHoverOnAQuorumsDecisionDescribesIt is the hover counterpart: before the
// quorum was carried into the model the hover is built from, it said the step
// does not produce `decision`.
func TestHoverOnAQuorumsDecisionDescribesIt(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///wait-quorum-hover.yaml"
	c.open(uri, waitQuorumFile)

	at := positionOf(t, waitQuorumFile, "message: ${steps.gate.decision}", len("message: ${steps.gate.")+1)
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h)

	text := hoverText(h)
	assert.Contains(t, text, "steps.gate.decision")
	assert.Contains(t, text, "How the quorum ended")
	assert.NotContains(t, text, "does not produce")
}

// TestAPlainBatchStillOffersOnlyItsThreeOutputs is the other direction: a
// batch with no `quorum:` must not be offered the quorum's names.
func TestAPlainBatchStillOffersOnlyItsThreeOutputs(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///wait-batch-no-quorum-completion.yaml"
	c.open(uri, waitBatchFile)

	at := positionOf(t, waitBatchFile, "message: ${string(steps.orders.count)}", len("message: ${string(steps.orders."))
	got := labels(c.complete(uri, at.Line, at.Character).Items)
	assert.NotContains(t, got, "decision")
}
