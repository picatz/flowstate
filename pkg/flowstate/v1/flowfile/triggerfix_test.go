package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` corrupting a valid file is the worst thing this repository can do
// (CLAUDE.md, "A rewriter has to know what the grammar binds"), so these are
// asserted on bytes rather than on validity: a file with a webhook trigger and
// nothing to change must come back identical, and a file with a bare step
// reference inside a trigger must get the same rooting the rest of the grammar
// already gets. Issue #505 found the audit walk blind to a trigger's expressions;
// this pins the rewriter, the surface an author actually runs, against the same
// blind spot.

// currentWebhookTrigger is already on the current edition with every reference
// rooted: nothing here is fix's to change.
const currentWebhookTrigger = `edition: v2026.3
name: order-webhook
inputs:
  order_id: { type: string, required: true }
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${steps.amount.value}
    with:
      order_id: ${event.body.order_id}
steps:
  - id: amount
    log:
      message: hi
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`

// TestFixLeavesACurrentWebhookTriggerByteForByte is the unchanged direction:
// a valid file with a webhook trigger is not touched at all.
func TestFixLeavesACurrentWebhookTriggerByteForByte(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(currentWebhookTrigger))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.Equal(t, currentWebhookTrigger, string(result.Source),
		"a file with nothing to change inside its trigger was rewritten anyway")
}

// TestFixRootsAStepReferencedBareInsideATrigger is the rewrite direction: a bare
// step reference written inside `idempotency_key:`, a position the audit walk in
// #505 could not see either, must be rooted the same way one written inside a
// step is.
func TestFixRootsAStepReferencedBareInsideATrigger(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: order-webhook
inputs:
  order_id: { type: string, required: true }
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${amount.value}
    with:
      order_id: ${event.body.order_id}
steps:
  - id: amount
    log:
      message: hi
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.Equal(t, currentWebhookTrigger, string(result.Source),
		"a bare step reference inside a trigger's idempotency_key was not rooted "+
			"the way the same reference would be inside a step")

	_, err = flowfile.ValidateSource(result.Source)
	require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
}

// TestFixDoesNotRootTheEventBinding is the negative case the same position needs:
// `event` is bound throughout a trigger by the grammar itself, and a step
// happening to share that name must not have the reference rewritten out from
// under the binding.
func TestFixDoesNotRootTheEventBinding(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: order-webhook
inputs:
  order_id: { type: string, required: true }
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order_id: ${event.body.order_id}
steps:
  - id: event
    log:
      message: hi
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.Contains(t, string(result.Source), `idempotency_key: ${event.headers["stripe-signature"]}`,
		"the trigger's own event binding was rooted as if it were the step sharing its name")
	assert.Contains(t, string(result.Source), "with:\n      order_id: ${event.body.order_id}",
		"the trigger's own event binding was rooted as if it were the step sharing its name")

	_, err = flowfile.ValidateSource(result.Source)
	require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
}
