package server_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The webhook half of #1770: the receiver bounded a body in bytes and verified
// its signature, and a correctly signed body nested forty levels beside the
// fields `with:` binds started a run. A run input that deep is refused at
// submit; a delivery that deep is now refused at the receiver, in the same
// sentence, with the status that tells a provider not to retry.

// deepDeliveryBody is [deliveryBody] with a `doc` beside the fields `with:`
// reads, nested so that the body as a whole is levels deep — the body's own
// object is the first level, which is how a run input's depth is counted too.
func deepDeliveryBody(event string, levels int) string {
	doc := strings.Repeat(`{"k":`, levels-1) + `"leaf"` + strings.Repeat("}", levels-1)

	return fmt.Sprintf(`{"id":%q,"order":{"id":"ord_H1x9","total_cents":4200},"doc":%s}`, event, doc)
}

// bodyDepthRefusal is the sentence a delivery is refused with: the submit
// door's input refusal, naming the webhook in place of the input.
func bodyDepthRefusal(webhook string, levels int) string {
	return fmt.Sprintf("the body delivered to webhook %q nests %d levels deep, over the %d levels this "+
		"server can walk cheaply while evaluating an expression over it", webhook, levels, v1.MaxStructureDepth)
}

// TestADeliveryNestedPastTheDepthBoundIsRefused drives the receiver with no
// Temporal behind it: the refusal is decided before a run would start, as 422
// — the payload can never work, so a provider must not retry it — carrying
// the depth and the bound for whoever holds the signing key.
func TestADeliveryNestedPastTheDepthBoundIsRefused(t *testing.T) {
	t.Parallel()

	receiver := newReceiver(t)
	levels := v1.MaxStructureDepth + 1

	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", deepDeliveryBody("evt_deep", levels), signed)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode,
		"a delivery nested past the bound was not refused as the payload's own fault")

	said, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(said), bodyDepthRefusal("storefront", levels),
		"the refusal does not name the depth and the bound in the submit door's words")
}

// TestADeliveryAtTheDepthBoundStartsARun is the other direction, against a
// real cluster: a body exactly at the bound is admitted and starts a run, so
// the bound is where it says it is rather than one level short.
func TestADeliveryAtTheDepthBoundStartsARun(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront",
		deepDeliveryBody("evt_at_bound", v1.MaxStructureDepth), signed)
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"a body at exactly the bound must start a run, or the bound is a lie by one")

	accepted := readAccepted(t, resp)
	require.False(t, accepted.Joined)
	require.NotEmpty(t, accepted.RunID)
}
