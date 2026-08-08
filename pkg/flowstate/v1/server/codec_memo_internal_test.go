package server

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/toycodec"
)

// What a payload codec does to this server's memos, recorded as a test because
// it is the finding that decides the memo policy rather than a bug to fix here.
//
// # The mechanism
//
// A memo is not exempt from the data converter. The Go SDK encodes memo values
// with the *user's* converter and falls back to the default one only if that
// fails (go.temporal.io/sdk@v1.47.0 internal/internal_workflow_client.go:1998
// encodeMemoValue, reached from getWorkflowMemo at :2023), gated on
// SDKFlagMemoUserDCEncode — which defaults to true (internal/internal_flags.go:56).
// So on a deployment with a payload codec, every memo this server writes is
// ciphertext.
//
// Search attributes behave the opposite way: they are always encoded with
// converter.GetDefaultDataConverter() and never with the user's
// (internal/internal_search_attributes.go:391 and :405), because the cluster
// has to index them. A codec cannot cover them, which is why nothing
// payload-derived may ever be projected into one.
//
// # Why this matters more than it looks
//
// This server reads its memos back with the default converter — memoTenant,
// memoStarter, signalPolicies, the workflow-name entry — which is correct today
// and becomes a fail-closed outage the moment a codec is configured: every read
// fails, ownedBy answers false, and every run in the deployment reports "no such
// run" to the tenant that owns it. Fail-closed is the right direction and a
// silent one is still an outage, so the real slice needs the memo path to say
// which converter it is on, explicitly, on both sides.
func TestCodecMakesMemosUnreadableByTheDefaultConverter(t *testing.T) {
	t.Parallel()

	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)
	dc := payloadcodec.Config{Codec: toy}.DataConverter()

	// Exactly what the SDK does with map[string]any{namespaceMemoKey: "acme"}
	// when the client carries a codec converter.
	encoded, err := dc.ToPayload("acme")
	require.NoError(t, err)

	memo := &common.Memo{Fields: map[string]*common.Payload{namespaceMemoKey: encoded}}

	// The tenancy read, as this package performs it today.
	_, err = memoTenant(memo)
	require.Error(t, err,
		"the default converter read a codec-encoded memo, so this no longer describes the seam")

	// Which is what ownedBy turns into: not "belongs to someone else" but
	// "nothing can be concluded", and therefore nobody may act on it.
	require.False(t, ownedBy("acme", memo),
		"a codec-encoded memo authorized its own tenant, which would mean the decode silently succeeded")

	// And the same bytes read with the codec converter are the tenant again,
	// which is what makes this a wiring question rather than a data loss.
	var namespace string
	require.NoError(t, dc.FromPayload(encoded, &namespace))
	require.Equal(t, "acme", namespace)

	// Sanity: the plaintext path this server has always used is unaffected.
	plain, err := converter.GetDefaultDataConverter().ToPayload("acme")
	require.NoError(t, err)
	recorded, err := memoTenant(&common.Memo{Fields: map[string]*common.Payload{namespaceMemoKey: plain}})
	require.NoError(t, err)
	require.Equal(t, "acme", recorded)
}
