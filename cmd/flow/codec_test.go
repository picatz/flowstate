package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// The codec slot has two entry points in this binary, and the parity claim of
// #353 is not that they encrypt the same way. `flow run local` encrypts nothing,
// on purpose (see [localPayloadCodec]). It is that they *resolve and refuse* the
// same way: a codec whose ciphertext cannot fit inside Temporal's blob limit is
// refused by the rehearsal exactly as it is refused by the worker, so an author
// meets the misconfiguration where they can act on it rather than on a run that
// wedges in production.
//
// Both directions are asserted for both entry points. One that only refused
// would be a command nobody could run; one that only accepted would be the
// check missing.

// codecTestWorkflow is the smallest run there is: what these tests are about is
// the configuration a run is refused under, not the run.
const codecTestWorkflow = `edition: v2026.3
name: codec-configuration
steps:
  - id: hello
    log:
      message: hello
`

// oversizedCodec declares an expansion no reserve under the blob limit could
// cover: base64 armour over the ciphertext costs a third of two mebibytes.
//
// It encodes nothing. The refusal is a refusal of what a codec says about
// itself, decided before a payload exists, which is the only moment it can be
// decided without a run dying of it.
type oversizedCodec struct{}

func (oversizedCodec) Name() string { return "test-armoured" }

func (oversizedCodec) CurrentKeyID() string { return "test-armoured-key" }

func (oversizedCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (oversizedCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (oversizedCodec) MaxEncodedSize(plain int) int { return (plain+2)/3*4 + 64 }

// withResolvedCodec puts a codec in the slot the plugin lookup will eventually
// fill, for one test.
func withResolvedCodec(t *testing.T, codec payloadcodec.Codec) {
	t.Helper()

	previous := resolvePayloadCodec
	resolvePayloadCodec = func() (payloadcodec.Config, error) {
		return payloadcodec.Config{Codec: codec}, nil
	}
	t.Cleanup(func() { resolvePayloadCodec = previous })
}

// requireRefusal checks the diagnosis rather than only the exit status: an
// operator meeting this at startup has to be told which codec, by how much, and
// that the cluster's blob limit is not the lever to reach for.
func requireRefusal(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err, "a codec whose ciphertext cannot fit was allowed to start")

	msg := err.Error()
	require.Contains(t, msg, `"test-armoured"`, "the refusal does not name the codec")
	require.Contains(t, msg, "Raising the cluster's blob limit is not the fix")
	require.Contains(t, msg, "leaner")
}

// TestWorkerAndServerRefuseACodecThatCannotFit covers the durable entry point.
// [temporalConfig] is where every client this binary dials is configured,
// including the pool `flow server` builds one client per mapped namespace from,
// so a refusal here is a refusal for all of them.
func TestWorkerAndServerRefuseACodecThatCannotFit(t *testing.T) {
	withResolvedCodec(t, oversizedCodec{})

	_, err := temporalConfig(t.Context(), temporalFlags{})
	requireRefusal(t, err)
}

// TestRunLocalRefusesACodecThatCannotFit is the same refusal at the rehearsal.
//
// A local run has no serialization boundary and so encrypts nothing, which is
// exactly why this test matters: without it the local driver would accept a
// configuration the worker rejects, and the rehearsal would be rehearsing a
// deployment that cannot start.
func TestRunLocalRefusesACodecThatCannotFit(t *testing.T) {
	withResolvedCodec(t, oversizedCodec{})

	stdout, _, err := runLocal(t, codecTestWorkflow)

	requireRefusal(t, err)
	require.Empty(t, strings.TrimSpace(stdout),
		"a refused run printed a result; the refusal happens before the workflow runs")
}

// TestBothEntryPointsAcceptACodecThatFits is the other direction, and it is not
// a formality: a check that refuses everything passes every negative test in
// this file.
func TestBothEntryPointsAcceptACodecThatFits(t *testing.T) {
	withResolvedCodec(t, fittingCodec{})

	_, err := temporalConfig(t.Context(), temporalFlags{})
	require.NoError(t, err)

	_, _, err = runLocal(t, codecTestWorkflow)
	require.NoError(t, err)
}

// fittingCodec expands by a nonce, a tag, and a key id: the shape of every codec
// that encrypts one payload as one payload, and the shape that has to keep
// working.
type fittingCodec struct{}

func (fittingCodec) Name() string { return "test-fits" }

func (fittingCodec) CurrentKeyID() string { return "test-fits-key" }

func (fittingCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (fittingCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (fittingCodec) MaxEncodedSize(plain int) int { return plain + 128 }

// TestTheDefaultResolutionStartsBothEntryPoints pins that the null codec, which
// is what every deployment configuring nothing runs, is not caught by any of
// this. The check is arithmetic rather than an exemption, so the way to know it
// is arithmetic that comes out right is to run it.
func TestTheDefaultResolutionStartsBothEntryPoints(t *testing.T) {
	cfg, err := payloadCodecConfig()
	require.NoError(t, err)
	require.False(t, cfg.Enabled())

	local, err := localPayloadCodec()
	require.NoError(t, err)
	require.Equal(t, cfg.Name(), local.Name(),
		"the rehearsal resolved a different codec than the worker")

	require.Equal(t, v1.MaxRunStateBytes, payloadcodec.Null().MaxEncodedSize(v1.MaxRunStateBytes))
}
