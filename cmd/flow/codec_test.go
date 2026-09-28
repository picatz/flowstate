package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
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
	resolvePayloadCodec = func(payloadEncryptionFlags) (payloadcodec.Config, error) {
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
	t.Setenv(payloadKeyringEnv, "")
	t.Setenv(requirePayloadEncryptionEnv, "")

	cfg, err := payloadCodecConfig(payloadEncryptionFlags{})
	require.NoError(t, err)
	require.False(t, cfg.Enabled())

	local, err := localPayloadCodec()
	require.NoError(t, err)
	require.Equal(t, cfg.Name(), local.Name(),
		"the rehearsal resolved a different codec than the worker")

	require.Equal(t, v1.MaxRunStateBytes, payloadcodec.Null().MaxEncodedSize(v1.MaxRunStateBytes))
}

// writeTestKeyring writes a keyring for the default namespace with one key, and
// returns its path.
func writeTestKeyring(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "default.key"), local.Generate(), 0o600))
	path := filepath.Join(dir, "keyring.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
namespaces:
  default:
    current: default-1
    keys:
      - id: default-1
        file: default.key
`), 0o600))
	return path
}

// TestAKeyringResolvesTheSameForBothEntryPoints is the stock configuration
// path: a keyring named in the environment is what the worker and the server
// build their clients with, and what a local run validates.
func TestAKeyringResolvesTheSameForBothEntryPoints(t *testing.T) {
	t.Setenv(payloadKeyringEnv, writeTestKeyring(t))
	t.Setenv(requirePayloadEncryptionEnv, "true")

	cfg, err := payloadCodecConfig(payloadEncryptionFromEnv())
	require.NoError(t, err)
	require.True(t, cfg.Enabled())

	one, err := cfg.ForNamespace("default")
	require.NoError(t, err)
	require.Equal(t, "default-1", one.Codec.CurrentKeyID())

	tc, err := temporalConfig(t.Context(), temporalFlags{payloadEncryption: payloadEncryptionFromEnv()})
	require.NoError(t, err)
	opts, err := tc.Options()
	require.NoError(t, err)
	require.NotEqual(t, payloadcodec.Serializer(), opts.DataConverter, "the client was built without the codec")

	// A client for a namespace nobody keyed is refused, not built in plaintext.
	tc.Namespace = "somewhere-else"
	_, err = tc.Options()
	require.ErrorContains(t, err, `"somewhere-else"`)

	_, _, err = runLocal(t, codecTestWorkflow)
	require.NoError(t, err)
}

// TestRequiredEncryptionRefusesToStartWithoutAKeyring is the fail-closed mode:
// a production deployment that lost its keyring variable must not come up
// writing plaintext, and the rehearsal refuses the same way.
func TestRequiredEncryptionRefusesToStartWithoutAKeyring(t *testing.T) {
	t.Setenv(payloadKeyringEnv, "")
	t.Setenv(requirePayloadEncryptionEnv, "1")

	_, err := temporalConfig(t.Context(), temporalFlags{payloadEncryption: payloadEncryptionFromEnv()})
	require.ErrorContains(t, err, "payload encryption is required")

	_, _, err = runLocal(t, codecTestWorkflow)
	require.ErrorContains(t, err, "payload encryption is required")

	// A value that does not parse as a boolean requires rather than waives.
	t.Setenv(requirePayloadEncryptionEnv, "yes-please")
	require.True(t, payloadEncryptionFromEnv().required)
}

// TestABrokenKeyringRefusesBothEntryPoints: a keyring that names a key file
// with loose permissions stops the worker and the rehearsal alike, rather than
// starting in plaintext or with a partial ring.
func TestABrokenKeyringRefusesBothEntryPoints(t *testing.T) {
	path := writeTestKeyring(t)
	require.NoError(t, os.Chmod(filepath.Join(filepath.Dir(path), "default.key"), 0o644))
	t.Setenv(payloadKeyringEnv, path)
	t.Setenv(requirePayloadEncryptionEnv, "")

	_, err := temporalConfig(t.Context(), temporalFlags{payloadEncryption: payloadEncryptionFromEnv()})
	require.ErrorContains(t, err, "chmod 600")

	_, _, err = runLocal(t, codecTestWorkflow)
	require.ErrorContains(t, err, "chmod 600")
}

type timedCodec struct {
	payloadcodec.Codec
	timeout time.Duration
}

func (c timedCodec) ProviderTimeout() time.Duration { return c.timeout }

// TestTheCodecServerWaitsAsLongAsItsProvidersMay: the handler stops starting
// work at its budget, and the payload started just before may take a
// provider's deadline twice over for a login; the response deadline leaves
// room for both, so the answer is written rather than cut off.
func TestTheCodecServerWaitsAsLongAsItsProvidersMay(t *testing.T) {
	t.Parallel()

	require.Equal(t, 30*time.Second, codecWriteTimeout(payloadcodec.Config{}))
	require.Equal(t, 40*time.Second, codecWriteTimeout(payloadcodec.Config{Codec: timedCodec{timeout: 5 * time.Second}}))
	require.Equal(t, 150*time.Second, codecWriteTimeout(payloadcodec.Config{Codec: timedCodec{timeout: time.Minute}}))
}
