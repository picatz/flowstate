package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

// TestCodecKeygenWritesAPrivateKeyAndNeverOverwrites: the key lands owner-only,
// nothing of it reaches either stream, and a second keygen onto the same path
// is refused, because the file it would replace may still protect history.
func TestCodecKeygenWritesAPrivateKeyAndNeverOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "k.key")

	stdout, stderr, err := runCLI(t, "codec", "keygen", "--out", path)
	require.NoError(t, err)

	key, err := os.ReadFile(path)
	require.NoError(t, err)
	material := strings.TrimSpace(string(key))
	require.Len(t, material, 44, "a key is 32 bytes of padded base64")
	require.NotContains(t, stdout+stderr, material, "keygen printed the key")

	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	_, _, err = runCLI(t, "codec", "keygen", "--out", path)
	require.ErrorContains(t, err, "already exists")
	again, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, key, again, "a refused keygen changed the existing key")
}

// TestCodecStatusReportsIDsAndFingerprintsOnly checks the document `flow codec
// status` writes against the schema, and that the key material is nowhere in
// either rendering.
func TestCodecStatusReportsIDsAndFingerprintsOnly(t *testing.T) {
	t.Setenv(requirePayloadEncryptionEnv, "")
	path := writeTestKeyring(t)
	material, err := os.ReadFile(filepath.Join(filepath.Dir(path), "default.key"))
	require.NoError(t, err)
	secret := strings.TrimSpace(string(material))

	stdout, _, err := runCLI(t, "codec", "status", "--payload-keyring", path, "-o", "json")
	require.NoError(t, err)
	require.NotContains(t, stdout, secret)

	var status v1.PayloadEncryptionStatus
	require.NoError(t, protojson.Unmarshal([]byte(stdout), &status))
	require.NoError(t, v1.Validate(&status))
	require.True(t, status.GetEnabled())
	require.Len(t, status.GetNamespaces(), 1)
	require.Equal(t, "default-1", status.GetNamespaces()[0].GetCurrentKeyId())
	require.Len(t, status.GetNamespaces()[0].GetKeys()[0].GetFingerprint(), 16)

	text, _, err := runCLI(t, "codec", "status", "--payload-keyring", path)
	require.NoError(t, err)
	require.Contains(t, text, "default-1")
	require.NotContains(t, text, secret)

	// Without a keyring, and required: the same refusal every command that
	// dials Temporal gives.
	_, _, err = runCLI(t, "codec", "status", "--payload-keyring", "", "--require-payload-encryption")
	require.ErrorContains(t, err, "payload encryption is required")
}

// TestCodecKeygenWritesAnEscrowPairAnEscrowKeyringReads: the pair --hpke
// writes is the pair an escrow keyring names, the private half at 0600 and
// never printed, and status reports the escrow key as wrap-only on a worker.
func TestCodecKeygenWritesAnEscrowPairAnEscrowKeyringReads(t *testing.T) {
	t.Setenv(requirePayloadEncryptionEnv, "")
	dir := t.TempDir()
	private := filepath.Join(dir, "break-glass.key")

	stdout, stderr, err := runCLI(t, "codec", "keygen", "--hpke", "--out", private)
	require.NoError(t, err)
	secret, err := os.ReadFile(private)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(secret), "flowstate-hpke-v1-private:647a:"))
	require.NotContains(t, stdout+stderr, strings.SplitN(strings.TrimSpace(string(secret)), ":", 3)[2])
	if runtime.GOOS != "windows" {
		info, err := os.Stat(private)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	_, err = os.Stat(private + ".pub")
	require.NoError(t, err)

	_, _, err = runCLI(t, "codec", "keygen", "--hpke", "--out", private)
	require.ErrorContains(t, err, "already exists")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "default.key"), local.Generate(), 0o600))
	keyring := filepath.Join(dir, "keyring.yaml")
	require.NoError(t, os.WriteFile(keyring, []byte(`
namespaces:
  default:
    current: default-1
    keys: [{id: default-1, file: default.key}]
    escrow: [break-glass]
escrow_keys:
  - id: break-glass
    hpke: {public_key: {file: break-glass.key.pub}}
`), 0o600))

	stdout, _, err = runCLI(t, "codec", "status", "--payload-keyring", keyring)
	require.NoError(t, err)
	require.Contains(t, stdout, "seals with AES256_GCM")
	require.Contains(t, stdout, "escrow, wrap only")
	require.Contains(t, stdout, "data keys roll over every 10m0s")
}
