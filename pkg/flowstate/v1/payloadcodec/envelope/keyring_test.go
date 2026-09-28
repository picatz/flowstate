package envelope_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
)

// writeKey writes a freshly generated key file with owner-only permissions.
func writeKey(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, envelope.GenerateKey(), 0o600))
	return path
}

// twoTenants is the shape the two-tenant scenarios share: identical key ids
// would be refused, so each namespace names its own, and each has its own
// material.
func twoTenants(t *testing.T) (*envelope.Keyring, string) {
	t.Helper()
	dir := t.TempDir()
	writeKey(t, dir, "a.key")
	writeKey(t, dir, "b.key")
	cfgPath := filepath.Join(dir, "keyring.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`
namespaces:
  tenant-a:
    current: a-2026-09
    keys:
      - id: a-2026-09
        file: a.key
  tenant-b:
    current: b-2026-09
    keys:
      - id: b-2026-09
        file: b.key
`), 0o600))
	kr, err := envelope.LoadFile(cfgPath)
	require.NoError(t, err)
	return kr, dir
}

func TestKeyringSeparatesNamespaces(t *testing.T) {
	t.Parallel()

	kr, _ := twoTenants(t)
	require.Equal(t, []string{"tenant-a", "tenant-b"}, kr.Namespaces())

	a, ok := kr.Codec("tenant-a")
	require.True(t, ok)
	b, ok := kr.Codec("tenant-b")
	require.True(t, ok)

	sealedA, err := a.Encode([]*commonpb.Payload{plainPayload(marker)})
	require.NoError(t, err)

	// Tenant B's codec, which is what B's workers and B's client hold, cannot
	// read A's payload: it does not hold A's key.
	_, err = b.Decode(sealedA)
	require.ErrorIs(t, err, envelope.ErrUnknownKey)

	// Relabeling A's payload with B's key id does not help either.
	forged := &commonpb.Payload{Metadata: map[string][]byte{}, Data: sealedA[0].GetData()}
	for k, v := range sealedA[0].GetMetadata() {
		forged.Metadata[k] = v
	}
	forged.Metadata[payloadcodec.KeyIDMetadataKey] = []byte("b-2026-09")
	_, err = b.Decode([]*commonpb.Payload{forged})
	require.ErrorIs(t, err, envelope.ErrAuthentication)

	// The reader holds both and opens each against its own namespace.
	got, err := kr.Reader().Decode(sealedA)
	require.NoError(t, err)
	require.Equal(t, marker, string(got[0].GetData()))

	// And the reader never writes.
	_, err = kr.Reader().Encode([]*commonpb.Payload{plainPayload("x")})
	require.ErrorIs(t, err, envelope.ErrReaderCannotEncode)
}

func TestKeyringIsTheCodecSlot(t *testing.T) {
	t.Parallel()

	kr, _ := twoTenants(t)
	cfg := kr.PayloadCodecConfig()
	require.NoError(t, cfg.Validate())
	require.True(t, cfg.Enabled())

	one, err := cfg.ForNamespace("tenant-a")
	require.NoError(t, err)
	require.Equal(t, "a-2026-09", one.Codec.CurrentKeyID())

	// A namespace nobody configured is refused, never given plaintext or a
	// neighbour's keys.
	_, err = cfg.ForNamespace("tenant-c")
	require.Error(t, err)
	require.Contains(t, err.Error(), `"tenant-c"`)
}

func TestKeyringConfigurationIsChecked(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"empty":            ``,
		"no keys":          "namespaces:\n  a:\n    current: k\n",
		"missing current":  "namespaces:\n  a:\n    current: other\n    keys:\n      - {id: k, env: X}\n",
		"both sources":     "namespaces:\n  a:\n    current: k\n    keys:\n      - {id: k, env: X, file: y}\n",
		"no source":        "namespaces:\n  a:\n    current: k\n    keys:\n      - {id: k}\n",
		"bad id":           "namespaces:\n  a:\n    current: k/1\n    keys:\n      - {id: k/1, env: X}\n",
		"unknown field":    "namespaces:\n  a:\n    current: k\n    material: AAAA\n    keys:\n      - {id: k, env: X}\n",
		"shared id":        "namespaces:\n  a:\n    current: k\n    keys:\n      - {id: k, env: X}\n  b:\n    current: k\n    keys:\n      - {id: k, env: Y}\n",
		"duplicate in one": "namespaces:\n  a:\n    current: k\n    keys:\n      - {id: k, env: X}\n      - {id: k, env: Y}\n",
		"duplicate key":    "namespaces:\n  a:\n    current: k\n    current: j\n    keys:\n      - {id: k, env: X}\n",
		"bad env name":     "namespaces:\n  a:\n    current: k\n    keys:\n      - {id: k, env: \"X Y\"}\n",
		"not a mapping":    "- a\n- b\n",
	}
	for name, doc := range cases {
		_, err := envelope.ParseConfig([]byte(doc))
		require.Error(t, err, name)
	}

	// JSON is YAML, and the schema's JSON field spelling is accepted beside
	// its proto one.
	_, err := envelope.ParseConfig([]byte(`{"namespaces":{"a":{"current":"k","acceptUnencrypted":true,"keys":[{"id":"k","env":"X"}]}}}`))
	require.NoError(t, err)
}

func TestKeyringRefusesKeysItCannotRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	good := writeKey(t, dir, "good.key")

	open := func(k *v1.PayloadKeySource, env map[string]string) error {
		cfg := &v1.PayloadKeyring{Namespaces: map[string]*v1.PayloadKeyringNamespace{
			"ns": {Current: k.GetId(), Keys: []*v1.PayloadKeySource{k}},
		}}
		_, err := envelope.Open(cfg, envelope.OpenOptions{BaseDir: dir, Getenv: func(n string) string { return env[n] }})
		return err
	}
	file := func(name string) *v1.PayloadKeySource {
		return &v1.PayloadKeySource{Id: "k", Source: &v1.PayloadKeySource_File{File: name}}
	}
	envKey := func(name string) *v1.PayloadKeySource {
		return &v1.PayloadKeySource{Id: "k", Source: &v1.PayloadKeySource_Env{Env: name}}
	}

	require.NoError(t, open(file(filepath.Base(good)), nil))
	require.Error(t, open(file("missing.key"), nil))
	require.Error(t, open(envKey("UNSET"), nil), "an unset variable was accepted")

	text, err := os.ReadFile(good)
	require.NoError(t, err)
	require.NoError(t, open(envKey("KEY"), map[string]string{"KEY": string(text)}))
	require.Error(t, open(envKey("KEY"), map[string]string{"KEY": "short"}))

	if runtime.GOOS != "windows" {
		loose := writeKey(t, dir, "loose.key")
		require.NoError(t, os.Chmod(loose, 0o640))
		err := open(file("loose.key"), nil)
		require.Error(t, err, "a group-readable key file was accepted")
		require.Contains(t, err.Error(), "chmod 600")
	}

	big := filepath.Join(dir, "big.key")
	require.NoError(t, os.WriteFile(big, make([]byte, envelope.MaxKeyFileBytes+1), 0o600))
	require.Error(t, open(file("big.key"), nil))
}

// A reader serving a namespace that still reads pre-encryption history, beside
// one that requires encryption, must not relax the strict one.
func TestReaderTakesTheStrictestUnencryptedPolicy(t *testing.T) {
	t.Parallel()

	env := map[string]string{"A": string(envelope.GenerateKey()), "B": string(envelope.GenerateKey())}
	cfg, err := envelope.ParseConfig([]byte(`
namespaces:
  migrating:
    current: a
    accept_unencrypted: true
    keys: [{id: a, env: A}]
  strict:
    current: b
    keys: [{id: b, env: B}]
`))
	require.NoError(t, err)
	kr, err := envelope.Open(cfg, envelope.OpenOptions{Getenv: func(n string) string { return env[n] }})
	require.NoError(t, err)

	migrating, _ := kr.Codec("migrating")
	require.True(t, migrating.AcceptsUnencrypted())
	_, err = kr.Reader().Decode([]*commonpb.Payload{plainPayload("x")})
	require.ErrorIs(t, err, envelope.ErrUnencrypted)
}
