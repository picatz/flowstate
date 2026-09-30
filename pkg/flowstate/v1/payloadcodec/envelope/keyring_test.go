package envelope_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/hpke"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/vault/vaulttest"
	secretsvault "github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// writeFile writes data under dir with mode.
func writeFile(t *testing.T, dir, name string, data []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, data, mode))
	return path
}

func load(t *testing.T, dir, config string) (*envelope.Keyring, error) {
	t.Helper()
	return envelope.LoadFile(t.Context(), writeFile(t, dir, "keyring.yaml", []byte(config), 0o600), envelope.OpenOptions{})
}

// twoTenants is the shape the two-tenant scenarios share: identical key ids
// would be refused, so each namespace names its own, and each has its own
// material.
func twoTenants(t *testing.T) *envelope.Keyring {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "a.key", local.Generate(), 0o600)
	writeFile(t, dir, "b.key", local.Generate(), 0o600)
	kr, err := load(t, dir, `
namespaces:
  tenant-a:
    current: a-2026-09
    keys:
      - {id: a-2026-09, file: a.key}
  tenant-b:
    current: b-2026-09
    keys:
      - {id: b-2026-09, file: b.key}
`)
	require.NoError(t, err)
	return kr
}

func TestKeyringSeparatesNamespaces(t *testing.T) {
	t.Parallel()

	kr := twoTenants(t)
	require.Equal(t, []string{"tenant-a", "tenant-b"}, kr.Namespaces())
	a, _ := kr.Codec("tenant-a")
	b, _ := kr.Codec("tenant-b")

	sealedA := seal(t, a, marker)

	// Tenant B's codec, which is what B's workers and B's client hold, cannot
	// read A's payload: it does not hold A's key.
	_, err := b.Decode([]*commonpb.Payload{sealedA})
	require.ErrorIs(t, err, envelope.ErrUnknownKey)

	// The reader holds both and opens each against its own namespace.
	out, err := kr.Reader().Decode([]*commonpb.Payload{sealedA, seal(t, b, marker)})
	require.NoError(t, err)
	require.Len(t, out, 2)
	_, err = kr.Reader().Encode([]*commonpb.Payload{plainPayload(marker)})
	require.ErrorIs(t, err, envelope.ErrReaderCannotEncode)
}

func TestKeyringIsTheCodecSlot(t *testing.T) {
	t.Parallel()

	cfg := twoTenants(t).PayloadCodecConfig()
	require.NoError(t, cfg.Validate())
	for _, ns := range []string{"tenant-a", "tenant-b"} {
		c, err := cfg.ForNamespace(ns)
		require.NoError(t, err)
		require.NotEmpty(t, c.Codec.CurrentKeyID())
	}
	_, err := cfg.ForNamespace("tenant-c")
	require.Error(t, err, "a namespace the keyring does not cover is refused rather than written in plaintext")
}

func TestKeyringConfigurationIsChecked(t *testing.T) {
	t.Parallel()

	for name, config := range map[string]string{
		"no namespaces":             `namespaces: {}`,
		"current not listed":        `namespaces: {ns: {current: k2, keys: [{id: k1, env: K}]}}`,
		"id reused across tenants":  "namespaces:\n  a: {current: k1, keys: [{id: k1, env: K}]}\n  b: {current: k1, keys: [{id: k1, env: K}]}\n",
		"id reused as escrow":       "namespaces: {a: {current: k1, keys: [{id: k1, env: K}]}}\nescrow_keys: [{id: k1, env: K}]\n",
		"escrow not listed":         `namespaces: {a: {current: k1, keys: [{id: k1, env: K}], escrow: [nope]}}`,
		"vault provider not listed": `namespaces: {a: {current: k1, keys: [{id: k1, vault: {provider: corp, key: k}}]}}`,
		"no source":                 `namespaces: {ns: {current: k1, keys: [{id: k1}]}}`,
		"unknown field":             `namespaces: {ns: {current: k1, keys: [{id: k1, env: K, material: x}]}}`,
		"unknown suite":             `namespaces: {ns: {current: k1, keys: [{id: k1, env: K}], suite: 7}}`,
		"nothing to read with":      `namespaces: {ns: {}}`,
		"too many escrow": "namespaces: {a: {current: k1, keys: [{id: k1, env: K}], escrow: [e1, e2, e3, e4]}}\n" +
			"escrow_keys: [{id: e1, env: K}, {id: e2, env: K}, {id: e3, env: K}, {id: e4, env: K}]\n",
		"a data key window over a day": `namespaces: {ns: {current: k1, keys: [{id: k1, env: K}], data_key: {max_age: 90000s}}}`,
		"hpke as a namespace key":      `namespaces: {ns: {current: h1, keys: [{id: h1, hpke: {public_key: {env: P}}}]}}`,
	} {
		_, err := envelope.ParseConfig([]byte(config))
		require.Error(t, err, name)
	}

	_, err := envelope.ParseConfig([]byte(`
namespaces:
  ns:
    current: k1
    keys: [{id: k1, env: K}]
    suite: PAYLOAD_SUITE_XCHACHA20_POLY1305
    decrypt_suites: [PAYLOAD_SUITE_AES256_GCM, PAYLOAD_SUITE_XCHACHA20_POLY1305]
    data_key: {max_age: 3600s, max_messages: 1000, stale_grace: 30s}
`))
	require.NoError(t, err, "every documented field parses")
}

func TestKeyringRefusesKeysItCannotRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "loose.key", local.Generate(), 0o644)
	writeFile(t, dir, "bad.key", []byte("not a key"), 0o600)

	cases := map[string]string{
		"missing file":   `namespaces: {ns: {current: k1, keys: [{id: k1, file: absent.key}]}}`,
		"unset variable": `namespaces: {ns: {current: k1, keys: [{id: k1, env: FLOWSTATE_TEST_ABSENT_KEY}]}}`,
		"not a key":      `namespaces: {ns: {current: k1, keys: [{id: k1, file: bad.key}]}}`,
		"vault token unset": "namespaces: {ns: {current: k1, keys: [{id: k1, vault: {provider: corp, key: k}}]}}\n" +
			"providers: [{name: corp, vault: {address: 'http://127.0.0.1:1', token_env: FLOWSTATE_TEST_ABSENT_TOKEN}}]\n",
	}
	if runtime.GOOS != "windows" {
		cases["readable by others"] = `namespaces: {ns: {current: k1, keys: [{id: k1, file: loose.key}]}}`
	}
	for name, config := range cases {
		_, err := load(t, dir, config)
		require.Error(t, err, name)
	}
}

func TestReaderTakesTheStrictestUnencryptedPolicy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "a.key", local.Generate(), 0o600)
	writeFile(t, dir, "b.key", local.Generate(), 0o600)
	kr, err := load(t, dir, `
namespaces:
  a: {current: a1, keys: [{id: a1, file: a.key}], accept_unencrypted: true}
  b: {current: b1, keys: [{id: b1, file: b.key}]}
`)
	require.NoError(t, err)
	_, err = kr.Reader().Decode([]*commonpb.Payload{plainPayload(marker)})
	require.ErrorIs(t, err, envelope.ErrUnencrypted)
}

// TestARecoveryKeyringReadsThroughEscrow is the break-glass journey through
// configuration alone: workers name the escrow public key; the primary key is
// then lost; a recovery keyring holding only the escrow private key, with the
// namespace decode-only, reads the history.
func TestARecoveryKeyringReadsThroughEscrow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	private, public, err := hpke.Generate(0)
	require.NoError(t, err)
	writeFile(t, dir, "primary.key", local.Generate(), 0o600)
	writeFile(t, dir, "break-glass.key", private, 0o600)
	writeFile(t, dir, "break-glass.key.pub", public, 0o644)

	workers, err := load(t, dir, `
namespaces:
  ns:
    current: primary
    keys: [{id: primary, file: primary.key}]
    escrow: [break-glass]
escrow_keys:
  - id: break-glass
    hpke: {public_key: {file: break-glass.key.pub}}
`)
	require.NoError(t, err)
	w, _ := workers.Codec("ns")
	sealed := seal(t, w, marker)

	status := workers.Status().GetNamespaces()[0]
	var escrow *v1.PayloadKeyStatus
	for _, k := range status.GetKeys() {
		if k.GetEscrow() {
			escrow = k
		}
	}
	require.NotNil(t, escrow)
	require.Equal(t, "hpke", escrow.GetKind())
	require.False(t, escrow.GetCanUnwrap(), "a worker holds only the public half")

	recovery, err := load(t, dir, `
namespaces:
  ns:
    escrow: [break-glass]
escrow_keys:
  - id: break-glass
    hpke:
      public_key: {file: break-glass.key.pub}
      private_key: {file: break-glass.key}
`)
	require.NoError(t, err)
	r, _ := recovery.Codec("ns")
	out, err := r.Decode([]*commonpb.Payload{sealed})
	require.NoError(t, err)
	require.True(t, proto.Equal(plainPayload(marker), out[0]))
	require.Empty(t, recovery.Status().GetNamespaces()[0].GetCurrentKeyId())

	// The private key is a secret file like any other.
	if runtime.GOOS != "windows" {
		require.NoError(t, os.Chmod(filepath.Join(dir, "break-glass.key"), 0o640))
		_, err = load(t, dir, `
namespaces: {ns: {escrow: [break-glass]}}
escrow_keys: [{id: break-glass, hpke: {public_key: {file: break-glass.key.pub}, private_key: {file: break-glass.key}}}]
`)
		require.Error(t, err)

		// The public key is not secret, but it decides who can unwrap every
		// new data key: one another account could have written is refused,
		// and so is a keyring configuration it could have written.
		require.NoError(t, os.Chmod(filepath.Join(dir, "break-glass.key.pub"), 0o664))
		_, err = load(t, dir, `
namespaces: {ns: {current: primary, keys: [{id: primary, file: primary.key}], escrow: [break-glass]}}
escrow_keys: [{id: break-glass, hpke: {public_key: {file: break-glass.key.pub}}}]
`)
		require.ErrorContains(t, err, "go-w")

		require.NoError(t, os.Chmod(filepath.Join(dir, "break-glass.key.pub"), 0o644))
		config := writeFile(t, dir, "shared.yaml", []byte(`namespaces: {ns: {current: primary, keys: [{id: primary, file: primary.key}]}}`), 0o644)
		_, err = envelope.LoadFile(t.Context(), config, envelope.OpenOptions{})
		require.NoError(t, err, "a configuration readable by others is fine")
		require.NoError(t, os.Chmod(config, 0o666))
		_, err = envelope.LoadFile(t.Context(), config, envelope.OpenOptions{})
		require.ErrorContains(t, err, "go-w")
	}
}

// TestAVaultKeyringSealsThroughTransit: a keyring naming a Vault provider
// wraps its data keys through Transit, reads them back, and fails startup
// when Transit refuses.
func TestAVaultKeyringSealsThroughTransit(t *testing.T) {
	t.Setenv("FLOWSTATE_TEST_VAULT_TOKEN", vaulttest.Token)

	server := vaulttest.NewServer(t)
	server.Create("flowstate-ns", "aes256-gcm96")
	dir := t.TempDir()
	config := `
namespaces:
  ns:
    current: ns-vault
    keys: [{id: ns-vault, vault: {provider: corp, key: flowstate-ns}}]
providers:
  - name: corp
    vault: {address: '` + server.URL() + `', token_env: FLOWSTATE_TEST_VAULT_TOKEN}
`
	kr, err := load(t, dir, config)
	require.NoError(t, err)
	c, _ := kr.Codec("ns")
	sealed := seal(t, c, marker)
	require.True(t, strings.HasPrefix(string(header(t, sealed).GetWrappedKey()), "vault:v1:"))
	require.EqualValues(t, 1, header(t, sealed).GetKeyVersion())

	// A second process, with its own cache, unwraps through Transit.
	other, err := load(t, t.TempDir(), config)
	require.NoError(t, err)
	oc, _ := other.Codec("ns")
	out, err := oc.Decode([]*commonpb.Payload{sealed})
	require.NoError(t, err)
	require.True(t, proto.Equal(plainPayload(marker), out[0]))

	status := kr.Status().GetNamespaces()[0].GetKeys()[0]
	require.Equal(t, "vault", status.GetKind())
	require.EqualValues(t, 1, status.GetVersion())
	require.Empty(t, status.GetFingerprint(), "a Vault key's material never reaches the process")

	// Rotating the Transit key is Vault's business: new data keys are
	// wrapped under the new version, old payloads still read.
	server.Rotate("flowstate-ns")
	fresh, err := load(t, t.TempDir(), config)
	require.NoError(t, err)
	fc, _ := fresh.Codec("ns")
	require.EqualValues(t, 2, header(t, seal(t, fc, marker)).GetKeyVersion())
	_, err = fc.Decode([]*commonpb.Payload{sealed})
	require.NoError(t, err)

	// A key Transit will not serve fails startup rather than the first write.
	server.SetStatus(403)
	_, err = load(t, t.TempDir(), config)
	require.ErrorIs(t, err, envelope.ErrKeyDenied)

}

// TestARecoveryNamespaceIsNotRefusedOverASuiteItNeverWrites: a decode-only
// namespace narrowed to the one suite its history used has no encrypt suite
// to check, and opens; a writing namespace with the same list is still
// refused, since it would write AES-256-GCM it then could not read.
func TestARecoveryNamespaceIsNotRefusedOverASuiteItNeverWrites(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "k.key", local.Generate(), 0o600)
	_, err := load(t, dir, `
namespaces:
  ns:
    keys: [{id: k, file: k.key}]
    decrypt_suites: [PAYLOAD_SUITE_XCHACHA20_POLY1305]
`)
	require.NoError(t, err)

	_, err = load(t, dir, `
namespaces:
  ns:
    current: k
    keys: [{id: k, file: k.key}]
    decrypt_suites: [PAYLOAD_SUITE_XCHACHA20_POLY1305]
`)
	require.ErrorContains(t, err, "not among the decrypt suites")
}

// TestAVaultTimeoutReachesTheCodecsDeadline: a provider's configured timeout
// bounds each request to it, so the deadline the codecs put on a wrap or
// unwrap must leave room for it, and for the login before it, rather than
// cutting a call the provider was told it could take off at the default.
func TestAVaultTimeoutReachesTheCodecsDeadline(t *testing.T) {
	t.Setenv("FLOWSTATE_TEST_VAULT_TOKEN", vaulttest.Token)

	server := vaulttest.NewServer(t)
	server.Create("flowstate-ns", "aes256-gcm96")
	config := func(timeout string) string {
		return `
namespaces:
  ns:
    current: ns-vault
    keys: [{id: ns-vault, vault: {provider: corp, key: flowstate-ns}}]
providers:
  - name: corp
    vault: {address: '` + server.URL() + `', token_env: FLOWSTATE_TEST_VAULT_TOKEN` + timeout + `}
`
	}

	for _, tc := range []struct {
		timeout string
		want    time.Duration
	}{
		// No timeout is the Vault client's default per request, not none.
		{"", 4 * secretsvault.DefaultTimeout},
		{", timeout: 1s", envelope.DefaultProviderTimeout},
		// Four requests: a login, the call, and after a 403 on a renewable
		// token a second login and the call again.
		{", timeout: 30s", 2 * time.Minute},
	} {
		kr, err := load(t, t.TempDir(), config(tc.timeout))
		require.NoError(t, err)
		c, _ := kr.Codec("ns")
		require.Equal(t, tc.want, c.ProviderTimeout(), "codec, timeout%q", tc.timeout)
		require.Equal(t, tc.want, kr.Reader().ProviderTimeout(), "reader, timeout%q", tc.timeout)
	}
}

// TestTheDocumentedKeyringsParse holds docs/ENCRYPTION.md to the schema: every
// YAML block there that configures a keyring is one ParseConfig accepts, so
// the page cannot teach a field that was renamed or a rule that was added.
func TestTheDocumentedKeyringsParse(t *testing.T) {
	t.Parallel()

	doc, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", "docs", "ENCRYPTION.md"))
	require.NoError(t, err)

	blocks := 0
	for chunk := range strings.SplitSeq(string(doc), "```yaml\n") {
		block, _, found := strings.Cut(chunk, "```")
		if !found || !strings.Contains(block, "namespaces:") {
			continue
		}
		blocks++
		_, err := envelope.ParseConfig([]byte(block))
		require.NoError(t, err, "docs/ENCRYPTION.md teaches a keyring the schema refuses:\n%s", block)
	}
	require.GreaterOrEqual(t, blocks, 6, "the keyring examples were not found, so nothing was checked")
}

// TestAVaultCAFileIsCheckedLikeTheKeyring: a provider's CA bundle decides
// which server receives the Vault token and every wrap, so it is read like
// the keyring's other such files: bounded, and refused if another account
// could have written it, before anything is contacted.
func TestAVaultCAFileIsCheckedLikeTheKeyring(t *testing.T) {
	t.Setenv("T", "synthetic-token")
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not checked on Windows")
	}

	dir := t.TempDir()
	config := func(ca string) string {
		return `
namespaces:
  ns: {current: k, keys: [{id: k, vault: {provider: corp, key: k}}]}
providers:
  - name: corp
    vault: {address: 'https://vault.invalid', token_env: T, ca_file: ` + ca + `}
`
	}

	// Chmod after writing, so the process umask cannot mask the bits.
	require.NoError(t, os.Chmod(writeFile(t, dir, "writable.pem", []byte("not a certificate"), 0o644), 0o666))
	_, err := load(t, dir, config("writable.pem"))
	require.ErrorContains(t, err, "go-w")

	writeFile(t, dir, "empty.pem", []byte("not a certificate"), 0o644)
	_, err = load(t, dir, config("empty.pem"))
	require.ErrorContains(t, err, "holds no PEM certificate")

	big := make([]byte, envelope.MaxCAFileBytes+1)
	writeFile(t, dir, "big.pem", big, 0o644)
	_, err = load(t, dir, config("big.pem"))
	require.ErrorContains(t, err, "byte limit")
}

// TestTheStartupBudgetCoversEveryStartupCall: a keyring whose providers were
// given long timeouts gets the time its startup calls may take, so opening it
// is not cut short by a fixed bound, and a local keyring keeps the floor.
func TestTheStartupBudgetCoversEveryStartupCall(t *testing.T) {
	t.Parallel()

	plain, err := envelope.ParseConfig([]byte(`
namespaces:
  ns: {current: k, keys: [{id: k, env: K}]}
`))
	require.NoError(t, err)
	require.Equal(t, envelope.MinStartupBudget, envelope.StartupBudget(plain))

	vault, err := envelope.ParseConfig([]byte(`
namespaces:
  a: {current: a, keys: [{id: a, vault: {provider: corp, key: a}}]}
  b: {current: b, keys: [{id: b, vault: {provider: corp, key: b}}]}
providers:
  - name: corp
    vault: {address: 'https://vault.example.com', token_env: T, timeout: 60s}
`))
	require.NoError(t, err)
	// One login, four calls to describe each key, one wrap per writing
	// namespace: 11 calls, each within four of the 60s requests.
	require.Equal(t, 11*4*time.Minute, envelope.StartupBudget(vault))

	// An escrow key a provider holds is described again by every namespace
	// that names it.
	shared, err := envelope.ParseConfig([]byte(`
namespaces:
  a: {current: a, keys: [{id: a, env: A}], escrow: [e]}
  b: {current: b, keys: [{id: b, env: B}], escrow: [e]}
escrow_keys:
  - {id: e, vault: {provider: corp, key: e}}
providers:
  - name: corp
    vault: {address: 'https://vault.example.com', token_env: T, timeout: 60s}
`))
	require.NoError(t, err)
	// One login, four calls to describe e in each of two namespaces, two
	// wraps in each: 13 calls.
	require.Equal(t, 13*4*time.Minute, envelope.StartupBudget(shared))

	// A provider that names no timeout still lets each request run for the
	// Vault client's default, and the budget allows it that.
	defaulted, err := envelope.ParseConfig([]byte(`
namespaces:
  a: {current: a, keys: [{id: a, vault: {provider: corp, key: a}}]}
  b: {current: b, keys: [{id: b, vault: {provider: corp, key: b}}]}
providers:
  - name: corp
    vault: {address: 'https://vault.example.com', token_env: T}
`))
	require.NoError(t, err)
	require.Equal(t, 11*4*secretsvault.DefaultTimeout, envelope.StartupBudget(defaulted))
}
