package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// runKeysGenerateInto executes `flow keys generate` and captures both streams.
func runKeysGenerateInto(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer

	cmd := newKeysGenerateCommand()
	for i := 0; i+1 < len(args); i += 2 {
		require.NoError(t, cmd.Flags().Set(args[i], args[i+1]))
	}
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetContext(t.Context())

	err = runKeysGenerate(cmd, nil)

	return out.String(), errOut.String(), err
}

func runKeysPublicInto(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer

	cmd := newKeysPublicCommand()
	for i := 0; i+1 < len(args); i += 2 {
		require.NoError(t, cmd.Flags().Set(args[i], args[i+1]))
	}
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetContext(t.Context())

	err = runKeysPublic(cmd, nil)

	return out.String(), errOut.String(), err
}

func TestKeysGenerateWritesA0600PrivateKeyAndPrintsOnlyThePublicHalf(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "2026-08.pem")

	stdout, stderr, err := runKeysGenerateInto(t, "out", path)
	require.NoError(t, err)
	require.Contains(t, stderr, "2026-08")
	require.Contains(t, stderr, "0600")

	info, err := os.Stat(path)
	require.NoError(t, err)
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	var jwk map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &jwk))
	require.Equal(t, "2026-08", jwk["kid"])
	require.Equal(t, "sig", jwk["use"])
	require.Equal(t, "ES256", jwk["alg"])

	// The private key must never appear anywhere in either stream: not the raw
	// PEM, not a PKCS#8/EC private key marker.
	require.NotContains(t, stdout, "PRIVATE KEY")
	require.NotContains(t, stderr, "PRIVATE KEY")

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(raw), "PRIVATE KEY")
}

func TestKeysGenerateRefusesToOverwriteAnExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "2026-08.pem")

	_, _, err := runKeysGenerateInto(t, "out", path)
	require.NoError(t, err)

	before, err := os.ReadFile(path)
	require.NoError(t, err)

	_, _, err = runKeysGenerateInto(t, "out", path)
	require.Error(t, err)
	require.ErrorContains(t, err, "already exists")

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "a refused generate must not touch the existing key")
}

func TestKeysGenerateEveryAlgorithm(t *testing.T) {
	for _, algorithm := range []string{"es256", "rs256", "ed25519"} {
		t.Run(algorithm, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "key.pem")

			stdout, _, err := runKeysGenerateInto(t, "algorithm", algorithm, "out", path)
			require.NoError(t, err)

			var jwk map[string]any
			require.NoError(t, json.Unmarshal([]byte(stdout), &jwk))
			require.NotEmpty(t, jwk["kty"])
		})
	}
}

func TestKeysGenerateRejectsAnUnknownAlgorithm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key.pem")

	_, _, err := runKeysGenerateInto(t, "algorithm", "rot13", "out", path)
	require.Error(t, err)
	require.ErrorContains(t, err, "rot13")

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "a rejected algorithm must not leave a file behind")
}

// TestKeysGenerateErrorTeachesTheAliasItsOwnHelpUsed pins picatz/flowstate#395's
// first item: `flow keys generate --help`'s own example teaches
// `--algorithm ed25519`, so the error a caller sees after mistyping something
// else has to admit that spelling exists rather than listing only "eddsa" — the
// help page and the rejection must agree on what this flag accepts.
//
// Mutation-proven: reverting algorithmNames to plain
// strings.ToLower(alg) for every entry (dropping the "(also: ed25519)"
// annotation) makes this fail.
func TestKeysGenerateErrorTeachesTheAliasItsOwnHelpUsed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key.pem")

	_, _, err := runKeysGenerateInto(t, "algorithm", "rot13", "out", path)
	require.Error(t, err)
	require.ErrorContains(t, err, "ed25519",
		"the rejection message does not name the alias --help's own example teaches")
}

func TestKeysGenerateIDDefaultsToTheFileNameWithoutExtension(t *testing.T) {
	path := filepath.Join(t.TempDir(), "team-a-signing.pem")

	stdout, _, err := runKeysGenerateInto(t, "out", path)
	require.NoError(t, err)

	var jwk map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &jwk))
	require.Equal(t, "team-a-signing", jwk["kid"])
}

func TestKeysGenerateIDCanBeOverridden(t *testing.T) {
	path := filepath.Join(t.TempDir(), "team-a-signing.pem")

	stdout, _, err := runKeysGenerateInto(t, "out", path, "id", "explicit-kid")
	require.NoError(t, err)

	var jwk map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &jwk))
	require.Equal(t, "explicit-kid", jwk["kid"])
}

func TestKeysPublicMatchesWhatGenerateAlreadyPrinted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "2026-08.pem")

	generated, _, err := runKeysGenerateInto(t, "out", path)
	require.NoError(t, err)

	public, _, err := runKeysPublicInto(t, "in", path)
	require.NoError(t, err)

	require.JSONEq(t, generated, public)
}

func TestKeysPublicRefusesAFileThatIsNotAPrivateKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-key.pem")
	require.NoError(t, os.WriteFile(path, []byte("not pem data at all"), 0o600))

	_, _, err := runKeysPublicInto(t, "in", path)
	require.Error(t, err)
	require.ErrorContains(t, err, "not PEM-encoded")
}

func TestKeysGenerateRefusesAMissingOutDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist", "key.pem")

	_, _, err := runKeysGenerateInto(t, "out", path)
	require.Error(t, err)
}

func TestNewKeysCommandWiresBothSubcommands(t *testing.T) {
	cmd := newKeysCommand()

	names := map[string]bool{}
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	require.True(t, names["generate"])
	require.True(t, names["public"])
}
