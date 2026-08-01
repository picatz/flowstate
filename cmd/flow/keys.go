package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Issue #111 asked for a first-class way to generate and inspect the signing
// keys an [auth.Issuer] uses, for the same reason `flow tasks` exists: an
// operator debugging identity should not need a throwaway Go program to answer
// "what does this key publish" or "why won't this token verify."
//
// [auth.SigningKey] deliberately has no way to get the private key back out —
// [auth.SigningKey.sign] is unexported and there is no accessor for the raw
// material, which is the whole point of the type. So these commands generate
// and parse the raw [crypto.PrivateKey] themselves, the same way
// [auth.GenerateSigningKey] and [parseSigningKey] do internally, and call
// [auth.NewSigningKey] purely for its validation (RSA modulus size, P-256
// curve) before ever touching the filesystem or stdout. The private key never
// leaves this file; only the derived public JWK does.

// minGeneratedRSABits mirrors the floor [auth.NewSigningKey] enforces. It is
// redeclared here, rather than exported from auth, because the auth package's
// job is to validate a key it is handed, not to hand out the policy as a
// number a caller could generate just under.
const minGeneratedRSABits = 2048

// signingAlgorithms are the algorithms `flow keys generate` accepts, in the
// order [auth.GenerateSigningKey] documents them: smallest and fastest first.
var signingAlgorithms = []jwa.Algorithm{jwa.ES256, jwa.RS256, jwa.EdDSA}

func newKeysCommand() *cobra.Command {
	keysCmd := &cobra.Command{
		Use:   "keys",
		Short: "Generate and inspect signing keys for workload identity",
		Long: "Generate and inspect the asymmetric keys an issuer signs workload " +
			"identity assertions with. Only the public half is ever printed; the " +
			"private key stays on disk at the path given to --out.",
	}

	keysCmd.AddCommand(newKeysGenerateCommand())
	keysCmd.AddCommand(newKeysPublicCommand())

	return keysCmd
}

func newKeysGenerateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate a new signing key and write it to a file",
		Long: "Generate a private key, write it PKCS#8-PEM-encoded to --out at " +
			"file mode 0600, and print the public key as a JWK. Refuses to " +
			"overwrite an existing file: rotating a key is naming a new file, " +
			"the same convention the server's --identity-key flag uses.",
		Args: cobra.NoArgs,
		RunE: runKeysGenerate,
		Example: `# Generate an Ed25519 key, the smallest and fastest option:
flow keys generate --out identity/2026-08.pem

# RSA-2048, for a relying party that requires it:
flow keys generate --algorithm rs256 --out identity/2026-08.pem

# Override the key id the file name would otherwise supply:
flow keys generate --out identity/key.pem --id 2026-08`,
	}

	cmd.Flags().String("algorithm", string(jwa.ES256),
		"signing algorithm: "+algorithmNames())
	cmd.Flags().String("out", "", "path to write the private key PEM to (required)")
	cmd.Flags().String("id", "", "key id published in the JWK and the JWT \"kid\" header "+
		"(default: --out's file name, without its extension)")
	_ = cmd.MarkFlagRequired("out")

	return cmd
}

func newKeysPublicCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "public",
		Short: "Print the public JWK for an existing signing key",
		Args:  cobra.NoArgs,
		RunE:  runKeysPublic,
		Example: `# What does this key publish?
flow keys public --in identity/2026-08.pem`,
	}

	cmd.Flags().String("in", "", "path to a PKCS#8 private key PEM (required)")
	cmd.Flags().String("id", "", "key id published in the JWK "+
		"(default: --in's file name, without its extension)")
	_ = cmd.MarkFlagRequired("in")

	return cmd
}

func algorithmNames() string {
	names := make([]string, 0, len(signingAlgorithms))
	for _, alg := range signingAlgorithms {
		names = append(names, strings.ToLower(alg))
	}
	return strings.Join(names, ", ")
}

// keyIDFromPath derives a key id the way [parseSigningKey] does: the file's
// base name without its extension, so `2026-08.pem` becomes "2026-08". Naming
// the file is the whole of key rotation, and `flow keys` and the server it
// generates keys for have to agree on that convention or a generated key
// would publish under a different id than the server that loads it uses.
func keyIDFromPath(path string) string {
	return strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
}

// readPrivateKeyPEM loads a PKCS#8 private key the same way [parseSigningKey]
// does for the server's --identity-key flag, so `flow keys`/`flow jwt` and the
// server accept exactly the same files.
func readPrivateKeyPEM(path string) (crypto.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("%s is not PEM-encoded", path)
	}

	private, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%s is not a PKCS#8 private key "+
			"(convert one with: openssl pkcs8 -topk8 -nocrypt -in old.pem -out new.pem): %w", path, err)
	}

	return private, nil
}

// resolveSigningAlgorithm accepts the flag's user-facing spelling — including
// "ed25519", which is what someone typing --algorithm actually thinks of the
// key as, even though the JWA name for it is "EdDSA" — and returns the jwa
// constant it means, or an error naming exactly what was typed.
func resolveSigningAlgorithm(flagValue string) (jwa.Algorithm, error) {
	switch strings.ToLower(flagValue) {
	case strings.ToLower(jwa.ES256):
		return jwa.ES256, nil
	case strings.ToLower(jwa.RS256):
		return jwa.RS256, nil
	case strings.ToLower(jwa.EdDSA), "ed25519":
		return jwa.EdDSA, nil
	default:
		return "", fmt.Errorf("--algorithm %q is not one this understands; want one of: %s",
			flagValue, algorithmNames())
	}
}

func generatePrivateKey(algorithm jwa.Algorithm) (crypto.PrivateKey, error) {
	switch algorithm {
	case jwa.RS256:
		return rsa.GenerateKey(rand.Reader, minGeneratedRSABits)
	case jwa.ES256:
		return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case jwa.EdDSA:
		_, private, err := ed25519.GenerateKey(rand.Reader)
		return private, err
	default:
		return nil, fmt.Errorf("cannot generate a %q signing key, want one of: %s",
			algorithm, algorithmNames())
	}
}

// publicKeyOf returns the public half of a private key this package generated
// or parsed. auth.SigningKey does the identical type switch internally but
// never exposes either half, so building the JWK here means doing it again
// rather than borrowing the answer.
func publicKeyOf(private crypto.PrivateKey) (crypto.PublicKey, error) {
	switch typed := private.(type) {
	case *rsa.PrivateKey:
		return &typed.PublicKey, nil
	case *ecdsa.PrivateKey:
		return &typed.PublicKey, nil
	case ed25519.PrivateKey:
		return typed.Public(), nil
	default:
		return nil, fmt.Errorf("%T is not a signing key this understands", private)
	}
}

// publicJWK renders the public JWK the same way [auth.NewSigningKey] does
// internally (issuer.go), so a key generated here and one loaded by the
// server publish byte-for-byte the same document.
func publicJWK(id string, key auth.SigningKey, private crypto.PrivateKey) (jwk.Value, error) {
	public, err := publicKeyOf(private)
	if err != nil {
		return nil, err
	}

	value, err := jwk.ValueFromPublicKey(public)
	if err != nil {
		return nil, fmt.Errorf("rendering public key %q: %w", id, err)
	}
	value[jwk.KeyID] = id
	value[jwk.Algorithm] = key.Algorithm()
	value[jwk.PublicKeyUse] = "sig"

	return value, nil
}

// writePrivateKeyPEM writes a PKCS#8-encoded private key to path, refusing to
// touch anything already there.
//
// O_EXCL is the refusal: rotating a key is naming a new file, not silently
// replacing an old one out from under whatever still trusts its key id. The
// permission bits are asked for at open time and then verified by a
// follow-up Stat, because a caller-supplied path under a restrictive
// umask policy or an unusual filesystem could hand back something looser
// than what was requested — and a private key written world-readable, even
// briefly, is not a mistake this command gets a second chance to catch.
func writePrivateKeyPEM(path string, private crypto.PrivateKey) error {
	encoded, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return fmt.Errorf("encoding private key: %w", err)
	}

	block := &pem.Block{Type: "PRIVATE KEY", Bytes: encoded}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("%s already exists; generate a new key under a new "+
				"file name instead of overwriting one that may still be trusted", path)
		}
		return fmt.Errorf("creating %s: %w", path, err)
	}

	if err := pem.Encode(file, block); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", path, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("verifying permissions of %s: %w", path, err)
	}
	if info.Mode().Perm() != 0o600 {
		return fmt.Errorf("%s was created with mode %s instead of the requested "+
			"0600; refusing to leave a private key at that path", path, info.Mode().Perm())
	}

	return nil
}

func runKeysGenerate(cmd *cobra.Command, _ []string) error {
	surface := newSurface(cmd)

	algorithmFlag, _ := cmd.Flags().GetString("algorithm")
	out, _ := cmd.Flags().GetString("out")
	id, _ := cmd.Flags().GetString("id")

	algorithm, err := resolveSigningAlgorithm(algorithmFlag)
	if err != nil {
		return err
	}

	if id == "" {
		id = keyIDFromPath(out)
	}

	private, err := generatePrivateKey(algorithm)
	if err != nil {
		return err
	}

	// Reused purely for its validation: the same bit-length and curve checks
	// the server applies when it loads a key back in, so a key this command
	// accepts is a key the server will too.
	key, err := auth.NewSigningKey(id, private)
	if err != nil {
		return err
	}

	if err := writePrivateKeyPEM(out, private); err != nil {
		return err
	}

	fmt.Fprintf(surface.Err, "wrote %s signing key %q to %s (mode 0600)\n", algorithm, id, out)

	jwkValue, err := publicJWK(id, key, private)
	if err != nil {
		return err
	}

	return writeJWK(surface, jwkValue)
}

func runKeysPublic(cmd *cobra.Command, _ []string) error {
	surface := newSurface(cmd)

	in, _ := cmd.Flags().GetString("in")
	id, _ := cmd.Flags().GetString("id")

	if id == "" {
		id = keyIDFromPath(in)
	}

	private, err := readPrivateKeyPEM(in)
	if err != nil {
		return err
	}

	key, err := auth.NewSigningKey(id, private)
	if err != nil {
		return fmt.Errorf("%s: %w", in, err)
	}

	jwkValue, err := publicJWK(id, key, private)
	if err != nil {
		return err
	}

	return writeJWK(surface, jwkValue)
}

func writeJWK(surface *ui.UI, value jwk.Value) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("rendering public JWK: %w", err)
	}

	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)
	return err
}
