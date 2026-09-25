package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

const (
	devAuthIssuer    = "https://flowstate.local/dev"
	devAuthSubject   = "developer"
	devAuthNamespace = "default"
	devAuthKeyID     = "flowstate-dev"
)

// devAuthentication is the explicit authenticated posture `flow server dev
// --auth` assembled for this one local stack. The files are intentionally
// ordinary key, JWKS, and trust-policy documents: the posture teaches the same
// inputs a split production deployment consumes rather than introducing a
// dev-only verifier.
type devAuthentication struct {
	enabled    bool
	issuer     string
	resource   string
	subject    string
	namespace  string
	keyPath    string
	jwksPath   string
	policyPath string
	verifier   auth.Verifier
	cleanup    func()
}

func configureDevAuthentication(flags devFlags, address string) (devAuthentication, error) {
	if !flags.auth {
		return devAuthentication{
			verifier: auth.InsecureAnonymousVerifier(),
			cleanup:  func() {},
		}, nil
	}

	dir, cleanup, err := devAuthDirectory(flags.db)
	if err != nil {
		return devAuthentication{}, err
	}
	fail := func(err error) (devAuthentication, error) {
		cleanup()
		return devAuthentication{}, err
	}

	keyPath := filepath.Join(dir, "signing-key.pem")
	private, err := readPrivateKeyPEM(keyPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fail(fmt.Errorf("loading dev authentication key: %w", err))
		}
		private, err = generatePrivateKey(jwa.ES256)
		if err != nil {
			return fail(fmt.Errorf("generating dev authentication key: %w", err))
		}
		if err := writePrivateKeyPEM(keyPath, private); err != nil {
			return fail(fmt.Errorf("persisting dev authentication key: %w", err))
		}
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(keyPath)
		if err != nil {
			return fail(fmt.Errorf("checking dev authentication key permissions: %w", err))
		}
		if info.Mode().Perm() != 0o600 {
			return fail(fmt.Errorf("dev authentication key %s has mode %s; want 0600", keyPath, info.Mode().Perm()))
		}
	}

	signingKey, err := auth.NewSigningKey(devAuthKeyID, private)
	if err != nil {
		return fail(fmt.Errorf("loading dev authentication key: %w", err))
	}
	public, err := publicJWK(devAuthKeyID, signingKey, private)
	if err != nil {
		return fail(err)
	}

	jwksPath := filepath.Join(dir, "signing-keys.jwks")
	if err := writeDevAuthJSON(jwksPath, map[string]any{"keys": []jwk.Value{public}}, 0o600); err != nil {
		return fail(err)
	}

	resource := "http://" + address
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:           "flowstate-dev",
		Issuer:         devAuthIssuer,
		Audiences:      []string{resource},
		Algorithms:     []jwa.Algorithm{signingKey.Algorithm()},
		Role:           "developer",
		NamespaceClaim: "namespace",
		JWKSFile:       jwksPath,
	}}}
	policyPath := filepath.Join(dir, "trust-policy.json")
	if err := writeDevAuthJSON(policyPath, policy, 0o600); err != nil {
		return fail(err)
	}

	verifier, err := auth.NewOIDCVerifier(policy)
	if err != nil {
		return fail(fmt.Errorf("configuring dev authentication: %w", err))
	}

	return devAuthentication{
		enabled:    true,
		issuer:     devAuthIssuer,
		resource:   resource,
		subject:    devAuthSubject,
		namespace:  devAuthNamespace,
		keyPath:    keyPath,
		jwksPath:   jwksPath,
		policyPath: policyPath,
		verifier:   verifier,
		cleanup:    cleanup,
	}, nil
}

func devAuthDirectory(database string) (string, func(), error) {
	if database == "" {
		dir, err := os.MkdirTemp("", "flowstate-dev-auth-")
		if err != nil {
			return "", nil, fmt.Errorf("creating temporary dev authentication directory: %w", err)
		}
		return dir, func() { _ = os.RemoveAll(dir) }, nil
	}

	abs, err := filepath.Abs(database)
	if err != nil {
		return "", nil, fmt.Errorf("resolving dev database path: %w", err)
	}
	dir := abs + ".flowstate-auth"
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", nil, fmt.Errorf("creating persistent dev authentication directory %s: %w", dir, err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(dir)
		if err != nil {
			return "", nil, fmt.Errorf("checking dev authentication directory %s: %w", dir, err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			return "", nil, fmt.Errorf("dev authentication directory %s has mode %s; want no group or other access", dir, info.Mode().Perm())
		}
	}
	return dir, func() {}, nil
}

func writeDevAuthJSON(path string, value any, mode os.FileMode) (err error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding dev authentication file %s: %w", path, err)
	}
	data = append(data, '\n')

	// Persistent dev stacks overwrite these generated documents. Open without
	// truncation first so a FIFO can be opened nonblocking and rejected before
	// either waiting for a reader or destroying an existing regular file.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|syscall.O_NONBLOCK, mode)
	if err != nil {
		return fmt.Errorf("writing dev authentication file %s: %w", path, err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspecting dev authentication file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("dev authentication file %s is not a regular file (%s)", path, info.Mode())
	}
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("truncating dev authentication file %s: %w", path, err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seeking dev authentication file %s: %w", path, err)
	}
	written, err := file.Write(data)
	if err != nil {
		return fmt.Errorf("writing dev authentication file %s: %w", path, err)
	}
	if written != len(data) {
		return fmt.Errorf("writing dev authentication file %s: %w", path, io.ErrShortWrite)
	}
	return nil
}

func (a devAuthentication) tokenCommand() string {
	if !a.enabled {
		return ""
	}
	return "flow jwt sign --key " + shellArg(a.keyPath) +
		" --id " + shellArg(devAuthKeyID) +
		" --issuer " + shellArg(a.issuer) +
		" --subject " + shellArg(a.subject) +
		" --audience " + shellArg(a.resource) +
		" --claim namespace=" + shellArg(a.namespace) +
		" > " + shellArg(a.tokenPath())
}

// tokenPath keeps the bearer credential under the same mode-0700 directory as
// the signing material. Shell redirection commonly creates a mode-0644 file;
// the private parent keeps that otherwise-copyable command safe even then.
func (a devAuthentication) tokenPath() string {
	if !a.enabled {
		return ""
	}
	return filepath.Join(filepath.Dir(a.keyPath), "token.jwt")
}
