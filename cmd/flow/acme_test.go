package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/acme/autocert"
)

// validACMEFlags returns a configuration [resolveACMESettings] accepts, so
// each negative test below can start from something valid and change exactly
// the one thing it means to test.
func validACMEFlags(t *testing.T) acmeFlags {
	t.Helper()

	// t.TempDir() is created with the process umask, not 0700, so it is
	// chmod'd here to what an operator is expected to leave --tls-acme-cache
	// at — otherwise every test using this helper would trip
	// [checkACMECacheDir]'s own refusal rather than exercise the behavior
	// each test actually means to.
	dir := t.TempDir()
	if runtime.GOOS != "windows" {
		require.NoError(t, os.Chmod(dir, 0o700))
	}

	return acmeFlags{
		hosts:     []string{"flowstate.example.com"},
		cacheDir:  dir,
		acceptTOS: true,
	}
}

func TestResolveACMESettingsNotRequestedIsNoop(t *testing.T) {
	t.Parallel()

	settings, err := resolveACMESettings(acmeFlags{}, tlsFlags{}, "", "")
	require.NoError(t, err)
	require.Nil(t, settings, "no ACME flags set must mean ACME was not requested at all")
}

func TestResolveACMESettingsEmptyHostAllowlistIsRefused(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	flags.hosts = nil

	_, err := resolveACMESettings(flags, tlsFlags{}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-acme-hosts is empty")
}

// TestResolveACMESettingsBareAcceptTOSWithoutHostsIsRefused is
// [acmeRequested]'s edge case: a lone --tls-acme-accept-tos, with no
// --tls-acme-hosts, must not be silently ignored just because the "main"
// flag is unset.
func TestResolveACMESettingsBareAcceptTOSWithoutHostsIsRefused(t *testing.T) {
	t.Parallel()

	_, err := resolveACMESettings(acmeFlags{acceptTOS: true}, tlsFlags{}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-acme-hosts is empty")
}

func TestResolveACMESettingsWithoutAcceptTOSIsRefused(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	flags.acceptTOS = false

	_, err := resolveACMESettings(flags, tlsFlags{}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-acme-accept-tos")
}

func TestResolveACMESettingsWithoutCacheDirIsRefused(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	flags.cacheDir = ""

	_, err := resolveACMESettings(flags, tlsFlags{}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-acme-cache")
}

func TestResolveACMESettingsTogetherWithCertFileNamesBoth(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	_, err := resolveACMESettings(flags, tlsFlags{certFile: "cert.pem", keyFile: "key.pem"}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-acme-hosts")
	require.Contains(t, err.Error(), "--tls-cert-file")
}

func TestResolveACMESettingsTogetherWithTLSTerminatedUpstreamNamesBoth(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	_, err := resolveACMESettings(flags, tlsFlags{tlsTerminatedUpstream: true}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-acme-hosts")
	require.Contains(t, err.Error(), "--tls-terminated-upstream")
}

func TestResolveACMESettingsOnInternalListenerIsRefused(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	_, err := resolveACMESettings(flags, tlsFlags{}, "127.0.0.1:9090", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-acme-hosts")
	require.Contains(t, err.Error(), "--internal-listen")
}

func TestResolveACMESettingsRejectsAnIPHost(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	flags.hosts = []string{"203.0.113.5"}

	_, err := resolveACMESettings(flags, tlsFlags{}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "IP address")
}

func TestResolveACMESettingsRejectsAWildcardHost(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	flags.hosts = []string{"*.example.com"}

	_, err := resolveACMESettings(flags, tlsFlags{}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "wildcard")
}

func TestResolveACMESettingsRejectsALocalName(t *testing.T) {
	t.Parallel()

	for _, host := range []string{"localhost", "flowstate.local", "flowstate.internal"} {
		flags := validACMEFlags(t)
		flags.hosts = []string{host}

		_, err := resolveACMESettings(flags, tlsFlags{}, "", "")
		require.Errorf(t, err, "host %q should have been refused as not publicly resolvable", host)
	}
}

func TestResolveACMESettingsAcceptsAValidConfiguration(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	settings, err := resolveACMESettings(flags, tlsFlags{minVersion: "1.3"}, "", "")
	require.NoError(t, err)
	require.NotNil(t, settings)
	require.Equal(t, []string{"flowstate.example.com"}, settings.hosts)
	require.NotNil(t, settings.manager)
	require.NotNil(t, settings.manager.HostPolicy, "a Manager with no HostPolicy issues for any SNI")

	// HostWhitelist admits the configured host and refuses everything else —
	// this is what turns the allowlist into an actual bound, not just a
	// field that is populated.
	require.NoError(t, settings.manager.HostPolicy(context.Background(), "flowstate.example.com"))
	require.Error(t, settings.manager.HostPolicy(context.Background(), "attacker.example.com"))

	cfg := settings.tlsConfig()
	require.NotNil(t, cfg.GetCertificate)
	require.Contains(t, cfg.NextProtos, "acme-tls/1", "TLS-ALPN-01 needs this ALPN protocol advertised")
	require.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion,
		"an ACME listener must preserve the configured TLS protocol floor")
}

func TestResolveACMESettingsRejectsInvalidTLSMinVersion(t *testing.T) {
	t.Parallel()

	_, err := resolveACMESettings(validACMEFlags(t), tlsFlags{minVersion: "1.1"}, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-min-version")
}

// TestResolveACMESettingsFederationCrossCheck is the join #581 asks for: a
// trust policy's federation.issuer and --tls-acme-hosts are two different
// flags naming what has to be the same public name, and disagreement between
// them must be caught here rather than surfacing later as a relying party's
// confusing TLS error.
func TestResolveACMESettingsFederationCrossCheck(t *testing.T) {
	t.Parallel()

	t.Run("issuer host present in the allowlist is accepted", func(t *testing.T) {
		t.Parallel()
		flags := validACMEFlags(t)
		settings, err := resolveACMESettings(flags, tlsFlags{}, "", "https://flowstate.example.com")
		require.NoError(t, err)
		require.NotNil(t, settings)
	})

	t.Run("issuer host absent from the allowlist names both", func(t *testing.T) {
		t.Parallel()
		flags := validACMEFlags(t)
		_, err := resolveACMESettings(flags, tlsFlags{}, "", "https://issuer.example.org")
		require.Error(t, err)
		require.Contains(t, err.Error(), "flowstate.example.com")
		require.Contains(t, err.Error(), "issuer.example.org")
	})

	t.Run("no federation configured skips the cross-check", func(t *testing.T) {
		t.Parallel()
		flags := validACMEFlags(t)
		settings, err := resolveACMESettings(flags, tlsFlags{}, "", "")
		require.NoError(t, err)
		require.NotNil(t, settings)
	})
}

func TestCheckACMECacheDirCreatesAMissingDirectory(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "acme-cache")
	require.NoError(t, checkACMECacheDir(dir))

	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	}
}

func TestCheckACMECacheDirRefusesAFile(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	err := checkACMECacheDir(file)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a directory")
}

func TestCheckACMECacheDirRefusesGroupOrWorldAccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits do not apply on windows")
	}
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o750))

	err := checkACMECacheDir(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "0700")
}

// TestServerBaseURLInfersHTTPSForAnACMEServedAddress is CLAUDE.md's "test the
// join": [TestServerBaseURLAgreesWithRefusePlaintextListener] already pins
// serverBaseURL and refusePlaintextListener to the same address, purely in
// terms of "is there a certificate". This proves ACME's contribution reaches
// that same join rather than opening a new gap: a *tls.Config built from an
// autocert.Manager is a certificate exactly as far as refusePlaintextListener
// is concerned (it never inspects where the certificate came from), and the
// address ACME configures is always non-loopback (a public DNS host, refused
// otherwise by [validateACMEHost]), so serverBaseURL must default to https
// for it — the same guarantee an explicit --tls-cert-file gets, reached
// through a different call path.
func TestServerBaseURLInfersHTTPSForAnACMEServedAddress(t *testing.T) {
	t.Parallel()

	flags := validACMEFlags(t)
	settings, err := resolveACMESettings(flags, tlsFlags{}, "", "")
	require.NoError(t, err)

	addr := "flowstate.example.com:443"
	tlsCfg := settings.tlsConfig()

	require.NoError(t, refusePlaintextListener(addr, tlsCfg, false),
		"an ACME-built tls.Config must be treated as a certificate, not as plaintext")

	base := serverBaseURL(addr)
	require.True(t, strings.HasPrefix(base, "https://"),
		"serverBaseURL(%q) = %q, but this is exactly the address ACME is configured to serve "+
			"a certificate for", addr, base)
}

// fakeACMECache is an in-memory autocert.Cache, so tests can drive
// [cacheCertExpiry] without touching a filesystem or a real ACME server.
type fakeACMECache map[string][]byte

func (c fakeACMECache) Get(_ context.Context, name string) ([]byte, error) {
	data, ok := c[name]
	if !ok {
		return nil, autocert.ErrCacheMiss
	}
	return data, nil
}

func (c fakeACMECache) Put(_ context.Context, name string, data []byte) error {
	c[name] = data
	return nil
}

func (c fakeACMECache) Delete(_ context.Context, name string) error {
	delete(c, name)
	return nil
}

// pemCertWithExpiry builds the PEM autocert's own DirCache stores: a private
// key block followed by a certificate block, in the shape [cacheCertExpiry]
// parses.
func pemCertWithExpiry(t *testing.T, notAfter time.Time) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "flowstate-test"},
		NotBefore:    notAfter.Add(-24 * time.Hour),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	var out []byte
	out = append(out, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)
	out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	return out
}

func TestCacheCertExpiryReadsTheCachedLeaf(t *testing.T) {
	t.Parallel()

	want := time.Now().Add(72 * time.Hour).Truncate(time.Second)
	cache := fakeACMECache{"flowstate.example.com": pemCertWithExpiry(t, want)}

	got, err := cacheCertExpiry(cache)(context.Background(), "flowstate.example.com")
	require.NoError(t, err)
	require.WithinDuration(t, want, got, time.Second)
}

func TestCacheCertExpiryPropagatesACacheMiss(t *testing.T) {
	t.Parallel()

	_, err := cacheCertExpiry(fakeACMECache{})(context.Background(), "flowstate.example.com")
	require.Error(t, err)
	require.True(t, errors.Is(err, autocert.ErrCacheMiss))
}

// TestAcmeExpiryWatchdogRenewalFailureDecision is #581's hardest question,
// asserted directly rather than only argued in prose: a certificate that is
// still valid, however close to expiry, never reaches the fatal channel —
// only actual expiry does.
func TestAcmeExpiryWatchdogRenewalFailureDecision(t *testing.T) {
	t.Parallel()

	t.Run("a healthy certificate produces no fatal signal", func(t *testing.T) {
		t.Parallel()
		notAfter := time.Now().Add(60 * 24 * time.Hour)
		expiryOf := func(context.Context, string) (time.Time, error) { return notAfter, nil }

		fatal := acmeExpiryWatchdog(context.Background(), discardLogger(), expiryOf, []string{"flowstate.example.com"})
		select {
		case err := <-fatal:
			t.Fatalf("a certificate valid for 60 more days must not be fatal, got: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("a certificate inside the overdue window is not fatal", func(t *testing.T) {
		t.Parallel()
		notAfter := time.Now().Add(acmeRenewalOverdueWindow - time.Hour)
		expiryOf := func(context.Context, string) (time.Time, error) { return notAfter, nil }

		fatal := acmeExpiryWatchdog(context.Background(), discardLogger(), expiryOf, []string{"flowstate.example.com"})
		select {
		case err := <-fatal:
			t.Fatalf("a certificate that has not actually expired must not be fatal, got: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("an expired certificate is fatal, naming the host and expiry", func(t *testing.T) {
		t.Parallel()
		notAfter := time.Now().Add(-time.Hour)
		expiryOf := func(context.Context, string) (time.Time, error) { return notAfter, nil }

		fatal := acmeExpiryWatchdog(context.Background(), discardLogger(), expiryOf, []string{"flowstate.example.com"})
		select {
		case err := <-fatal:
			require.Error(t, err)
			require.Contains(t, err.Error(), "flowstate.example.com")
			require.Contains(t, err.Error(), "expired")
		case <-time.After(2 * time.Second):
			t.Fatal("an expired certificate with no renewal must reach the fatal channel")
		}
	})

	t.Run("a cache miss after start-up is not fatal on its own", func(t *testing.T) {
		t.Parallel()
		expiryOf := func(context.Context, string) (time.Time, error) {
			return time.Time{}, autocert.ErrCacheMiss
		}

		fatal := acmeExpiryWatchdog(context.Background(), discardLogger(), expiryOf, []string{"flowstate.example.com"})
		select {
		case err := <-fatal:
			t.Fatalf("a cache-read error alone must not be fatal, got: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	})
}
