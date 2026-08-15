package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

// Automatic certificates via ACME's TLS-ALPN-01 challenge
// (golang.org/x/crypto/acme/autocert), the picatz/flowstate#581 slice of
// picatz/flowstate#549. This is the third way the public listener can be
// given a certificate, beside --tls-cert-file/--tls-key-file (cmd/flow/tls.go)
// and --tls-terminated-upstream: an operator names the public DNS hosts this
// process serves, and a certificate is obtained and renewed automatically
// from a public ACME CA (Let's Encrypt by default).
//
// # Why autocert and not certmagic
//
// #581 considered both. certmagic buys DNS-01 (wildcards, no inbound port),
// on-demand issuance, and a storage interface with distributed locking — the
// shared cache a multi-replica deployment would need. This slice does not
// support multiple replicas sharing one cache (see the doc on --tls-acme-cache),
// so none of that is reachable yet, and autocert is one module that drops
// straight into the tls.Config [serverTLSConfig] already builds. The four
// settings below (hosts, cache, directory, email) are shaped so a later
// migration to certmagic would not require renaming any of them.
//
// # Why TLS-ALPN-01 only
//
// autocert also supports HTTP-01, but that needs a second listener on port 80
// (Manager.HTTPHandler) and a redirect path — a third socket in a design that
// just finished arguing about the second one (#569's internal listener).
// TLS-ALPN-01 answers the challenge on the same 443 socket the public
// listener already binds, through the same GetCertificate the tls.Config
// already calls. Nothing here ever registers an HTTP-01 handler or binds
// port 80.
//
// # Why the host allowlist is required
//
// A Manager with a nil HostPolicy will attempt issuance for any name a
// stranger sends in SNI — an unbounded write into the cache directory, and a
// denial-of-service pointed at the ACME provider's rate limits, which is the
// CLAUDE.md rule ("bound the resource the attacker controls") applied to SNI.
// [resolveACMESettings] refuses to build a Manager without a non-empty,
// validated host list; there is no code path in this file that constructs one
// with HostPolicy left nil.

// addACMEFlags declares the public listener's automatic-certificate flags on
// cmd. Unset (--tls-acme-hosts empty and nothing else in the group set) means
// ACME was not requested at all — see [acmeRequested] — which is what lets a
// deployment using --tls-cert-file or --tls-terminated-upstream see no change.
func addACMEFlags(cmd *cobra.Command) {
	cmd.Flags().StringArray("tls-acme-hosts", splitComma(os.Getenv("FLOWSTATE_TLS_ACME_HOSTS")),
		"public DNS host(s) to obtain a certificate for automatically via ACME's "+
			"TLS-ALPN-01 challenge (repeatable, or comma-separated in "+
			"FLOWSTATE_TLS_ACME_HOSTS); required to turn ACME on, and the whole of what a "+
			"certificate may be obtained for — refused empty rather than defaulting to "+
			"issuing for whatever SNI a caller sends. Mutually exclusive with "+
			"--tls-cert-file and --tls-terminated-upstream, and refused together with "+
			"--internal-listen")
	cmd.Flags().String("tls-acme-cache", os.Getenv("FLOWSTATE_TLS_ACME_CACHE"),
		"directory holding the ACME account key and issued certificates, required when "+
			"--tls-acme-hosts is set. An in-memory-only cache re-issues on every restart, "+
			"which burns a CA's rate limit; this must be a real, persistent directory. "+
			"Created with mode 0700 if it does not exist, and refused if it exists but is "+
			"readable or writable by anyone but its owner — it holds private keys")
	cmd.Flags().String("tls-acme-email", os.Getenv("FLOWSTATE_TLS_ACME_EMAIL"),
		"contact email the ACME CA may use to warn about a problem with an issued "+
			"certificate; optional")
	cmd.Flags().String("tls-acme-directory", os.Getenv("FLOWSTATE_TLS_ACME_DIRECTORY"),
		"ACME directory URL to request certificates from; unset means Let's Encrypt's "+
			"production directory. Point this at a staging or private directory (Pebble, "+
			"an enterprise ACME server) for anything other than a real production "+
			"certificate")
	cmd.Flags().Bool("tls-acme-accept-tos",
		os.Getenv("FLOWSTATE_TLS_ACME_ACCEPT_TOS") != "",
		"agree to the ACME CA's subscriber agreement (overrides "+
			"FLOWSTATE_TLS_ACME_ACCEPT_TOS); required to turn ACME on. Not defaulted: "+
			"agreeing to a third party's terms on an operator's behalf is not this "+
			"process's decision to make quietly")
}

// acmeFlags is what an operator asked for, read once before anything binds.
type acmeFlags struct {
	hosts        []string
	cacheDir     string
	email        string
	directoryURL string
	acceptTOS    bool
}

// acmeFlagsOf reads them off the command being run.
func acmeFlagsOf(cmd *cobra.Command) acmeFlags {
	hosts, _ := cmd.Flags().GetStringArray("tls-acme-hosts")
	cacheDir, _ := cmd.Flags().GetString("tls-acme-cache")
	email, _ := cmd.Flags().GetString("tls-acme-email")
	directoryURL, _ := cmd.Flags().GetString("tls-acme-directory")
	acceptTOS, _ := cmd.Flags().GetBool("tls-acme-accept-tos")

	return acmeFlags{
		hosts:        hosts,
		cacheDir:     cacheDir,
		email:        email,
		directoryURL: directoryURL,
		acceptTOS:    acceptTOS,
	}
}

// acmeRequested reports whether an operator asked for ACME at all. Any one of
// the group being set is enough — it is what lets [resolveACMESettings]
// distinguish "ACME not configured" (all zero, return nil, nil) from "ACME
// configured wrong" (something set, hosts empty or TOS not accepted: refuse).
// A bare --tls-acme-accept-tos with nothing else, for instance, must still be
// refused for its missing host list rather than silently ignored.
func acmeRequested(flags acmeFlags) bool {
	return len(flags.hosts) > 0 || flags.cacheDir != "" || flags.email != "" ||
		flags.directoryURL != "" || flags.acceptTOS
}

// acmeSettings is the outcome of validating an operator's ACME configuration:
// a Manager ready to hand a *tls.Config, and the watchdog's inputs.
type acmeSettings struct {
	hosts   []string
	manager *autocert.Manager
}

// resolveACMESettings validates every fail-closed rule #581 decided and, only
// if all of them hold, builds the autocert.Manager. nil, nil means ACME was
// not requested; every other outcome that is not a fully valid configuration
// is an error naming what is wrong, checked before any network I/O.
//
// federationIssuer is the trust policy's federation.issuer (empty if none
// configured); see the cross-check below.
func resolveACMESettings(flags acmeFlags, tlsListener tlsFlags, internalListenAddr string, federationIssuer string) (*acmeSettings, error) {
	if !acmeRequested(flags) {
		return nil, nil
	}

	// ACME together with an explicit certificate file is refused rather than
	// one silently winning: two sources for one certificate is a
	// configuration whose meaning nobody can state, the same reasoning
	// [serverTLSConfig] already applies to being given only one of
	// --tls-cert-file/--tls-key-file.
	if tlsListener.certFile != "" || tlsListener.keyFile != "" {
		return nil, fmt.Errorf("--tls-acme-hosts and --tls-cert-file/--tls-key-file were both " +
			"given; configure at most one certificate source for the public listener")
	}

	// ACME together with --tls-terminated-upstream is refused for the
	// opposite reason a reviewer might expect one to imply the other:
	// --tls-terminated-upstream asserts something *in front of* this process
	// already terminates TLS, in which case this process should serve
	// plaintext and ACME has no certificate to obtain that anything would
	// ever present — the two flags describe incompatible deployments, and
	// which one the operator meant is exactly the question this refusal asks
	// them to answer explicitly.
	if tlsListener.tlsTerminatedUpstream {
		return nil, fmt.Errorf("--tls-acme-hosts and --tls-terminated-upstream were both given; " +
			"these describe incompatible deployments — --tls-terminated-upstream says a proxy " +
			"in front of this process already terminates TLS, in which case this process should " +
			"serve plaintext and has no certificate to obtain here. If a proxy already terminates " +
			"TLS, drop --tls-acme-hosts and keep --tls-terminated-upstream; if this process is " +
			"the one meant to terminate TLS with an automatic certificate, drop " +
			"--tls-terminated-upstream and keep --tls-acme-hosts")
	}

	// The internal listener is loopback or a private address by design
	// (checkInternalListenAddress refuses anything else unconditionally), and
	// a public CA cannot issue a certificate for either — so ACME can never
	// cover it. Refused explicitly rather than left as a combination nothing
	// says anything about, per #581: an operator turning on --internal-listen
	// alongside ACME should not have to infer from the internal listener
	// staying plaintext that ACME was never going to reach it.
	if internalListenAddr != "" {
		return nil, fmt.Errorf("--tls-acme-hosts and --internal-listen were both given; the "+
			"internal listener (%s) is loopback or a private address by design and a public "+
			"CA cannot issue a certificate for it, so ACME can never cover it. Drop "+
			"--internal-listen, or leave it configured and understand it continues to serve "+
			"plaintext with no certificate of any kind, ACME or otherwise", internalListenAddr)
	}

	if len(flags.hosts) == 0 {
		return nil, errors.New("--tls-acme-hosts is empty: an ACME Manager with no host " +
			"allowlist would attempt issuance for whatever SNI a caller sends, which is a " +
			"denial-of-service amplifier pointed at the ACME provider and a way to be " +
			"rate-limited out of certificates entirely. Name every public DNS host this " +
			"process serves")
	}

	seen := make(map[string]struct{}, len(flags.hosts))
	hosts := make([]string, 0, len(flags.hosts))
	for _, host := range flags.hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if err := validateACMEHost(host); err != nil {
			return nil, fmt.Errorf("--tls-acme-hosts: %w", err)
		}
		if _, dup := seen[host]; dup {
			continue
		}
		seen[host] = struct{}{}
		hosts = append(hosts, host)
	}
	if len(hosts) == 0 {
		return nil, errors.New("--tls-acme-hosts contained only empty entries; name at least " +
			"one public DNS host")
	}

	if !flags.acceptTOS {
		return nil, errors.New("--tls-acme-hosts was given without --tls-acme-accept-tos: " +
			"agreeing to the ACME CA's subscriber agreement is an operator decision this " +
			"process will not make quietly on anyone's behalf")
	}

	if flags.cacheDir == "" {
		return nil, errors.New("--tls-acme-hosts was given without --tls-acme-cache: an " +
			"in-memory-only cache re-issues a certificate on every restart, which burns the " +
			"CA's rate limit; name a persistent directory")
	}
	if err := checkACMECacheDir(flags.cacheDir); err != nil {
		return nil, err
	}

	// The cross-check #581 asks for: a trust policy configuring federation
	// has already written this deployment's public name down once, as the
	// issuer URL federation.Broker mints assertions under
	// (pkg/flowstate/v1/auth/federation.go). --tls-acme-hosts is that same
	// name written a second time, by a different flag. Disagreement here is
	// CLAUDE.md's "one value, written down twice" defect, and left
	// unchecked it fails somewhere remote and confusing: a relying party
	// fetches the JWKS from the issuer URL, gets a certificate for a
	// different name, and sees a TLS error about a service that looks
	// healthy from every angle this process can see.
	if federationIssuer != "" {
		issuerURL, err := url.Parse(federationIssuer)
		if err != nil {
			return nil, fmt.Errorf("federation.issuer %q could not be parsed to cross-check "+
				"against --tls-acme-hosts: %w", federationIssuer, err)
		}
		issuerHost := issuerURL.Hostname()
		if _, ok := seen[issuerHost]; issuerHost != "" && !ok {
			return nil, fmt.Errorf("the trust policy's federation.issuer is %q, whose host "+
				"%q does not appear in --tls-acme-hosts (%s); a relying party fetching this "+
				"deployment's JWKS from the issuer URL would get a certificate for a "+
				"different name. Add %q to --tls-acme-hosts, or correct whichever of the "+
				"two names this deployment's public name",
				federationIssuer, issuerHost, strings.Join(hosts, ", "), issuerHost)
		}
	}

	manager := &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Cache:      autocert.DirCache(flags.cacheDir),
		HostPolicy: autocert.HostWhitelist(hosts...),
		Email:      flags.email,
	}
	if flags.directoryURL != "" {
		client := acmeClientFor(flags.directoryURL)
		manager.Client = &client
	}

	return &acmeSettings{hosts: hosts, manager: manager}, nil
}

// acmeClientFor builds the low-level ACME client [resolveACMESettings] hands
// the Manager when --tls-acme-directory points somewhere other than Let's
// Encrypt's production directory (a staging directory, Pebble, an enterprise
// ACME server). Split out only so its return value can be taken by address
// above without a temporary variable at the call site.
func acmeClientFor(directoryURL string) acme.Client {
	return acme.Client{DirectoryURL: directoryURL}
}

// tlsConfig returns the tls.Config the public listener serves from: autocert's
// own TLSConfig(), which sets GetCertificate and the "acme-tls/1" ALPN
// protocol TLS-ALPN-01 needs. No HTTP-01 fallback is ever configured — see
// this file's package comment — so this is the whole of how a certificate
// reaches a connection.
func (s *acmeSettings) tlsConfig() *tls.Config {
	return s.manager.TLSConfig()
}

// acmeHostNamePattern is deliberately conservative: RFC 1035 labels
// separated by dots, no wildcard, no trailing dot. A public CA cannot issue
// for an IP address or a wildcard under this slice's TLS-ALPN-01-only
// challenge, and both failures are more useful reported now, with the value
// named, than minutes later as an opaque ACME error — the same "report what
// is a property of the file" rule CLAUDE.md gives validators generally,
// applied to a flag instead of a Flowfile.
var acmeHostNamePattern = regexp.MustCompile(`^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,63}$`)

// validateACMEHost refuses a host --tls-acme-hosts names that a public CA
// could never issue a certificate for under TLS-ALPN-01.
func validateACMEHost(host string) error {
	if ip := net.ParseIP(host); ip != nil {
		return fmt.Errorf("%q is an IP address; a public CA cannot issue a certificate for "+
			"one, only for a DNS name", host)
	}
	if strings.Contains(host, "*") {
		return fmt.Errorf("%q is a wildcard; this slice supports TLS-ALPN-01 only, which "+
			"cannot prove ownership of a wildcard name (that needs DNS-01, which "+
			"--tls-acme-hosts does not offer)", host)
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") ||
		strings.HasSuffix(strings.ToLower(host), ".local") || strings.HasSuffix(strings.ToLower(host), ".internal") {
		return fmt.Errorf("%q is not a publicly resolvable name; a public CA has to reach it "+
			"over the internet to validate a TLS-ALPN-01 challenge, and a name reserved for "+
			"local or internal use fails at DNS before it fails at anything interesting", host)
	}
	if !acmeHostNamePattern.MatchString(host) {
		return fmt.Errorf("%q does not look like a DNS name a public CA could issue a "+
			"certificate for", host)
	}
	return nil
}

// checkACMECacheDir refuses to hand autocert a cache directory this process
// cannot keep private. The directory holds an ACME account key and issued
// certificates' private keys, so DirCache's own "will happily use /tmp"
// behavior is exactly what this refuses.
//
// Created with 0700 if it does not exist yet, matching what DirCache.Put
// would create it with on first write — checked here, rather than left to
// DirCache, so a permission problem is a start-up failure instead of the
// first certificate write failing minutes into serving.
func checkACMECacheDir(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if mkErr := os.MkdirAll(path, 0o700); mkErr != nil {
			return fmt.Errorf("creating --tls-acme-cache directory %s: %w", path, mkErr)
		}
		info, err = os.Stat(path)
	}
	if err != nil {
		return fmt.Errorf("checking --tls-acme-cache directory %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("--tls-acme-cache %s is not a directory", path)
	}

	return checkACMECacheDirSecurity(path, info)
}

// acmeWatchdogInterval is how often [acmeExpiryWatchdog] re-checks every
// configured host's cached certificate.
const acmeWatchdogInterval = 1 * time.Hour

// acmeRenewalOverdueWindow is how close to actual expiry a cached certificate
// has to be before the watchdog treats a still-unrenewed certificate as
// worth an error log rather than routine background activity. autocert's own
// renewal timer fires around 30 days (or a third of the certificate's
// lifetime, whichever is less) before expiry — comfortably outside this
// window in the ordinary case — so a certificate still inside this window
// means either the far side of that timer is failing repeatedly or has not
// run yet on a very short-lived certificate; both are worth an operator's
// attention well before the certificate is actually unusable.
const acmeRenewalOverdueWindow = 72 * time.Hour

// acmeCertificateExpiry reports the NotAfter of the certificate currently
// cached for host, so [acmeExpiryWatchdog] can be driven by a fake in tests
// without a live ACME server or a real autocert.Manager.
type acmeCertificateExpiry func(ctx context.Context, host string) (time.Time, error)

// cacheCertExpiry is the production [acmeCertificateExpiry]: it reads the PEM
// autocert's own Cache.Get returns and parses the leaf certificate's
// NotAfter, without depending on any of autocert's unexported renewal state
// — which is the whole reason this watchdog exists as a separate mechanism
// rather than a hook into the library: autocert exposes no public signal for
// "a background renewal attempt just failed", so this reasons instead from
// the one fact it does expose, what is actually cached right now.
func cacheCertExpiry(cache autocert.Cache) acmeCertificateExpiry {
	return func(ctx context.Context, host string) (time.Time, error) {
		data, err := cache.Get(ctx, host)
		if err != nil {
			return time.Time{}, err
		}
		for {
			var block *pem.Block
			block, data = pem.Decode(data)
			if block == nil {
				return time.Time{}, fmt.Errorf("no CERTIFICATE block found in the cached entry for %s", host)
			}
			if block.Type != "CERTIFICATE" {
				continue
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return time.Time{}, fmt.Errorf("parsing the cached certificate for %s: %w", host, err)
			}
			return cert.NotAfter, nil
		}
	}
}

// acmeExpiryWatchdog is #581's renewal-failure decision, made concrete:
//
//   - A background renewal failure alone is not fatal. Failing closed there
//     would take down a working control plane because a CA had a bad hour,
//     converting a supplier's outage into ours — the opposite of what
//     CLAUDE.md's "fail closed" is for. So a certificate that is still valid
//     keeps being served no matter how many renewal attempts have failed;
//     this function never touches the listener's tls.Config.
//   - It must not be invisible, though: a renewal that keeps failing until
//     the certificate expires is a real defect, and finding out at expiry
//     is the actual failure CLAUDE.md's "fail closed" warns about ("a bound
//     nothing reaches is a bound nothing tests"). So once a cached
//     certificate is within [acmeRenewalOverdueWindow] of its NotAfter, this
//     logs at error, by design well before expiry, so an operator has days
//     of runway rather than none.
//   - Expiry is where it stops being a background condition. A certificate
//     that has actually expired is one honest clients now reject, so this
//     process is no longer serving what was configured; the returned
//     channel receives exactly one error at that point, naming the host and
//     the expiry time, for the caller to treat as fatal — refusing to keep
//     running on a certificate that lies about identity, which is the
//     mirror image of refusing to start on one that never loaded.
//
// The trade CLAUDE.md's "fail closed" makes explicit here: refuse at actual
// expiry, not at the first failed renewal attempt, because only one of those
// two moments has nothing left to degrade to.
func acmeExpiryWatchdog(ctx context.Context, logger *slog.Logger, expiryOf acmeCertificateExpiry, hosts []string) <-chan error {
	fatal := make(chan error, 1)

	check := func() bool {
		for _, host := range hosts {
			notAfter, err := expiryOf(ctx, host)
			if err != nil {
				// No cached certificate yet is expected before the first
				// successful acquisition; primeACMECertificates makes that a
				// start-up failure rather than something this loop discovers
				// later, so past start-up this is worth a log but not a
				// crash on its own — the watchdog's fatal case is reserved
				// for a certificate this process once had and let expire.
				logger.Error("acme: could not read the cached certificate for a configured host",
					"host", host, "error", err)
				continue
			}

			remaining := time.Until(notAfter)
			switch {
			case remaining <= 0:
				err := fmt.Errorf("acme: the certificate for %s expired at %s with no "+
					"successful renewal; refusing to keep serving an identity clients "+
					"already reject", host, notAfter.Format(time.RFC3339))
				logger.Error(err.Error(), "host", host, "expired_at", notAfter)
				select {
				case fatal <- err:
				default:
				}
				return true
			case remaining <= acmeRenewalOverdueWindow:
				logger.Error("acme: certificate renewal has not completed and expiry is "+
					"approaching; still serving the valid certificate on hand",
					"host", host, "expires_at", notAfter, "remaining", remaining.Round(time.Minute))
			}
		}
		return false
	}

	go func() {
		if check() {
			return
		}
		ticker := time.NewTicker(acmeWatchdogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if check() {
					return
				}
			}
		}
	}()

	return fatal
}
