package envelope

import (
	"crypto/x509"
	"fmt"
	"io/fs"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
	kpvault "github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/vault"
	secretsvault "github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// vaultConnection is one configured Vault or OpenBao server's Transit engine,
// shared by every key that names it, so they share one token and its renewal.
type vaultConnection struct {
	transit *secretsvault.Transit

	// timeout is the configured bound on one request to the server, or zero
	// for the client's default.
	timeout time.Duration
}

func (c vaultConnection) key(name string) keyprovider.Key { return kpvault.New(c.transit, name) }

// MaxCAFileBytes bounds a provider's CA bundle, which is read at startup:
// far more than any real bundle, and a bound on what a replaced file costs.
const MaxCAFileBytes = 1 << 20

// loadCAFile reads a provider's CA bundle as the keyring reads every other
// file that decides where keys go (see [checkPublicFileMode]): bounded, and
// refused if another account could have written it, since a substituted CA
// lets an impersonated server receive the Vault token and every wrap.
func loadCAFile(path string) (*x509.CertPool, error) {
	pem, err := readBounded(path, MaxCAFileBytes, func(info fs.FileInfo) error { return checkPublicFileMode(path, info) })
	if err != nil {
		return nil, fmt.Errorf("reading CA bundle %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA bundle %q holds no PEM certificate", path)
	}
	return pool, nil
}

// openProviders connects every configured key service. Nothing is contacted
// here: each key's Describe, which Open calls, is the first request, and the
// one that fails startup if the server cannot be reached or refuses.
func openProviders(cfgs []*v1.PayloadKeyProvider, opts OpenOptions) (map[string]vaultConnection, error) {
	out := make(map[string]vaultConnection, len(cfgs))
	for _, p := range cfgs {
		vc := p.GetVault()
		if vc == nil {
			return nil, fmt.Errorf("envelope: provider %q has no connection", p.GetName())
		}
		var vopts []secretsvault.Option
		switch {
		case vc.GetTokenEnv() != "":
			token := opts.Getenv(vc.GetTokenEnv())
			if token == "" {
				return nil, fmt.Errorf("envelope: provider %q: environment variable %s is unset or empty",
					p.GetName(), vc.GetTokenEnv())
			}
			vopts = append(vopts, secretsvault.WithToken(token))
		case vc.GetKubernetes() != nil:
			k := vc.GetKubernetes()
			vopts = append(vopts, secretsvault.WithKubernetesAuth(k.GetRole()))
			if k.GetMount() != "" {
				vopts = append(vopts, secretsvault.WithKubernetesAuthMount(k.GetMount()))
			}
			if k.GetTokenFile() != "" {
				vopts = append(vopts, secretsvault.WithKubernetesJWTPath(keyLoader{opts: opts}.resolve(k.GetTokenFile())))
			}
		}
		if vc.GetVaultNamespace() != "" {
			vopts = append(vopts, secretsvault.WithVaultNamespace(vc.GetVaultNamespace()))
		}
		if vc.GetCaFile() != "" {
			pool, err := loadCAFile(keyLoader{opts: opts}.resolve(vc.GetCaFile()))
			if err != nil {
				return nil, fmt.Errorf("envelope: provider %q: %w", p.GetName(), err)
			}
			vopts = append(vopts, secretsvault.WithRootCAs(pool))
		}
		// The client's own default when none is configured: that is what a
		// request is allowed, and what the deadlines here must allow it.
		timeout := secretsvault.DefaultTimeout
		if d := vc.GetTimeout().AsDuration(); vc.GetTimeout() != nil && d > 0 {
			timeout = d
			vopts = append(vopts, secretsvault.WithTimeout(d))
		}

		transit, err := secretsvault.NewTransit(vc.GetAddress(), vc.GetMount(), vopts...)
		if err != nil {
			return nil, fmt.Errorf("envelope: provider %q: %w", p.GetName(), err)
		}
		out[p.GetName()] = vaultConnection{transit: transit, timeout: timeout}
	}
	return out, nil
}

// MinStartupBudget is the least time [Open] allows for its startup calls.
const MinStartupBudget = 30 * time.Second

// StartupBudget is how long [Open] allows cfg's startup calls: every one it
// can make, each within the per-call provider timeout. Each namespace
// describes every key it names, its own and its escrow keys, and describing a
// key a provider holds is four calls (reading the key, and an encryption and
// two decryptions probing that it binds the encryption context); each
// provider may log in once; and each writing namespace wraps its first data
// key to its current key and every escrow key. Never less than
// [MinStartupBudget].
func StartupBudget(cfg *v1.PayloadKeyring) time.Duration {
	var longest time.Duration
	for _, p := range cfg.GetProviders() {
		if vc := p.GetVault(); vc != nil {
			longest = max(longest, vaultRequestTimeout(vc))
		}
	}
	return startupBudget(cfg, max(DefaultProviderTimeout, requestsPerCall*longest))
}

// vaultRequestTimeout is how long one request to a Vault provider may take:
// its configured timeout, or the client's default when it names none.
func vaultRequestTimeout(vc *v1.PayloadVaultProvider) time.Duration {
	if d := vc.GetTimeout().AsDuration(); vc.GetTimeout() != nil && d > 0 {
		return d
	}
	return secretsvault.DefaultTimeout
}

func startupBudget(cfg *v1.PayloadKeyring, perCall time.Duration) time.Duration {
	const describeCalls = 4
	calls := len(cfg.GetProviders())
	remoteEscrow := map[string]bool{}
	for _, k := range cfg.GetEscrowKeys() {
		remoteEscrow[k.GetId()] = k.GetVault() != nil
	}
	for _, n := range cfg.GetNamespaces() {
		for _, k := range n.GetKeys() {
			if k.GetVault() != nil {
				calls += describeCalls
			}
		}
		for _, id := range n.GetEscrow() {
			if remoteEscrow[id] {
				calls += describeCalls
			}
		}
		if n.GetCurrent() != "" {
			calls += 1 + len(n.GetEscrow())
		}
	}
	return max(MinStartupBudget, time.Duration(calls)*perCall)
}

// requestsPerCall is how many requests to Vault one wrap, unwrap or startup
// call can make, each within the provider's request timeout: a login when no
// token is cached, the operation, and, when that is refused with 403 and the
// token can be renewed, one more login and the operation again
// (secrets/vault Provider.send).
const requestsPerCall = 4

// providerTimeout is the deadline a keyring's codecs put on one wrap or
// unwrap: never shorter than a configured provider allows its own calls, so
// [requestsPerCall] of its requests. The default stays the floor, since a
// shorter provider timeout already ends its own requests first.
func providerTimeout(providers map[string]vaultConnection) time.Duration {
	d := DefaultProviderTimeout
	for _, c := range providers {
		d = max(d, requestsPerCall*c.timeout)
	}
	return d
}
