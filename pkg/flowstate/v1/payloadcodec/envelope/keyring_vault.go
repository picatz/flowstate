package envelope

import (
	"fmt"
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
			vopts = append(vopts, secretsvault.WithRootCAsFile(keyLoader{opts: opts}.resolve(vc.GetCaFile())))
		}
		var timeout time.Duration
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
// can make, each within the per-call provider timeout. A key held by a
// provider is described (one call) and has its encryption-context binding
// probed (two), each provider may log in once, and each writing namespace
// wraps its first data key to its current key and every escrow key. Never
// less than [MinStartupBudget].
func StartupBudget(cfg *v1.PayloadKeyring) time.Duration {
	var longest time.Duration
	for _, p := range cfg.GetProviders() {
		longest = max(longest, p.GetVault().GetTimeout().AsDuration())
	}
	return startupBudget(cfg, max(DefaultProviderTimeout, 2*longest))
}

func startupBudget(cfg *v1.PayloadKeyring, perCall time.Duration) time.Duration {
	calls := len(cfg.GetProviders())
	remote := func(k *v1.PayloadKey) bool { return k.GetVault() != nil }
	for _, k := range cfg.GetEscrowKeys() {
		if remote(k) {
			calls += 3
		}
	}
	for _, n := range cfg.GetNamespaces() {
		for _, k := range n.GetKeys() {
			if remote(k) {
				calls += 3
			}
		}
		if n.GetCurrent() != "" {
			calls += 1 + len(n.GetEscrow())
		}
	}
	return max(MinStartupBudget, time.Duration(calls)*perCall)
}

// providerTimeout is the deadline a keyring's codecs put on one wrap or
// unwrap: never shorter than a configured provider allows its own calls. A
// configured timeout bounds one request, and a wrap may first have to log in,
// so a call is given two of them; the default stays the floor, since a shorter
// provider timeout already ends its own request first.
func providerTimeout(providers map[string]vaultConnection) time.Duration {
	d := DefaultProviderTimeout
	for _, c := range providers {
		d = max(d, 2*c.timeout)
	}
	return d
}
