package envelope

import (
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
	kpvault "github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/vault"
	secretsvault "github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// vaultConnection is one configured Vault or OpenBao server's Transit engine,
// shared by every key that names it, so they share one token and its renewal.
type vaultConnection struct {
	transit *secretsvault.Transit
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
		if d := vc.GetTimeout().AsDuration(); vc.GetTimeout() != nil && d > 0 {
			vopts = append(vopts, secretsvault.WithTimeout(d))
		}

		transit, err := secretsvault.NewTransit(vc.GetAddress(), vc.GetMount(), vopts...)
		if err != nil {
			return nil, fmt.Errorf("envelope: provider %q: %w", p.GetName(), err)
		}
		out[p.GetName()] = vaultConnection{transit: transit}
	}
	return out, nil
}
