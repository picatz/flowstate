package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// identitySignerTenantPlaceholder stands for a tenant's namespace in an
// --identity-signer key name, so one value names every tenant's Transit key:
//
//	vault-transit://vault.example.com:8200/flowstate-{tenant}
//
// A worker expands it with its own --tenant and the server once per tenant the
// trust policy lists, so the two units share one line and cannot disagree about
// which key is whose.
const identitySignerTenantPlaceholder = "{tenant}"

// maxIdentityKeysPerTenant bounds how many public keys one tenant's directory
// may publish: the current key and the few a rotation overlaps. The files are an
// operator's, but a directory is the one place a runaway count is easy to make.
const maxIdentityKeysPerTenant = 16

// addIdentityKeyDirFlag declares --identity-key-dir on the server, the one
// command that publishes the keys of more than one tenant.
func addIdentityKeyDirFlag(cmd *cobra.Command) {
	cmd.Flags().String("identity-key-dir", os.Getenv("FLOWSTATE_IDENTITY_KEY_DIR"),
		"directory of per-tenant public keys, DIR/TENANT/KEY.pem, each a PKIX public key PEM "+
			"(`flow keys public --in KEY.pem --pem`) of a key that tenant's worker signs with, for every "+
			"tenant the auth policy lists under `federation.tenants`. The server publishes each tenant's keys "+
			"at its own issuer, <issuer>/tenants/TENANT, and holds no private key. Fails to start if a listed "+
			"tenant has none, or the directory names a tenant the policy does not list. "+
			"The default tenant's keys are `--identity-key`")
}

// expandSignerTenant fills --identity-signer's {tenant} placeholder with a
// tenant. A URL without the placeholder names one key outright and is returned
// as it is: a worker given a key of its own by name is the simplest deployment,
// and the server refuses to publish one key for two tenants
// ([auth.FederationPolicy.PublishOnlyIssuers]), so a key shared by mistake is
// still found at start-up. The placeholder with no tenant to fill it is refused,
// because the default tenant has no name to put in a key's.
func expandSignerTenant(raw, tenant string) (string, error) {
	if !strings.Contains(raw, identitySignerTenantPlaceholder) {
		return raw, nil
	}

	if tenant == "" {
		return "", fmt.Errorf("--identity-signer names a per-tenant key with %s, and this serves the default tenant, which has no name to put in it: "+
			"pass --tenant, or give the default tenant a key of its own",
			identitySignerTenantPlaceholder)
	}

	// The tenant is held to the namespace grammar (lowercase letters, digits and
	// dashes) by the policy that lists it, so the substitution adds nothing a
	// URL path would treat as structure.
	if err := auth.ValidateNamespace(tenant); err != nil {
		return "", fmt.Errorf("--identity-signer: %w", err)
	}

	return strings.ReplaceAll(raw, identitySignerTenantPlaceholder, tenant), nil
}

// tenantPublicKeys reads the public keys of every tenant the trust policy lists,
// for the server that publishes them, from the one source the flags name: a key
// directory or a per-tenant Transit key template.
//
// The roster comes from the policy and nothing else decides it. A tenant listed
// with no keys, and keys for a tenant not listed, both refuse start-up: one is an
// issuer advertised with nothing to verify against, the other a key published
// for a tenant nobody agreed to.
func tenantPublicKeys(flags authFlags, policy *auth.Policy) (map[string][]auth.FederationOption, error) {
	roster := policy.Federation.Tenants
	templated := strings.Contains(flags.identitySigner, identitySignerTenantPlaceholder)

	switch {
	case flags.identityKeyDir != "" && templated:
		return nil, fmt.Errorf("configure one source of per-tenant keys, not both --identity-key-dir and a %s --identity-signer",
			identitySignerTenantPlaceholder)
	case len(roster) == 0 && (flags.identityKeyDir != "" || templated):
		return nil, fmt.Errorf("--identity-key-dir or a %s --identity-signer was given but the trust policy lists no federation.tenants: "+
			"list the tenants, or drop the per-tenant key source", identitySignerTenantPlaceholder)
	case len(roster) == 0:
		return map[string][]auth.FederationOption{}, nil
	case flags.identityKeyDir == "" && !templated:
		return nil, fmt.Errorf("the trust policy lists federation.tenants (%s) but no per-tenant key source was given: "+
			"pass --identity-key-dir with a directory of DIR/TENANT/KEY.pem public keys, or --identity-signer with a "+
			"vault-transit:// URL naming each tenant's Transit key with %s, since the server publishes each tenant's keys",
			strings.Join(roster, ", "), identitySignerTenantPlaceholder)
	}

	keys := make(map[string][]auth.FederationOption, len(roster))

	if templated {
		for _, tenant := range roster {
			raw, err := expandSignerTenant(flags.identitySigner, tenant)
			if err != nil {
				return nil, err
			}
			opts, err := identitySignerPublicKeys(raw, policy)
			if err != nil {
				return nil, fmt.Errorf("tenant %q: %w", tenant, err)
			}
			keys[tenant] = opts
		}

		return keys, nil
	}

	return tenantKeyDirectory(flags.identityKeyDir, roster)
}

// tenantKeyDirectory reads DIR/TENANT/KEY.pem for every listed tenant. Each key is
// a PKIX public key, read by the same code as the default tenant's
// --identity-key, so a private key is refused here for the same reason and the
// key id is the file name for the same reason.
func tenantKeyDirectory(dir string, roster []string) (map[string][]auth.FederationOption, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading --identity-key-dir: %w", err)
	}

	// A directory for a tenant the policy does not list is a tenant somebody
	// meant to add and did not: refused, so the omission is found at start-up
	// and not by the tenant whose assertions nobody verifies. The directory's
	// names are only compared with the roster, never joined into a path.
	for _, entry := range entries {
		if entry.IsDir() && !slices.Contains(roster, entry.Name()) {
			return nil, fmt.Errorf("--identity-key-dir holds a directory for tenant %q, which the trust policy's federation.tenants does not list: "+
				"list it, or remove the directory", entry.Name())
		}
	}

	keys := make(map[string][]auth.FederationOption, len(roster))
	for _, tenant := range roster {
		// The roster is validated against the namespace grammar when the policy
		// loads, so the name is one path element with no separator or dot.
		tenantDir := filepath.Join(dir, tenant)

		files, err := os.ReadDir(tenantDir)
		if err != nil {
			return nil, fmt.Errorf("tenant %q has no key directory: %w", tenant, err)
		}

		var paths []string
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".pem") {
				paths = append(paths, filepath.Join(tenantDir, file.Name()))
			}
		}

		switch {
		case len(paths) == 0:
			return nil, fmt.Errorf("tenant %q has no public key (*.pem) in %s", tenant, tenantDir)
		case len(paths) > maxIdentityKeysPerTenant:
			return nil, fmt.Errorf("tenant %q has %d keys in %s, and at most %d are published",
				tenant, len(paths), tenantDir, maxIdentityKeysPerTenant)
		}

		opts, err := identityPublicFileKeys(paths)
		if err != nil {
			return nil, fmt.Errorf("tenant %q: %w", tenant, err)
		}
		keys[tenant] = opts
	}

	return keys, nil
}
