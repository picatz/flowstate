package main

import (
	"cmp"
	"context"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth/signers/vaulttransit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// identitySignerScheme is the URL scheme --identity-signer takes.
const identitySignerScheme = "vault-transit"

// identitySignerEnv is the default of --identity-signer.
const identitySignerEnv = "FLOWSTATE_IDENTITY_SIGNER"

// identitySignerTimeout bounds start-up's round trips to the backend: reading
// the key, and for a worker the one signature that proves its public half.
const identitySignerTimeout = 30 * time.Second

// identitySignerParams are the query parameters a signer URL takes. Anything
// else is refused, so a misspelled parameter is not a setting that silently
// never applied.
var identitySignerParams = []string{"mount", "namespace", "scheme", "token_file", "kubernetes_role", "kubernetes_mount"}

// addIdentitySignerFlag declares --identity-signer on a command that loads
// identity keys. publishing is the server's: it only reads public keys.
func addIdentitySignerFlag(cmd *cobra.Command, publishing bool) {
	usage := "vault-transit://HOST[:PORT]/KEY[?mount=transit&namespace=NS&scheme=https&token_file=PATH&kubernetes_role=ROLE] " +
		"naming a Vault or OpenBao Transit key whose private half never leaves it, in place of --identity-key. "
	if publishing {
		usage += "The server reads the key's public versions from the backend and publishes them, and needs only " +
			"`read` on transit/keys/KEY. "
	} else {
		usage += "The worker signs through Transit (`update` on transit/sign/KEY, `read` on transit/keys/KEY) and " +
			"publishes the key's previous versions for the rotation overlap. "
	}
	usage += "The Vault token is never part of the URL: it comes from token_file, $FLOWSTATE_SECRET_VAULT_TOKEN_FILE, " +
		"$FLOWSTATE_SECRET_VAULT_TOKEN, or Kubernetes auth. Requests are bounded by the trust policy's `egress:` section"

	cmd.Flags().String("identity-signer", os.Getenv(identitySignerEnv), usage)
}

// parseIdentitySigner turns a vault-transit:// URL into the signer's
// configuration.
//
// The URL carries where the key is and how to reach it, and never a secret: a
// token in a URL ends up in shell history, process listings, and logs, so a
// "token" parameter is refused by name rather than treated as an unknown one.
// The token comes from a file or the environment, the same places the Vault
// secrets provider reads it from.
//
// The egress policy is the trust policy's, so a Vault on a private network is
// reached by the same named loosening every other identity fetch uses, and not
// by a setting of this URL's own.
func parseIdentitySigner(raw string, policy *auth.Policy) (vaulttransit.Config, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		// url.Parse quotes the URL, which may be where a token was pasted.
		return vaulttransit.Config{}, fmt.Errorf("--identity-signer is not a URL")
	}

	key := strings.TrimPrefix(parsed.Path, "/")

	switch {
	case parsed.Scheme != identitySignerScheme:
		return vaulttransit.Config{}, fmt.Errorf("--identity-signer must be a %s:// URL, such as %s://vault.example.com:8200/flowstate-identity",
			identitySignerScheme, identitySignerScheme)
	case parsed.Host == "":
		return vaulttransit.Config{}, fmt.Errorf("--identity-signer names no host")
	case parsed.User != nil:
		return vaulttransit.Config{}, fmt.Errorf("--identity-signer carries credentials in the URL; the token comes from token_file or the environment")
	case key == "" || strings.Contains(key, "/"):
		return vaulttransit.Config{}, fmt.Errorf("--identity-signer names the key as its path, one segment: %s://HOST/KEY", identitySignerScheme)
	case parsed.Fragment != "":
		return vaulttransit.Config{}, fmt.Errorf("--identity-signer has a fragment, which means nothing here")
	}

	query := parsed.Query()
	for name, values := range query {
		switch {
		case strings.EqualFold(name, "token"):
			return vaulttransit.Config{}, fmt.Errorf("--identity-signer must not carry the Vault token: pass token_file=PATH, " +
				"or set $FLOWSTATE_SECRET_VAULT_TOKEN_FILE or $FLOWSTATE_SECRET_VAULT_TOKEN")
		case !slices.Contains(identitySignerParams, name):
			return vaulttransit.Config{}, fmt.Errorf("--identity-signer has no parameter %q; it takes %s", name, strings.Join(identitySignerParams, ", "))
		case len(values) != 1:
			return vaulttransit.Config{}, fmt.Errorf("--identity-signer gives %q more than once", name)
		}
	}

	scheme := "https"
	if value := query.Get("scheme"); value != "" {
		if value != "https" && value != "http" {
			return vaulttransit.Config{}, fmt.Errorf("--identity-signer scheme is https, or http for a loopback Vault")
		}
		scheme = value
	}

	cfg := vaulttransit.Config{
		Address: scheme + "://" + parsed.Host,
		Mount:   query.Get("mount"),
		Key:     key,
	}

	if policy != nil {
		cfg.EgressPolicy, err = policy.EgressPolicy()
		if err != nil {
			return vaulttransit.Config{}, fmt.Errorf("building the identity egress policy: %w", err)
		}
	}

	var opts []vault.Option

	tokenFile := cmp.Or(query.Get("token_file"), os.Getenv(secretVaultTokenFileEnv))
	role := query.Get("kubernetes_role")

	switch {
	case tokenFile != "" && role != "":
		return vaulttransit.Config{}, fmt.Errorf("configure one Vault authentication method, not both a token file and kubernetes_role")
	case tokenFile != "":
		// Re-read when Vault rejects the token, so an agent's rotated sink is
		// picked up without a restart.
		opts = append(opts, vault.WithTokenFile(tokenFile))
	case role != "":
		opts = append(opts, vault.WithKubernetesAuth(role))
		if mount := query.Get("kubernetes_mount"); mount != "" {
			opts = append(opts, vault.WithKubernetesAuthMount(mount))
		}
	case os.Getenv(secretVaultTokenEnv) != "":
		opts = append(opts, vault.WithToken(os.Getenv(secretVaultTokenEnv)))
	default:
		return vaulttransit.Config{}, fmt.Errorf("--identity-signer has no way to authenticate to Vault: pass token_file=PATH, "+
			"set $%s or $%s, or pass kubernetes_role=ROLE for a worker in a cluster", secretVaultTokenFileEnv, secretVaultTokenEnv)
	}

	if namespace := query.Get("namespace"); namespace != "" {
		opts = append(opts, vault.WithVaultNamespace(namespace))
	}
	cfg.Vault = opts

	return cfg, nil
}

// identitySignerKeys builds a worker's signing key from a Transit key, and
// the key's previous versions as verify-only keys.
//
// The key is proved before it is returned: [vaulttransit.Signer.SigningKey]
// asks the backend for one signature and refuses the key unless it verifies
// against the public half the backend reported, so a worker whose Transit key
// and published key disagree fails here and not at every relying party.
func identitySignerKeys(raw string, policy *auth.Policy) (auth.SigningKey, []auth.FederationOption, error) {
	cfg, err := parseIdentitySigner(raw, policy)
	if err != nil {
		return auth.SigningKey{}, nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), identitySignerTimeout)
	defer cancel()

	signer, err := vaulttransit.New(ctx, cfg)
	if err != nil {
		return auth.SigningKey{}, nil, fmt.Errorf("configuring identity signer: %w", err)
	}

	key, err := signer.SigningKey(ctx)
	if err != nil {
		return auth.SigningKey{}, nil, fmt.Errorf("configuring identity signer: %w", err)
	}

	previous := signer.Previous()
	opts := make([]auth.FederationOption, 0, len(previous))
	for _, version := range previous {
		opts = append(opts, auth.WithFederationVerifyOnlyKey(version.ID, version.Key))
	}

	return key, opts, nil
}

// identitySignerPublicKeys reads every public version a Transit key holds, for a
// server that publishes and never signs.
func identitySignerPublicKeys(raw string, policy *auth.Policy) ([]auth.FederationOption, error) {
	cfg, err := parseIdentitySigner(raw, policy)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), identitySignerTimeout)
	defer cancel()

	set, err := vaulttransit.Read(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("reading identity signer's public keys: %w", err)
	}

	opts := []auth.FederationOption{auth.WithFederationVerifyOnlyKey(set.Current.ID, set.Current.Key)}
	for _, version := range set.Previous {
		opts = append(opts, auth.WithFederationVerifyOnlyKey(version.ID, version.Key))
	}

	return opts, nil
}
