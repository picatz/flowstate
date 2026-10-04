package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/gates"
)

// Turning `--gates-ui` into the browser page for approval gates (#1748).
//
// A page that lets a person answer a gate is a surface a deployment opts into,
// the way it opts into --webhook: where it is not asked for, the routes do not
// exist, so there is nothing to probe and nothing to harden on a deployment whose
// approvers use the CLI. See pkg/flowstate/v1/gates for what the page is and
// what authenticates it.

// addGatesUIFlags declares the gate page's surface on the server command.
func addGatesUIFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("gates-ui", false,
		"serve a page at /gates/<workflow-id>/<signal> for each pending `wait_for_signal:` gate, "+
			"showing the question it asks and Approve and Deny buttons that deliver the signal. "+
			"The page holds no credential: it forwards the visitor's Authorization header to this "+
			"server's own API, so who may see or answer a gate is decided by the same authenticator, "+
			"tenancy check and `signals:` policy `flow signal` meets, and every answer is audited the "+
			"same way. Reach it through an identity-aware proxy that sets the header. Without this "+
			"flag the routes do not exist")

	// Browser sign-in (#1748): optional, and all of it needs --gates-ui.
	cmd.Flags().String("gates-ui-issuer", "",
		"OpenID Connect issuer a visitor with no credential signs in with, so the gate page works "+
			"without an identity-aware proxy. It is the OAuth 2.1 authorization code flow with PKCE; "+
			"the access token it returns is presented to this server's own API like any bearer token, "+
			"so the issuer must be one the trust policy accepts. Requires --gates-ui-client-id and "+
			"--gates-ui-redirect-url")
	cmd.Flags().String("gates-ui-client-id", "", "client id registered with --gates-ui-issuer for the gate page")
	cmd.Flags().String("gates-ui-client-secret-file", "",
		"file holding the client secret, when the registration is a confidential client; omit it for a "+
			"public client, which PKCE already protects")
	cmd.Flags().String("gates-ui-redirect-url", "",
		"absolute URL of /gates/callback on this deployment as a browser reaches it, registered exactly "+
			"with the issuer (for example https://flow.example.com/gates/callback)")
	cmd.Flags().String("gates-ui-resource", "",
		"resource indicator (RFC 8707) the sign-in asks the issuer to mint a token for; defaults to this "+
			"server's own API resource")
	cmd.Flags().StringSlice("gates-ui-scope", nil, "scope to request at sign-in; repeatable. None is required by the page")
	cmd.Flags().String("gates-ui-session-key-file", "",
		"file holding the base64 of a 32-byte key that seals the sign-in cookies. Replicas that should "+
			"accept each other's sessions share one; without it a random key is made at start and a "+
			"restart signs everyone out. Generate one with `head -c32 /dev/urandom | base64`")
}

// maxGatesSecretFileBytes bounds the client secret and session key files: a
// secret is under a kilobyte, a base64 key is 44 bytes.
const maxGatesSecretFileBytes = 4 << 10

// gatesUIOptions is the handler options --gates-ui asks for: none when it is
// not given, so a deployment that never asked builds the handler it always did.
//
// policy is the trust policy the server verifies with (nil under
// --insecure-no-auth) and rpcResource its API's resource: sign-in leaves the
// process through the policy's identity egress and asks for a token for the
// resource the API will accept.
func gatesUIOptions(cmd *cobra.Command, policy *auth.Policy, rpcResource string, logger *slog.Logger) ([]handlerOption, error) {
	flags := cmd.Flags()

	on, _ := flags.GetBool("gates-ui")
	issuer, _ := flags.GetString("gates-ui-issuer")

	var stray []string
	for _, name := range []string{
		"gates-ui-issuer", "gates-ui-client-id", "gates-ui-client-secret-file", "gates-ui-redirect-url",
		"gates-ui-resource", "gates-ui-scope", "gates-ui-session-key-file",
	} {
		if flags.Changed(name) {
			stray = append(stray, "--"+name)
		}
	}
	if !on {
		if len(stray) > 0 {
			return nil, fmt.Errorf("%s only configure the gate page's sign-in, which --gates-ui turns on", strings.Join(stray, ", "))
		}

		return nil, nil
	}
	if issuer == "" {
		if len(stray) > 0 {
			return nil, fmt.Errorf("%s configure sign-in, which needs --gates-ui-issuer", strings.Join(stray, ", "))
		}

		return []handlerOption{withGatesUI()}, nil
	}

	clientID, _ := flags.GetString("gates-ui-client-id")
	redirect, _ := flags.GetString("gates-ui-redirect-url")
	resource, _ := flags.GetString("gates-ui-resource")
	scopes, _ := flags.GetStringSlice("gates-ui-scope")
	if resource == "" {
		resource = rpcResource
	}

	secret, err := gatesSecretFile(cmd, "gates-ui-client-secret-file", "the gate page's client secret")
	if err != nil {
		return nil, err
	}

	key, err := gatesSessionKey(cmd, logger)
	if err != nil {
		return nil, err
	}

	// The token this sign-in obtains is verified by the trust policy on every
	// request, so an issuer or audience the policy would refuse is a sign-in that
	// can never work, and one the API never asks for (an anonymous or mTLS-only
	// deployment) is one no visitor is sent to. Refuse both at start.
	if !policy.AcceptsBearerFrom(issuer, resource) {
		return nil, fmt.Errorf("--gates-ui-issuer %q is not an OIDC issuer in the trust policy that accepts "+
			"audience %q: the API would refuse every token the sign-in obtained; add the issuer with that "+
			"audience to the policy, or set --gates-ui-resource to an audience it accepts", issuer, resource)
	}

	egress := auth.DefaultEgressPolicy()
	if policy != nil {
		if egress, err = policy.EgressPolicy(); err != nil {
			return nil, err
		}
	}

	login, err := gates.NewLogin(gates.LoginConfig{
		Issuer:       issuer,
		ClientID:     clientID,
		ClientSecret: secret,
		RedirectURL:  redirect,
		Resource:     resource,
		Scopes:       scopes,
		SessionKey:   key,
		HTTPClient:   egress.Client(),
	})
	if err != nil {
		return nil, err
	}

	return []handlerOption{withGatesUI(gates.WithLogin(login))}, nil
}

// gatesSecretFile reads a flag's file as one trimmed line, or "" when the flag
// is not given.
func gatesSecretFile(cmd *cobra.Command, flag, what string) (string, error) {
	path, _ := cmd.Flags().GetString(flag)
	if path == "" {
		return "", nil
	}

	data, err := readBoundedFile(path, what, maxGatesSecretFileBytes)
	if err != nil {
		return "", fmt.Errorf("--%s: %w", flag, err)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("--%s: %s is empty", flag, path)
	}

	return value, nil
}

// gatesSessionKey is the key that seals sign-in cookies: the file's, or a fresh
// random one the operator is told about, because a restart then ends every
// session and a second replica cannot read the first's.
func gatesSessionKey(cmd *cobra.Command, logger *slog.Logger) ([]byte, error) {
	encoded, err := gatesSecretFile(cmd, "gates-ui-session-key-file", "the gate page's session key")
	if err != nil {
		return nil, err
	}
	if encoded == "" {
		logger.Warn("gate page sign-in has no --gates-ui-session-key-file: sessions use a key made at start, " +
			"so a restart signs everyone out and replicas do not share sessions")

		return gates.NewSessionKey()
	}

	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("--gates-ui-session-key-file: not base64: %w", err)
	}
	if len(key) != 32 {
		return nil, errors.New("--gates-ui-session-key-file: the key must decode to exactly 32 bytes")
	}

	return key, nil
}
