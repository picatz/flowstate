package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/deviceflow"
)

// defaultLoginScope asks for an ID-token-capable grant and, where the IdP
// honors it, a refresh token — without which a login lasts one access token.
const defaultLoginScope = "openid offline_access"

// newDeviceflowClient builds the client `flow login` and `flow logout` talk to
// the IdP through. A variable so tests can inject a clock and sleeper.
var newDeviceflowClient = func() *deviceflow.Client { return deviceflow.New() }

// loginSelectorFromEnv is FLOWSTATE_ISSUER and FLOWSTATE_CLIENT_ID: which
// stored login a command presents when it was not told with a flag.
func loginSelectorFromEnv() (issuer, clientID string) {
	return os.Getenv("FLOWSTATE_ISSUER"), os.Getenv("FLOWSTATE_CLIENT_ID")
}

// addLoginSelectorFlags declares --issuer and --client-id, defaulting from the
// environment at declaration like every sibling flag.
func addLoginSelectorFlags(cmd *cobra.Command) {
	issuer, clientID := loginSelectorFromEnv()
	cmd.Flags().String("issuer", issuer,
		"OIDC issuer URL of the identity provider (overrides FLOWSTATE_ISSUER); https, or http on loopback")
	cmd.Flags().String("client-id", clientID,
		"OAuth client ID registered for this CLI at the identity provider (overrides FLOWSTATE_CLIENT_ID)")
}

// newLoginCommand builds `flow login`.
func newLoginCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Sign in to an OIDC identity provider from the terminal",
		Long: "Sign in with the OAuth 2.0 Device Authorization Grant (RFC 8628): the command prints " +
			"a verification URL and a short code to stderr, you open the URL in any browser and approve, " +
			"and the command stores the resulting tokens. Later commands present the access token " +
			"automatically, after `--token-file` and FLOWSTATE_TOKEN, and renew it with the refresh token " +
			"shortly before it expires; a login that can no longer be renewed fails with a message to run " +
			"`flow login` again rather than sending an expired token or going anonymous.\n\n" +
			"The issuer's /.well-known/openid-configuration must advertise a " +
			"device_authorization_endpoint and a token_endpoint, and the client must be a public client " +
			"allowed to use the device grant. The access token is what the server verifies, so the " +
			"identity provider must issue a JWT access token the deployment's auth policy trusts; see " +
			"docs/DEPLOYMENT.md for per-provider notes.\n\n" +
			"Tokens are kept in a file under the user config directory (flowstate/login, mode 0600 in a " +
			"0700 directory), one per issuer and client ID, bound to the server `--address` names: it is " +
			"presented to that origin only, and a command pointed at any other server is refused rather than " +
			"sent the token. They are never printed and never accepted " +
			"as arguments.",
		Args: cobra.NoArgs,
		RunE: runLogin,
		Example: `# Sign in, then ask the server who it thinks you are:
flow login --issuer https://idp.example.com --client-id flow-cli \
  --address flowstate.example.com:9233
flow auth whoami --address flowstate.example.com:9233

# Name the API an Auth0-style provider should address the token to:
flow login --issuer https://tenant.us.auth0.com/ --client-id flow-cli \
  --address flowstate.example.com:9233 --audience flowstate`,
	}
	addLoginSelectorFlags(cmd)
	cmd.Flags().String("address", os.Getenv("FLOWSTATE_ADDRESS"),
		"address of the Flowstate server this login is for (overrides FLOWSTATE_ADDRESS; required); the "+
			"token is presented to that server only, and any other address is refused")
	cmd.Flags().String("scope", cmp.Or(os.Getenv("FLOWSTATE_SCOPE"), defaultLoginScope),
		"space-delimited OAuth scope to request (overrides FLOWSTATE_SCOPE); offline_access asks for a refresh token")
	cmd.Flags().String("audience", os.Getenv("FLOWSTATE_AUDIENCE"),
		"audience to request for the access token, for providers that choose the API by an audience "+
			"parameter (overrides FLOWSTATE_AUDIENCE)")
	return cmd
}

func runLogin(cmd *cobra.Command, _ []string) error {
	issuer, _ := cmd.Flags().GetString("issuer")
	clientID, _ := cmd.Flags().GetString("client-id")
	scope, _ := cmd.Flags().GetString("scope")
	audience, _ := cmd.Flags().GetString("audience")

	address, _ := cmd.Flags().GetString("address")

	if issuer == "" || clientID == "" {
		return newUsageError(errors.New("flow login needs --issuer and --client-id (or FLOWSTATE_ISSUER and FLOWSTATE_CLIENT_ID)"))
	}
	if address == "" {
		return newUsageError(errors.New("flow login needs --address (or FLOWSTATE_ADDRESS): the stored token is " +
			"presented only to the server it was made for"))
	}
	serverOrigin, err := deviceflow.NormalizeOrigin(serverBaseURL(address))
	if err != nil {
		return newUsageError(fmt.Errorf("--address: %w", err))
	}
	store, err := deviceflow.DefaultStore()
	if err != nil {
		return err
	}

	stderr := cmd.ErrOrStderr()
	entry, err := newDeviceflowClient().Login(cmd.Context(), deviceflow.Config{
		Issuer:   issuer,
		ClientID: clientID,
		Scope:    scope,
		Audience: audience,
	}, func(a deviceflow.Authorization) {
		// The user code and URL are for a person; the device code and every
		// token stay out of the output.
		_, _ = fmt.Fprintf(stderr, "To sign in, open:\n\n    %s\n\nand enter the code:\n\n    %s\n\n", a.VerificationURI, a.UserCode)
		if a.VerificationURIComplete != "" {
			_, _ = fmt.Fprintf(stderr, "Or open this link, which includes the code:\n\n    %s\n\n", a.VerificationURIComplete)
		}
		_, _ = fmt.Fprintf(stderr, "Waiting for approval (the code expires in %s)...\n", a.ExpiresIn.Round(time.Second))
	})
	if err != nil {
		switch {
		case errors.Is(err, deviceflow.ErrAccessDenied):
			return errors.New("login was denied at the identity provider")
		case errors.Is(err, deviceflow.ErrExpiredToken):
			return errors.New("login timed out before it was approved; run `flow login` again")
		}
		return fmt.Errorf("login failed: %w", err)
	}

	entry.ServerOrigin = serverOrigin
	if err := store.Save(entry); err != nil {
		return fmt.Errorf("storing the login: %w", err)
	}

	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Logged in to %s as client %s for %s; the access token expires %s.\n",
		entry.Issuer, entry.ClientID, entry.ServerOrigin, entry.Tokens.ExpiresAt.Local().Format(time.RFC1123))
	return err
}

// newLogoutCommand builds `flow logout`.
func newLogoutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Forget a stored login and revoke its tokens",
		Long: "Delete the login stored by `flow login` and, when the identity provider advertises a " +
			"revocation_endpoint (RFC 7009), ask it to revoke the stored refresh token (the access token " +
			"when there is no refresh token). Revocation is best effort: an access token already issued " +
			"may stay valid until it expires, and if revocation fails the stored login is still deleted " +
			"and a warning is printed.\n\n" +
			"With no flags, the only stored login is removed; when several are stored, choose one with " +
			"`--issuer` and `--client-id`. Logging out when nothing is stored is not an error.",
		Args: cobra.NoArgs,
		RunE: runLogout,
		Example: `flow logout

flow logout --issuer https://idp.example.com --client-id flow-cli`,
	}
	addLoginSelectorFlags(cmd)
	return cmd
}

func runLogout(cmd *cobra.Command, _ []string) error {
	issuer, _ := cmd.Flags().GetString("issuer")
	clientID, _ := cmd.Flags().GetString("client-id")

	store, err := deviceflow.DefaultStore()
	if err != nil {
		return err
	}
	entry, err := store.Select(issuer, clientID)
	switch {
	case errors.Is(err, deviceflow.ErrNotLoggedIn):
		_, err = fmt.Fprintln(cmd.OutOrStdout(), "Not logged in.")
		return err
	case err != nil:
		return fmt.Errorf("reading the stored login: %w", err)
	}

	revokedWhat, revoked := revokeLogin(cmd.Context(), entry)
	if err := store.Delete(entry.Issuer, entry.ClientID); err != nil && !errors.Is(err, deviceflow.ErrNotLoggedIn) {
		return fmt.Errorf("deleting the stored login: %w", err)
	}

	switch {
	case revoked == nil:
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Removed the stored login for %s and revoked its %s at the identity provider. "+
			"An access token already issued may stay valid until it expires.\n", entry.Issuer, revokedWhat)
	case errors.Is(revoked, deviceflow.ErrRevocationUnsupported):
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Removed the stored login for %s. The identity provider advertises no revocation endpoint, so nothing was revoked and its tokens stay valid until they expire.\n", entry.Issuer)
	default:
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "warning: could not revoke the %s at the identity provider: %v\n", revokedWhat, revoked)
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Removed the stored login for %s. Nothing was revoked.\n", entry.Issuer)
	}
	return err
}

// revokeLogin asks the IdP to revoke the stored tokens: the refresh token,
// which ends the session, else the access token.
// It returns a name for the one token it asked about, and the error.
func revokeLogin(ctx context.Context, entry deviceflow.Entry) (string, error) {
	token, hint, what := entry.Tokens.RefreshToken, "refresh_token", "refresh token"
	if token == "" {
		token, hint, what = entry.Tokens.AccessToken, "access_token", "access token"
	}
	return what, newDeviceflowClient().Revoke(ctx, entry.Endpoints, entry.ClientID, token, hint)
}
