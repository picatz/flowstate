package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

// newAuthCommand builds `flow auth`, the operator tools for the caller trust
// boundary. It is separate from `flow jwt`: jwt inspects an assertion without
// knowing a deployment's policy, while this command answers whether the exact
// verifier a deployment runs would admit it.
func newAuthCommand() *cobra.Command {
	authCmd := &cobra.Command{
		Use:   "auth",
		Short: "Diagnose caller authentication against a trust policy",
	}
	authCmd.AddCommand(newAuthCheckCommand())
	return authCmd
}

func newAuthCheckCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Check a bearer token against a trust policy",
		Long: "Verify one bearer token against a trust policy using the same OIDC verifier and " +
			"issuer-rule matching path as `flow server`. This diagnoses policy-entry overlap; it does " +
			"not simulate the server's surface-specific --rpc-resource check. The token is read only from a file " +
			"or stdin: there is deliberately no token argument or --token flag, because credentials " +
			"in argv leak through process listings, shell history, logs, and completion. Output names " +
			"only the policy entry that admitted the token, or the policy entries that made it ambiguous; " +
			"it never prints token claims. This is a concrete-token probe, not a static proof that every " +
			"possible token matches at most one entry: check each representative caller before deploying " +
			"a policy change.",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 0 {
				// Do not quote args here. A caller who mistakes this for a token
				// argument must not have the credential copied into diagnostics.
				return newUsageError(errors.New("flow auth check takes no positional arguments; pass a path with --token-file, or use --token-file - to read stdin"))
			}
			return nil
		},
		RunE: runAuthCheck,
		Example: `# Check a projected workload token without putting it in argv:
flow auth check --auth-policy trust.yaml --token-file /var/run/secrets/tokens/flowstate

# Read a token from stdin instead:
flow auth check --auth-policy trust.yaml --token-file - < "$TOKEN_FILE"`,
	}
	cmd.Flags().String("auth-policy", "", "path to the trust policy to check (required)")
	cmd.Flags().String("token-file", "", `path containing the bearer token, or "-" to read stdin (required; the token itself is never accepted in argv)`)
	_ = cmd.MarkFlagRequired("auth-policy")
	_ = cmd.MarkFlagRequired("token-file")
	return cmd
}

func runAuthCheck(cmd *cobra.Command, _ []string) error {
	policyPath, _ := cmd.Flags().GetString("auth-policy")
	tokenPath, _ := cmd.Flags().GetString("token-file")

	policyData, err := os.ReadFile(policyPath)
	if err != nil {
		// As with --token-file below, do not copy a path-bearing error into
		// diagnostics. A caller can accidentally swap the two flag values and
		// make the auth-policy "path" the credential itself.
		return errors.New("reading auth policy: policy could not be read; check --auth-policy")
	}
	policy, err := auth.ParsePolicy(policyData)
	if err != nil {
		// Parser diagnostics can quote source excerpts. The policy path may
		// accidentally name the token file, so no parser detail is safe here.
		return errors.New("parsing auth policy: policy is malformed")
	}
	verifier, err := auth.NewOIDCVerifier(policy)
	if err != nil {
		return fmt.Errorf("configuring token verification: %w", err)
	}

	rawToken, err := authCheckToken(cmd, tokenPath)
	if err != nil {
		return err
	}
	principal, err := verifier.Verify(cmd.Context(), rawToken)
	if err != nil {
		var ambiguous *auth.AmbiguousIssuerError
		if errors.As(err, &ambiguous) {
			// This error is intentionally credential-free and names the exact
			// policy rows the operator has to make disjoint.
			return ambiguous
		}
		var blocked *auth.IssuerBlockedError
		if errors.As(err, &blocked) {
			// Also credential-free: it names the issuer's own URL and the
			// egress rule that refused it, none of which comes from the token,
			// so an operator can tell a policy denial from a down issuer
			// instead of PublicReason's identical "temporarily unavailable"
			// for both (picatz/flowstate#1303).
			return blocked
		}
		// Other verifier errors can carry verified claim values or parser text.
		// The public classification is the same redacted vocabulary exposed at
		// the network boundary and is sufficient to fix the token class.
		return fmt.Errorf("token refused: %s", auth.PublicReason(err))
	}

	for index, entry := range policy.Issuers {
		if entry.Name == principal.IssuerName {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "accepted by issuers[%d] (%q)\n", index, entry.Name)
			return err
		}
	}

	// ParsePolicy requires unique, non-empty names and the verifier copied its
	// winner from that policy, so this is an internal consistency failure, not
	// an unauthenticated success with missing attribution.
	return errors.New("token was accepted without a corresponding trust policy entry")
}

func authCheckToken(cmd *cobra.Command, path string) (string, error) {
	if path != stdinArg {
		token, err := credentialsource.NewFileSource(path).Token(cmd.Context())
		if err != nil {
			// credentialsource's error names path. That is useful for ordinary
			// clients and unsafe here: a caller can accidentally paste the token
			// itself as --token-file's value, making the "path" the credential we
			// promised never to copy into diagnostics.
			return "", errors.New("reading token file: credential could not be read; check --token-file")
		}
		raw, ok := token.Bearer()
		if !ok {
			return "", errors.New("reading token file: credential is not a bearer token")
		}
		return raw, nil
	}

	contents, err := io.ReadAll(io.LimitReader(cmd.InOrStdin(), credentialsource.MaxFileTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("reading token from stdin: %w", err)
	}
	if len(contents) > credentialsource.MaxFileTokenBytes {
		return "", fmt.Errorf("reading token from stdin: credential is larger than %d bytes", credentialsource.MaxFileTokenBytes)
	}
	raw := strings.TrimSpace(string(contents))
	if raw == "" {
		return "", errors.New("reading token from stdin: credential is empty")
	}
	return raw, nil
}
