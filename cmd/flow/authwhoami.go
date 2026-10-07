package main

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// newAuthWhoamiCommand builds `flow auth whoami`, which asks the server who it
// believes the caller is.
//
// It is the one `flow auth` verb that talks to a server: `check` verifies a
// token against a policy file offline, while this reports what a deployment
// actually admitted, through the same flags and credential flow as every other
// server verb.
func newAuthWhoamiCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "whoami",
		Short: "Show the identity the server established for this caller",
		Long: "Ask the server which principal it established for the credential this command " +
			"presents: the issuer and subject, the namespace (tenant), the principal kind, the " +
			"trust policy entry that admitted the caller (issuer_entry), the actions that entry " +
			"grants (narrowed by any actor), the RFC 8693 actor chain when a delegated token was " +
			"admitted (`actors`, current actor first), and the claims it carries. This is the answer to \"why was I refused\" and " +
			"\"which tenant am I in\" before any workflow runs.\n\n" +
			"Every authenticated caller may ask, whatever its policy entry's `actions:` list holds, " +
			"because the answer is only the caller's own identity. A server that admits anonymous " +
			"callers (`flow server dev` without `--auth`) answers `authenticated: false` rather than " +
			"refusing. The credential itself is never printed.\n\n" +
			"The credential is read as every server verb reads it: `--token-file`, " +
			"FLOWSTATE_TOKEN_FILE or FLOWSTATE_TOKEN, the login stored by `flow login`, or " +
			"`--credential-source`.",
		Args: cobra.NoArgs,
		RunE: runAuthWhoami,
		Example: `# Who does the server think I am?
flow auth whoami --address flowstate.example.com:9233 \
  --token-file ~/.config/flowstate/token

# Which tenant and policy entry admitted this CI job?
flow auth whoami -o json \
  --credential-source github-actions --audience flowstate \
  | jq '{namespace: .principal.namespace, entry: .principal.issuerEntry}'`,
	}

	addServerFlags(cmd)
	addOutputFlag(cmd)

	return cmd
}

// runAuthWhoami asks the server for the caller's principal and prints it.
func runAuthWhoami(cmd *cobra.Command, _ []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	server := serverFlagsOf(cmd)

	response, err := newWorkflowServiceClient(server).Whoami(cmd.Context(), connect.NewRequest(&v1.WhoamiRequest{}))
	if err != nil {
		switch connect.CodeOf(err) {
		case connect.CodeUnauthenticated, connect.CodePermissionDenied:
			return fmt.Errorf("refused while asking who you are: %w", err)
		case connect.CodeUnavailable:
			return unreachableServer(server, "", err)
		default:
			return fmt.Errorf("asking who you are: %w", err)
		}
	}

	if format.Machine() {
		return writeJSON(surface, format, response.Msg)
	}

	return writeWhoamiText(surface.Out, response.Msg)
}

// actorChainText renders an RFC 8693 `act` chain as `issuer#subject` entries,
// current actor first and joined by ", via ", so a delegated caller reads
// "acting via A, via B". Empty for a caller acting for themselves.
func actorChainText(actors []*v1.Actor) string {
	via := make([]string, len(actors))
	for i, actor := range actors {
		via[i] = principal.Qualified(actor.GetIssuer(), actor.GetSubject())
	}

	return strings.Join(via, ", via ")
}

// writeWhoamiText renders the answer as aligned `key: value` lines, one fact per
// line, in the order the schema declares them. Claims are printed one per line
// as JSON values, in name order, so the output is stable and a list or object
// claim stays on its line.
func writeWhoamiText(out io.Writer, msg *v1.WhoamiResponse) error {
	principal := msg.GetPrincipal()

	var b strings.Builder

	fmt.Fprintf(&b, "authenticated: %t\n", msg.GetAuthenticated())
	fmt.Fprintf(&b, "issuer: %s\n", principal.GetIssuer())
	fmt.Fprintf(&b, "subject: %s\n", principal.GetSubject())
	fmt.Fprintf(&b, "namespace: %s\n", principal.GetNamespace())
	fmt.Fprintf(&b, "kind: %s\n", v1.PrincipalKindName(principal.GetKind()))
	fmt.Fprintf(&b, "issuer_entry: %s\n", principal.GetIssuerEntry())
	fmt.Fprintf(&b, "actions: %s\n", strings.Join(principal.GetActions(), ", "))
	fmt.Fprintf(&b, "actors: %s\n", actorChainText(principal.GetActors()))

	b.WriteString("claims:\n")
	claims := principal.GetClaims()
	for _, name := range slices.Sorted(maps.Keys(claims)) {
		encoded, err := json.Marshal(claims[name].AsInterface())
		if err != nil {
			return fmt.Errorf("rendering claim %q: %w", name, err)
		}
		fmt.Fprintf(&b, "  %s: %s\n", name, encoded)
	}

	_, err := out.Write([]byte(b.String()))

	return err
}
