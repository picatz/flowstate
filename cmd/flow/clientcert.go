package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Client-certificate configuration for the `flow` client, picatz/flowstate#630:
// the other half of mTLS. cmd/flow/mtls.go lets `flow server --tls-client-auth
// require` demand a client certificate on every handshake; until this file,
// nothing in cmd/flow could present one, so turning that server posture on
// locked every control-plane command out at the handshake, before any of them
// reached authentication. This is the capability rule (CLAUDE.md, "a
// capability is not done until it is reachable") pointed at the client side of
// a server option that already shipped.
//
// # Spelled like the rest of the client's credentials
//
// --token-file/FLOWSTATE_TOKEN_FILE names a path, never a secret value, and is
// re-read per request. A client certificate and key are not secrets in the
// same sense — CLAUDE.md's own rule is that a private key *path* is not itself
// a secret value — but the same shape still fits: two flags naming files,
// each with a FLOWSTATE_TLS_CLIENT_* environment default the way --address
// already has FLOWSTATE_ADDRESS (see [addServerFlags], cmd/flow/client.go).
// No flag ever takes a certificate or key's PEM bytes directly, for the same
// reason there is no --token flag: argv is visible in `ps` and in shell
// history.
//
// A third flag, --tls-ca-file, is the trust material for the *other*
// direction: the CA bundle this client verifies the server's certificate
// against, in place of the system roots. It is deliberately not folded into
// the server's own --tls-client-auth machinery (cmd/flow/mtls.go), which
// governs a completely different question — whether *this process, acting as
// a server*, trusts a certificate presented *to* it. Here the client is
// verifying the peer it dials, the same relationship
// FLOWSTATE_SECRET_VAULT_CA_FILE already has to a Vault server
// (cmd/flow/secrets.go) — named the same way for the same reason.
//
// # Fail closed
//
// [clientTLSConfig] never drops a certificate silently. Only one of
// --tls-client-cert-file/--tls-client-key-file given, a certificate or key
// that does not parse, or a certificate and key that do not match are all
// refusals naming the file, not a fallback to dialing with no client
// certificate — CLAUDE.md's rule for exactly this shape of misconfiguration:
// a client that silently drops its certificate turns an authentication
// failure at the application layer into a confusing handshake error with
// nothing pointing back at the flag that caused it.
func addClientCertFlags(cmd *cobra.Command) {
	cmd.Flags().String("tls-client-cert-file", os.Getenv("FLOWSTATE_TLS_CLIENT_CERT_FILE"),
		"PEM client certificate to present when a server requires one via --tls-client-auth "+
			"require (overrides FLOWSTATE_TLS_CLIENT_CERT_FILE); must be given with "+
			"--tls-client-key-file. Unset presents no certificate, which a server requiring one "+
			"refuses at the handshake")
	cmd.Flags().String("tls-client-key-file", os.Getenv("FLOWSTATE_TLS_CLIENT_KEY_FILE"),
		"PEM private key matching --tls-client-cert-file (overrides FLOWSTATE_TLS_CLIENT_KEY_FILE)")
	cmd.Flags().String("tls-ca-file", os.Getenv("FLOWSTATE_TLS_CA_FILE"),
		"PEM CA bundle to verify the server's certificate against, in place of the system roots "+
			"(overrides FLOWSTATE_TLS_CA_FILE). Unset trusts the system roots, which is what "+
			"reaches a server with a certificate from a public CA; set this to reach a server "+
			"whose certificate chains to a private CA instead")
}

// clientCertFlags is what an operator asked for, read once before anything
// dials.
type clientCertFlags struct {
	certFile string
	keyFile  string
	caFile   string
}

// clientCertFlagsOf reads them off the command being run.
func clientCertFlagsOf(cmd *cobra.Command) clientCertFlags {
	certFile, _ := cmd.Flags().GetString("tls-client-cert-file")
	keyFile, _ := cmd.Flags().GetString("tls-client-key-file")
	caFile, _ := cmd.Flags().GetString("tls-ca-file")

	return clientCertFlags{certFile: certFile, keyFile: keyFile, caFile: caFile}
}

// clientTLSConfig builds the client's TLS configuration from flags, or
// reports that nothing was configured.
//
// A nil, nil return means neither a client certificate nor a custom trust
// root was asked for: the caller dials with Go's default TLS behavior
// (system roots, no client certificate), exactly as it did before this file
// existed. Every other outcome that is not a fully loadable, matched
// configuration is an error — never a silent fallback to a lesser posture,
// per CLAUDE.md's fail-closed rule. Each error names the file(s) at fault, so
// a misconfigured pair reads as "flow refused to start with this file" rather
// than as a TLS handshake failure with nothing pointing back at the cause.
func clientTLSConfig(flags clientCertFlags) (*tls.Config, error) {
	if flags.certFile == "" && flags.keyFile == "" && flags.caFile == "" {
		return nil, nil
	}

	cfg := &tls.Config{}

	switch {
	case flags.certFile != "" && flags.keyFile != "":
		cert, err := tls.LoadX509KeyPair(flags.certFile, flags.keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client TLS certificate %s and key %s: %w",
				flags.certFile, flags.keyFile, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	case flags.certFile != "" || flags.keyFile != "":
		return nil, fmt.Errorf("--tls-client-cert-file and --tls-client-key-file must be given " +
			"together; got only one of them")
	}

	if flags.caFile != "" {
		pem, err := os.ReadFile(flags.caFile)
		if err != nil {
			return nil, fmt.Errorf("reading --tls-ca-file %s: %w", flags.caFile, err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--tls-ca-file %s contains no usable PEM certificate", flags.caFile)
		}

		cfg.RootCAs = pool
	}

	return cfg, nil
}
