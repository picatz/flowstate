package main

import (
	"cmp"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Mutual TLS for the public listener, picatz/flowstate#582: a client
// certificate is another issuer, not a parallel identity system. This file
// only ever decides two things — whether the listener requires a client
// certificate at the connection level, and whether a verified one is also
// consulted for identity — and both decisions compose with the trust policy
// this process already loads, through [auth.MTLSVerifier].
//
// # One CA source, not two
//
// The design this slice's issue sketched names a CA file on the command line
// (--tls-client-ca-file) independently of the trust policy's own kind: mtls
// entries, each of which also names a client_ca_file. Built as sketched, that
// is two independently-configured sources for the same fact, and CLAUDE.md's
// "one value, written down twice" is exactly the defect that produces:
// nothing would ever compare the CLI flag's CA against the policy's, so a
// deployment could trust a certificate at the transport level that the
// policy's own kind: mtls entries never see, or vice versa.
//
// This lands with one source instead: the trust policy's kind: mtls entries
// are the only place an operator names a trusted CA, for both purposes.
// [resolveMTLS] builds the public listener's tls.Config.ClientCAs from
// [auth.MTLSVerifier.ClientCAPool] — the union of every kind: mtls entry's
// CA — which is the same verifier consulted for identity when
// --tls-client-auth-identity is also set. There is no second CA flag to
// drift out of agreement with the first.
//
// # Two facts, two flags — never three
//
// --tls-client-auth (off | require) is the connection-level fence: a
// listener refuses a handshake with no client certificate, or with one that
// does not chain to a configured kind: mtls entry's CA, purely at the
// crypto/tls layer, whether or not a verified certificate goes on to name a
// [auth.Principal].
//
// --tls-client-auth-identity is the authentication statement: a verified
// certificate also produces the caller's Principal, through
// [auth.WithPeerVerifier]. It requires --tls-client-auth require — see
// [resolveMTLS] — because consulting a certificate for identity without also
// requiring one at the connection level is exactly [tls.VerifyClientCertIfGiven]'s
// shape: a caller who omits the certificate would be admitted by whatever
// bearer-token path remains, and a caller who presents one gets an identity,
// making the certificate a privilege escalation the client itself controls.
// Only two values are ever offered at the connection level — off and
// require — the same restriction [addTLSFlags] already documents for the
// standard library's own options.

// addMTLSFlags declares the public listener's client-certificate flags on
// cmd.
func addMTLSFlags(cmd *cobra.Command) {
	cmd.Flags().String("tls-client-auth", cmp.Or(os.Getenv("FLOWSTATE_TLS_CLIENT_AUTH"), "off"),
		`whether the public listener requires a client certificate: "off" (the default) or `+
			`"require". The trusted CAs are the client_ca_file of every kind: mtls entry in `+
			`--auth-policy — there is no separate CA flag — so "require" needs at least one such `+
			`entry. Only these two values are offered: this repository never configures `+
			`tls.VerifyClientCertIfGiven, which would admit a caller with no certificate through `+
			`whatever bearer-token path remains and hand one who presents a certificate an identity `+
			`escalation the client itself controls`)
	cmd.Flags().Bool("tls-client-auth-identity",
		os.Getenv("FLOWSTATE_TLS_CLIENT_AUTH_IDENTITY") != "",
		"also authenticate the caller from a verified client certificate, through the same kind: "+
			"mtls trust policy entry that admitted it (overrides FLOWSTATE_TLS_CLIENT_AUTH_IDENTITY). "+
			"Requires --tls-client-auth require. Without it, a certificate required by "+
			"--tls-client-auth is a connection-level fence only, and a caller still needs a bearer "+
			"token this server's --auth-policy accepts")
}

// mtlsFlags is what an operator asked for, read once before anything binds.
type mtlsFlags struct {
	clientAuth         string
	clientAuthIdentity bool
}

// mtlsFlagsOf reads them off the command being run.
func mtlsFlagsOf(cmd *cobra.Command) mtlsFlags {
	clientAuth, _ := cmd.Flags().GetString("tls-client-auth")
	clientAuthIdentity, _ := cmd.Flags().GetBool("tls-client-auth-identity")

	return mtlsFlags{clientAuth: clientAuth, clientAuthIdentity: clientAuthIdentity}
}

// resolveMTLS validates every fail-closed rule this slice decided and, when
// a client certificate was requested, mutates tlsCfg in place to require and
// verify one against the trust policy's kind: mtls entries.
//
// tlsCfg is the public listener's already-resolved TLS configuration — from
// [serverTLSConfig] or from ACME — and may be nil, meaning this process is
// about to serve plaintext. tlsTerminatedUpstream is
// [tlsFlags.tlsTerminatedUpstream], checked here because a proxy terminating
// TLS in front of this process is exactly the deployment that can never see
// a client certificate: --tls-client-auth would require one, and there is no
// certificate for this process to require, because crypto/tls never runs
// here at all.
//
// The returned [auth.MTLSVerifier] is non-nil only when
// --tls-client-auth-identity was also given, which is the caller's cue to
// wire it into [auth.NewAuthenticator] with [auth.WithPeerVerifier]. nil, nil
// covers both "mTLS was not requested" and "mTLS is a connection-level fence
// only" — tlsCfg has already been mutated in the second case, so there is
// nothing further for the caller to do with a nil verifier beyond skipping
// [auth.WithPeerVerifier].
func resolveMTLS(flags mtlsFlags, policy *auth.Policy, tlsTerminatedUpstream bool, tlsCfg *tls.Config) (*auth.MTLSVerifier, error) {
	var required bool
	switch flags.clientAuth {
	case "", "off":
		required = false
	case "require":
		required = true
	default:
		return nil, fmt.Errorf(`--tls-client-auth %q is not supported: use "off" or "require"`, flags.clientAuth)
	}

	if flags.clientAuthIdentity && !required {
		return nil, fmt.Errorf("--tls-client-auth-identity requires --tls-client-auth require: " +
			"consulting a peer certificate for identity without also requiring one at the connection " +
			"level would let a caller who omits the certificate fall back to whatever bearer-token " +
			"path remains while a caller who presents one gets an identity — the fail-open shape " +
			"tls.VerifyClientCertIfGiven has and this repository refuses to offer. Pass " +
			"--tls-client-auth require")
	}

	if !required {
		// A trust policy naming a kind: mtls entry that nothing will ever
		// consult is a deployment that believes it authenticates by
		// certificate and does not — the same "one value, written down
		// twice" class this file's package doc names, just with the second
		// half missing entirely rather than disagreeing.
		if name, ok := firstMTLSIssuer(policy); ok {
			return nil, fmt.Errorf("the trust policy configures kind: mtls issuer %q, but "+
				"--tls-client-auth was never set to require: that entry's CA would never be trusted "+
				"at the connection level and could never admit anyone. Pass --tls-client-auth "+
				"require, or drop the kind: mtls entry", name)
		}
		return nil, nil
	}

	if tlsTerminatedUpstream {
		return nil, fmt.Errorf("--tls-client-auth require was given together with " +
			"--tls-terminated-upstream: a proxy terminating TLS in front of this process strips the " +
			"client certificate along with everything else about the TLS connection, so this process " +
			"never sees one to require or verify. Terminate TLS in this process instead " +
			"(--tls-cert-file/--tls-key-file or --tls-acme-hosts) if client certificates matter, or " +
			"drop --tls-client-auth if the proxy is meant to be the one enforcing it")
	}

	if tlsCfg == nil {
		return nil, fmt.Errorf("--tls-client-auth require needs this process to terminate TLS itself " +
			"(--tls-cert-file/--tls-key-file, or --tls-acme-hosts): a client certificate cannot be " +
			"presented over plain HTTP")
	}

	if policy == nil {
		return nil, fmt.Errorf("--tls-client-auth require was given but no --auth-policy is " +
			"configured: the trusted client CAs come from that policy's kind: mtls issuer entries, " +
			"and there is nowhere to read one from")
	}

	verifier, err := auth.NewMTLSVerifier(*policy)
	if err != nil {
		return nil, fmt.Errorf("loading kind: mtls trust policy entries: %w", err)
	}
	if verifier == nil {
		return nil, fmt.Errorf("--tls-client-auth require was given but --auth-policy configures no " +
			"kind: mtls issuer entry: there is no CA to require a client certificate against. Add one, " +
			"naming client_ca_file and subject_from, or drop --tls-client-auth")
	}

	tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	tlsCfg.ClientCAs = verifier.ClientCAPool()

	if !flags.clientAuthIdentity {
		// Fence only: the pool and ClientAuth above are all mTLS contributes.
		// A caller still authenticates with a bearer token, exactly as if
		// --tls-client-auth had never been set.
		return nil, nil
	}

	return verifier, nil
}

// firstMTLSIssuer returns the name of the first kind: mtls entry in policy,
// or false when policy is nil or names none.
func firstMTLSIssuer(policy *auth.Policy) (string, bool) {
	if policy == nil {
		return "", false
	}
	for _, issuer := range policy.Issuers {
		if issuer.Kind == auth.IssuerKindMTLS || issuer.Kind == auth.IssuerKindSPIFFE {
			return issuer.Name, true
		}
	}
	return "", false
}
