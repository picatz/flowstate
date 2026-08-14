package main

import (
	"cmp"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Explicit TLS for the public listener: a certificate and key an operator
// names, loaded once at start-up. No ACME here — that is a later slice
// (picatz/flowstate#549) — and no implicit fallback to plaintext: a
// certificate this process cannot load is a start-up failure, never a
// silently downgraded connection, per CLAUDE.md's "fail closed".
//
// # Why the refusal lives beside the loader
//
// cmd/flow/credentials.go already refuses to send a bearer token to a
// non-loopback address over plain HTTP — [tokenFor]'s whole reason for
// existing. Until now the server had no opinion of its own, so a deployment
// could bind 0.0.0.0 with no certificate and every client talking to it would
// simply be refusing to help. [refusePlaintextListener] is the server taking
// the same position: no certificate and an address anything but this machine
// can reach is refused at start-up, not warned about after the fact.

// addTLSFlags declares the public listener's certificate flags on cmd.
//
// Unset leaves both empty, which [serverTLSConfig] reads as "no TLS
// configured" rather than as an error — the refusal is
// [refusePlaintextListener]'s job, and it is what turns "no certificate"
// into a start-up failure precisely when the listen address is not loopback.
func addTLSFlags(cmd *cobra.Command) {
	cmd.Flags().String("tls-cert-file", os.Getenv("FLOWSTATE_TLS_CERT_FILE"),
		"PEM certificate (or chain) for the public listener; unset serves plain HTTP, which is "+
			"refused on any address but loopback. Must be given with --tls-key-file")
	cmd.Flags().String("tls-key-file", os.Getenv("FLOWSTATE_TLS_KEY_FILE"),
		"PEM private key matching --tls-cert-file")
	cmd.Flags().String("tls-min-version", cmp.Or(os.Getenv("FLOWSTATE_TLS_MIN_VERSION"), "1.2"),
		`minimum TLS protocol version to accept: "1.2" (the default and the floor) or "1.3"`)
}

// tlsFlags is what an operator asked for, read once before anything binds.
type tlsFlags struct {
	certFile   string
	keyFile    string
	minVersion string
}

// tlsFlagsOf reads them off the command being run.
func tlsFlagsOf(cmd *cobra.Command) tlsFlags {
	certFile, _ := cmd.Flags().GetString("tls-cert-file")
	keyFile, _ := cmd.Flags().GetString("tls-key-file")
	minVersion, _ := cmd.Flags().GetString("tls-min-version")

	return tlsFlags{certFile: certFile, keyFile: keyFile, minVersion: minVersion}
}

// serverTLSConfig loads the public listener's certificate, or reports that
// none was configured.
//
// A nil, nil return means plaintext was asked for (or nothing was said, which
// defaults to the same thing); [refusePlaintextListener] is what decides
// whether that is acceptable for the address this process is about to bind.
// Every other outcome that is not a valid, loadable certificate pair is an
// error: a path that does not exist, a key that does not match, or a minimum
// version this build refuses to go below all fail the command before it
// listens on anything, rather than falling back to a posture nobody chose.
func serverTLSConfig(flags tlsFlags) (*tls.Config, error) {
	if flags.certFile == "" && flags.keyFile == "" {
		return nil, nil
	}
	if flags.certFile == "" || flags.keyFile == "" {
		return nil, fmt.Errorf("--tls-cert-file and --tls-key-file must be given together; " +
			"got only one of them")
	}

	minVersion, err := tlsMinVersion(flags.minVersion)
	if err != nil {
		return nil, err
	}

	cert, err := tls.LoadX509KeyPair(flags.certFile, flags.keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading TLS certificate %s and key %s: %w",
			flags.certFile, flags.keyFile, err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minVersion,
	}, nil
}

// tlsMinVersion resolves the flag's spelling to the constant the standard
// library wants, refusing anything below the 1.2 floor CLAUDE.md sets for
// this slice. There is no "unset" case here: [addTLSFlags] always gives the
// flag a default of "1.2", so an empty string only reaches this function from
// a caller that built [tlsFlags] by hand, and it is treated the same way.
func tlsMinVersion(v string) (uint16, error) {
	switch v {
	case "", "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf(`--tls-min-version %q is not supported: use "1.2" or "1.3"; `+
			"nothing below 1.2 is offered", v)
	}
}

// refusePlaintextListener is the server's half of the posture
// cmd/flow/credentials.go already holds on the client: a plaintext
// connection to anywhere but this machine hands a bearer token, or anything
// else on the wire, to whatever sits between the two ends.
//
// tlsConfig nil means the listener is about to serve plain HTTP.
// [isLoopbackAddress] (cmd/flow/client.go) is the exact test the client uses
// to decide whether to send a credential at all, reused rather than
// reimplemented so the two sides cannot drift into disagreeing about what
// counts as "this machine".
func refusePlaintextListener(addr string, tlsConfig *tls.Config) error {
	if tlsConfig != nil {
		return nil
	}
	if isLoopbackAddress(addr) {
		return nil
	}

	return fmt.Errorf("refusing to listen on %s over plain HTTP: this address reaches past this "+
		"machine, and cmd/flow/credentials.go already refuses to send a bearer token to a "+
		"plaintext address that is not loopback — the server takes the same position now. "+
		"Configure --tls-cert-file and --tls-key-file (or FLOWSTATE_TLS_CERT_FILE and "+
		"FLOWSTATE_TLS_KEY_FILE), or bind loopback for local development", addr)
}
