package main

import (
	"cmp"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Explicit TLS for the public listener: a certificate and key an operator
// names, loaded once at start-up. ACME lives beside this in acme.go (the
// slice picatz/flowstate#549 asked for), and neither path has an implicit
// fallback to plaintext: a certificate this process cannot load is a start-up
// failure, never a silently downgraded connection, per CLAUDE.md's "fail
// closed".
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

	// The one way around refusePlaintextListener that is not a certificate: off
	// by default, and only on because an operator said so — the same shape as
	// --insecure-no-auth, but deliberately not named "insecure", because its
	// two legitimate callers are not the same claim.
	//
	// One caller really is shipping plaintext nowhere safe (there is no such
	// caller this flag should serve — see the refusal's own message for what
	// to do instead: a certificate, or loopback). The other is a process bound
	// to a non-loopback address only because something *in front of it*
	// terminates TLS or otherwise bounds who can reach that address — an
	// Ingress or reverse proxy that speaks plaintext to the backend it
	// forwards to (the ordinary shape for nginx-ingress, an ALB target group,
	// Cloud Run and fly.io's edges), or a container that must bind 0.0.0.0 for
	// Docker's published-port NAT to reach it at all, where 0.0.0.0 is not
	// "reachable past this machine" the way it would be on bare metal because
	// the publish binding (127.0.0.1) is the real boundary. See
	// docs/DEPLOYMENT.md's Kubernetes and Cloud Run/fly.io sections and
	// examples/observability/docker-compose.yaml, which sets this for the
	// second reason and says so in its own comments.
	//
	// The flag cannot tell those two cases apart — it is a bool, and the second
	// one is a fact about the network this process cannot observe — so the
	// help text below is the whole of how an operator is asked to tell them
	// apart before setting it, and [refusePlaintextListener]'s error message
	// repeats the same question for whoever hits the refusal first.
	cmd.Flags().Bool("tls-terminated-upstream",
		os.Getenv("FLOWSTATE_TLS_TERMINATED_UPSTREAM") != "",
		"allow the public listener to serve plain HTTP on a non-loopback address with no "+
			"certificate configured (overrides FLOWSTATE_TLS_TERMINATED_UPSTREAM). Set this "+
			"when, and only when, something in front of this process already terminates TLS or "+
			"otherwise bounds who can reach this address — a reverse proxy, a Kubernetes Ingress, "+
			"a load balancer, a container's published-port binding — so the plaintext this "+
			"process serves never actually reaches an open network. Do NOT set it to ship "+
			"plaintext to the internet: if nothing in front of this process terminates TLS, "+
			"configure --tls-cert-file/--tls-key-file instead, or bind loopback for local "+
			"development")
}

// tlsFlags is what an operator asked for, read once before anything binds.
type tlsFlags struct {
	certFile   string
	keyFile    string
	minVersion string

	// tlsTerminatedUpstream is --tls-terminated-upstream: an assertion that
	// something in front of this process, not this process, is the reason
	// binding a non-loopback address without a certificate here is still safe.
	tlsTerminatedUpstream bool
}

// tlsFlagsOf reads them off the command being run.
func tlsFlagsOf(cmd *cobra.Command) tlsFlags {
	certFile, _ := cmd.Flags().GetString("tls-cert-file")
	keyFile, _ := cmd.Flags().GetString("tls-key-file")
	minVersion, _ := cmd.Flags().GetString("tls-min-version")
	tlsTerminatedUpstream, _ := cmd.Flags().GetBool("tls-terminated-upstream")

	return tlsFlags{
		certFile:              certFile,
		keyFile:               keyFile,
		minVersion:            minVersion,
		tlsTerminatedUpstream: tlsTerminatedUpstream,
	}
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
//
// tlsTerminatedUpstream is --tls-terminated-upstream, said out loud: the one
// way this refusal does not apply besides a certificate. It exists for a
// deployment where "reaches past this machine" is not actually true of addr
// even though [isLoopbackAddress] cannot tell — an Ingress or reverse proxy
// that terminates TLS and forwards plaintext to this process, or a container
// that must bind 0.0.0.0 for Docker's published-port NAT to reach it at all,
// where the real boundary is enforced by publishing that port to 127.0.0.1
// alone. Off by default, same as every other fail-closed refusal in this
// command — and see [addTLSFlags]'s comment on the flag for why it is not
// named "insecure": that word fits only the caller this flag must refuse to
// help, not the one it exists for.
func refusePlaintextListener(addr string, tlsConfig *tls.Config, tlsTerminatedUpstream bool) error {
	if tlsConfig != nil {
		return nil
	}
	if isLoopbackAddress(addr) {
		return nil
	}
	if tlsTerminatedUpstream {
		return nil
	}

	return fmt.Errorf("refusing to listen on %s over plain HTTP: this address reaches past this "+
		"machine, and cmd/flow/credentials.go already refuses to send a bearer token to a "+
		"plaintext address that is not loopback — the server takes the same position now. Three "+
		"ways forward: configure --tls-cert-file and --tls-key-file (or FLOWSTATE_TLS_CERT_FILE "+
		"and FLOWSTATE_TLS_KEY_FILE) to terminate TLS here; bind loopback for local development; "+
		"or, only when a reverse proxy, Ingress, load balancer or NAT boundary in front of this "+
		"process already terminates TLS or bounds who can reach %s, say so with "+
		"--tls-terminated-upstream (or FLOWSTATE_TLS_TERMINATED_UPSTREAM) — never as a substitute "+
		"for one of the first two when nothing actually stands in front of this process", addr, addr)
}
