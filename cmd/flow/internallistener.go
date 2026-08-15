package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// The internal listener: a second socket, bound separately from the public
// one, carrying what an operator expects on a private port — health and
// pprof today; a metrics scrape target is not in this slice (see
// [internalHandler] in routing.go for why).
//
// A separate *listener* rather than more routes on the public mux, because
// pprof's profile and trace endpoints can read this process's memory and its
// running goroutines, which is a very different thing to expose than the
// Connect RPC surface. Putting it behind a socket an operator binds to a
// private network — a service mesh, an SSH tunnel, a sidecar — is the whole
// of the protection; there is no authentication in front of it, so it must
// never be reachable from wherever the public listener is.

// exampleInternalListenAddress is the loopback address named in help text and
// error messages as what turning the internal listener *on* looks like. It is
// not a default: [addInternalListenerFlags] leaves the flag empty unless an
// operator sets it, per CLAUDE.md's "fail closed" — a listener nobody asked
// for is a surface every deployment carries whether or not anyone decided to
// have it, and pprof behind it is a worse thing to expose by omission than
// the RPC surface the public listener already requires an explicit choice
// for.
const exampleInternalListenAddress = "127.0.0.1:9090"

// addInternalListenerFlags declares the internal listener's address flag on
// cmd.
//
// The zero value is empty, which [startInternalListener] reads as "bind
// nothing" — an operator opts in by naming a loopback address, most commonly
// [exampleInternalListenAddress], rather than opting out of a default that
// was there whether they read this flag's help or not.
func addInternalListenerFlags(cmd *cobra.Command) {
	cmd.Flags().String("internal-listen", os.Getenv("FLOWSTATE_INTERNAL_ADDRESS"),
		"address for health and pprof, on a socket separate from the public listener; "+
			"empty (the default) means no internal listener at all. Pass a loopback address, "+
			"such as --internal-listen "+exampleInternalListenAddress+", to turn it on — "+
			"nothing else is accepted: this listener carries no authentication and no TLS "+
			"configuration of its own, so reach it over a private network rather than exposing it")
}

// internalListenerFlags is what an operator asked for, read once before
// anything binds.
type internalListenerFlags struct {
	address string
}

// internalListenerFlagsOf reads them off the command being run.
func internalListenerFlagsOf(cmd *cobra.Command) internalListenerFlags {
	address, _ := cmd.Flags().GetString("internal-listen")

	return internalListenerFlags{address: address}
}

// checkInternalListenAddress refuses anything but loopback or empty, without
// binding anything. Split out from [startInternalListener] so a caller can
// validate the address (and fail the command) before doing any of the other
// start-up work, and only actually bind the socket once everything else has
// succeeded — a validation failure here must never leave a listener open
// behind an error return.
//
// Refused off loopback rather than warned about, for the same reason
// [refusePlaintextListener] refuses the public listener off loopback with no
// certificate: this slice gives the internal listener no TLS option at all,
// so there is no configuration that would make a non-loopback bind safe, and
// pprof is worse to leave unauthenticated on a reachable network than the
// RPC surface — it can read the process's own memory.
func checkInternalListenAddress(addr string) error {
	if addr == "" || isLoopbackAddress(addr) {
		return nil
	}

	return fmt.Errorf("refusing to bind the internal listener on %s: it serves "+
		"pprof, which can read this process's memory and goroutines through its profile "+
		"endpoints, and this release gives it no TLS configuration of its own. Bind loopback "+
		"(for example, --internal-listen %s) and reach it over a private network — a service "+
		"mesh, an SSH tunnel, a sidecar — rather than exposing it, or leave --internal-listen "+
		"unset (the default) to disable it entirely",
		addr, exampleInternalListenAddress)
}

// startInternalListener binds the internal listener and builds the server
// for it, or reports that none was configured: address == "" returns three
// nils, which is the caller's cue to skip serving and shutting one down.
//
// Callers that need to fail a command early, before this binds anything,
// should call [checkInternalListenAddress] first; this repeats that same
// check rather than trusting a caller that skipped it, since a nil error
// from this function is also what a caller with no early check relies on.
func startInternalListener(logger *slog.Logger, addr string) (*http.Server, net.Listener, error) {
	if addr == "" {
		return nil, nil, nil
	}

	if err := checkInternalListenAddress(addr); err != nil {
		return nil, nil, err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("binding the internal listener on %s: %w", addr, err)
	}

	server := &http.Server{
		Handler: internalHandler(logger),

		// The same timeouts the public listener sets, for the same reason:
		// Go's zero values mean no timeout at all.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}

	return server, listener, nil
}
