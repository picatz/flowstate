package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// The internal listener: a socket of its own, carrying what an operator
// expects on a private port — health and pprof today; a metrics scrape target
// is not in this slice (see [internalHandler] in routing.go for why).
//
// On `flow server` it is a *second* listener rather than more routes on the
// public mux, because pprof's profile and trace endpoints can read this
// process's memory and its running goroutines, which is a very different
// thing to expose than the Connect RPC surface. Putting it behind a socket an
// operator binds to a private network — a service mesh, an SSH tunnel, a
// sidecar — is the whole of the protection; there is no authentication in
// front of it, so it must never be reachable from wherever the public
// listener is.
//
// On `flow worker` it is the only socket the process binds, and the same
// reasoning is what keeps it opt-in and loopback-only: a worker resolves
// secrets into its own address space (pkg/flowstate/v1/secret), so a heap
// profile of one is exactly the material CLAUDE.md's containment sections
// keep out of workflow history. The worker gets the identical flag, handler
// and refusals — one spelling — because the question an operator is asking on
// either process is the same one (#916).

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
//
// Declared on both `flow server` and `flow worker`, with one usage string
// between them: the help text names no listener the other process does not
// have, so the flag reads correctly wherever it is found.
func addInternalListenerFlags(cmd *cobra.Command) {
	cmd.Flags().String("internal-listen", os.Getenv("FLOWSTATE_INTERNAL_ADDRESS"),
		"address for health and pprof, on a private socket of this process's own; "+
			"empty (the default) means no internal listener at all. Pass a loopback address, "+
			"such as --internal-listen "+exampleInternalListenAddress+", to turn it on — "+
			"nothing else is accepted: it serves pprof, whose profiles carry this process's "+
			"memory and running goroutines (secret values resolved into it among them), and it "+
			"carries no authentication and no TLS configuration of its own, so reach it over a "+
			"private network rather than exposing it")
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

		// And the same header-count bound, for the same reason: loopback is
		// not a trust boundary this listener gets to assume, since anything
		// that can reach it can read this process's memory through pprof.
		MaxHeaderValueCount: maxHeaderValueCount,
	}

	return server, listener, nil
}

// internalListenerShutdownTimeout bounds how long a caller waits for the
// internal listener to close its connections before giving up on the polite
// version. Nothing long-lived is served here — a health probe answers
// immediately, and a pprof profile is a download an operator started
// deliberately — so this is a bound on a hang, not on legitimate work.
const internalListenerShutdownTimeout = 10 * time.Second

// serveInternalListener serves an already-bound internal listener in the
// background and returns the function that stops it, which blocks until the
// serving goroutine has actually returned.
//
// Nil server (nobody opted in — see [startInternalListener]) serves nothing
// and returns a stop function that does nothing, so a caller never has to ask
// whether the listener exists.
//
// This is the shape `flow worker` needs and not the one [runServer] has:
// there, both listeners' outcomes plus the ACME watchdog's are multiplexed
// into one select that decides the command's exit, so the goroutines report
// into a shared channel. A worker has no such select — it blocks on its
// context, drains, and returns — so what it needs from this socket is start
// and stop, and a serve failure is something to log rather than something to
// end the process for: the worker's job is running steps, and it goes on
// doing that with the ops port gone. A bind failure is the opposite, and is
// already an error from [startInternalListener], because that one is an
// operator's explicit request that could not be honored.
func serveInternalListener(logger *slog.Logger, server *http.Server, listener net.Listener) (stop func()) {
	if server == nil {
		return func() {}
	}

	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("internal listener stopped serving",
				"address", listener.Addr().String(), "error", err)
		}
	}()

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), internalListenerShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			// Shutdown's own deadline expired with connections still open —
			// a held-open pprof download, most likely. Close is what actually
			// releases the socket in that case, so the process does not exit
			// still holding the port.
			logger.Warn("internal listener did not shut down cleanly; closing it",
				"address", listener.Addr().String(), "error", err)
			_ = server.Close()
		}

		<-served
	}
}
