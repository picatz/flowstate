package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/spf13/cobra"
)

// maxResponseBytes bounds a single RPC response body.
//
// Larger than the request bound because a response carries a run's outputs, which
// can legitimately be bigger than the specification that produced them, and smaller
// than unlimited because "however much the server feels like sending" is not a
// number a client should accept.
const maxResponseBytes = 32 << 20 // 32 MiB

// requestTimeout bounds one RPC, so a peer cannot hold a command open forever.
//
// Deliberately much longer than any healthy answer to a unary call, because the job
// here is to make a hang finite rather than to police latency. A watch layers its own
// outage allowance on top: a stall costs one of these before the allowance can start
// noticing, so the worst case to give-up is this plus the allowance.
//
// A variable rather than a constant for one reason, which is worth stating so nobody
// takes it for a knob: a test asserting that the deadline exists cannot spend thirty
// seconds proving it. Nothing reads it from configuration and nothing should — a bound
// a peer can talk the client out of is not a bound.
var requestTimeout = 30 * time.Second

// newWorkflowServiceClient builds the client the CLI talks to a Flowstate server
// with.
//
// # Why the transport is bounded
//
// connect.WithReadMaxBytes bounds a *successful* response. On a non-200, connect-go
// builds a separate unmarshaler for the error body and does not carry the limit over,
// so the bound covers the path a cooperative server takes and not the one a hostile
// or compromised server would. Limiting at the transport covers both, because every
// response passes through it whatever its status.
//
// The consequence here is smaller than it is on a worker — this is a CLI the user
// invoked, pointed at a server the user chose, so exhausting its memory costs a
// process rather than a service. It is bounded anyway, because "the peer is probably
// fine" is not a bound.
// serverFlags is what a command needs in order to reach a Flowstate server.
//
// A value read off the command being run rather than a package variable, which is
// what `--address` and `--token-file` used to be. Both were bound by every verb that
// contacts a server, so pflag wrote them at declaration and each verb's default
// overwrote the last — one address for the process, assembled by whichever command
// was built most recently.
//
// Carried as a pair because they are always needed together: an address with no
// credential reaches a server that refuses, and a credential with no address is a
// token sent nowhere.
type serverFlags struct {
	address   string
	tokenFile string
}

// addServerFlags declares them on a verb that contacts a server.
//
// One place, so a verb added later cannot be given a group and left without an
// address — the way `get` and `signal` were first written. The defaults come from
// the environment at declaration time, which is what makes FLOWSTATE_ADDRESS and
// FLOWSTATE_TOKEN_FILE reach a flag nobody passed.
func addServerFlags(cmd *cobra.Command) {
	cmd.Flags().String("address", cmp.Or(os.Getenv("FLOWSTATE_ADDRESS"), defaultServerAddress),
		"address of the Flowstate server (overrides FLOWSTATE_ADDRESS); "+
			"an explicit https:// scheme is honored")

	// A path, never the token. A credential in argv is a credential in `ps` and in
	// shell history — and the file form is the one federated identity arrives in
	// anyway, since Kubernetes projects a service account token to a path and
	// rotates it there. Read per request for that reason.
	cmd.Flags().String("token-file", os.Getenv("FLOWSTATE_TOKEN_FILE"),
		"file holding the bearer token to authenticate with (overrides FLOWSTATE_TOKEN_FILE); "+
			"re-read per request, so a rotating token keeps working. "+
			"Without it, FLOWSTATE_TOKEN is used, and neither means anonymous")
}

// defaultServerAddress is where a Flowstate server runs unless told otherwise.
const defaultServerAddress = "localhost:9233"

// serverFlagsOf reads them off the command being run.
//
// Defaults come from the environment at declaration, so an unset flag answers
// FLOWSTATE_ADDRESS rather than the empty string — which is the direction the
// `--verbose` bug went wrong in, where a hardcoded default silently overwrote what
// the environment had supplied.
func serverFlagsOf(cmd *cobra.Command) serverFlags {
	address, _ := cmd.Flags().GetString("address")
	tokenFile, _ := cmd.Flags().GetString("token-file")

	return serverFlags{address: address, tokenFile: tokenFile}
}

func newWorkflowServiceClient(server serverFlags) flowstatev1connect.WorkflowServiceClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	baseURL := serverBaseURL(server.address)

	// The client half of the tracing the server has carried all along, and a
	// trace that now begins at the person rather than at the server.
	//
	// Telemetry is started before the interceptor is built, and that ordering is
	// the whole of it: otelconnect reads the global tracer provider and the
	// global text-map propagator once, at construction, and keeps whatever it
	// found. Built first, it captures the no-op pair and injects nothing for the
	// life of the process — which is what this comment used to have to admit.
	// Started first, the interceptor opens a client span per RPC and injects
	// traceparent, so the server's own interceptor extracts it and its spans are
	// children of the command somebody typed.
	//
	// Off unless the operator pointed OTEL_EXPORTER_OTLP_* somewhere: no
	// exporter, no propagator, no headers, and this interceptor goes on
	// recording into the no-op provider exactly as before.
	//
	// A warning rather than a refusal when telemetry cannot be configured. The
	// command a person asked for is `flow get`, not `flow get with tracing`, and
	// a mistyped endpoint should cost them the trace rather than the answer —
	// but silently, and they would be reading an empty Grafana wondering which
	// half was broken. Said once, on stderr, alongside the other things this
	// client warns about.
	if _, err := startTelemetry(context.Background()); err != nil {
		log.Printf("WARNING: telemetry is configured but could not be started, "+
			"so this command emits no trace: %v", err)
	}

	var interceptors []connect.Interceptor
	if otelInterceptor, err := otelconnect.NewInterceptor(); err == nil {
		interceptors = append(interceptors, otelInterceptor)
	}

	return flowstatev1connect.NewWorkflowServiceClient(
		&http.Client{
			Transport: &authorizingTransport{
				base:      &boundedTransport{base: transport, max: maxResponseBytes},
				baseURL:   baseURL,
				tokenFile: server.tokenFile,
			},

			// Bounded in time as well as in bytes, and for the same reason: the peer
			// decides how this goes otherwise.
			//
			// Every RPC here is unary and answered in milliseconds by a healthy
			// server. A server that accepts the connection and then sends no headers
			// at all is a different thing, and without this it blocks forever — the
			// cloned default transport sets no ResponseHeaderTimeout, and the context
			// belongs to the command rather than the request. `flow get` hung. Worse,
			// `flow watch` hung *silently*: its outage allowance only advances when a
			// poll returns, so a stall produced no failure for the allowance to
			// count, and a bound stated in seconds never started.
			//
			// Set loose on purpose. Too tight manufactures a failure on a healthy but
			// slow server, which is a false report a pipeline would act on; too loose
			// only lengthens the worst case, which is now finite either way.
			Timeout: requestTimeout,

			// A credential must not follow a redirect. net/http strips the
			// Authorization header when a redirect crosses to another host, which
			// covers the obvious case and not the one that matters: a redirect to
			// a different *path* on the same host keeps the header, and a
			// compromised or merely misconfigured server could use that to collect
			// tokens at an endpoint that only logs them.
			//
			// Connect has no use for redirects — an RPC endpoint either answers or
			// does not — so refusing them costs nothing and removes the question.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		baseURL,
		connect.WithInterceptors(interceptors...),
	)
}

// serverBaseURL turns the configured address into a base URL.
//
// An explicit scheme is honored, so pointing the CLI at a TLS-terminated server is a
// matter of saying so. A bare address keeps defaulting to http, because that is what
// it has always done and a local development server does not speak TLS — but a bare
// *remote* address earns a warning, because a request going somewhere else in the
// clear is worth knowing about even when it carries nothing secret.
//
// The credential half of this is no longer a warning. [tokenFor] refuses to put a
// token on a plaintext connection to anywhere but this machine, which is the same
// concern enforced rather than announced: a warning nobody reads is not a control,
// and by the time it matters the token is already on the wire.
func serverBaseURL(address string) string {
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return address
	}

	if !isLoopbackAddress(address) {
		log.Printf("WARNING: talking to %s over plain HTTP. Use https:// in --address "+
			"(or FLOWSTATE_ADDRESS) to encrypt it.", address)
	}

	return "http://" + address
}

// isLoopbackAddress reports whether an address names this machine.
//
// A name that does not resolve to an address is treated as remote: the question
// being asked is whether to warn, and warning about something local is a smaller
// mistake than staying quiet about something remote.
func isLoopbackAddress(address string) bool {
	host := address
	if h, _, err := net.SplitHostPort(address); err == nil {
		host = h
	}

	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// refusedRun turns a refused request about an existing run into something a
// person can act on.
//
// The server answers not-found for a run that does not exist and for a run in
// another tenant alike, and that conflation is deliberate: distinguishing them
// would confirm that an id belongs to somebody, which is precisely the fact a
// caller in the wrong tenant must not learn. Right for the wire, unhelpful on a
// terminal — a bare "no such run" reads as "you mistyped the id" and sends the
// reader to check the one thing that is probably fine.
//
// So this restates the ambiguity the server chose rather than resolving it, and
// names all three causes rather than the likeliest one. The client learns nothing
// it did not already have, and the person reading it knows what to rule out. Note
// that a *finished* run is still readable and still signalable-looking: Temporal
// keeps closed executions for its retention period, so ageing out is a separate
// cause from having finished.
//
// The verb says what was being attempted, because "no run X is addressable" is a
// different problem depending on whether it came from reading one or signalling
// one.
func refusedRun(verb, workflowID string, server serverFlags, err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeNotFound:
		return fmt.Errorf("no run %q is addressable: check the id, or it belongs to a tenant "+
			"your credentials do not establish, or it has aged out of Temporal's retention", workflowID)
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return fmt.Errorf("refused while %s %q: %w", verb, workflowID, err)
	case connect.CodeUnavailable:
		return fmt.Errorf("no Flowstate server answered at %s (set --address or FLOWSTATE_ADDRESS "+
			"to point somewhere else): %w", server.address, err)
	default:
		return fmt.Errorf("%s %q: %w", verb, workflowID, err)
	}
}

// boundedTransport caps every response body, including the ones Connect's own limit
// does not reach.
type boundedTransport struct {
	base *http.Transport
	max  int64
}

// RoundTrip implements [http.RoundTripper].
func (t *boundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// One byte past the limit, so a body at exactly the limit still parses and one
	// over it fails rather than being silently truncated into something that might.
	resp.Body = boundedBody{
		Reader: io.LimitReader(resp.Body, t.max+1),
		Closer: resp.Body,
	}

	return resp, nil
}

// boundedBody is a response body read through a limit, still closing the original.
type boundedBody struct {
	io.Reader
	io.Closer
}
