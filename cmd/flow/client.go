package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// maxResponseBytes bounds a single RPC response body.
//
// Larger than the request bound because a response carries a run's outputs, which
// can legitimately be bigger than the specification that produced them, and smaller
// than unlimited because "however much the server feels like sending" is not a
// number a client should accept.
const maxResponseBytes = 32 << 20 // 32 MiB

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
func newWorkflowServiceClient() flowstatev1connect.WorkflowServiceClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	baseURL := serverBaseURL(flowstateAddress)

	return flowstatev1connect.NewWorkflowServiceClient(
		&http.Client{
			Transport: &authorizingTransport{
				base:    &boundedTransport{base: transport, max: maxResponseBytes},
				baseURL: baseURL,
			},

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
func refusedRun(verb, workflowID string, err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeNotFound:
		return fmt.Errorf("no run %q is addressable: check the id, or it belongs to a tenant "+
			"your credentials do not establish, or it has aged out of Temporal's retention", workflowID)
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return fmt.Errorf("refused while %s %q: %w", verb, workflowID, err)
	case connect.CodeUnavailable:
		return fmt.Errorf("no Flowstate server answered at %s (set --address or FLOWSTATE_ADDRESS "+
			"to point somewhere else): %w", flowstateAddress, err)
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
