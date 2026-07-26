package main

import (
	"io"
	"log"
	"net"
	"net/http"
	"strings"

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

	return flowstatev1connect.NewWorkflowServiceClient(
		&http.Client{Transport: &boundedTransport{base: transport, max: maxResponseBytes}},
		serverBaseURL(flowstateAddress),
	)
}

// serverBaseURL turns the configured address into a base URL.
//
// An explicit scheme is honored, so pointing the CLI at a TLS-terminated server is a
// matter of saying so. A bare address keeps defaulting to http, because that is what
// it has always done and a local development server does not speak TLS — but a bare
// *remote* address earns a warning, since a credential sent that way crosses the
// network in the clear and nothing else in the output would say so.
func serverBaseURL(address string) string {
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return address
	}

	if !isLoopbackAddress(address) {
		log.Printf("WARNING: talking to %s over plain HTTP; any credential sent "+
			"travels in the clear. Use https:// in --address (or FLOWSTATE_ADDRESS) "+
			"to encrypt it.", address)
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
