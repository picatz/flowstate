package plugin

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"connectrpc.com/connect"

	pluginv1connect "github.com/picatz/flowstate/pkg/flowstate/plugin/v1/pluginv1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// pluginBaseURL is the base URL every request to a plugin is built against.
//
// Connect needs an absolute URL to construct a request and net/http needs a host
// in it, but the transport below ignores both: it dials one Unix socket no
// matter what the URL says. The name still has to be chosen carefully, because
// it is what a request would go to if this client were ever used with a
// different transport. ".invalid" is reserved by RFC 2606 precisely so that it
// resolves nowhere, so that mistake fails to connect rather than reaching a real
// host.
const pluginBaseURL = "http://flowstate-plugin.invalid"

// dialTimeout bounds connecting to the socket. The peer is a process on this
// machine that has already announced it is listening, so this is short: a dial
// that does not complete immediately is a plugin that closed its listener, not a
// slow network.
const dialTimeout = 5 * time.Second

// clients are the Connect clients for one running plugin, all sharing one
// connection to its socket.
type clients struct {
	plugin pluginv1connect.PluginServiceClient
	secret pluginv1connect.SecretServiceClient
	task   pluginv1connect.TaskServiceClient

	// transport is retained only so that its idle connections can be closed
	// when the plugin goes away. Leaving them open would keep a file descriptor
	// per dead plugin.
	transport *http.Transport
}

// close releases the connection to the plugin's socket.
func (c *clients) close() {
	if c != nil && c.transport != nil {
		c.transport.CloseIdleConnections()
	}
}

// newClients builds the Connect clients for a plugin listening on socketPath,
// authenticating every request with token.
//
// Connect over HTTP needs a *http.Client, and reaching a Unix socket means a
// transport whose DialContext ignores the address in the URL and dials the
// socket instead. That is the whole trick: net/http's URL is a routing detail
// here, not a destination.
//
// The transport is built from scratch rather than cloned from
// http.DefaultTransport, which matters for one reason that is easy to miss: the
// default transport consults HTTP_PROXY, and a worker that has one set would
// otherwise try to reach a plugin's socket through a proxy. A transport with no
// proxy function cannot.
func newClients(socketPath, token string, maxResponseBytes int, plugin string) *clients {
	transport := &http.Transport{
		// Proxy is deliberately nil; see above.
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: dialTimeout}
			return dialer.DialContext(ctx, protocol.NetworkUnix, socketPath)
		},

		// One plugin, one socket: a handful of connections is plenty, and
		// bounding them bounds the descriptors a misbehaving plugin can cost.
		MaxIdleConns:        4,
		MaxIdleConnsPerHost: 4,
		MaxConnsPerHost:     8,
		IdleConnTimeout:     90 * time.Second,

		// HTTP/2 is not attempted. Connect's unary RPCs work over HTTP/1.1, the
		// peer is a process on the same machine reached over a stream socket,
		// and h2c would add a negotiation with nothing to gain here.
		ForceAttemptHTTP2: false,
	}

	httpClient := &http.Client{
		Transport: &boundedTransport{base: transport, max: int64(maxResponseBytes)},
		// No client-level timeout: every call carries its own deadline through
		// the context, and a timeout here would silently override a caller's
		// shorter one and be invisible to a caller's longer one.
	}

	opts := []connect.ClientOption{
		// A plugin is not trusted because an operator installed it. Bounding the
		// response before it is read is what stops one making the host allocate
		// without limit.
		connect.WithReadMaxBytes(maxResponseBytes),
		connect.WithInterceptors(authInterceptor(token), propagationInterceptor(plugin, "")),
	}

	return &clients{
		plugin:    pluginv1connect.NewPluginServiceClient(httpClient, pluginBaseURL, opts...),
		secret:    pluginv1connect.NewSecretServiceClient(httpClient, pluginBaseURL, opts...),
		task:      pluginv1connect.NewTaskServiceClient(httpClient, pluginBaseURL, opts...),
		transport: transport,
	}
}

// boundedTransport caps every response body, including the ones Connect's own
// limit does not reach.
//
// connect.WithReadMaxBytes bounds a *successful* response. On a non-200 it
// builds a separate unmarshaler for the error body without carrying that limit
// over, so the whole body is buffered — which means the bound is on the path a
// hostile plugin would not use, and absent on the one it would. A plugin that
// answers any request with an HTTP 500 and a body of its choosing could
// otherwise exhaust the worker's memory, which is precisely the "bound the
// resource the attacker controls" rule this package is built on.
//
// Limiting at the transport covers both paths, because every response goes
// through it whatever its status.
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

	// One byte past the limit, so that a body at exactly the limit still parses
	// and one over it fails to rather than being silently truncated into
	// something that might parse.
	resp.Body = boundedBody{
		Reader: io.LimitReader(resp.Body, t.max+1),
		Closer: resp.Body,
	}

	return resp, nil
}

// boundedBody is a response body read through a limit, still closing the
// original.
type boundedBody struct {
	io.Reader
	io.Closer
}

// authInterceptor presents the per-launch secret on every request, unary or
// streaming.
//
// This has to be a full [connect.Interceptor], not a
// [connect.UnaryInterceptorFunc]: the latter's WrapStreamingClient is a
// documented no-op, so ExecuteStream — the one streaming RPC this package's
// client calls — would leave the socket with no token attached at all,
// silently, rather than failing to compile or to run. See
// [requireToken] in the sdk package for the handler side of the same
// mistake.
//
// The token is held in this closure rather than in a struct field for the reason
// the secrets package gives for doing the same with a resolved value: fmt
// reaches a struct's fields by reflection when it cannot call the value's
// methods, and a credential in a field is a credential that prints. Nothing can
// reflect into a captured variable.
func authInterceptor(token string) connect.Interceptor {
	return &tokenClientInterceptor{token: token}
}

// tokenClientInterceptor sets the per-launch token header on every request
// this plugin's client makes, unary or streaming.
type tokenClientInterceptor struct{ token string }

func (t *tokenClientInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set(protocol.TokenHeader, t.token)
		return next(ctx, req)
	}
}

func (t *tokenClientInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set(protocol.TokenHeader, t.token)
		return conn
	}
}

// WrapStreamingHandler is a no-op: this interceptor is only ever installed on
// a client (see [newClients]), and a plugin's client never serves as a
// streaming handler.
func (t *tokenClientInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// maxSocketPathLen bounds a Unix socket path.
//
// The kernel's sockaddr_un has a fixed-size path: 104 bytes on Darwin, 108 on
// Linux, and the failure when it is exceeded is a bare "invalid argument" from
// bind with nothing pointing at the cause. Checking it here turns that into a
// message naming the path and the fix. The bound is the smaller platform's,
// minus room for the null terminator.
const maxSocketPathLen = 100

// checkSocketPath reports whether a socket path will fit in a sockaddr_un.
func checkSocketPath(path string) error {
	if len(path) <= maxSocketPathLen {
		return nil
	}

	return fmt.Errorf(
		"socket path %q is %d bytes, longer than the %d a Unix socket address holds; set Config.SocketDir to something shorter, such as /run/flowstate",
		path, len(path), maxSocketPathLen,
	)
}
