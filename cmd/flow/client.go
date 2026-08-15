package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
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

	// credentialSource is --credential-source / FLOWSTATE_CREDENTIAL_SOURCE:
	// which named [credentialsource.Source] to acquire a token from, rather
	// than the CLI's original token-file-or-environment default. Empty means
	// that default; see [credentialSourceFor].
	credentialSource string

	// audience is --audience / FLOWSTATE_AUDIENCE: the relying party a
	// minted token, such as one from the "github-actions" source, is
	// addressed to. Ignored by sources that present a token they did not
	// mint.
	audience string
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

	// Names a credentialsource.Source explicitly. "github-actions" is what
	// turns a CI job's ambient OIDC identity into a token without any
	// hand-written curl; "file" and "env" force the same reading --token-file
	// and FLOWSTATE_TOKEN already do, but as a refusal rather than silent
	// anonymity when the named source turns out empty.
	cmd.Flags().String("credential-source", os.Getenv("FLOWSTATE_CREDENTIAL_SOURCE"),
		"acquire a credential from a named source instead of --token-file/FLOWSTATE_TOKEN "+
			"(overrides FLOWSTATE_CREDENTIAL_SOURCE); one of github-actions, file, env. "+
			"An unknown or unusable source is an error, never anonymous")

	cmd.Flags().String("audience", os.Getenv("FLOWSTATE_AUDIENCE"),
		"the relying party a minted credential should be addressed to (overrides "+
			"FLOWSTATE_AUDIENCE); required by --credential-source=github-actions")
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
	credentialSource, _ := cmd.Flags().GetString("credential-source")
	audience, _ := cmd.Flags().GetString("audience")

	return serverFlags{
		address:          address,
		tokenFile:        tokenFile,
		credentialSource: credentialSource,
		audience:         audience,
	}
}

func newWorkflowServiceClient(server serverFlags) flowstatev1connect.WorkflowServiceClient {
	// Resolving the source is local and cheap — no network call, just naming
	// which one to build — so it happens once here rather than on every
	// request. A failure (an unknown --credential-source, or one missing a
	// required --audience) is a configuration error, not a transient one, so
	// carrying it forward to be returned from the first RoundTrip is
	// equivalent to failing now: nothing about a later request could make it
	// succeed.
	//
	// Every caller of this function makes one RPC and reports whatever error
	// comes back directly — nothing here retries — so surfacing the failure
	// through the transport rather than as a second return value costs
	// nothing and keeps every existing caller unchanged. A caller that polls
	// on a loop, and so *would* retry, does not go through this function —
	// see [newFollowClient].
	source, sourceErr := credentialSourceFor(server)

	return newWorkflowServiceClientWithSource(server, source, sourceErr)
}

// newFollowClient builds the client a follow polls through: `flow watch`, and
// the follow phase of `flow run` once a workload has started.
//
// Built once and reused for every poll, unlike [newWorkflowServiceClient].
// [clientPoller.Poll] used to call newWorkflowServiceClient itself on every
// tick, which resolved the credential source fresh each time too — an empty
// cache every poll, so `flow watch` and the follow phase of `flow run` minted
// a new OIDC token roughly once a second (up to four times a second at the
// allowed --interval floor) instead of retaining one until its own refresh
// margin. A long-running CI job following a durable workload could mint
// thousands of tokens over its life, which is the kind of thing a runner's
// token endpoint throttles.
//
// The credential source's construction error is also returned directly here,
// rather than carried into the transport the way newWorkflowServiceClient
// does. That distinction is what keeps a misconfigured --credential-source
// out of [classifyPollError]'s retry path: an unknown source name or a
// github-actions source with no --audience can never succeed by being polled
// again, so a caller of this function fails before the follow loop — and its
// thirty-second outage allowance — ever starts, rather than exhausting that
// allowance on an error a transport-level retry treats as an outage.
func newFollowClient(server serverFlags) (flowstatev1connect.WorkflowServiceClient, error) {
	source, err := credentialSourceFor(server)
	if err != nil {
		return nil, fmt.Errorf("%w\n  --credential-source names how this command should authenticate; "+
			"omit it to use --token-file/FLOWSTATE_TOKEN instead", err)
	}

	return newWorkflowServiceClientWithSource(server, source, nil), nil
}

// newWorkflowServiceClientWithSource builds the client both constructors above
// share, given a credential source already resolved (or its resolution error,
// for [newWorkflowServiceClient]'s deferred-to-the-transport path).
func newWorkflowServiceClientWithSource(
	server serverFlags,
	source credentialsource.Source,
	sourceErr error,
) flowstatev1connect.WorkflowServiceClient {
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
				source:    source,
				sourceErr: sourceErr,
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
	// `--address http://localhost:9233` is a supported spelling, and handing
	// the whole URL to SplitHostPort fails both parses below, which would read
	// an explicitly schemed local address as remote and silently withhold the
	// local remedies. The scheme is not part of the question being asked.
	if strings.Contains(address, "://") {
		if u, err := url.Parse(address); err == nil && u.Hostname() != "" {
			address = u.Hostname()
		}
	}

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
		return unreachableServer(server, "", err)
	default:
		return fmt.Errorf("%s %q: %w", verb, workflowID, err)
	}
}

// refusedStart explains a run that never started.
//
// Separate from [refusedRun] because there is no run id yet to be addressable or
// not, and because this is the one refusal that has a Flowfile in hand: the path
// the caller named is what makes `flow run local <file>` a remedy this command
// can spell out rather than allude to. `flow run` is also the likeliest first
// command anybody types, so it is the one that can least afford to report a bare
// dial error.
func refusedStart(file, name string, arguments []string, server serverFlags, err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable:
		return unreachableServerWithArguments(server, file, arguments, err)
	default:
		return fmt.Errorf("starting %s: %w", name, err)
	}
}

// noServerError is the one report of a Flowstate server that did not answer.
//
// One type rather than the four copies of one sentence this replaces, per the
// one-constant rule: a wording change here used to be a four-file hunt, and the
// fourth copy had already drifted.
//
// It carries the address rather than formatting it in at each call site, because
// the remedies depend on which address was tried. A refusal from loopback means
// there is very likely no server at all, which is a situation the CLI can answer
// with a command to type; a refusal from somewhere else means a deployment that
// is down or misnamed, and a suggestion to start a dev stack would be advice
// about the wrong machine.
type noServerError struct {
	// address is what the client dialled, exactly as `--address` or
	// FLOWSTATE_ADDRESS spelled it.
	address string

	// workflowFile is the Flowfile the verb was given, when it had one. Empty
	// for every verb that addresses a run, a schedule, or nothing at all.
	workflowFile string

	// runArguments are the pre-quoted input flags the failed invocation
	// carried, appended to the suggested commands so following one starts the
	// workload that was asked for.
	runArguments []string

	// err is the dial failure underneath, kept so the reason survives: a
	// connection refused and a TLS handshake that failed are the same code and
	// very different afternoons.
	err error
}

// unreachableServer reports that nothing answered at the address this command
// dialled.
//
// file names the Flowfile the caller passed, or is empty when the verb has none.
func unreachableServer(server serverFlags, file string, err error) error {
	return unreachableServerWithArguments(server, file, nil, err)
}

// unreachableServerWithArguments is the run-shaped form: the arguments ride
// along so the suggested command is the invocation that failed, not a flagless
// cousin of it.
func unreachableServerWithArguments(server serverFlags, file string, arguments []string, err error) error {
	return &noServerError{address: server.address, workflowFile: file, runArguments: arguments, err: err}
}

// Error is the sentence every verb that dials the server now prints, written down
// once.
//
// The address is named because a caller who set FLOWSTATE_ADDRESS in a shell they
// have since forgotten about is the caller most confused by "no server"; and both
// the flag and the variable are named because pointing at an existing deployment
// is the remedy that has nothing to do with this machine, so it belongs in the
// sentence rather than in a list of things to run.
func (e *noServerError) Error() string {
	return fmt.Sprintf("no Flowstate server answered at %s (set --address or FLOWSTATE_ADDRESS "+
		"to point at a deployment that is already running): %v", e.address, e.err)
}

func (e *noServerError) Unwrap() error { return e.err }

// nextCommands offers the way out, as runnable commands.
//
// `flow server dev` leads because it is the answer to the situation that is
// actually most common on a refusal from this machine: no server exists yet. It
// is one command since #377, and the second line of its block is the verb the
// caller already wanted, so the two lines together are the whole path from here
// to a durable run.
//
// `flow run local` comes second and only where a Flowfile was named, because it
// is the answer to a different question: somebody who never wanted a server at
// all. It stays below the durable path rather than beside it, the way `flow
// init`'s NEXT block orders the same two choices, so the two surfaces teach in
// one voice.
//
// Nothing is offered for a remote address. Both remedies are about this machine,
// and telling somebody whose staging deployment is down to start a dev stack
// would be answering a question they did not ask; the address hint in the
// sentence above is the lead there, which is where it belongs.
func (e *noServerError) nextCommands() []commandBlock {
	if !isLoopbackAddress(e.address) {
		return nil
	}

	durable := []string{"flow server dev"}
	if e.workflowFile != "" {
		invocation := shellArgument(e.workflowFile) + e.runArgumentSuffix()
		durable = append(durable, "flow run "+invocation)

		return append([]commandBlock{{commands: durable}}, commandBlock{
			lead:     "or rehearse it here, with no server at all:",
			commands: []string{"flow run local " + invocation},
		})
	}

	return []commandBlock{{commands: durable}}
}

// runArgumentSuffix renders the input flags the failed invocation carried, so
// the suggested command starts the workload that was asked for rather than a
// flagless cousin a workflow with required inputs refuses outright.
func (e *noServerError) runArgumentSuffix() string {
	var suffix strings.Builder
	for _, argument := range e.runArguments {
		suffix.WriteString(" ")
		suffix.WriteString(argument)
	}

	return suffix.String()
}

// shellArgument renders a value safe to copy into a shell.
//
// The path arrived through this process's argv, where the shell's quoting has
// already been consumed, so pasting it back bare re-splits on whitespace and
// re-expands metacharacters. A leading dash additionally reads as a flag, which
// `./` settles without asking the copier to know about `--`.
func shellArgument(value string) string {
	if strings.HasPrefix(value, "-") {
		value = "./" + value
	}

	safe := true
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.', r == '/', r == '_', r == '-', r == '=', r == ':', r == ',', r == '@', r == '%', r == '+':
		default:
			safe = false
		}
	}
	if safe {
		return value
	}

	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
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
