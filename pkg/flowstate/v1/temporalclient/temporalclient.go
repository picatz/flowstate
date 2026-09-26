// Package temporalclient connects Flowstate to Temporal, whether that is a
// development server on a laptop, a self-hosted cluster, or Temporal Cloud.
//
// Moving a workload between those environments should be a matter of
// configuration, never of editing a Flowfile or rebuilding a binary. This package
// is where that portability lives.
//
// # Configuration
//
// Configuration follows Temporal's own environment configuration convention,
// via [go.temporal.io/sdk/contrib/envconfig], rather than a scheme invented
// here. That means the standard variables work as they do everywhere else in the
// Temporal ecosystem:
//
//	TEMPORAL_ADDRESS                  frontend address
//	TEMPORAL_NAMESPACE                namespace
//	TEMPORAL_API_KEY                  API key, as used by Temporal Cloud
//	TEMPORAL_TLS                      enable TLS
//	TEMPORAL_TLS_CLIENT_CERT_PATH     client certificate, for mTLS
//	TEMPORAL_TLS_CLIENT_KEY_PATH      client key, for mTLS
//	TEMPORAL_TLS_SERVER_CA_CERT_PATH  CA bundle to verify the server
//	TEMPORAL_TLS_SERVER_NAME          expected server name
//	TEMPORAL_CONFIG_FILE              path to a TOML configuration file
//	TEMPORAL_PROFILE                  profile within that file
//
// The TOML file is the same one the `temporal` CLI reads, so a profile already
// configured for the CLI works here without being restated. Profiles are how one
// installation addresses several environments:
//
//	TEMPORAL_PROFILE=staging flow worker
//
// Nothing about the defaults assumes a hosted service. With no configuration at
// all, this connects to a local development server, which is what
// `temporal server start-dev` provides.
package temporalclient

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/interceptor"
	"google.golang.org/grpc"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// DefaultAddress is the frontend address used when nothing is configured, which
// matches where `temporal server start-dev` listens.
const DefaultAddress = "localhost:7233"

// DefaultNamespace is the namespace used when nothing is configured.
const DefaultNamespace = "default"

// Config describes how to reach Temporal.
//
// The zero value is meaningful: it loads Temporal's environment configuration and
// falls back to a local development server, so a laptop needs no configuration at
// all. Fields set here take precedence over the environment, which is what lets a
// command-line flag override a profile.
type Config struct {
	// Address overrides the frontend address.
	Address string

	// Namespace overrides the namespace.
	Namespace string

	// Profile selects a profile from the TOML configuration file. Empty uses
	// TEMPORAL_PROFILE, or the default profile.
	Profile string

	// ConfigFile overrides the path of the TOML configuration file. Empty uses
	// TEMPORAL_CONFIG_FILE, or the conventional location.
	ConfigFile string

	// MetricsHandler receives the SDK's own metrics — task-queue backlog,
	// workflow-task latency, poller counts, activity failures. Nil keeps the
	// SDK's no-op default, which is what an unconfigured deployment wants and
	// what every deployment silently had before this field existed: the SDK
	// measured all of it and the options never carried a handler, so every
	// number was discarded.
	MetricsHandler client.MetricsHandler

	// Interceptors are installed on every client this configuration dials.
	//
	// The one that matters today is Temporal's own tracing interceptor, which
	// writes the caller's span context into the header a workflow is started
	// with — the half that makes a trace beginning at `flow run` continue into
	// the workflow rather than stopping at the server. Nil is the unconfigured
	// deployment and costs nothing.
	//
	// It lives on Config rather than being passed to Dial because Dial is not
	// the only place a client is born: [NewPool] dials one per mapped Temporal
	// namespace from this same value. A parameter would have instrumented the
	// fallback client and left every tenant's namespace untraced — the same
	// shape of gap MetricsHandler was added to close.
	Interceptors []interceptor.ClientInterceptor

	// Codec is the payload codec every client this configuration dials is built
	// with, together with the failure converter that must accompany it. See
	// [payloadcodec.Config].
	//
	// It lives here for the reason MetricsHandler and Interceptors do, and the
	// reason matters more for this field than for either of those: [NewPool]
	// dials one client per mapped Temporal namespace from this same value, so a
	// codec passed to Dial as a parameter would encrypt the fallback client's
	// payloads and write every tenant's namespace in plaintext. A seam with a
	// per-tenant hole is not a seam.
	//
	// The zero value is the null codec, which is byte-for-byte what a
	// deployment had before this field existed.
	Codec payloadcodec.Config
}

// Options resolves c into Temporal client options.
//
// Resolution order is environment configuration first, then any explicit
// overrides in c, then defaults for whatever is still unset. A missing or
// unreadable configuration file is not an error: the common case is not having
// one.
func (c Config) Options() (client.Options, error) {
	req := envconfig.LoadClientOptionsRequest{
		ConfigFilePath:    c.ConfigFile,
		ConfigFileProfile: c.Profile,
	}

	opts, err := envconfig.LoadClientOptions(req)
	if err != nil {
		return client.Options{}, fmt.Errorf("loading Temporal environment configuration: %w", err)
	}

	// Explicit values win over configuration, so a flag can override a profile.
	if c.Address != "" {
		opts.HostPort = c.Address
	}
	if c.Namespace != "" {
		opts.Namespace = c.Namespace
	}

	// Fall back to a local development server rather than failing, so that the
	// first-run experience needs no setup.
	if opts.HostPort == "" {
		opts.HostPort = DefaultAddress
	}
	if opts.Namespace == "" {
		opts.Namespace = DefaultNamespace
	}

	if c.MetricsHandler != nil {
		opts.MetricsHandler = c.MetricsHandler
	}

	// Appended rather than assigned: environment configuration does not set
	// interceptors today, and a future SDK that does should not have them
	// silently dropped by this package.
	opts.Interceptors = append(opts.Interceptors, c.Interceptors...)

	// Appended for the same reason, and here rather than in dial so that a
	// caller dialing from these options itself gets it too: [WithStartRequest]
	// is inert on a client without it.
	opts.ConnectionOptions.DialOptions = append(opts.ConnectionOptions.DialOptions,
		grpc.WithChainUnaryInterceptor(StartInterceptor()))

	// Checked here rather than at the first payload, and applied last so that
	// nothing above can leave a client half-configured: a data converter with
	// the codec and a failure converter without it is the fail-open pairing
	// [payloadcodec.Config.Apply] exists to make unrepresentable.
	if err := c.Codec.Validate(); err != nil {
		return client.Options{}, err
	}
	c.Codec.Apply(&opts)

	return opts, nil
}

// Dial connects to Temporal using c.
//
// The returned client is safe for concurrent use and should be closed when no
// longer needed. It is long-lived by design: it maintains connections and caches,
// so creating one per request would be wasteful and would defeat those caches.
func Dial(ctx context.Context, c Config) (client.Client, error) {
	cl, _, err := dial(ctx, c)
	return cl, err
}

// DialWithNamespace is [Dial], also reporting the namespace the returned client is
// dialed for.
//
// Use it wherever something other than the client itself needs that namespace
// named — Temporal's raw APIs take it as a request field, and
// `AddSearchAttributes` does too — because the alternative is resolving the same
// [Config] a second time, and a second resolution is not a copy of the first.
// [Config.Options] reads the process environment and a TOML file on disk every
// time it is called, so two calls straddling any amount of work can answer
// differently. What that produces is the failure this whole seam exists to
// prevent: a namespace recorded or requested that is not the one the client
// beside it is connected to, used to address a request that then succeeds
// against the wrong tenant's namespace, with nothing anywhere saying so
// (Codex, #1139).
//
// So the rule is one resolution per client, carried rather than recomputed —
// the same rule [NewPool] follows for its own fallback namespace, stated here
// for callers outside this package.
//
// Only the namespace is reported, not the whole of [client.Options]. Nothing has
// needed more, and the options carry credentials — see [Describe] for the care
// this package takes not to let them reach a formatter.
func DialWithNamespace(ctx context.Context, c Config) (client.Client, string, error) {
	cl, opts, err := dial(ctx, c)
	if err != nil {
		return nil, "", err
	}
	return cl, opts.Namespace, nil
}

// dial connects to Temporal using c and reports the options it resolved.
//
// It is the one place a client and the options it was dialed with are available
// together, and the three exported entry points are each a projection of it:
// [Dial] drops the options, [DialWithNamespace] keeps one field of them, and
// [NewPool] keeps that same field on the pool.
//
// The field they keep is the namespace, and the namespace they keep is
// emphatically not c.Namespace. That field is an *override*: [Config.Options]
// takes the namespace from a TOML profile or TEMPORAL_NAMESPACE, lets c.Namespace
// displace it only when non-empty, and falls back to [DefaultNamespace] when
// nothing named one. So c.Namespace is empty on every deployment that configured
// a namespace the ordinary way, and anything recording it would record "" for
// exactly those deployments.
//
// The resolved value is read back from the options the returned client was
// dialed with rather than recomputed, because a second resolution is not a copy
// of the first: Options reads the environment and a file on every call, so two
// calls straddling any amount of work can answer differently, and the caller
// would then hold a namespace its client is not dialed for.
// TestASecondResolutionCanDisagreeWithTheDial makes that happen rather than
// arguing about it. Nothing but the namespace leaves this function's callers:
// client options carry credentials, and this package's rule is that they are
// never held anywhere a formatter can reach them (see [Describe]).
func dial(ctx context.Context, c Config) (client.Client, client.Options, error) {
	opts, err := c.Options()
	if err != nil {
		return nil, client.Options{}, err
	}

	cl, err := client.DialContext(ctx, opts)
	if err != nil {
		return nil, client.Options{}, fmt.Errorf("connecting to Temporal at %s (namespace %q): %w",
			opts.HostPort, opts.Namespace, err)
	}
	return cl, opts, nil
}

// Describe returns a short, human-readable summary of where a configuration
// points, for logging at startup.
//
// It deliberately reports only the address, namespace, and which credential
// mechanism is in use — never the credential itself.
func Describe(opts client.Options) string {
	security := "no TLS"
	switch {
	case opts.Credentials != nil:
		security = "API key"
	case opts.ConnectionOptions.TLS != nil:
		if len(opts.ConnectionOptions.TLS.Certificates) > 0 {
			security = "mTLS"
		} else {
			security = "TLS"
		}
	}
	return fmt.Sprintf("%s namespace=%s (%s)", opts.HostPort, opts.Namespace, security)
}

// StartRequest is one caller's say over the StartWorkflowExecution calls
// made under a context carrying it; see [WithStartRequest].
type StartRequest struct {
	// ID is the request id those calls are sent with, in place of the SDK's
	// per-call random one. Empty keeps the SDK's.
	//
	// Temporal answers a start whose request id the current run already
	// carries with that run, before it consults the conflict policy, which
	// makes a deterministic request id the deduplication key for a start.
	ID string

	// applied is set by [StartInterceptor] when a start passes through it.
	applied atomic.Bool
}

// Applied reports whether a StartWorkflowExecution call made under a context
// carrying r has passed through [StartInterceptor]. False after such a call
// returned means the client was not dialed with the interceptor, and so an
// ID set on r would not have been sent either: the one signal a caller that
// depends on the ID has, before it depends on it.
func (r *StartRequest) Applied() bool {
	return r.applied.Load()
}

// startRequestKey is [WithStartRequest]'s context key.
type startRequestKey struct{}

// WithStartRequest returns ctx carrying r, which a client dialed with
// [StartInterceptor] applies to every StartWorkflowExecution call it makes
// under the returned context.
//
// The SDK keeps [client.StartWorkflowOptions]' own request id unexported,
// and it derives the gRPC call's context from the one given to
// ExecuteWorkflow, so the context is the one seam from a caller to that
// field.
func WithStartRequest(ctx context.Context, r *StartRequest) context.Context {
	return context.WithValue(ctx, startRequestKey{}, r)
}

// StartInterceptor returns the gRPC interceptor that makes
// [WithStartRequest] effective. A call whose context carries no
// [StartRequest], or that is not a StartWorkflowExecution, passes through
// untouched.
//
// [Config.Options] installs it on every client this package dials. A client
// dialed some other way (an embedder's own, or a test's) installs it through
// [client.ConnectionOptions.DialOptions]; without it, every start keeps the
// SDK's random request id, and [StartRequest.Applied] stays false.
func StartInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start, isStart := req.(*workflowservice.StartWorkflowExecutionRequest)
		r, _ := ctx.Value(startRequestKey{}).(*StartRequest)
		if isStart && r != nil {
			if r.ID != "" {
				start.RequestId = r.ID
			}
			r.applied.Store(true)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
