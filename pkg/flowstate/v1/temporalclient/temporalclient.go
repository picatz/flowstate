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

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/interceptor"
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

	return opts, nil
}

// Dial connects to Temporal using c.
//
// The returned client is safe for concurrent use and should be closed when no
// longer needed. It is long-lived by design: it maintains connections and caches,
// so creating one per request would be wasteful and would defeat those caches.
func Dial(ctx context.Context, c Config) (client.Client, error) {
	opts, err := c.Options()
	if err != nil {
		return nil, err
	}

	cl, err := client.DialContext(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("connecting to Temporal at %s (namespace %q): %w",
			opts.HostPort, opts.Namespace, err)
	}
	return cl, nil
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
