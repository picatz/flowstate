package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"connectrpc.com/authn"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/codecserver"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
)

// defaultCodecServerAddress is loopback, so the zero-configuration server is
// reachable from this machine's browser and nowhere else.
const defaultCodecServerAddress = "127.0.0.1:8089"

func newCodecServeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve Temporal's remote codec protocol, decoding history for authorized callers",
		Long: "Serve the remote payload codec that Temporal's Web UI (Codec Server setting) and CLI " +
			"(`--codec-endpoint`) call to show encrypted history as plaintext, using this deployment's " +
			"payload keyring. Every caller is authenticated against the trust policy, must hold the " +
			"`payload.decode` (or `payload.encode`) action explicitly, and may address only the Temporal " +
			"namespace its own tenant maps to. A namespace shared by several tenants is refused unless " +
			"`--allow-shared-namespaces` is given, because a payload does not say whose it is. Responses " +
			"are never cacheable, and every decision is written to the audit trail. " +
			"Workers and servers do not use this: they decrypt in process with the same keyring.",
		Args: cobra.NoArgs,
		Example: `# Serve Temporal Web on temporal.example.com, behind TLS:
flow codec serve --listen 0.0.0.0:8089 \
  --auth-policy /etc/flowstate/auth.yaml \
  --payload-keyring /etc/flowstate/payload-keyring.yaml \
  --cors-origin https://temporal.example.com \
  --tls-cert-file codec.crt --tls-key-file codec.key

# Local development, loopback only, against a dev keyring:
flow codec serve --insecure-no-auth --payload-keyring keyring.yaml \
  --cors-origin http://localhost:8233`,
		RunE: runCodecServe,
	}

	cmd.Flags().String("listen", cmp.Or(os.Getenv("FLOWSTATE_CODEC_ADDRESS"), defaultCodecServerAddress),
		"address to listen on (default $FLOWSTATE_CODEC_ADDRESS, or loopback)")
	cmd.Flags().String("auth-policy", os.Getenv("FLOWSTATE_AUTH_POLICY"),
		"path to the trust policy (YAML) that authenticates callers, assigns their actions, and maps "+
			"each tenant to its Temporal namespace (default $FLOWSTATE_AUTH_POLICY)")
	cmd.Flags().Bool("insecure-no-auth", false,
		"serve any caller with no authentication or authorization, for local development: refused on "+
			"any address but loopback")
	cmd.Flags().StringArray("cors-origin", nil,
		"a browser origin allowed to call this server, exactly, such as https://temporal.example.com "+
			"(repeatable); unset admits no browser")
	cmd.Flags().Bool("allow-shared-namespaces", false,
		"decode in a Temporal namespace several tenants share, accepting that any tenant authorized "+
			"there can read every tenant's payloads in it")
	cmd.Flags().String("temporal-namespace", "",
		"the Temporal namespace tenants the trust policy does not map run in (default: TEMPORAL_NAMESPACE, "+
			"the Temporal profile, or \"default\")")
	addPayloadEncryptionFlags(cmd)
	addTLSFlags(cmd)
	addAuditRequiredFlag(cmd)

	return cmd
}

func runCodecServe(cmd *cobra.Command, _ []string) error {
	logger := infraLogger()
	listen, _ := cmd.Flags().GetString("listen")
	origins, _ := cmd.Flags().GetStringArray("cors-origin")
	allowShared, _ := cmd.Flags().GetBool("allow-shared-namespaces")
	namespace, _ := cmd.Flags().GetString("temporal-namespace")
	authCfg := authFlagsOf(cmd)

	if authCfg.insecure && !isLoopbackAddress(listen) {
		return fmt.Errorf("--insecure-no-auth serves plaintext history to anyone who can reach the "+
			"listener, and %s is not loopback: bind 127.0.0.1, or configure --auth-policy", listen)
	}

	tlsFlags := tlsFlagsOf(cmd)
	tlsCfg, err := serverTLSConfig(tlsFlags)
	if err != nil {
		return err
	}
	if err := refusePlaintextListener(listen, tlsCfg, tlsFlags.tlsTerminatedUpstream); err != nil {
		return err
	}

	flags := payloadEncryptionFlagsOf(cmd)
	codecs, err := payloadCodecConfig(flags)
	if err != nil {
		return err
	}

	verifier, policy, err := authVerifier(authCfg)
	if err != nil {
		return err
	}

	auditRequired, _ := cmd.Flags().GetBool(auditRequiredFlag)
	recorder, err := startAudit(cmd.Context(), auditRequired)
	if err != nil {
		return fmt.Errorf("configuring the audit trail: %w", err)
	}
	defer flushAudit()

	// Resolved the way every command that dials Temporal resolves it: the
	// flag, then TEMPORAL_NAMESPACE or the profile, then "default".
	resolved, err := temporalclient.Config{Namespace: namespace}.Options()
	if err != nil {
		return err
	}
	defaultNamespace := resolved.Namespace
	opts := codecserver.Options{
		Codecs:                codecs,
		DefaultNamespace:      defaultNamespace,
		AllowSharedNamespaces: allowShared,
		Insecure:              authCfg.insecure,
		AllowedOrigins:        origins,
		Auditor:               recorder,
		Logger:                logger,
	}
	if policy != nil {
		opts.Tenancy = policy.Tenancy
	}
	handler, err := codecserver.New(opts)
	if err != nil {
		return err
	}

	mux := codecServeHandler(logger, verifier, handler)

	httpServer := &http.Server{
		Addr:                listen,
		Handler:             mux,
		TLSConfig:           tlsCfg,
		ReadHeaderTimeout:   5 * time.Second,
		ReadTimeout:         30 * time.Second,
		WriteTimeout:        codecWriteTimeout(codecs),
		IdleTimeout:         time.Minute,
		MaxHeaderBytes:      64 << 10,
		MaxHeaderValueCount: maxHeaderValueCount,
	}

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", listen, err)
	}

	logger.Info("serving the payload codec", "address", listener.Addr().String(), "tls", tlsCfg != nil,
		"namespaces", len(codecs.Namespaces), "cors_origins", origins, "shared_namespaces_allowed", allowShared)
	if authCfg.insecure {
		logger.Warn("codec server authentication is disabled: anyone on this machine can decode history",
			"use", "local development only")
	}

	serveErr := make(chan error, 1)
	go func() {
		var err error
		if tlsCfg != nil {
			err = httpServer.ServeTLS(listener, "", "")
		} else {
			err = httpServer.Serve(listener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("serving on %s: %w", listen, err)
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-cmd.Context().Done():
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return httpServer.Shutdown(ctx)
	}
}

// codecServeHandler is the codec server's routing: liveness unauthenticated,
// a browser's CORS preflight answered before authentication (it carries no
// credentials by specification, and it reveals and releases nothing), and
// everything else authenticated by the trust policy's verifier before the
// codec handler sees it. The handler's own headers go on first, so an
// authentication refusal reaches an allowed browser origin as a readable 401
// rather than an opaque CORS failure.
func codecServeHandler(logger *slog.Logger, verifier auth.Verifier, handler *codecserver.Handler) http.Handler {
	authenticated := authn.NewMiddleware(auth.NewAuthenticator(verifier,
		auth.WithFailureObserver(func(ctx context.Context, req *http.Request, err error) {
			logger.WarnContext(ctx, "codec server: rejected unauthenticated request",
				"peer", req.RemoteAddr, "reason", auth.PublicReason(err))
		})).Authenticate).Wrap(handler)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler())
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			handler.ServeHTTP(w, r)
			return
		}
		if !handler.Headers(w, r) {
			return
		}
		authenticated.ServeHTTP(w, r)
	}))
	return mux
}

// codecWriteTimeout is how long `flow codec serve` gives a response: thirty
// seconds, or enough for a decode that must first ask a key provider to
// unwrap and may wait out a login and the call, whichever is longer.
//
// A request that unwraps more unseen data keys than that allows still has its
// response cut off; the keys it unwrapped are cached, so the client's retry
// is answered from them.
func codecWriteTimeout(codecs payloadcodec.Config) time.Duration {
	const floor = 30 * time.Second
	timed, ok := codecs.Codec.(interface{ ProviderTimeout() time.Duration })
	if !ok {
		return floor
	}
	return max(floor, 2*timed.ProviderTimeout()+10*time.Second)
}
