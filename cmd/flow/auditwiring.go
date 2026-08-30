package main

import (
	"context"
	"os"
	"sync"

	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/spf13/cobra"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// auditRequiredFlag is how an operator says a decision that cannot be recorded
// must not happen.
//
// Named for what is being accepted rather than for the check being skipped, the
// same idiom [allowUnversionedFlag] states its reasoning for: a deployment's
// refusal to operate belongs at the command, with a --help entry, not in an
// environment variable documented only in prose — picatz/flowstate#1018's
// design comment cites --allow-unversioned-interpreter as the precedent to
// follow rather than a second spelling.
const auditRequiredFlag = "audit-required"

// addAuditRequiredFlag registers --audit-required on a server command.
//
// Called once per command (`flow server`, `flow server dev`) rather than
// shared through a persistent flag on a parent, in the same shape every other
// server-only flag in this file takes.
func addAuditRequiredFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(auditRequiredFlag, false,
		"fail a request whose authorization decision could not be written to every audit sink, "+
			"trading availability for a complete trail: an operator's collector outage becomes an "+
			"outage of this service rather than a gap in the record. Auditing itself is always on — "+
			"stderr carries every decision unconditionally, and OTEL_LOGS_EXPORTER/"+
			"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT add an OTel sink — this flag only decides what a sink's "+
			"own failure does to the caller")
}

// auditState mirrors [telemetryState]: a package-level fact — the process's one
// audit recorder — guarded by a mutex rather than left to sync.Once alone, so
// [flushAudit] can read the shutdown safely regardless of which goroutine is
// tearing the command down.
var auditState struct {
	mu       sync.Mutex
	started  bool
	recorder *audit.Recorder
	shutdown func(context.Context)
	err      error
}

// startAudit builds the process's audit recorder once and remembers the flush.
//
// Memoized for the same reason [startTelemetry] is: `flow server` resolves its
// Temporal configuration more than once when the trust policy maps tenants onto
// namespaces, and a second call must not build a second recorder that
// overwrites the server's reference to the first, leaving that one's OTel
// LoggerProvider alive, unreachable and unflushed.
//
// Auditing has no off switch: every deployment gets stderr, unconditionally,
// because a record that depends on an operator remembering to opt in is not an
// audit trail — it is a feature nobody enabled until the day they needed it.
// required is the only knob cmd/flow exposes, and it only changes what a sink's
// own failure does to the caller; see --audit-required's help and
// [audit.Required].
//
// The OTel sink reuses cmd/flow's own telemetry wiring rather than inventing a
// second way to read OTEL_LOGS_EXPORTER/OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: the
// same [telemetryConfigFromEnv], the same [telemetryResource], the same
// [newLogExporter] seam a test already reassigns. What differs from
// [initTelemetry] is the provider: audit builds and owns its own
// *sdklog.LoggerProvider, on its own scope, never the global one — see
// pkg/flowstate/v1/audit's package doc for why the global provider is exactly
// wrong here — and its processor depends on required: a required recorder needs
// [audit.NewSyncProcessor], because a batch processor's export happens after
// the request has already been answered and would prove nothing at the decision
// point; a best-effort recorder uses an ordinary [sdklog.BatchProcessor], the
// same shape every other log signal in this binary uses, so it costs the
// process nothing beyond what telemetry logs already would.
func startAudit(ctx context.Context, required bool) (*audit.Recorder, error) {
	auditState.mu.Lock()
	defer auditState.mu.Unlock()

	if !auditState.started {
		auditState.started = true
		auditState.recorder, auditState.shutdown, auditState.err = initAudit(ctx, required)
	}

	return auditState.recorder, auditState.err
}

// initAudit is [startAudit]'s single build path, split out so the memo above
// stays the only place that decides whether this has already run.
func initAudit(ctx context.Context, required bool) (*audit.Recorder, func(context.Context), error) {
	opts := []audit.Option{}
	shutdowns := []func(context.Context){}
	if required {
		opts = append(opts, audit.Required())
	} else {
		// Best-effort auditing must not turn a stalled process logger into RPC
		// backpressure. Keep the unconditional stderr floor, but put its writes
		// behind a bounded queue; Required mode deliberately retains the
		// synchronous default because returning success must prove the write.
		stderr, flush := audit.NewAsyncWriterEmitter(os.Stderr, audit.DefaultWriterQueueSize)
		opts = append(opts, audit.WithoutStderr(), audit.WithEmitter(stderr))
		shutdowns = append(shutdowns, func(ctx context.Context) { _ = flush(ctx) })
	}

	config, err := telemetryConfigFromEnv()
	if err != nil {
		return nil, nil, err
	}

	if config.logs {
		res, err := telemetryResource(ctx)
		if err != nil {
			return nil, nil, err
		}

		exporter, err := newLogExporter(ctx)
		if err != nil {
			return nil, nil, err
		}

		// Required mode needs the synchronous path: see [audit.NewSyncProcessor]'s
		// doc for why a batch processor cannot back it. A recorder that has not
		// asked for required gets the ordinary batch processor, the same shape
		// [initTelemetry] gives every other log signal, so a best-effort audit
		// trail costs this process nothing beyond what telemetry logs already do.
		var processor sdklog.Processor
		if required {
			processor = audit.NewSyncProcessor(exporter)
		} else {
			processor = sdklog.NewBatchProcessor(exporter)
		}

		provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(processor), sdklog.WithResource(res))
		opts = append(opts, audit.WithEmitter(audit.NewLogEmitter(provider)))
		shutdowns = append(shutdowns, func(ctx context.Context) { _ = provider.Shutdown(ctx) })
	}

	recorder, err := audit.NewRecorder(opts...)
	if err != nil {
		return nil, nil, err
	}

	shutdown := func(ctx context.Context) {
		for _, stop := range shutdowns {
			stop(ctx)
		}
	}
	return recorder, shutdown, nil
}

// flushAudit pushes whatever the audit OTel sink has buffered before the
// process leaves, including records accepted by the asynchronous best-effort
// stderr sink.
//
// Mirrors [flushTelemetry]: best-effort, safe to call when auditing was never
// started, and safe to call twice. Required stderr and OTel paths already
// export synchronously at every call; shutdown still closes connections and
// gives the best-effort stderr queue a bounded chance to drain.
func flushAudit() {
	auditState.mu.Lock()
	shutdown := auditState.shutdown
	auditState.mu.Unlock()

	if shutdown == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
	defer cancel()

	shutdown(ctx)
}
