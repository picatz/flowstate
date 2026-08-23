package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	flowaudit "github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	logapi "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

const auditFlushTimeout = 5 * time.Second

type auditPolicy struct {
	destination string
	required    bool
}

// loadAuditPolicy is intentionally independent of telemetryConfigured. Audit
// policy is validated at process startup even when traces, metrics and ordinary
// logs are all disabled.
func loadAuditPolicy() (auditPolicy, error) {
	d := strings.ToLower(strings.TrimSpace(os.Getenv("FLOWSTATE_AUDIT_DESTINATION")))
	if d == "" {
		d = "none"
	}
	if d != "none" && d != "stderr" && d != "otlp" && d != "both" {
		return auditPolicy{}, fmt.Errorf("FLOWSTATE_AUDIT_DESTINATION must be stderr, otlp, both, or none, got %q", d)
	}
	r := strings.ToLower(strings.TrimSpace(os.Getenv("FLOWSTATE_AUDIT_REQUIRED")))
	if r != "" && r != "true" && r != "false" {
		return auditPolicy{}, fmt.Errorf("FLOWSTATE_AUDIT_REQUIRED must be true or false")
	}
	p := auditPolicy{destination: d, required: r == "true"}
	if p.required && p.destination == "none" {
		return auditPolicy{}, errors.New("audit is required but FLOWSTATE_AUDIT_DESTINATION is none")
	}
	return p, nil
}

type stderrAuditEmitter struct {
	mu  sync.Mutex
	out io.Writer
}

func (e *stderrAuditEmitter) Emit(_ context.Context, r flowaudit.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(e.out, "%s\n", b)
	return err
}

type otlpAuditEmitter struct {
	provider *sdklog.LoggerProvider
	logger   logapi.Logger
	required bool
}

func (e *otlpAuditEmitter) Emit(ctx context.Context, r flowaudit.Record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	var rec logapi.Record
	rec.SetTimestamp(r.Time)
	rec.SetBody(attribute.StringValue(string(b)))
	e.logger.Emit(ctx, rec)
	if e.required {
		return e.provider.ForceFlush(ctx)
	}
	return nil
}

type multiAuditEmitter []flowaudit.Emitter

func (m multiAuditEmitter) Emit(ctx context.Context, r flowaudit.Record) error {
	for _, e := range m {
		if err := e.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func initAudit(ctx context.Context, p auditPolicy, stderr io.Writer) (flowaudit.Emitter, func(context.Context) error, error) {
	var emitters []flowaudit.Emitter
	if p.destination == "stderr" || p.destination == "both" {
		emitters = append(emitters, &stderrAuditEmitter{out: stderr})
	}
	if p.destination == "otlp" || p.destination == "both" {
		ex, err := otlploghttp.New(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("configuring audit OTLP exporter: %w", err)
		}
		// A provider and batch processor owned only by audit: sampling and the
		// ordinary log provider can neither suppress nor flush this queue.
		provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(ex)))
		o := &otlpAuditEmitter{provider: provider, logger: provider.Logger("github.com/picatz/flowstate/audit"), required: p.required}
		emitters = append(emitters, o)
		return multiAuditEmitter(emitters), provider.Shutdown, nil
	}
	if len(emitters) == 0 {
		return flowaudit.NopEmitter{}, func(context.Context) error { return nil }, nil
	}
	return multiAuditEmitter(emitters), func(context.Context) error { return nil }, nil
}

var auditState struct {
	sync.Mutex
	started  bool
	emitter  flowaudit.Emitter
	shutdown func(context.Context) error
	err      error
}

func startAudit(ctx context.Context) (flowaudit.Emitter, error) {
	auditState.Lock()
	defer auditState.Unlock()
	if !auditState.started {
		auditState.started = true
		p, err := loadAuditPolicy()
		if err == nil {
			auditState.emitter, auditState.shutdown, err = initAudit(ctx, p, os.Stderr)
		}
		auditState.err = err
	}
	return auditState.emitter, auditState.err
}
func flushAudit() error {
	auditState.Lock()
	shutdown := auditState.shutdown
	auditState.Unlock()
	if shutdown == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), auditFlushTimeout)
	defer cancel()
	return shutdown(ctx)
}
