package audit

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ScopeName is the instrumentation scope every audit record carries.
//
// Its own scope, on its own provider, so that a collector can route audit
// records without having to recognise them by shape, and so that nothing an
// operator does to the ordinary log pipeline — sampling it, filtering it,
// turning it off — can reach these.
const ScopeName = "flowstate.audit"

// EventName names the record for a consumer that keys on events rather than
// on attributes. One event, because there is one kind of record here.
const EventName = "flowstate.audit.authorization_decision"

// Attribute keys. Written out once here rather than spelled at each use: a
// consumer's query is a contract, and a typo in one of these is a record that
// silently stops matching it.
const (
	attrAction       = "flowstate.audit.action"
	attrDecision     = "flowstate.audit.decision"
	attrRPC          = "flowstate.audit.rpc"
	attrMCPTool      = "flowstate.audit.mcp.tool"
	attrResourceKind = "flowstate.audit.resource.kind"
	attrResourceKey  = "flowstate.audit.resource.key"
	attrDenyCode     = "flowstate.audit.deny_code"

	// The worker's half (picatz/flowstate#1379). Spelled flat, like deny_code
	// and unlike the dotted identity keys, because they are fields of the
	// record rather than of something inside it.
	attrEnforcementPoint = "flowstate.audit.enforcement_point"
	attrRule             = "flowstate.audit.rule"
	attrAttempt          = "flowstate.audit.attempt"
	attrDispatchID       = "flowstate.audit.dispatch_id"

	attrSubject    = "flowstate.audit.identity.subject"
	attrIssuer     = "flowstate.audit.identity.issuer"
	attrNamespace  = "flowstate.audit.identity.namespace"
	attrDeployment = "flowstate.audit.identity.deployment"
	attrIssuerName = "flowstate.audit.identity.issuer_name"
	attrRole       = "flowstate.audit.identity.role"
)

// NewLogEmitter sends records through an audit-owned LoggerProvider.
//
// The provider must be the audit's own, not the global one. The global logger
// provider is nil by default and a no-op when unconfigured, which is invariant
// 8 working correctly for telemetry and exactly wrong here: an audit trail
// that disappears when nobody configured a collector is not one.
//
// Pair it with [NewSyncProcessor] when the recorder is [Required]; see that
// function for why a batch processor cannot be.
func NewLogEmitter(provider *sdklog.LoggerProvider) Emitter {
	if provider == nil {
		return nil
	}

	return &logEmitter{logger: provider.Logger(ScopeName)}
}

type logEmitter struct {
	logger otellog.Logger
}

func (e *logEmitter) Emit(ctx context.Context, record *v1.AuditRecord) error {
	var out otellog.Record

	out.SetEventName(EventName)
	out.SetTimestamp(record.GetDecidedAt().AsTime())
	out.SetObservedTimestamp(record.GetDecidedAt().AsTime())

	// A constant body, and the decision in attributes. A body assembled from
	// values would be free text arriving at a durable sink, which is the one
	// thing this record does not carry — see the redaction note in
	// proto/flowstate/v1/audit.proto.
	out.SetBody(attribute.StringValue(EventName))

	if record.GetDecision() == v1.AuditDecision_AUDIT_DECISION_DENY {
		out.SetSeverity(otellog.SeverityWarn)
		out.SetSeverityText("WARN")
	} else {
		out.SetSeverity(otellog.SeverityInfo)
		out.SetSeverityText("INFO")
	}

	attrs := []attribute.KeyValue{
		attribute.String(attrAction, record.GetAction().String()),
		attribute.String(attrDecision, record.GetDecision().String()),
		attribute.String(attrResourceKind, record.GetResourceKind().String()),
		attribute.String(attrResourceKey, record.GetResourceKey()),
		attribute.String(attrDenyCode, record.GetDenyCode().String()),
	}
	if record.GetRpc() != "" {
		attrs = append(attrs, attribute.String(attrRPC, record.GetRpc()))
	}
	if record.GetMcpTool() != "" {
		attrs = append(attrs, attribute.String(attrMCPTool, record.GetMcpTool()))
	}
	// Present exactly on an enforcement record, which is how a consumer tells
	// the two halves of one trail apart: absent rather than UNSPECIFIED,
	// because a query for the worker's decisions should select on the
	// attribute existing rather than on a sentinel value. The schema's own
	// message rule holds the same line from the other side — an enforcement
	// point and an action are never both set.
	if record.GetEnforcementPoint() != v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_UNSPECIFIED {
		attrs = append(attrs, attribute.String(attrEnforcementPoint, record.GetEnforcementPoint().String()))
	}

	// Present only on a record about a dispatch attempt, for the reason the
	// enforcement point is: a consumer separating one attempt's decision from
	// another's should select on the attribute existing rather than on a zero.
	if record.GetAttempt() != 0 {
		attrs = append(attrs, attribute.Int64(attrAttempt, int64(record.GetAttempt())))
	}
	if record.GetDispatchId() != "" {
		attrs = append(attrs, attribute.String(attrDispatchID, record.GetDispatchId()))
	}

	// Verbatim, because it is the operator's own rule and the seam that set it
	// already bounded it and held it to "only a rule that matched" — see
	// AuditRecord.rule for what that excludes. Truncating or normalizing here
	// would make a collector's copy of a decision disagree with stderr's.
	if record.GetRule() != "" {
		attrs = append(attrs, attribute.String(attrRule, record.GetRule()))
	}

	if record.GetIssuerName() != "" {
		attrs = append(attrs, attribute.String(attrIssuerName, record.GetIssuerName()))
	}
	if record.GetRole() != "" {
		attrs = append(attrs, attribute.String(attrRole, record.GetRole()))
	}

	if identity := record.GetIdentity(); identity != nil {
		attrs = append(attrs,
			attribute.String(attrSubject, identity.GetSubject()),
			attribute.String(attrIssuer, identity.GetIssuer()),
			attribute.String(attrNamespace, identity.GetNamespace()),
			attribute.String(attrDeployment, identity.GetDeployment()),
		)
	}

	out.AddAttributes(attrs...)

	// The provider's Emit returns nothing, so the error comes back through the
	// context: see [NewSyncProcessor].
	slot := &errorSlot{}
	e.logger.Emit(contextWithErrorSlot(ctx, slot), out)

	return slot.err()
}

// NewSyncProcessor is the processor a required audit sink needs: it exports
// synchronously, on the calling goroutine, and reports the exporter's error
// back to the caller that emitted the record.
//
// Both halves are the point.
//
// A [sdklog.BatchProcessor] cannot back a required sink. It is asynchronous,
// so under it a "required" emitter proves nothing at the decision point — the
// export it would have failed at happens after the request has already been
// answered, which means a deployment that asked for "an action that cannot be
// recorded does not happen" would get "an action that could not be recorded
// happened anyway, and something logged about it later". Batch stays correct
// for a deployment that has not asked for required.
//
// [sdklog.NewSimpleProcessor] gets the synchrony right and loses the error:
// LoggerProvider's Emit has no error to return, so the SDK hands OnEmit's
// error to the global error handler and the caller never learns. This wraps
// the same synchronous export and puts the error in a slot the caller placed
// in the context — per call, so there is no shared state to race on, and no
// error from one request can be attributed to another.
func NewSyncProcessor(exporter sdklog.Exporter) sdklog.Processor {
	return &syncProcessor{exporter: exporter}
}

type syncProcessor struct {
	exporter sdklog.Exporter

	mu   sync.Mutex
	done bool
}

func (p *syncProcessor) Enabled(context.Context, sdklog.EnabledParameters) bool {
	return true
}

func (p *syncProcessor) OnEmit(ctx context.Context, record *sdklog.Record) error {
	p.mu.Lock()
	done := p.done
	p.mu.Unlock()

	if done {
		// After Shutdown a processor performs no operation, per the SDK's own
		// contract. A required recorder still learns: the shutdown is reported
		// to whoever emitted, rather than a record being quietly dropped.
		err := errors.New("audit: the log processor is shut down")
		errorSlotFromContext(ctx).set(err)

		return err
	}

	// Cloned because the SDK's Record is not concurrent safe and an exporter
	// may hold it; the same reason SimpleProcessor clones.
	err := p.exporter.Export(ctx, []sdklog.Record{record.Clone()})
	if err != nil {
		err = fmt.Errorf("audit: exporting the record: %w", err)
	}

	errorSlotFromContext(ctx).set(err)

	return err
}

func (p *syncProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.done = true
	p.mu.Unlock()

	return p.exporter.Shutdown(ctx)
}

func (p *syncProcessor) ForceFlush(ctx context.Context) error {
	// Nothing is held: every record was exported before OnEmit returned. This
	// still reaches the exporter, because an exporter may buffer on its own.
	return p.exporter.ForceFlush(ctx)
}

// errorSlot carries one emit's export error back to the goroutine that emitted
// it.
type errorSlot struct {
	mu   sync.Mutex
	err_ error
}

func (s *errorSlot) set(err error) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.err_ = errors.Join(s.err_, err)
}

func (s *errorSlot) err() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.err_
}

type errorSlotKey struct{}

func contextWithErrorSlot(ctx context.Context, slot *errorSlot) context.Context {
	return context.WithValue(ctx, errorSlotKey{}, slot)
}

// errorSlotFromContext answers nil when there is none, and a nil *errorSlot's
// methods do nothing: a record emitted through this processor by something
// other than a [Recorder] — the SDK's own machinery, a test — is exported
// exactly as it otherwise would be.
func errorSlotFromContext(ctx context.Context) *errorSlot {
	slot, _ := ctx.Value(errorSlotKey{}).(*errorSlot)

	return slot
}
