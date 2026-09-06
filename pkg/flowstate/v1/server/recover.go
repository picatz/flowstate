package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// What a handler panic becomes (picatz/flowstate#1761).
//
// Without this interceptor a panic in an RPC handler is caught by net/http,
// one connection up: the connection is reset, `http: panic serving …` goes
// through the standard library's logger, the caller sees a transport error,
// and the audit trail — which wrote an allow before the handler ran, because
// it records the decision and not the effect — holds a record saying the
// request was permitted and nothing saying it was never answered. No metric
// moves. Temporal's SDK converts a workflow or activity panic into a failure
// on the worker side; this is the server's equivalent, and it is first in the
// chain so that nothing between the transport and the handler runs outside
// it.
//
// Four effects, in this order, on the goroutine that panicked: the process log
// gets the panic value and its stack at ERROR, with the method and the
// caller's attested coordinates and never the payload;
// [metricschema.InstrumentServerPanics] increments under the method; the audit
// trail gets an INTERNAL_ERROR record carrying the same correlation id the
// request's allow record carried; and the caller receives CodeInternal whose
// message is that correlation id and nothing else — no stack, no panic text,
// because either can quote the request that caused it. The LSP dispatcher
// (flowfile/lsp/server.go) and the plugin SDK's task service make the same
// split between what is logged and what is returned, and this follows their
// shape.
//
// The correlation id is minted here, before the handler runs, and placed in
// the context so that [FlowstateServer.auditSubject] stamps it on every record
// the request writes. That is what lets an operator join the allow to the
// failure: the trail's records carry no caller-chosen request id by design
// (see AuditRecord in proto/flowstate/v1/audit.proto), and a server-minted one
// is the only kind that can reach a durable sink.
//
// [connect.WithRecover] is the library's own recover interceptor and would do
// the catching; it is not used because it hands its callback the request only
// after the panic, and the correlation id has to exist before the handler's
// first audit record. The catching itself — including the http.ErrAbortHandler
// re-panic net/http relies on — is the same as connect's.

// serverMeterName is the instrumentation scope the server's own instruments
// are attributed to: the package, the same scope the webhook delivery span
// already carries.
const serverMeterName = webhookTracerName

// maxPanicTextBytes bounds the panic value's rendering on the log line. A
// panic is `panic(anything)`, and the anything can be as large as the request
// that provoked it; the stack beside it is what an operator reads, and a
// value beyond this is a value nobody was going to read on a log line.
const maxPanicTextBytes = 4096

// RecoverInterceptor returns the interceptor that converts a handler panic into
// a log line, a metric, an audit record, and a CodeInternal answer, as this
// file's doc describes. Install it first in the handler's interceptor chain.
//
// logger is the process logger; the line it writes is the one place the
// panic's value and stack go. A nil logger discards, which is a library
// default and not a deployment's answer.
func (s *FlowstateServer) RecoverInterceptor(logger *slog.Logger) connect.Interceptor {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return &recoverInterceptor{server: s, logger: logger}
}

type recoverInterceptor struct {
	server *FlowstateServer
	logger *slog.Logger
}

// WrapUnary implements [connect.Interceptor].
func (i *recoverInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (_ connect.AnyResponse, retErr error) {
		if req.Spec().IsClient {
			return next(ctx, req)
		}

		id := uuid.NewString()
		ctx = audit.ContextWithCorrelationID(ctx, id)

		defer func() {
			if r := recover(); r != nil {
				// net/http checks for ErrAbortHandler with ==, so this does
				// too: it is the one panic a handler raises on purpose, to
				// abort the response without a log line, and swallowing it
				// here would answer a request the handler chose not to.
				if r == http.ErrAbortHandler { //nolint:errorlint
					panic(r)
				}
				retErr = i.recovered(ctx, req.Spec(), id, r)
			}
		}()

		return next(ctx, req)
	}
}

// WrapStreamingHandler implements [connect.Interceptor].
func (i *recoverInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) (retErr error) {
		id := uuid.NewString()
		ctx = audit.ContextWithCorrelationID(ctx, id)

		defer func() {
			if r := recover(); r != nil {
				if r == http.ErrAbortHandler { //nolint:errorlint
					panic(r)
				}
				retErr = i.recovered(ctx, conn.Spec(), id, r)
			}
		}()

		return next(ctx, conn)
	}
}

// WrapStreamingClient implements [connect.Interceptor]. This is a handler-side
// interceptor; a client stream passes through untouched.
func (i *recoverInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// recovered performs the four effects for one caught panic and returns the
// error the caller is answered with.
//
// Called on the panicking goroutine, inside the deferred function, which is
// what makes debug.Stack here the stack of the panic rather than of the
// recovery. The audit record names the method and the caller and no
// resource: the interceptor does not read the request, so it cannot say
// which run or schedule was addressed, and an unspecified resource is the
// honest answer rather than a guess.
func (i *recoverInterceptor) recovered(ctx context.Context, spec connect.Spec, id string, value any) error {
	method := rpcMethodName(spec.Procedure)
	identity := i.server.identityFor(ctx)

	i.logger.ErrorContext(ctx, "recovered from panic in RPC handler",
		"rpc", method,
		"namespace", identity.GetNamespace(),
		"subject", identity.GetSubject(),
		"issuer", identity.GetIssuer(),
		"correlation_id", id,
		"panic", panicText(value),
		"stack", string(debug.Stack()),
	)

	// The provider is read per call, for the reason taskmetrics.go gives:
	// telemetry is configured partway through the process's assembly, and an
	// instrument built earlier would hold the no-op provider for good.
	panics, _ := otel.GetMeterProvider().Meter(serverMeterName).Int64Counter(
		metricschema.InstrumentServerPanics,
		metric.WithDescription("RPC handler panics recovered by the server, by method"))
	panics.Add(ctx, 1, metricschema.WithAttributes(
		attribute.String(metricschema.RPCMethod, method),
	))

	subject := i.server.auditSubject(ctx, method, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_UNSPECIFIED, "")
	if err := i.server.audit.InternalError(ctx, subject); err != nil {
		// A required recorder that could not record. The caller is refused
		// either way — the answer below is already an internal error — so the
		// operator's failure goes where the operator reads, rather than
		// replacing the correlation id in the caller's message with a sink's
		// own words.
		i.logger.ErrorContext(ctx, "the recovered panic could not be recorded in the audit trail",
			"rpc", method, "correlation_id", id, "error", err)
	}

	return connect.NewError(connect.CodeInternal,
		fmt.Errorf("internal error; correlation id %s", id))
}

// rpcMethodName reduces a connect procedure — "/flowstate.v1.WorkflowService/Get"
// — to the schema method name the audit record and the metric both key on,
// so the record's rpc field is the same spelling the handler's own allow
// record used.
func rpcMethodName(procedure string) string {
	return procedure[strings.LastIndexByte(procedure, '/')+1:]
}

// panicText renders a panic value for the log line, bounded, and without
// letting the value's own String or Error method take the recovery down with
// a second panic.
func panicText(value any) (text string) {
	defer func() {
		if recover() != nil {
			text = fmt.Sprintf("<%T could not be formatted>", value)
		}
	}()

	text = fmt.Sprint(value)
	if len(text) > maxPanicTextBytes {
		text = strings.ToValidUTF8(text[:maxPanicTextBytes], "") + "…[truncated]"
	}

	return text
}
