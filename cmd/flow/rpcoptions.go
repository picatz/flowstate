package main

import (
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"

	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// rpcHandlerOptions is the one option list both listeners hand
// flowstatev1connect.NewWorkflowServiceHandler — `flow server` and the
// control plane `flow server dev` embeds — so the two cannot disagree about
// what sits between the transport and a handler.
//
// The chain, outermost first:
//
//  1. Recovery (picatz/flowstate#1761), so that a handler panic is a log
//     line, a metric, an audit record and a CodeInternal answer rather than a
//     reset connection and a stdlib `http: panic serving` line. First, so
//     nothing below runs outside it; see server/recover.go.
//  2. Request validation against the schema's protovalidate rules. No error
//     to handle since connectrpc.com/validate v0.6.0: the interceptor builds
//     its validator lazily on first use, so construction cannot fail.
//  3. OpenTelemetry. Built here, at the moment the caller reaches this, and
//     not earlier: otelconnect captures the global tracer provider and
//     propagator at construction, so an interceptor built before
//     startTelemetry keeps the no-op ones for the life of the process — the
//     ordering [temporalConfig] states.
//
// And the read bound beside them: connect-go defaults to unlimited, and an
// anonymous caller must not choose how much this process allocates, whether
// with one request or a compressed one that inflates enormously.
func rpcHandlerOptions(flowServer *server.FlowstateServer, logger *slog.Logger) ([]connect.HandlerOption, error) {
	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		return nil, fmt.Errorf("error creating OpenTelemetry interceptor: %w", err)
	}

	return []connect.HandlerOption{
		connect.WithInterceptors(
			flowServer.RecoverInterceptor(logger),
			validate.NewInterceptor(),
			otelInterceptor,
		),
		connect.WithReadMaxBytes(maxRequestBytes),
	}, nil
}
