package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// The four effects of a handler panic (picatz/flowstate#1761), proved over a
// real HTTP/2 connection rather than by calling the interceptor's functions,
// because the defect being closed is what net/http does one layer up when
// nothing catches the panic: a reset connection and a `panic serving` line
// through the standard library's logger. The server's ErrorLog is captured for
// exactly that line, and the negative control at the bottom shows the capture
// sees it when the interceptor is absent.

// panickingHandler is the stub: Get writes the allow record a real handler
// writes, through the same seam, and then panics; List blocks until released
// and answers normally, so a second request can be in flight on the same
// connection while the first one dies.
type panickingHandler struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler

	server *FlowstateServer

	// started is closed when List is running on the server; release lets it
	// return. Both are nil when the test does not use List.
	started chan struct{}
	release chan struct{}
}

func (h *panickingHandler) Get(ctx context.Context, req *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	if err := h.server.auditAllow(ctx, "Get", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, req.Msg.GetWorkflowId()); err != nil {
		return nil, err
	}

	panic("boom: a nil map in a rarely taken branch")
}

func (h *panickingHandler) List(context.Context, *connect.Request[v1.ListRequest]) (*connect.Response[v1.ListResponse], error) {
	close(h.started)
	<-h.release

	return connect.NewResponse(&v1.ListResponse{}), nil
}

// lockedEmitter is [recordingEmitter] for records written from handler
// goroutines.
type lockedEmitter struct {
	mu      sync.Mutex
	records []*v1.AuditRecord
}

func (e *lockedEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, record)

	return nil
}

func (e *lockedEmitter) all() []*v1.AuditRecord {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]*v1.AuditRecord(nil), e.records...)
}

// lockedBuffer is a bytes.Buffer the server's goroutines and the test can
// share.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// withTestPrincipal installs the attested caller the authenticator would
// have, so the interceptor's log line and audit record have a principal to
// name.
func withTestPrincipal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := auth.ContextWithPrincipal(r.Context(), auth.Principal{
			Issuer:     "https://issuer.example",
			IssuerName: "production-issuer",
			Subject:    "agent-1",
			Namespace:  "acme",
			Role:       "operator",
			Claims:     map[string]any{"private": "claim-value"},
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// recoveringServer is one HTTP/2 server with the interceptor installed, its
// ErrorLog captured, and the process log and audit sink the test reads back.
type recoveringServer struct {
	http     *httptest.Server
	client   flowstatev1connect.WorkflowServiceClient
	handler  *panickingHandler
	errorLog *lockedBuffer
	slogOut  *lockedBuffer
	sink     *lockedEmitter
}

func startRecoveringServer(t *testing.T) *recoveringServer {
	t.Helper()

	sink := &lockedEmitter{}
	s := mustNew(t, nil, WithAudit(recorderFor(t, sink)))

	slogOut := &lockedBuffer{}
	logger := slog.New(slog.NewJSONHandler(slogOut, nil))

	handler := &panickingHandler{
		server:  s,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(handler,
		connect.WithInterceptors(s.RecoverInterceptor(logger))))

	errorLog := &lockedBuffer{}
	srv := httptest.NewUnstartedServer(withTestPrincipal(mux))
	srv.EnableHTTP2 = true
	srv.Config.ErrorLog = log.New(errorLog, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return &recoveringServer{
		http:     srv,
		client:   flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL),
		handler:  handler,
		errorLog: errorLog,
		slogOut:  slogOut,
		sink:     sink,
	}
}

// installManualReader points the global meter provider at a reader the test
// can collect from, and restores the previous provider afterwards. The
// interceptor reads the global per call, as every instrument in this
// repository does, so this is the seam.
func installManualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	return reader
}

// TestAHandlerPanicIsLoggedCountedAuditedAndAnsweredWithACorrelationID is the
// whole of the desired outcome: the panic value and stack reach the process
// log and nothing else; the metric moves under the method; the audit trail
// holds the allow and an INTERNAL_ERROR record sharing one correlation id;
// and the caller gets CodeInternal carrying that id alone.
func TestAHandlerPanicIsLoggedCountedAuditedAndAnsweredWithACorrelationID(t *testing.T) {
	// Not parallel: the global meter provider is swapped for the duration.
	reader := installManualReader(t)
	ts := startRecoveringServer(t)

	_, err := ts.client.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
	require.Error(t, err)

	// The caller's half: an internal error, a correlation id, and nothing
	// that came from inside the process.
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr, "a transport error is the defect; the answer must be a Connect error")
	require.Equal(t, connect.CodeInternal, connectErr.Code())
	require.Regexp(t, `^internal error; correlation id [0-9a-f-]{36}$`, connectErr.Message(),
		"the message carries the correlation id and nothing else")
	id := strings.TrimPrefix(connectErr.Message(), "internal error; correlation id ")
	for _, leak := range []string{"boom", "goroutine", "recover.go", "panic"} {
		require.NotContains(t, connectErr.Message(), leak, "the caller must not see the panic or its stack")
	}
	require.Empty(t, connectErr.Details(), "no detail carries what the message refuses")

	// The operator's half, on the process log: ERROR, the method, the
	// caller's attested coordinates, the panic and its stack — and not the
	// request.
	var line map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(ts.slogOut.String())), &line),
		"one log line for one panic: %s", ts.slogOut.String())
	require.Equal(t, "ERROR", line["level"])
	require.Equal(t, "recovered from panic in RPC handler", line["msg"])
	require.Equal(t, "Get", line["rpc"])
	require.Equal(t, "acme", line["namespace"])
	require.Equal(t, "agent-1", line["subject"])
	require.Equal(t, "https://issuer.example", line["issuer"])
	require.Equal(t, id, line["correlation_id"], "the log line and the caller's error name one request")
	require.Contains(t, line["panic"], "boom: a nil map")
	require.Contains(t, line["stack"], "goroutine ")
	require.Contains(t, line["stack"], "panickingHandler",
		"the stack is the panic's, not the recovery's: it names the frame that panicked")
	require.NotContains(t, ts.slogOut.String(), "orders-1", "the payload does not reach the log")
	require.NotContains(t, ts.slogOut.String(), "claim-value", "claims do not reach the log")

	// The metric, under the method.
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	require.Equal(t, int64(1), panicsCounted(t, collected, "Get"))

	// The trail: the allow the handler wrote before it died, and the record
	// that says the request was then answered by nobody, joined by the id
	// the caller was told.
	records := ts.sink.all()
	require.Len(t, records, 2, "one allow and one internal error, in that order")

	allow, failure := records[0], records[1]
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, allow.GetDecision())
	require.Equal(t, "orders-1", allow.GetResourceKey())
	require.Equal(t, id, allow.GetCorrelationId(), "the allow carries the id minted before the handler ran")

	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_INTERNAL_ERROR, failure.GetDecision())
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ, failure.GetAction())
	require.Equal(t, "Get", failure.GetRpc())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED, failure.GetDenyCode(), "nothing was decided")
	require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_UNSPECIFIED, failure.GetResourceKind(),
		"the interceptor does not read the request and does not guess the resource")
	require.Equal(t, id, failure.GetCorrelationId())
	require.Equal(t, "agent-1", failure.GetIdentity().GetSubject())
	require.Equal(t, "acme", failure.GetIdentity().GetNamespace())
	require.Equal(t, "production-issuer", failure.GetIssuerName())
	require.Equal(t, "operator", failure.GetRole())
	require.Empty(t, failure.GetIdentity().GetClaims())
	require.NotContains(t, failure.String(), "boom", "the panic's words do not reach the durable sink")

	// And the acceptance criterion the issue states: nothing reached the
	// standard library's logger.
	require.Empty(t, ts.errorLog.String(), "net/http saw no panic")
}

// TestAPanicInOneRequestDoesNotAffectAConcurrentRequestOnTheSameConnection is
// the multiplexing half: under HTTP/2 one connection carries many streams,
// and before the interceptor a panic on one stream reset the connection under
// all of them.
func TestAPanicInOneRequestDoesNotAffectAConcurrentRequestOnTheSameConnection(t *testing.T) {
	t.Parallel()

	ts := startRecoveringServer(t)

	// List first, blocked on the server so its stream is open when Get
	// arrives. The trace records whether each request's connection was one
	// the client already had, which is how the test knows Get rode List's
	// connection rather than dialing its own.
	var (
		listReused, getReused bool
		listErr               error
		done                  = make(chan struct{})
	)
	go func() {
		defer close(done)
		ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { listReused = info.Reused },
		})
		_, listErr = ts.client.List(ctx, connect.NewRequest(&v1.ListRequest{}))
	}()
	<-ts.handler.started

	ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { getReused = info.Reused },
	})
	_, err := ts.client.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	require.True(t, getReused, "Get must share List's HTTP/2 connection for this test to prove anything")

	close(ts.handler.release)
	<-done
	require.NoError(t, listErr, "the request beside the panic is answered as if nothing happened")
	require.False(t, listReused, "List opened the connection")
	require.Empty(t, ts.errorLog.String())
}

// TestWithoutTheInterceptorNetHTTPCatchesThePanic is the negative control for
// the ErrorLog capture above: the same stub, the same server, no interceptor,
// and the line the issue's grep looks for is exactly what appears. A test
// that asserts an empty buffer is only evidence if the buffer would have
// held something.
func TestWithoutTheInterceptorNetHTTPCatchesThePanic(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)
	handler := &panickingHandler{server: s}

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(handler))

	errorLog := &lockedBuffer{}
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.ErrorLog = log.New(errorLog, "", 0)
	srv.Start()
	t.Cleanup(srv.Close)

	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)
	_, err := client.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
	require.Error(t, err)
	require.NotEqual(t, connect.CodeInternal, connect.CodeOf(err),
		"the defect: the caller sees a transport failure, not an internal error it can report")
	require.Contains(t, errorLog.String(), "http: panic serving")
}

// TestTheInterceptorLeavesAnAbortedHandlerAborted pins the one panic that is
// not a defect: http.ErrAbortHandler is how a handler aborts its response on
// purpose, net/http recognises it and stays quiet, and converting it into an
// internal error would answer a request the handler chose not to.
func TestTheInterceptorLeavesAnAbortedHandlerAborted(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)
	i := s.RecoverInterceptor(nil)

	unary := i.WrapUnary(func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
		panic(http.ErrAbortHandler)
	})

	require.PanicsWithValue(t, http.ErrAbortHandler, func() {
		_, _ = unary(t.Context(), connect.NewRequest(&v1.GetRequest{}))
	})
}

// TestPanicTextIsBoundedAndSurvivesAValueThatCannotBeFormatted covers the two
// ways a panic value can be worse than a string: a value whose own String
// method panics, and one as large as the request that provoked it.
func TestPanicTextIsBoundedAndSurvivesAValueThatCannotBeFormatted(t *testing.T) {
	t.Parallel()

	// fmt recovers a String method's panic itself and renders it as
	// "%!v(PANIC=…)"; the guard in panicText is for whatever fmt does not
	// catch, and either way the recovery reaches the log line rather than
	// dying a second time.
	var text string
	require.NotPanics(t, func() { text = panicText(explodingStringer{}) })
	require.Contains(t, text, "PANIC")

	huge := panicText(strings.Repeat("x", 2*maxPanicTextBytes))
	require.Less(t, len(huge), maxPanicTextBytes+64)
	require.True(t, strings.HasSuffix(huge, "[truncated]"))

	require.Equal(t, "plain", panicText("plain"))
}

type explodingStringer struct{}

func (explodingStringer) String() string { panic("the panic value's own String panicked") }

// panicsCounted reads [metricschema.InstrumentServerPanics] for one method
// from a collection, or zero when the instrument was never recorded.
func panicsCounted(t *testing.T, collected metricdata.ResourceMetrics, method string) int64 {
	t.Helper()

	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != metricschema.InstrumentServerPanics {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is a counter", m.Name)
			for _, point := range sum.DataPoints {
				got, ok := point.Attributes.Value(attribute.Key(metricschema.RPCMethod))
				require.True(t, ok, "the panic count carries the method")
				require.Equal(t, 1, point.Attributes.Len(), "the method is the only label")
				if got.AsString() == method {
					return point.Value
				}
			}
		}
	}

	return 0
}
