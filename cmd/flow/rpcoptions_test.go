package main

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The chain both listeners install (picatz/flowstate#1761), proved through
// the same [rpcHandlerOptions] and the same [serverHandler] `flow server` and
// `flow server dev` use, mounted over a handler that panics. The server
// package proves what the interceptor does; this proves the listeners have
// it, and that it sits outside the interceptors it must protect.

// panickingWorkflowService panics on Get and nothing else.
type panickingWorkflowService struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler
}

func (panickingWorkflowService) Get(context.Context, *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	panic("boom")
}

// syncEmitter collects audit records written from handler goroutines.
type syncEmitter struct {
	mu      sync.Mutex
	records []*v1.AuditRecord
}

func (e *syncEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, record)

	return nil
}

// all copies the records out under the lock, so an assertion never holds the
// mutex across a request whose handler would need it to emit.
func (e *syncEmitter) all() []*v1.AuditRecord {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]*v1.AuditRecord(nil), e.records...)
}

// syncBuffer is a bytes.Buffer shared between the server's goroutines and the
// test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// TestTheListenersChainRecoversAHandlerPanic drives a panic through the
// option list both listeners hand NewWorkflowServiceHandler, behind the same
// routing and the anonymous authenticator `flow server dev` runs with, and
// asserts the answer is CodeInternal with a correlation id, the process log
// and the audit trail both have the failure, and net/http logged nothing.
func TestTheListenersChainRecoversAHandlerPanic(t *testing.T) {
	t.Parallel()

	sink := &syncEmitter{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)

	flowServer, err := server.New(nil, server.WithAudit(recorder))
	require.NoError(t, err)

	logOut := &syncBuffer{}
	opts, err := rpcHandlerOptions(flowServer, slog.New(slog.NewTextHandler(logOut, nil)))
	require.NoError(t, err)

	rpcMux := http.NewServeMux()
	rpcMux.Handle(flowstatev1connect.NewWorkflowServiceHandler(panickingWorkflowService{}, opts...))

	errorLog := &syncBuffer{}
	srv := httptest.NewUnstartedServer(serverHandler(discardLogger(), auth.InsecureAnonymousVerifier(),
		nil, nil, "", rpcMux, nil, nil))
	srv.Config.ErrorLog = log.New(errorLog, "", 0)
	srv.Start()
	t.Cleanup(srv.Close)

	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)

	_, err = client.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	require.Equal(t, connect.CodeInternal, connectErr.Code())
	require.Regexp(t, `^internal error; correlation id [0-9a-f-]{36}$`, connectErr.Message())
	id := strings.TrimPrefix(connectErr.Message(), "internal error; correlation id ")

	require.Contains(t, logOut.String(), "level=ERROR")
	require.Contains(t, logOut.String(), "correlation_id="+id)
	require.Contains(t, logOut.String(), "panic=boom")
	require.NotContains(t, logOut.String(), "orders-1", "the payload does not reach the log")

	records := sink.all()
	require.Len(t, records, 1, "the stub wrote no allow; the interceptor wrote the failure")
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_INTERNAL_ERROR, records[0].GetDecision())
	require.Equal(t, "Get", records[0].GetRpc())
	require.Equal(t, id, records[0].GetCorrelationId())

	require.Empty(t, errorLog.String(), "net/http saw no panic; before #1761 this held `http: panic serving`")

	// The validation interceptor still sits inside the chain: a request the
	// schema refuses is refused before the handler, and the refusal is not
	// mistaken for a panic.
	_, err = client.Get(t.Context(), connect.NewRequest(&v1.GetRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Len(t, sink.all(), 1, "a validation refusal writes no record and reaches no handler")
}

// TestEveryListenerTakesTheSharedHandlerOptions is the source-level half:
// the two calls that mount the WorkflowService handler — `flow server`'s and
// `flow server dev`'s — spread [rpcHandlerOptions]'s list and add nothing
// beside it, so neither listener can grow a chain of its own that the test
// above never saw.
func TestEveryListenerTakesTheSharedHandlerOptions(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	mounts := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "NewWorkflowServiceHandler" {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "flowstatev1connect" {
				return true
			}

			mounts++
			position := fset.Position(call.Pos())
			require.Len(t, call.Args, 2,
				"%s: the handler and the shared options, nothing else; an option added here is one the other listener does not have", position)
			require.NotEqual(t, token.NoPos, call.Ellipsis,
				"%s: the options are spread from rpcHandlerOptions, not written inline", position)
			spread, ok := call.Args[1].(*ast.Ident)
			require.True(t, ok && spread.Name == "rpcOpts",
				"%s: the second argument is rpcOpts, the list rpcHandlerOptions returned", position)

			return true
		})
	}

	require.Equal(t, 2, mounts, "`flow server` and `flow server dev` each mount the handler once")
}
