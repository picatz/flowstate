package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

// testingLogger routes a Temporal client's log through the test that owns it, and
// stops the moment that test is over.
//
// The stopping is the whole reason this is not three lines. A Temporal client owns
// worker goroutines that keep polling and logging for a short while after Stop
// returns, and testing.T.Logf called after its test has finished is not a stray
// line — it is a panic ("Log in goroutine after Test has completed"), or under
// -race a data race against the test framework's own state. It surfaced as three
// unrelated tests failing at once, including a pure mock test that touches none of
// this, because a race report fails whatever happened to be running.
//
// So a cleanup closes the logger before the test's own bookkeeping is torn down,
// and every line after that is dropped rather than written somewhere it must not
// be. Dropping is right: those lines belong to a test that has already reported.
type testingLogger struct {
	t *testing.T

	// Guards closed, and orders the write in Close against reads from whichever
	// worker goroutine is still going.
	mu     sync.Mutex
	closed bool
}

// newTestingLogger returns a logger that writes to t until t finishes.
func newTestingLogger(t *testing.T) *testingLogger {
	t.Helper()

	logger := &testingLogger{t: t}
	t.Cleanup(func() {
		logger.mu.Lock()
		defer logger.mu.Unlock()

		logger.closed = true
	})

	return logger
}

func renderKeyvals(keyvals ...any) string {
	result := ""
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			result += fmt.Sprintf("%s=%v ", keyvals[i], keyvals[i+1])
		} else {
			result += fmt.Sprintf("%s=<missing> ", keyvals[i])
		}
	}
	return result
}

// log writes one line, unless the test it belongs to has finished.
func (l *testingLogger) log(level, msg string, keyvals ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return
	}

	l.t.Logf("%s: %s %s", level, msg, renderKeyvals(keyvals...))
}

func (l *testingLogger) Debug(msg string, keyvals ...any) {
	l.log("DEBUG", msg, keyvals...)
}

func (l *testingLogger) Debugf(msg string, keyvals ...any) {
	l.log("DEBUG", msg, keyvals...)
}

func (l *testingLogger) Info(msg string, keyvals ...any) {
	l.log("INFO", msg, keyvals...)
}

func (l *testingLogger) Warn(msg string, keyvals ...any) {
	l.log("WARN", msg, keyvals...)
}

func (l *testingLogger) Error(msg string, keyvals ...any) {
	l.log("ERROR", msg, keyvals...)
}

/*
func newTemporalClient(t *testing.T) client.Client {
	t.Helper()

	temporalClient, err := client.DialContext(
		t.Context(),
		client.Options{
			HostPort:  "localhost:7233",
			Namespace: "default",
			Logger:    &testingLogger{},
		},
	)
	if err != nil {
		t.Fatalf("Failed to create Temporal client: %v", err)
	}
	return temporalClient
}
*/

func TestFlowstateServer(t *testing.T) {
	// $ temporal server start-dev, shared by the package, in a namespace of this
	// test's own; and $ go run cmd/flow/main.go worker, polling only that one.
	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	interceptor := validate.NewInterceptor()

	otelInterceptor, err := otelconnect.NewInterceptor()
	require.NoError(t, err)

	flowstateServer := mustNew(t, temporal)

	mux := http.NewServeMux()
	mux.Handle(
		flowstatev1connect.NewWorkflowServiceHandler(
			flowstateServer,
			connect.WithInterceptors(
				interceptor,
				otelInterceptor,
			),
		),
	)

	// Create a public/private key pair (ECDSA)
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	const keyID = "test-key"

	// Create a JWT token, sign it with the private key.
	token, err := jwt.New(
		header.Parameters{
			header.Type:      jwt.Type,
			header.Algorithm: jwa.ES256,
		},
		jwt.ClaimsSet{
			jwt.Audience: "flowstate",
			jwt.IssuedAt: time.Now().Unix(),
			jwt.Subject:  "test-user",
			jwt.Issuer:   "flowstate-test",
			jwk.KeyID:    keyID,
		},
		private,
	)
	require.NoError(t, err)

	authMiddleware := authn.NewMiddleware(func(ctx context.Context, req *http.Request) (any, error) {
		bearerToken, hasToken := authn.BearerToken(req)
		if !hasToken {
			return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("failed to extract bearer token: %w", err))
		}

		token, err = jwt.ParseAndVerify(bearerToken, jwt.WithIdentifiableKey(keyID, &private.PublicKey))
		if err != nil {
			return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("failed to parse and verify JWT: %w", err))
		}

		return token, nil
	})

	httpServer := httptest.NewServer(authMiddleware.Wrap(mux))
	t.Cleanup(httpServer.Close)

	flowstateClient := flowstatev1connect.NewWorkflowServiceClient(
		httpServer.Client(),
		httpServer.URL,
		connect.WithInterceptors(
			otelInterceptor,
			connect.UnaryInterceptorFunc(
				func(uf connect.UnaryFunc) connect.UnaryFunc {
					return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
						req.Header().Set("Authorization", fmt.Sprintf("Bearer %s", token.String()))
						return uf(ctx, req)
					}
				},
			),
		),
	)

	runResp, err := flowstateClient.Run(t.Context(), &connect.Request[v1.RunRequest]{
		Msg: &v1.RunRequest{
			Workflow: &v1.Workflow{
				Name: "test",
				Steps: []*v1.Node{
					{
						Id: "a",
						Kind: &v1.Node_Task{
							Task: &v1.Task{
								Name: "log",
								Inputs: map[string]*v1.Value{
									"message": v1.NewLiteral("hello world"),
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, runResp)

	// The workflow should complete quickly, but give it a moment.
	time.Sleep(1 * time.Second)

	expResp := &v1.GetResponse{
		WorkflowId: runResp.Msg.GetWorkflowId(),
		RunId:      runResp.Msg.GetRunId(),
		Status:     v1.RunResponse_STATUS_COMPLETED,
		Kind: &v1.GetResponse_Outputs{
			Outputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					// Present and empty: the step ran and produced nothing, which is
					// what a `log` step does. Absent would mean it never ran.
					"a": {},
				},
			},
		},
	}

	getResp, err := flowstateClient.Get(t.Context(), &connect.Request[v1.GetRequest]{
		Msg: &v1.GetRequest{
			WorkflowId: runResp.Msg.GetWorkflowId(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getResp)

	// Everything but the clock. A response now carries when the run started and
	// finished, which are facts about the moment this test ran and cannot be written
	// into an expected value — but they are also not nothing, so they are asserted
	// just below rather than dropped. What stays exact is the part that is a claim
	// about the *server*: the ids, the status, and the outputs.
	require.Empty(t, cmp.Diff(expResp, getResp.Msg, protocmp.Transform(),
		protocmp.IgnoreFields(&v1.GetResponse{}, "start_time", "close_time")),
		"the response differs from what this run should have produced")

	require.NotNil(t, getResp.Msg.GetStartTime(), "a finished run does not say when it began")
	require.NotNil(t, getResp.Msg.GetCloseTime(), "a finished run does not say when it finished")
}
