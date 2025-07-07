package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

type testingLogger struct {
	t *testing.T
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

func (l *testingLogger) Debug(msg string, keyvals ...any) {
	l.t.Logf("DEBUG: %s %s", msg, renderKeyvals(keyvals...))
}
func (l *testingLogger) Debugf(msg string, keyvals ...any) {
	l.t.Logf("DEBUG: %s %v", msg, renderKeyvals(keyvals...))
}

func (l *testingLogger) Info(msg string, keyvals ...any) {
	l.t.Logf("INFO: %s %v", msg, renderKeyvals(keyvals...))
}

func (l *testingLogger) Warn(msg string, keyvals ...any) {
	l.t.Logf("WARN: %s %v", msg, renderKeyvals(keyvals...))
}

func (l *testingLogger) Error(msg string, keyvals ...any) {
	l.t.Logf("ERROR: %s %v", msg, renderKeyvals(keyvals...))
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
	// $ temporal server start-dev
	devServer, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ClientOptions: &client.Options{
			Logger: &testingLogger{t: t},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = devServer.Stop() })

	// $ go run cmd/flow/main.go worker
	w := worker.New(devServer.Client(), engine.RunTaskQueueName, worker.Options{})
	w.RegisterWorkflow(engine.Run)
	w.RegisterActivity(engine.Task)

	err = w.Start()
	require.NoError(t, err)
	t.Cleanup(w.Stop)

	interceptor, err := validate.NewInterceptor()
	require.NoError(t, err)

	otelInterceptor, err := otelconnect.NewInterceptor()
	require.NoError(t, err)

	flowstateServer := server.New(devServer.Client())

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
								Name: "echo",
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
					"a": {
						NamedValues: map[string]*v1.Value{
							"result": v1.NewLiteral("hello world"),
						},
					},
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
	require.True(
		t,
		proto.Equal(
			getResp.Msg,
			expResp,
		),
		"Expected and actual responses differ:\n%s",
		cmp.Diff(
			getResp.Msg,
			expResp,
			protocmp.Transform(),
		),
	)
}
