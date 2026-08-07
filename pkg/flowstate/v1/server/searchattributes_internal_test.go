package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/mocks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeOperatorService implements operatorservice.OperatorServiceClient by
// embedding it — nil, so every method it does not override panics on use,
// which is deliberate: a test reaching one is exercising the wrong call.
type fakeOperatorService struct {
	operatorservice.OperatorServiceClient
	addSearchAttributes func(context.Context, *operatorservice.AddSearchAttributesRequest) (*operatorservice.AddSearchAttributesResponse, error)
}

func (f *fakeOperatorService) AddSearchAttributes(ctx context.Context, in *operatorservice.AddSearchAttributesRequest, _ ...grpc.CallOption) (*operatorservice.AddSearchAttributesResponse, error) {
	return f.addSearchAttributes(ctx, in)
}

// TestEnsureSearchAttributesRegisteredTreatsAlreadyExistsAsSuccess is the
// idempotency this function exists for: a second `flow server` process, or
// this one restarting, registers again and must not be told that is an
// error — see [EnsureSearchAttributesRegistered]'s own doc.
func TestEnsureSearchAttributesRegisteredTreatsAlreadyExistsAsSuccess(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}
	temporal.On("OperatorService").Return(&fakeOperatorService{
		addSearchAttributes: func(_ context.Context, req *operatorservice.AddSearchAttributesRequest) (*operatorservice.AddSearchAttributesResponse, error) {
			require.Equal(t, "my-namespace", req.GetNamespace())

			// Both of Flowstate's own attributes, and only those — a third
			// application's attributes sharing the namespace must be left
			// untouched, which this request shape (naming exactly two keys)
			// already guarantees rather than something this test can violate
			// by accident.
			require.Len(t, req.GetSearchAttributes(), 2)
			require.Contains(t, req.GetSearchAttributes(), namespaceSearchAttribute.GetName())
			require.Contains(t, req.GetSearchAttributes(), workflowNameSearchAttribute.GetName())

			return nil, status.Error(codes.AlreadyExists, "already registered")
		},
	})

	require.NoError(t, EnsureSearchAttributesRegistered(t.Context(), temporal, "my-namespace"))
}

// TestEnsureSearchAttributesRegisteredPropagatesOtherErrors is the other half
// of the same rule: only ALREADY_EXISTS is treated as success. Anything
// else — no operator permission, an unreachable cluster — is reported, so
// `cmd/flow/main.go` can log it and fall back to the zero-configuration
// path rather than silently believing registration succeeded.
func TestEnsureSearchAttributesRegisteredPropagatesOtherErrors(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}
	temporal.On("OperatorService").Return(&fakeOperatorService{
		addSearchAttributes: func(context.Context, *operatorservice.AddSearchAttributesRequest) (*operatorservice.AddSearchAttributesResponse, error) {
			return nil, status.Error(codes.PermissionDenied, "not allowed")
		},
	})

	err := EnsureSearchAttributesRegistered(t.Context(), temporal, "my-namespace")
	require.Error(t, err)
}
