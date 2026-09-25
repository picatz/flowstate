package conformance

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// HTTPAttemptOutcomeCase is the shared proof that a response classified as a
// transient upstream failure does not, by itself, authorize replaying a
// mutation. Attempts reads the external peer's count rather than engine state.
type HTTPAttemptOutcomeCase struct {
	Workflow *v1.Workflow
	Attempts func() int32
}

// NewHTTPAttemptOutcomeCase returns a POST whose peer records the mutation and
// then answers 503. Three attempts are available, but both drivers must stop
// after the first because the response cannot establish whether the mutation
// took effect and the workflow did not opt into unknown-outcome retries.
func NewHTTPAttemptOutcomeCase(tb testing.TB) HTTPAttemptOutcomeCase {
	tb.Helper()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	tb.Cleanup(server.Close)
	allowLoopback(tb)

	return HTTPAttemptOutcomeCase{
		Workflow: &v1.Workflow{
			Name: "http-ambiguous-mutation",
			Steps: []*v1.Node{{
				Id: "mutate",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "http",
					Inputs: map[string]*v1.Value{
						"method": v1.NewLiteral(http.MethodPost),
						"url":    v1.NewLiteral(server.URL),
						"body":   v1.NewLiteral(`{"effect":true}`),
					},
				}},
				Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
					MaxAttempts:        3,
					InitialInterval:    durationpb.New(time.Millisecond),
					BackoffCoefficient: 1,
					MaxInterval:        durationpb.New(time.Millisecond),
				}},
			}},
		},
		Attempts: attempts.Load,
	}
}
