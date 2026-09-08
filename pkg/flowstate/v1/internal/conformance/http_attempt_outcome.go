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

// HTTPAttemptOutcomeCase is one shared proof that a response classified as a
// transient upstream failure does not, by itself, authorize replaying a
// mutation. Attempts reads the external peer's count rather than engine state.
type HTTPAttemptOutcomeCase struct {
	Name     string
	Workflow *v1.Workflow
	Attempts func() int32
}

// NewHTTPAttemptOutcomeCases returns POSTs whose peer records the mutation and
// then answers 503, including failures while interpreting the response. Three
// attempts are available, but both drivers must stop after the first because
// none of the result checks establish whether the mutation took effect and the
// workflow did not opt into unknown-outcome retries.
func NewHTTPAttemptOutcomeCases(tb testing.TB) []HTTPAttemptOutcomeCase {
	tb.Helper()

	return []HTTPAttemptOutcomeCase{
		newHTTPAttemptOutcomeCase(tb, "status", "", nil),
		newHTTPAttemptOutcomeCase(tb, "decode", "not-json", map[string]*v1.Value{
			"parse_json": v1.NewLiteral(true),
		}),
		newHTTPAttemptOutcomeCase(tb, "expect", `{"ok":false}`, map[string]*v1.Value{
			"parse_json": v1.NewLiteral(true),
			"expect":     v1.NewExpr("response.json.ok"),
		}),
		newHTTPRedirectAttemptOutcomeCase(tb),
	}
}

func newHTTPAttemptOutcomeCase(tb testing.TB, name, responseBody string, extraInputs map[string]*v1.Value) HTTPAttemptOutcomeCase {
	tb.Helper()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(responseBody))
	}))
	tb.Cleanup(server.Close)
	allowLoopback(tb)
	inputs := map[string]*v1.Value{
		"method": v1.NewLiteral(http.MethodPost),
		"url":    v1.NewLiteral(server.URL),
		"body":   v1.NewLiteral(`{"effect":true}`),
	}
	for key, value := range extraInputs {
		inputs[key] = value
	}

	return HTTPAttemptOutcomeCase{
		Name: name,
		Workflow: &v1.Workflow{
			Name: "http-ambiguous-mutation-" + name,
			Steps: []*v1.Node{{
				Id: "mutate",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "http",
					Inputs: inputs,
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

func newHTTPRedirectAttemptOutcomeCase(tb testing.TB) HTTPAttemptOutcomeCase {
	tb.Helper()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mutate" {
			attempts.Add(1)
			http.Redirect(w, r, "/status", http.StatusSeeOther)
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	tb.Cleanup(server.Close)
	allowLoopback(tb)

	return HTTPAttemptOutcomeCase{
		Name: "followed-redirect",
		Workflow: &v1.Workflow{
			Name: "http-ambiguous-mutation-followed-redirect",
			Steps: []*v1.Node{{
				Id: "mutate",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "http",
					Inputs: map[string]*v1.Value{
						"method": v1.NewLiteral(http.MethodPost),
						"url":    v1.NewLiteral(server.URL + "/mutate"),
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
