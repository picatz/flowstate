package flowstatev1_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A mistake in `outputs:` re-sent the request that had already succeeded.
//
// [ErrorKind.Retryable] states the rule it was breaking, and says why the default
// is permanent: "Retrying a POST that already took effect is worse than surfacing
// a failure that might have resolved on its own." [ErrorKindExpression] exists for
// precisely this case — "Retrying re-evaluates the same expression against the same
// data."
//
// The `outputs:` path returned bare `fmt.Errorf` at every failure, and
// [ClassifyError] answers [ErrorKindInternal] for an unwrapped error, which *is*
// retryable. So the step re-ran, and a step re-runs from the top: the request went
// out again. A typo — reading `response.json` without `parse_json: true` — sent a
// charge five times.
//
// `expect:`, one file over, has classified all along. Two halves of one feature,
// one of them wrong.

// TestAMistakeInOutputsDoesNotResendTheRequest is the count, which is the only
// thing that actually says this is fixed.
func TestAMistakeInOutputsDoesNotResendTheRequest(t *testing.T) {
	allowLoopback(t)

	for _, test := range []struct {
		name    string
		outputs string
		says    string
	}{
		{
			// The ordinary typo: `parse_json:` was not set, so there is no `json`.
			name:    "a name the response does not have",
			outputs: `{"id": response.json}`,
			says:    "evaluating outputs",
		},
		{
			// Deterministic in exactly the same way: the expression is fine and
			// produces the wrong shape, every time.
			name:    "an expression that is not a map",
			outputs: `"a string, not a map"`,
			says:    "must evaluate to a map",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var posts atomic.Int64

			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			t.Cleanup(endpoint.Close)

			_, err := v1.Run(t.Context(), &v1.Workflow{
				Name:    "charges-once",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{{
					Id: "charge",
					// A short interval so a regression costs a second rather than
					// fifteen. The count is what is under test, not the wait.
					Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
						InitialInterval: durationpb.New(time.Millisecond),
						MaxInterval:     durationpb.New(time.Millisecond),
					}},
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
						"method":  v1.NewLiteral(http.MethodPost),
						"url":     v1.NewLiteral(endpoint.URL + "/charge"),
						"outputs": v1.NewExpr(test.outputs),
					}}},
				}},
			})
			require.Error(t, err)

			assert.Equal(t, int64(1), posts.Load(),
				"the request was sent %d times for a mistake no number of attempts can fix, "+
					"and a POST that already took effect is the one thing retrying must not do",
				posts.Load())

			assert.Contains(t, err.Error(), test.says,
				"the failure does not say what went wrong with the outputs expression")
		})
	}
}

// TestAnOutputsFailureIsClassifiedTheWayExpectIs is the classification itself,
// asserted where a caller reads it.
//
// The count above is what matters, and it only holds because of this: the retry
// decision in both drivers goes through [ClassifyError], so a kind that is
// [ErrorKind.Retryable] sends the request again wherever the step runs.
func TestAnOutputsFailureIsClassifiedTheWayExpectIs(t *testing.T) {
	allowLoopback(t)

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(endpoint.Close)

	_, err := v1.Run(t.Context(), &v1.Workflow{
		Name:    "classified",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "read",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url":     v1.NewLiteral(endpoint.URL),
				"outputs": v1.NewExpr(`{"id": response.json}`),
			}}},
		}},
	})
	require.Error(t, err)

	kind := v1.ClassifyError(err)

	assert.Equal(t, v1.ErrorKindExpression, kind,
		"an outputs failure classified as %s; unwrapped it would be Internal, which is "+
			"retryable, and retrying re-sends the request", kind)
	assert.False(t, kind.Retryable(),
		"an expression that failed against fixed data was reported as worth attempting again")
}
