package flowstatev1

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/stretchr/testify/require"
)

// recordedRequest is what the stand-in server saw, so a test can assert on the
// request that would actually have gone out.
type recordedRequest struct {
	method      string
	path        string
	rawQuery    string
	contentType string
	body        string
}

// httpTaskServer starts a server that records the request it received and answers
// with the given status and body.
//
// One request at a time. The recording is a plain assignment from the handler
// goroutine, so two requests in flight against one of these is a data race — give
// each parallel subtest its own rather than sharing one across a table. That is not
// hypothetical: it is how it was found.
func httpTaskServer(t *testing.T, status int, respBody string, header http.Header) (*httptest.Server, *recordedRequest) {
	t.Helper()

	var seen recordedRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen = recordedRequest{
			method:      r.Method,
			path:        r.URL.Path,
			rawQuery:    r.URL.RawQuery,
			contentType: r.Header.Get("Content-Type"),
			body:        string(body),
		}

		for name, values := range header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}))
	t.Cleanup(server.Close)

	return server, &seen
}

// runHTTPTask executes the http task against a loopback server.
func runHTTPTask(t *testing.T, inputs map[string]any) (*Node_Outputs, error) {
	t.Helper()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithTimeout(5*time.Second))
	require.NoError(t, err)

	return taskFuncHTTP(policy)(t.Context(), NewNamedValues(inputs), nil)
}

func Test_httpTask_query(t *testing.T) {
	tests := []struct {
		name  string
		url   string
		query map[string]any
		check func(t *testing.T, seen *recordedRequest, err error)
	}{
		// Negative cases first.
		{
			name:  "a nested structure has no agreed encoding",
			query: map[string]any{"filter": map[string]any{"a": "b"}},
			check: func(t *testing.T, _ *recordedRequest, err error) {
				require.ErrorContains(t, err, "no agreed encoding")
				require.ErrorContains(t, err, "json body")
			},
		},
		{
			name:  "an empty parameter name is refused",
			query: map[string]any{"": "v"},
			check: func(t *testing.T, _ *recordedRequest, err error) {
				require.ErrorContains(t, err, "empty name")
			},
		},

		{
			name:  "values are escaped",
			query: map[string]any{"q": "a b&c=d"},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)

				// The escaping is the point: hand-building this is where the bugs are.
				parsed, perr := url.ParseQuery(seen.rawQuery)
				require.NoError(t, perr)
				require.Equal(t, "a b&c=d", parsed.Get("q"))
			},
		},
		{
			name:  "non-string scalars render",
			query: map[string]any{"page": 2, "exact": true},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)

				parsed, perr := url.ParseQuery(seen.rawQuery)
				require.NoError(t, perr)
				require.Equal(t, "2", parsed.Get("page"))
				require.Equal(t, "true", parsed.Get("exact"))
			},
		},
		{
			name:  "a list becomes a repeated parameter",
			query: map[string]any{"tag": []any{"a", "b"}},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)

				parsed, perr := url.ParseQuery(seen.rawQuery)
				require.NoError(t, perr)
				require.Equal(t, []string{"a", "b"}, parsed["tag"])
			},
		},
		{
			name:  "parameters already in the url are kept",
			url:   "/?fixed=1",
			query: map[string]any{"added": "2"},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)

				parsed, perr := url.ParseQuery(seen.rawQuery)
				require.NoError(t, perr)
				require.Equal(t, "1", parsed.Get("fixed"))
				require.Equal(t, "2", parsed.Get("added"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, seen := httpTaskServer(t, http.StatusOK, "ok", nil)

			inputs := map[string]any{"url": server.URL + test.url}
			if test.query != nil {
				inputs["query"] = test.query
			}

			_, err := runHTTPTask(t, inputs)
			test.check(t, seen, err)
		})
	}
}

func Test_httpTask_bodies(t *testing.T) {
	tests := []struct {
		name   string
		inputs map[string]any
		check  func(t *testing.T, seen *recordedRequest, err error)
	}{
		// Negative cases first: more than one body is a request whose meaning would
		// depend on field order.
		{
			name:   "json and form together are refused",
			inputs: map[string]any{"json": map[string]any{"a": 1}, "form": map[string]any{"b": "2"}},
			check: func(t *testing.T, _ *recordedRequest, err error) {
				require.ErrorContains(t, err, "mutually exclusive")
				require.ErrorContains(t, err, "form and json")
			},
		},
		{
			name:   "body and json together are refused",
			inputs: map[string]any{"body": "raw", "json": map[string]any{"a": 1}},
			check: func(t *testing.T, _ *recordedRequest, err error) {
				require.ErrorContains(t, err, "mutually exclusive")
			},
		},

		{
			name:   "a json body is serialized with a matching content type",
			inputs: map[string]any{"method": "POST", "json": map[string]any{"name": "flowstate", "count": 2}},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)
				require.Equal(t, "application/json", seen.contentType)
				require.JSONEq(t, `{"name":"flowstate","count":2}`, seen.body)
			},
		},
		{
			name:   "a nested json body round-trips",
			inputs: map[string]any{"method": "POST", "json": map[string]any{"outer": map[string]any{"inner": []any{1, "two", true}}}},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)
				require.JSONEq(t, `{"outer":{"inner":[1,"two",true]}}`, seen.body)
			},
		},
		{
			name:   "a form body is url-encoded with a matching content type",
			inputs: map[string]any{"method": "POST", "form": map[string]any{"grant_type": "client_credentials", "scope": "a b"}},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)
				require.Equal(t, "application/x-www-form-urlencoded", seen.contentType)

				parsed, perr := url.ParseQuery(seen.body)
				require.NoError(t, perr)
				require.Equal(t, "client_credentials", parsed.Get("grant_type"))
				require.Equal(t, "a b", parsed.Get("scope"))
			},
		},
		{
			name:   "a raw body implies no content type",
			inputs: map[string]any{"method": "POST", "body": "raw bytes"},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)
				require.Equal(t, "raw bytes", seen.body)
				require.Empty(t, seen.contentType, "an author spelling out bytes spells out the header too")
			},
		},
		{
			name: "an explicit content type overrides the implied one",
			inputs: map[string]any{
				"method":  "POST",
				"json":    map[string]any{"a": 1},
				"headers": map[string]any{"Content-Type": "application/ld+json"},
			},
			check: func(t *testing.T, seen *recordedRequest, err error) {
				require.NoError(t, err)
				require.Contains(t, seen.contentType, "ld+json")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, seen := httpTaskServer(t, http.StatusOK, "ok", nil)

			inputs := map[string]any{"url": server.URL}
			for k, v := range test.inputs {
				inputs[k] = v
			}

			_, err := runHTTPTask(t, inputs)
			test.check(t, seen, err)
		})
	}
}

func Test_httpTask_parseJSON(t *testing.T) {
	t.Run("parsing is off by default", func(t *testing.T) {
		// Parsing an untrusted body is work an attacker chooses, so it happens when
		// an author asked for it — not because a header said so.
		server, _ := httpTaskServer(t, http.StatusOK, `{"a":1}`,
			http.Header{"Content-Type": []string{"application/json"}})

		out, err := runHTTPTask(t, map[string]any{"url": server.URL})
		require.NoError(t, err)
		require.NotContains(t, out.GetNamedValues(), "json")
	})

	t.Run("a parsed body is available as an output", func(t *testing.T) {
		server, _ := httpTaskServer(t, http.StatusOK, `{"items":[{"id":7}]}`, nil)

		out, err := runHTTPTask(t, map[string]any{"url": server.URL, "parse_json": true})
		require.NoError(t, err)
		require.Contains(t, out.GetNamedValues(), "json")
	})

	t.Run("a body that is not json is an error naming the problem", func(t *testing.T) {
		// A step that asked for JSON and got HTML has a problem worth reporting, and
		// an empty value would hide it behind whatever expression read it next.
		server, _ := httpTaskServer(t, http.StatusOK, `<html>not json</html>`, nil)

		_, err := runHTTPTask(t, map[string]any{"url": server.URL, "parse_json": true})
		require.ErrorContains(t, err, "not valid json")

		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.False(t, taskErr.Retryable(), "the body will not become json on a retry")
	})

	t.Run("an outputs expression can read the parsed body", func(t *testing.T) {
		server, _ := httpTaskServer(t, http.StatusOK, `{"items":[{"id":7,"name":"seven"}]}`, nil)

		out, err := runHTTPTask(t, map[string]any{
			"url":        server.URL,
			"parse_json": true,
			"outputs":    NewExpr("{'name': response.json.items[0].name, 'id': response.json.items[0].id}"),
		})
		require.NoError(t, err)

		name, ok := out.GetNamedValues()["name"]
		require.True(t, ok, "outputs: %v", out.GetNamedValues())
		require.Equal(t, "seven", name.GetLiteral().GetStringValue())

		// A JSON number arrives as a double, because that is what encoding/json
		// produces for an untyped number and it matches what the json_parse CEL
		// function already does. Pinned rather than papered over: an author comparing
		// it needs to know.
		id, ok := out.GetNamedValues()["id"]
		require.True(t, ok)
		require.Equal(t, float64(7), id.GetLiteral().GetDoubleValue())
	})

	// TestCheckHTTPResponseElementBound (constraints_task_output_test.go)
	// pins the bound function itself; this pins that `taskFuncHTTP` actually
	// reaches it *before* it lets `outputs:` — or `expect:`, evaluated first
	// — touch the parsed body. A response body well under the byte cap can
	// still carry more elements than [maxListElements] allows, and both
	// `expect:` and `outputs:` evaluate a CEL expression directly against
	// `response.json` inside this same function, before
	// [checkTaskOutputElementBound] at Task.EvalInScope ever gets a chance to
	// see the result — #224 review's finding: bounding only the return value
	// of `def.Fn` is too late for the work `def.Fn` itself does internally.
	t.Run("an outputs comprehension over an oversized response is refused before it runs", func(t *testing.T) {
		// One element past the bound: [maxListElements] is only ever reached
		// once the element count *exceeds* it, so this is the smallest body
		// that must be refused.
		var body strings.Builder
		body.WriteByte('[')
		for i := 0; i <= maxListElements; i++ {
			if i > 0 {
				body.WriteByte(',')
			}
			body.WriteString(strconv.Itoa(i))
		}
		body.WriteByte(']')

		server, _ := httpTaskServer(t, http.StatusOK, body.String(), nil)

		start := time.Now()
		_, err := runHTTPTask(t, map[string]any{
			"url":        server.URL,
			"parse_json": true,
			// A genuine comprehension, not a pass-through: this is exactly
			// the shape ([maxListElements]'s own doc measures at 20k
			// elements/886ms, 40k/5.27s) that must never be allowed to run
			// against an oversized response.
			"outputs": NewExpr("{'evens': response.json.filter(x, int(x) % 2 == 0)}"),
		})
		elapsed := time.Since(start)

		require.Error(t, err, "an outputs: comprehension over an oversized response must be refused")
		// A generous backstop, not a tight one: what this actually needs to
		// distinguish is "the bound tripped before the comprehension ran" (a few
		// hundred microseconds) from "the comprehension ran to completion"
		// (#204 measured this shape at 10k elements/228ms — see
		// [maxListElements]), and five seconds keeps roughly 20x margin over
		// the latter. A one-second bound here is exactly the load-sensitive
		// assertion issue #431 is about: it is the scheduling delay on a
		// contended box, not the comprehension, that eats a tight budget. The
		// ErrorContains assertions below are the real proof that the bound
		// tripped rather than the comprehension failing on its own; this is a
		// sanity check on top; not the whole of it.
		require.Less(t, elapsed, 5*time.Second,
			"the element bound must trip before the comprehension runs, not after")

		require.ErrorContains(t, err, "list elements")
		require.ErrorContains(t, err, strconv.Itoa(maxListElements))
		require.ErrorContains(t, err, server.URL, "the refusal must name the response, not the workflow")

		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.Equal(t, ErrorKindLimitExceeded, taskErr.Kind)
		require.False(t, taskErr.Retryable(), "the response's own size cannot change on a retry")
	})

	// TestCheckHTTPResponseElementBound covers the depth dimension directly;
	// this pins that an `expect:` — evaluated *before* `outputs:` in
	// taskFuncHTTP — is stopped by the same bound, at the same point, since
	// expect: is the earlier of the two comprehension-capable evaluations
	// that read `response.json`.
	t.Run("an expect comprehension over an oversized response is refused before it runs", func(t *testing.T) {
		var body strings.Builder
		body.WriteByte('[')
		for i := 0; i <= maxListElements; i++ {
			if i > 0 {
				body.WriteByte(',')
			}
			body.WriteString(strconv.Itoa(i))
		}
		body.WriteByte(']')

		server, _ := httpTaskServer(t, http.StatusOK, body.String(), nil)

		_, err := runHTTPTask(t, map[string]any{
			"url":        server.URL,
			"parse_json": true,
			"expect":     NewExpr("response.json.all(x, int(x) >= 0)"),
		})

		require.Error(t, err, "an expect: comprehension over an oversized response must be refused")
		require.ErrorContains(t, err, "list elements")

		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.Equal(t, ErrorKindLimitExceeded, taskErr.Kind)
	})
}

func Test_httpTask_expect(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		inputs  map[string]any
		wantErr string
		check   func(t *testing.T, taskErr *TaskError)
	}{
		{
			name:   "the default rule is unchanged: 2xx succeeds",
			status: http.StatusOK,
		},
		{
			name:    "the default rule is unchanged: 4xx fails permanently",
			status:  http.StatusNotFound,
			wantErr: "returned status 404",
			check: func(t *testing.T, taskErr *TaskError) {
				require.False(t, taskErr.Retryable())
			},
		},
		{
			name:    "the default rule is unchanged: 5xx is retried",
			status:  http.StatusBadGateway,
			wantErr: "returned status 502",
			check: func(t *testing.T, taskErr *TaskError) {
				require.True(t, taskErr.Retryable())
			},
		},

		{
			name:   "an expected 404 succeeds",
			status: http.StatusNotFound,
			inputs: map[string]any{"expect": NewExpr("response.status_code == 200 || response.status_code == 404")},
		},
		{
			name:    "an unexpected status fails permanently",
			status:  http.StatusOK,
			inputs:  map[string]any{"expect": NewExpr("response.status_code == 201")},
			wantErr: "does not accept",
			check: func(t *testing.T, taskErr *TaskError) {
				// The author described what success looks like; this endpoint answered
				// in a way they said is wrong, and repeating will not change its mind.
				require.False(t, taskErr.Retryable())
			},
		},
		{
			name:   "expect can read the parsed body",
			status: http.StatusOK,
			body:   `{"error":"nope"}`,
			inputs: map[string]any{
				"parse_json": true,
				"expect":     NewExpr("response.status_code == 200 && !has(response.json.error)"),
			},
			wantErr: "does not accept",
		},
		{
			name:   "a 200 carrying no error is accepted",
			status: http.StatusOK,
			body:   `{"ok":true}`,
			inputs: map[string]any{
				"parse_json": true,
				"expect":     NewExpr("response.status_code == 200 && !has(response.json.error)"),
			},
		},
		{
			name:    "a non-boolean expect is refused",
			status:  http.StatusOK,
			inputs:  map[string]any{"expect": NewExpr("response.status_code")},
			wantErr: "must evaluate to a boolean",
		},
		{
			name:    "a literal expect is refused",
			status:  http.StatusOK,
			inputs:  map[string]any{"expect": true},
			wantErr: "must be an expression over the response",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := test.body
			if body == "" {
				body = "ok"
			}
			server, _ := httpTaskServer(t, test.status, body, nil)

			inputs := map[string]any{"url": server.URL}
			for k, v := range test.inputs {
				inputs[k] = v
			}

			_, err := runHTTPTask(t, inputs)

			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, test.wantErr)

			if test.check != nil {
				var taskErr *TaskError
				require.ErrorAs(t, err, &taskErr)
				test.check(t, taskErr)
			}
		})
	}
}

func Test_httpTask_retryAfter(t *testing.T) {
	tests := []struct {
		name   string
		status int
		header string
		want   time.Duration
	}{
		{name: "429 with seconds", status: http.StatusTooManyRequests, header: "30", want: 30 * time.Second},
		{name: "503 with seconds", status: http.StatusServiceUnavailable, header: "5", want: 5 * time.Second},
		{name: "no header", status: http.StatusTooManyRequests, header: ""},
		{name: "unparsable", status: http.StatusTooManyRequests, header: "soon"},
		{name: "beyond the cap is ignored", status: http.StatusTooManyRequests, header: "86400"},
		{name: "not a rate-limit status", status: http.StatusBadGateway, header: "30"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{}
			if test.header != "" {
				header.Set("Retry-After", test.header)
			}
			server, _ := httpTaskServer(t, test.status, "slow down", header)

			_, err := runHTTPTask(t, map[string]any{"url": server.URL})

			var taskErr *TaskError
			require.ErrorAs(t, err, &taskErr)
			require.Equal(t, test.want, taskErr.RetryAfter)
		})
	}
}

func Test_retryAfter(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		header string
		want   time.Duration
		wantOK bool
	}{
		{name: "empty", header: ""},
		{name: "whitespace", header: "   "},
		{name: "not a number or date", header: "later"},
		{name: "negative seconds", header: "-5", want: 0, wantOK: true},
		{name: "seconds", header: "30", want: 30 * time.Second, wantOK: true},
		{name: "zero seconds", header: "0", want: 0, wantOK: true},
		{name: "at the cap", header: "300", want: 5 * time.Minute, wantOK: true},
		{name: "beyond the cap", header: "301"},
		{name: "a day", header: "86400"},
		{
			name:   "an http date in the future",
			header: "Sun, 26 Jul 2026 12:00:30 GMT",
			want:   30 * time.Second,
			wantOK: true,
		},
		{
			// A date in the past means "now", not a negative wait.
			name:   "an http date in the past",
			header: "Sun, 26 Jul 2026 11:59:00 GMT",
			want:   0,
			wantOK: true,
		},
		{
			name:   "an http date beyond the cap",
			header: "Mon, 27 Jul 2026 12:00:00 GMT",
			wantOK: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := retryAfter(test.header, now)
			require.Equal(t, test.wantOK, ok)
			require.Equal(t, test.want, got)
		})
	}
}

func Test_httpTask_retryOnUnknownOutcome(t *testing.T) {
	// A POST whose response never came is not retried by default, because it may
	// have taken effect. An endpoint that is idempotency-keyed can say so.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	t.Cleanup(slow.Close)

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithTimeout(150*time.Millisecond))
	require.NoError(t, err)

	fn := taskFuncHTTP(policy)

	for _, test := range []struct {
		name      string
		optIn     bool
		wantRetry bool
	}{
		{name: "default is not retried", optIn: false, wantRetry: false},
		{name: "opting in makes it retryable", optIn: true, wantRetry: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			inputs := map[string]any{"method": "POST", "url": slow.URL, "body": "{}"}
			if test.optIn {
				inputs["retry_on_unknown_outcome"] = true
			}

			_, err := fn(t.Context(), NewNamedValues(inputs), nil)

			var taskErr *TaskError
			require.ErrorAs(t, err, &taskErr)
			require.Equal(t, test.wantRetry, taskErr.Retryable())
		})
	}
}

func Test_secretRefInQuery(t *testing.T) {
	// The intent is clear and only the placement is wrong, so the diagnostic says
	// where to put it instead of refusing anonymously. A query string is written to
	// access logs, kept in browser history, and sent onward in a Referer header on
	// redirect — a secret in one is a secret published.
	query := map[string]*Value{
		"token": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}

	_, err := applyQuery("https://api.example.com/v1", query)

	require.ErrorContains(t, err, "cannot go in a query parameter")
	require.ErrorContains(t, err, "access logs")
	require.ErrorContains(t, err, "bearer:")
	require.ErrorContains(t, err, "env:API_TOKEN", "the reference is named, since a reference is safe to log")
}

// Test_secretRefInJSONBody covers the position a body's reference is resolved in:
// here, as the body is encoded, and nowhere earlier.
//
// A JSON body is a legitimate place for a credential — an API taking its key in
// the body rather than in a header is ordinary — and it used to be refused, because
// a mapping holding a reference was an expression the *workflow* evaluated. What
// changed is the shape the reference travels in, not where it is read.
func Test_secretRefInJSONBody(t *testing.T) {
	const material = "material-that-must-not-appear-in-any-rendering"

	reference := &Value{Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}}
	body := NewStructureMap(map[string]*Value{
		"client_id":     NewLiteral("flowstate"),
		"client_secret": reference,
	})

	encoded, contentType, err := httpRequestBody(&Task_HTTP_Inputs{Json: body},
		func(*SecretRef) (string, error) { return material, nil })
	require.NoError(t, err)
	require.Equal(t, contentTypeJSON, contentType)
	require.JSONEq(t, `{"client_id":"flowstate","client_secret":"`+material+`"}`, encoded)

	// The inputs the activity was handed still name the reference and hold no
	// value, which is the whole of what rides the payload.
	require.NotContains(t, fmt.Sprintf("%v %+v %#v", body, body, body), material)
	require.Equal(t, "API_TOKEN", body.GetStructure().GetMap().GetEntries()["client_secret"].
		GetSecretRef().GetName())

	// And with nothing to resolve through, the reference is refused rather than
	// rendered as anything at all.
	_, _, err = httpRequestBody(&Task_HTTP_Inputs{Json: reference}, nil)
	require.ErrorContains(t, err, "secret reference")
	require.ErrorContains(t, err, "env:API_TOKEN")
}

// Test_secretRefInFormBody is the same position one step over: a form body is sent
// in the request like a JSON one, so a reference in it is resolved as it is
// encoded — while the query string built a few lines away stays refused.
func Test_secretRefInFormBody(t *testing.T) {
	const material = "material-that-must-not-appear-in-any-rendering"

	form := map[string]*Value{
		"grant_type":    NewLiteral("client_credentials"),
		"client_secret": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}

	encoded, contentType, err := httpRequestBody(&Task_HTTP_Inputs{Form: form},
		func(*SecretRef) (string, error) { return material, nil })
	require.NoError(t, err)
	require.Equal(t, contentTypeForm, contentType)
	require.Equal(t, "client_secret="+material+"&grant_type=client_credentials", encoded)

	_, _, err = httpRequestBody(&Task_HTTP_Inputs{Form: form}, nil)
	require.ErrorContains(t, err, "secret reference")
}

// Test_secretRefInHeaderStaysAReference covers the header path: the value is
// resolved as the header is set, and a control character in what came back is
// refused without the value being named.
func Test_secretRefInHeaderStaysAReference(t *testing.T) {
	const material = "material-that-must-not-appear-in-any-rendering"

	headers := NewStructureMap(map[string]*Value{
		"Accept":        NewLiteral("application/json"),
		"Authorization": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	})

	header, err := httpRequestHeaders(headers, func(*SecretRef) (string, error) { return material, nil })
	require.NoError(t, err)
	require.Equal(t, material, header.Get("Authorization"))
	require.Equal(t, "application/json", header.Get("Accept"))

	// Nothing that held the reference ever held the value.
	require.NotContains(t, fmt.Sprintf("%v %+v %#v %s", headers, headers, headers, headers), material)

	_, err = httpRequestHeaders(headers, func(*SecretRef) (string, error) {
		return "one\r\nX-Forged: yes", nil
	})
	require.ErrorContains(t, err, "forge further headers")
	require.NotContains(t, err.Error(), "X-Forged",
		"the refusal repeats the value it refused, which is the value it must not say")
}

// Test_httpTask_defaultOutputsDoubleCarryIsOverTheSizeBound pins the answer to
// #787's acceptance question about the http task's default-outputs shape: with
// `parse_json: true` and no `outputs:` declared, the result carries the body
// twice — Body as the raw string and Json as the parsed structure — so a JSON
// response at the 1 MiB body cap produces a result *over*
// [MaxTaskOutputBytes], and the choice pinned here is that it gets the
// diagnosis rather than being squeezed under the bound. The remedy is in the
// diagnosis itself: declare `outputs:` and select, instead of carrying the
// whole response twice.
//
// The check itself is applied at [Task.EvalInScope] — the conformance cases
// hold both drivers to that — so this test composes the task's own default
// shape with [CheckTaskOutputSize] directly rather than re-running a driver.
func Test_httpTask_defaultOutputsDoubleCarryIsOverTheSizeBound(t *testing.T) {
	// A JSON string just under the 1 MiB response cap
	// (netpolicy.DefaultMaxResponseBytes), so the read succeeds and the
	// double-carry is what crosses the bound.
	body := `"` + strings.Repeat("x", (1<<20)-1024) + `"`
	server, _ := httpTaskServer(t, http.StatusOK, body, http.Header{"Content-Type": []string{"application/json"}})

	out, err := runHTTPTask(t, map[string]any{
		"method":     http.MethodGet,
		"url":        server.URL,
		"parse_json": true,
	})
	require.NoError(t, err, "the body is under the response cap; reading it must succeed")

	err = CheckTaskOutputSize(out)
	require.Error(t, err,
		"a maximal parsed JSON body carried twice (Body + Json) must be over the output size bound")
	require.Contains(t, err.Error(), "byte limit")
	require.Contains(t, err.Error(), "outputs: input",
		"the diagnosis must point at the remedy: select fields instead of carrying the whole response")
}

// Test_httpTask_requestBodyFraming pins what the request body looks like on the
// wire, in both directions the credential-gating reorder moved through.
//
// The body is attached to a request that was already constructed, rather than
// handed to [http.NewRequestWithContext], so the framing rules that constructor
// applies are this file's to get right. Two of them are load-bearing and neither
// is visible from the body bytes alone:
//
//   - An author's explicitly empty body must go out as Content-Length: 0. A
//     non-nil Body with ContentLength 0 reads as *unknown* length
//     ([http.Request.outgoingLength]), which the transport frames as
//     Transfer-Encoding: chunked — a different request to a signing scheme, or to
//     a peer that refuses chunked.
//   - GetBody must be able to produce the body again, or a redirected POST
//     arrives at the second hop with nothing in it.
func Test_httpTask_requestBodyFraming(t *testing.T) {
	t.Run("an explicitly empty body is framed by length, not chunked", func(t *testing.T) {
		var contentLength, transferEncoding string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			contentLength = r.Header.Get("Content-Length")
			transferEncoding = strings.Join(r.TransferEncoding, ",")
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)

		_, err := runHTTPTask(t, map[string]any{
			"method": http.MethodPost,
			"url":    server.URL,
			"body":   "",
		})
		require.NoError(t, err)
		require.Equal(t, "0", contentLength)
		require.Empty(t, transferEncoding, "an empty body is a known length, so nothing is chunked")
	})

	t.Run("a body survives a redirect, which is what GetBody is for", func(t *testing.T) {
		var secondHop string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/start" {
				http.Redirect(w, r, "/next", http.StatusTemporaryRedirect)
				return
			}
			body, _ := io.ReadAll(r.Body)
			secondHop = string(body)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)

		_, err := runHTTPTask(t, map[string]any{
			"method": http.MethodPost,
			"url":    server.URL + "/start",
			"json":   map[string]any{"a": 1},
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"a":1}`, secondHop,
			"a 307 replays the body, so the second hop must receive it rather than an empty request")
	})
}
