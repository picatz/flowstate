package gates

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// fakeAPI answers the two methods the page calls and records what it was asked.
type fakeAPI struct {
	flowstatev1connect.WorkflowServiceClient

	mu         sync.Mutex
	get        getFunc
	signal     func(*connect.Request[v1.SignalRequest]) (*connect.Response[v1.SignalResponse], error)
	signals    []*v1.SignalRequest
	authorized []string
}

func (f *fakeAPI) GetGate(_ context.Context, req *connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
	f.mu.Lock()
	f.authorized = append(f.authorized, req.Header().Get("Authorization"))
	get := f.get
	f.mu.Unlock()

	return get(req)
}

func (f *fakeAPI) Signal(_ context.Context, req *connect.Request[v1.SignalRequest]) (*connect.Response[v1.SignalResponse], error) {
	f.mu.Lock()
	f.signals = append(f.signals, req.Msg)
	f.authorized = append(f.authorized, req.Header().Get("Authorization"))
	f.mu.Unlock()

	if f.signal == nil {
		return connect.NewResponse(&v1.SignalResponse{}), nil
	}

	return f.signal(req)
}

func (f *fakeAPI) sent() []*v1.SignalRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*v1.SignalRequest(nil), f.signals...)
}

func (f *fakeAPI) credentials() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.authorized...)
}

const (
	testWorkflow = "flowstate-workflow-1234"
	testSignal   = "deploy-approved"
)

type getFunc = func(*connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error)

// waiting is a gate the caller may answer.
func waiting(prompt string) getFunc {
	return gateFor(prompt, true)
}

// gateFor is the open gate the tests address, answerable or not.
func gateFor(prompt string, mayAnswer bool) getFunc {
	return func(*connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
		return connect.NewResponse(&v1.GetGateResponse{
			WorkflowId: testWorkflow,
			RunId:      "run-1",
			StepId:     "approval",
			SignalName: testSignal,
			Prompt:     prompt,
			Starter:    "https://issuer.example.com#requester@example.com",
			Deadline:   timestamppb.New(time.Date(2026, 10, 5, 12, 30, 0, 0, time.UTC)),
			MayAnswer:  mayAnswer,
		}), nil
	}
}

// notOpen is the one answer for a gate that is gone, closed, or not the caller's.
func notOpen(*connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
	return nil, connect.NewError(connect.CodeNotFound, nil)
}

func do(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	return rec
}

func gateGet(headers ...string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, Path(testWorkflow, testSignal), nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}

	return req
}

func gatePost(form url.Values, headers ...string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, Path(testWorkflow, testSignal), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}

	return req
}

func TestShowRendersTheGate(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{get: waiting("Approve deploying v1.4.2 to production?")}
	rec := do(newHandler(api), gateGet("Authorization", "Bearer alice"))

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "Approve deploying v1.4.2 to production?")
	require.Contains(t, body, "approval", "the step is shown")
	require.Contains(t, body, "requester@example.com", "who asked is shown")
	require.Contains(t, body, "2026-10-05 12:30 UTC", "the deadline is shown")
	require.Contains(t, body, `value="approve"`)
	require.Contains(t, body, `value="deny"`)
	require.Contains(t, body, `action="/gates/`+testWorkflow+`/`+testSignal+`"`)
	require.Equal(t, []string{"Bearer alice"}, api.credentials(), "the visitor's credential is what the API sees")
	require.Empty(t, api.sent(), "viewing a gate answers nothing")
}

func TestPagesAreHardened(t *testing.T) {
	t.Parallel()

	rec := do(newHandler(&fakeAPI{get: waiting("ok")}), gateGet())

	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"))

	csp := rec.Header().Get("Content-Security-Policy")
	require.Contains(t, csp, "default-src 'none'")
	require.Contains(t, csp, "frame-ancestors 'none'")
	require.NotContains(t, csp, "script-src")
	require.NotContains(t, csp, "unsafe-inline")

	// The policy names the page's own stylesheet by hash, so the one inline
	// style the page ships runs and no other would.
	style := regexp.MustCompile(`(?s)<style>(.*?)</style>`).FindStringSubmatch(rec.Body.String())
	require.Len(t, style, 2)
	sum := sha256.Sum256([]byte(style[1]))
	require.Contains(t, csp, "'sha256-"+base64.StdEncoding.EncodeToString(sum[:])+"'")
	require.NotContains(t, rec.Body.String(), "<script")
}

func TestAPromptIsTextNeverMarkup(t *testing.T) {
	t.Parallel()

	hostile := `</p><script>alert(1)</script><img src=x onerror=alert(2)>`
	rec := do(newHandler(&fakeAPI{get: waiting(hostile)}), gateGet())

	body := rec.Body.String()
	require.NotContains(t, body, "<script>")
	require.NotContains(t, body, "<img")
	require.Contains(t, body, "&lt;script&gt;")
}

func TestAGateThatIsNotOpenIsOneAnswer(t *testing.T) {
	t.Parallel()

	// The API answers every one of these the same way, which is the point: the
	// page has nothing to tell them apart with, and shows none of it.
	runs := map[string]getFunc{
		"finished run, other signal, or no such run or tenant": notOpen,
		"an answer for another gate": func(*connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
			return connect.NewResponse(&v1.GetGateResponse{WorkflowId: testWorkflow, RunId: "run-1", SignalName: "another", MayAnswer: true}), nil
		},
		"an answer naming no run": func(*connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
			return connect.NewResponse(&v1.GetGateResponse{WorkflowId: testWorkflow, SignalName: testSignal, MayAnswer: true}), nil
		},
	}

	var first string
	for name, get := range runs {
		api := &fakeAPI{get: get}
		rec := do(newHandler(api), gateGet())
		require.Equal(t, http.StatusNotFound, rec.Code, name)

		if first == "" {
			first = rec.Body.String()
		}
		require.Equal(t, first, rec.Body.String(), name)

		rec = do(newHandler(api), gatePost(url.Values{"decision": {"approve"}}))
		require.Equal(t, http.StatusNotFound, rec.Code, name)
		require.Empty(t, api.sent(), name)
	}
}

func TestApproveAndDenyDeliverTheSignal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		decision string
		approved bool
		comment  string
	}{
		{"approve", true, ""},
		{"deny", false, "not before the freeze ends"},
	} {
		t.Run(tc.decision, func(t *testing.T) {
			t.Parallel()

			api := &fakeAPI{get: waiting("go?")}
			rec := do(newHandler(api), gatePost(url.Values{
				"decision": {tc.decision},
				"comment":  {"  " + tc.comment + "  "},
			}, "Authorization", "Bearer alice"))

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			sent := api.sent()
			require.Len(t, sent, 1)
			require.Equal(t, testWorkflow, sent[0].GetWorkflowId())
			require.Equal(t, testSignal, sent[0].GetName())
			require.Equal(t, "run-1", sent[0].GetRunId(), "the answer is pinned to the run the gate was read on")

			named := sent[0].GetPayload().GetNamedValues()
			require.Equal(t, tc.approved, named["approved"].GetLiteral().GetBoolValue())
			if tc.comment == "" {
				require.NotContains(t, named, "comment")
			} else {
				require.Equal(t, tc.comment, named["comment"].GetLiteral().GetStringValue())
			}

			require.Equal(t, []string{"Bearer alice", "Bearer alice"}, api.credentials())
		})
	}
}

func TestAnAnswerNeedsTheBrowsersWordThatItCameFromThisPage(t *testing.T) {
	t.Parallel()

	form := url.Values{"decision": {"approve"}}

	for name, tc := range map[string]struct {
		site    string
		origin  string
		allowed bool
	}{
		"same-origin":                    {"same-origin", "", true},
		"cross-site":                     {"cross-site", "", false},
		"same-site but not same origin":  {"same-site", "", false},
		"user navigation":                {"none", "", false},
		"cross-site beats a good origin": {"cross-site", "http://example.com", false},
		"origin matches host":            {"", "http://example.com", true},
		"origin names another host":      {"", "https://evil.example", false},
		"origin null":                    {"", "null", false},
		"neither header":                 {"", "", false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := &fakeAPI{get: waiting("go?")}
			req := gatePost(form)
			req.Header.Del("Sec-Fetch-Site")
			if tc.site != "" {
				req.Header.Set("Sec-Fetch-Site", tc.site)
			}
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			rec := do(newHandler(api), req)

			if tc.allowed {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.Len(t, api.sent(), 1)

				return
			}

			require.Equal(t, http.StatusForbidden, rec.Code)
			require.Empty(t, api.sent(), "a request the browser does not vouch for must reach nothing")
			require.Empty(t, api.credentials(), "and must not even read the run")
		})
	}
}

func TestAnAnswerToAGateSomeoneElseAnsweredIsNotBuffered(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{get: notOpen}
	rec := do(newHandler(api), gatePost(url.Values{"decision": {"approve"}}))

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Empty(t, api.sent(), "a signal sent to a name nobody waits on is held for the next gate that does")
}

func TestMalformedAnswersSendNothing(t *testing.T) {
	t.Parallel()

	for name, form := range map[string]url.Values{
		"no decision":      {},
		"unknown decision": {"decision": {"maybe"}},
		"huge comment":     {"decision": {"approve"}, "comment": {strings.Repeat("x", MaxCommentBytes+1)}},
		"invalid utf-8":    {"decision": {"approve"}, "comment": {"\xff\xfe"}},
		"oversized body":   {"decision": {"approve"}, "comment": {strings.Repeat("x", maxFormBytes)}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := &fakeAPI{get: waiting("go?")}
			rec := do(newHandler(api), gatePost(form))

			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Empty(t, api.sent())
		})
	}
}

func TestRefusalsReportTheServersDecision(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		err      error
		status   int
		contains string
		absent   string
	}{
		"policy refusal": {
			err:      connect.NewError(connect.CodePermissionDenied, errString("subject carol is not allowed to send deploy-approved")),
			status:   http.StatusForbidden,
			contains: "carol is not allowed",
		},
		"no credential": {
			err:    connect.NewError(connect.CodeUnauthenticated, errString("missing bearer token")),
			status: http.StatusUnauthorized,
			absent: "missing bearer token",
		},
		"server failure": {
			err:    connect.NewError(connect.CodeInternal, errString("dial tcp 10.0.0.7:7233: refused")),
			status: http.StatusBadGateway,
			absent: "10.0.0.7",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := &fakeAPI{
				get: waiting("go?"),
				signal: func(*connect.Request[v1.SignalRequest]) (*connect.Response[v1.SignalResponse], error) {
					return nil, tc.err
				},
			}
			rec := do(newHandler(api), gatePost(url.Values{"decision": {"approve"}}))

			require.Equal(t, tc.status, rec.Code)
			if tc.contains != "" {
				require.Contains(t, rec.Body.String(), tc.contains)
			}
			if tc.absent != "" {
				require.NotContains(t, rec.Body.String(), tc.absent)
			}
			if tc.status == http.StatusUnauthorized {
				require.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestPathRoundTrips(t *testing.T) {
	t.Parallel()

	for _, workflow := range []string{"plain-id", "with space", "a/b", "100%", "ünï"} {
		var got string
		api := &fakeAPI{get: func(req *connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
			got = req.Msg.GetWorkflowId()

			return nil, connect.NewError(connect.CodeNotFound, nil)
		}}

		do(newHandler(api), httptest.NewRequest(http.MethodGet, Path(workflow, testSignal), nil))
		require.Equal(t, workflow, got, Path(workflow, testSignal))
	}
}

func TestUnknownPathsAndMethods(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{get: waiting("go?")}
	h := newHandler(api)

	require.Equal(t, http.StatusNotFound, do(h, httptest.NewRequest(http.MethodGet, "/gates/", nil)).Code)
	require.Equal(t, http.StatusNotFound, do(h, httptest.NewRequest(http.MethodGet, "/gates/only-one-segment", nil)).Code)
	require.Equal(t, http.StatusNotFound, do(h, httptest.NewRequest(http.MethodPut, Path(testWorkflow, testSignal), nil)).Code)
	require.Empty(t, api.credentials(), "none of them reached the API")
}

// getter is the API behind the loopback: it serves GetGate with an open gate.
type getter struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler
}

func (getter) GetGate(_ context.Context, req *connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
	return waiting("loopback prompt")(req)
}

func TestLoopbackReachesTheAPIWithTheVisitorsCredentialAndConnection(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(getter{}))

	var (
		authorization string
		remote        string
		overTLS       bool
	)
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization, remote, overTLS = r.Header.Get("Authorization"), r.RemoteAddr, r.TLS != nil
		mux.ServeHTTP(w, r)
	})

	req := gateGet("Authorization", "Bearer alice")
	req.RemoteAddr = "203.0.113.9:4444"
	req.TLS = &tls.ConnectionState{}
	rec := do(New(api), req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "loopback prompt")
	require.Equal(t, "Bearer alice", authorization)
	require.Equal(t, "203.0.113.9:4444", remote)
	require.True(t, overTLS)
}

func TestLoopbackRefusesAnAPIAnswerTooLargeToRead(t *testing.T) {
	t.Parallel()

	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, maxAPIResponseBytes+1))
	})
	rec := do(New(api), gateGet())

	require.Equal(t, http.StatusBadGateway, rec.Code)
}

func TestAnAPIThatIsUnauthenticatedMakesThePageUnauthenticated(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(getter{}))
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer alice" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
	h := New(api)

	rec := do(h, gateGet())
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.NotContains(t, rec.Body.String(), "loopback prompt")

	require.Equal(t, http.StatusOK, do(h, gateGet("Authorization", "Bearer alice")).Code)
}

func TestWithCredentialsReplacesTheDefault(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{get: waiting("go?")}
	h := newHandler(api, WithCredentials(func(r *http.Request) string {
		if c, err := r.Cookie("session"); err == nil {
			return "Bearer " + c.Value
		}

		return ""
	}))

	req := gateGet("Authorization", "Bearer ignored")
	req.AddCookie(&http.Cookie{Name: "session", Value: "from-cookie"})
	do(h, req)

	require.Equal(t, []string{"Bearer from-cookie"}, api.credentials())
}

func TestAnUnexpectedFailureIsLoggedOnOneLine(t *testing.T) {
	t.Parallel()

	var logged strings.Builder
	api := &fakeAPI{get: func(*connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
		return nil, connect.NewError(connect.CodeInternal, errString("bad id\nlevel=ERROR msg=forged"))
	}}
	h := newHandler(api, WithLogger(slog.New(slog.NewTextHandler(&logged, nil))))

	rec := do(h, gateGet())

	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, 1, strings.Count(strings.TrimRight(logged.String(), "\n"), "\n")+1, logged.String())
}

func TestAGateMissingFromATruncatedListIsNotReportedClosed(t *testing.T) {
	t.Parallel()

	// The API says the run holds more gates than one answer lists and this one
	// is not among them: FAILED_PRECONDITION, which the page tells from "not
	// open".
	api := &fakeAPI{get: func(*connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errString("the run holds more than 64 open gates"))
	}}
	h := newHandler(api)

	shown := do(h, gateGet())
	require.Equal(t, http.StatusServiceUnavailable, shown.Code)
	require.NotContains(t, shown.Body.String(), "not open")

	answered := do(h, gatePost(url.Values{"decision": {"approve"}}))
	require.Equal(t, http.StatusServiceUnavailable, answered.Code)
	require.Empty(t, api.sent(), "an answer must not be sent to a gate the page could not see")
}

func TestTheGateIsReadWithGetGateAndNeverWithGet(t *testing.T) {
	t.Parallel()

	// fakeAPI embeds a nil client: a call to Get (or anything but GetGate and
	// Signal) panics, so a regression to the read-scoped RPC cannot pass.
	var asked *v1.GetGateRequest
	api := &fakeAPI{get: func(req *connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
		asked = req.Msg

		return waiting("go?")(req)
	}}
	rec := do(newHandler(api), gateGet("Authorization", "Bearer signal-only"))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, testWorkflow, asked.GetWorkflowId())
	require.Equal(t, testSignal, asked.GetSignalName())
	require.Equal(t, []string{"Bearer signal-only"}, api.credentials())
}

func TestASignalOnlyApproverOpensAndAnswersTheGate(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{get: waiting("go?")}
	h := newHandler(api)

	require.Equal(t, http.StatusOK, do(h, gateGet("Authorization", "Bearer signal-only")).Code)

	rec := do(h, gatePost(url.Values{"decision": {"approve"}}, "Authorization", "Bearer signal-only"))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, api.sent(), 1)
}

func TestAVisitorThePolicyRefusesSeesTheGateReadOnlyAndSendsNothing(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{get: gateFor("Approve deploying v1.4.2?", false)}
	h := newHandler(api)

	shown := do(h, gateGet("Authorization", "Bearer carol"))
	require.Equal(t, http.StatusOK, shown.Code)
	body := shown.Body.String()
	require.Contains(t, body, "Approve deploying v1.4.2?", "the question is shown")
	require.Contains(t, body, "signal policy does not admit you")
	require.NotContains(t, body, "<form")
	require.NotContains(t, body, `value="approve"`)
	require.NotContains(t, body, `value="deny"`)

	answered := do(h, gatePost(url.Values{"decision": {"approve"}}, "Authorization", "Bearer carol"))
	require.Equal(t, http.StatusForbidden, answered.Code)
	require.Contains(t, answered.Body.String(), "signal policy does not admit you")
	require.Empty(t, api.sent(), "the answer must not be sent when the read said the policy refuses")
}
