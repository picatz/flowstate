package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The REST fallback goes through the same seam as the GraphQL walk: an
// *http.Client and an endpoint, pointed at a local server that serves the
// review-thread route. It covers the request the hook builds and the
// response it parses, not the proxy that makes the route exist.

const ccrThreadsPath = "/repos/picatz/flowstate/pulls/488/ccr/review_threads"

func ccrThreadsServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Errorf("request carried no Authorization header")
		}
		if r.Method != http.MethodGet || r.URL.Path != ccrThreadsPath {
			t.Errorf("request was %s %s, want GET %s", r.Method, r.URL.Path, ccrThreadsPath)
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	server.Client()
	return server
}

func TestUnresolvedThreadsRESTNamesTheUnresolvedOnes(t *testing.T) {
	t.Parallel()

	srv := ccrThreadsServer(t, http.StatusOK, `[
		{"resolved": false, "outdated": false, "path": ".claude/hooks/run-hook.sh", "line": 20, "comment_ids": [3981494174, 3981746799]},
		{"resolved": true, "outdated": true, "path": ".claude/hooks/session-env.sh", "line": null, "comment_ids": [3981487674]},
		{"resolved": false, "outdated": true, "path": ".claude/hooks/source-id.sh", "line": null, "comment_ids": []}
	]`)
	threads, err := unresolvedThreadsREST(context.Background(), srv.Client(), srv.URL, "tok", "picatz", "flowstate", 488)
	if err != nil {
		t.Fatalf("unresolvedThreadsREST: %v", err)
	}
	if len(threads) != 2 {
		t.Fatalf("got %d unresolved threads, want 2: %+v", len(threads), threads)
	}
	if want := "https://github.com/picatz/flowstate/pull/488#discussion_r3981494174"; threads[0].URL != want {
		t.Errorf("first thread URL = %q, want %q", threads[0].URL, want)
	}
	if want := ".claude/hooks/run-hook.sh:20"; threads[0].Body != want {
		t.Errorf("first thread body = %q, want %q", threads[0].Body, want)
	}
	if threads[1].URL != "" || threads[1].Body != ".claude/hooks/source-id.sh" {
		t.Errorf("thread without comments = %+v, want no URL and the bare path", threads[1])
	}
	if msg := denyMessage("picatz", "flowstate", 488, threads); !strings.Contains(msg, "2 unresolved") || !strings.Contains(msg, "discussion_r3981494174") {
		t.Errorf("denial does not name the REST-sourced threads:\n%s", msg)
	}
}

func TestUnresolvedThreadsRESTFailsClosedOnAnUnavailableRoute(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"no such route", http.StatusNotFound, `{"message":"No such CCR pull-request route."}`},
		{"not json", http.StatusOK, `<html>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := ccrThreadsServer(t, tc.status, tc.body)
			threads, err := unresolvedThreadsREST(context.Background(), srv.Client(), srv.URL, "tok", "picatz", "flowstate", 488)
			if err == nil {
				t.Fatalf("expected an error, got threads %+v", threads)
			}
		})
	}
}

// TestReviewThreadEvidenceFallsBackToRESTAndStillFailsClosed is the case
// the fallback exists for: GraphQL answers 403 the way a REST-only proxy
// does, REST answers, and the hook sees the thread. When REST is down too,
// the error names both, and no thread list is returned that could read as
// clean.
func TestReviewThreadEvidenceFallsBackToRESTAndStillFailsClosed(t *testing.T) {
	t.Parallel()

	// Two servers, so a server-bound test client would route every request
	// to one of them: these listen on loopback and a plain client follows
	// each URL to its own server.
	client := &http.Client{Timeout: requestTimeout}
	requests := map[string]int{}
	listening := func(status int, body string) *httptest.Server {
		var srv *httptest.Server
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests[srv.URL]++
			w.WriteHeader(status)
			w.Write([]byte(body))
		}))
		t.Cleanup(srv.Close)
		return srv
	}
	graphql := listening(http.StatusForbidden, `{"message":"GitHub GraphQL is not available from Claude Code sessions"}`)
	rest := listening(http.StatusOK, `[{"resolved": false, "path": "AGENTS.md", "line": 3, "comment_ids": [7]}]`)
	threads, err := reviewThreadEvidence(context.Background(), client, graphql.URL, rest.URL, "tok", "picatz", "flowstate", 488)
	if err != nil {
		t.Fatalf("reviewThreadEvidence with REST available: %v", err)
	}
	if len(threads) != 1 || threads[0].Body != "AGENTS.md:3" {
		t.Fatalf("threads = %+v, want the one REST reported", threads)
	}

	restDown := listening(http.StatusNotFound, `{}`)
	threads, err = reviewThreadEvidence(context.Background(), client, graphql.URL, restDown.URL, "tok", "picatz", "flowstate", 488)
	if err == nil {
		t.Fatalf("expected an error with both transports failing, got %+v", threads)
	}
	for _, want := range []string{"GraphQL:", "REST:", "403", "404"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	// GraphQL first: a healthy GraphQL answer is the whole check, and the
	// REST route is never asked.
	healthy := listening(http.StatusOK, `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[]}}}}}`)
	restUntouched := listening(http.StatusOK, `[{"resolved": false, "path": "AGENTS.md", "line": 1, "comment_ids": [9]}]`)
	threads, err = reviewThreadEvidence(context.Background(), client, healthy.URL, restUntouched.URL, "tok", "picatz", "flowstate", 488)
	if err != nil || len(threads) != 0 {
		t.Fatalf("GraphQL healthy: threads=%+v err=%v, want none and nil", threads, err)
	}
	if requests[healthy.URL] != 1 || requests[restUntouched.URL] != 0 {
		t.Fatalf("requests = %v, want one GraphQL request and no REST request", requests)
	}

	// A GraphQL attempt that consumed its whole context budget leaves the
	// REST attempt its own: an already-cancelled context fails GraphQL at
	// once and REST still answers.
	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()
	threads, err = reviewThreadEvidence(spent, client, graphql.URL, rest.URL, "tok", "picatz", "flowstate", 488)
	if err != nil || len(threads) != 1 {
		t.Fatalf("spent budget: threads=%+v err=%v, want the REST thread and nil", threads, err)
	}
}
