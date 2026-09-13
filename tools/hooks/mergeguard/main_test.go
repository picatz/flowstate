package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/tools/hooks/internal/hook"
)

// The seam: unresolvedThreads takes an *http.Client and an endpoint, so a
// test can point it at a local httptest.Server instead of api.github.com.
// That server round-trips real HTTP and real GraphQL-response JSON through
// the real request-building and response-parsing code below; it does not
// cover GitHub's actual API (auth semantics, real rate limiting, schema
// drift) or the mcp__github__merge_pull_request / gh CLI tools themselves.
// The connection-refused case goes through a genuinely dead listener, so
// the "API error" test exercises a real network failure, not a stubbed one.

// TestUnresolvedThreadsDeniesOnUnresolvedThread is case 1: a PR with one
// unresolved and one resolved thread reports exactly the unresolved one,
// and the hook's denial names it.
func TestUnresolvedThreadsDeniesOnUnresolvedThread(t *testing.T) {
	t.Parallel()

	srv := reviewThreadsServer(t, `{
		"data": {
			"repository": {
				"pullRequest": {
					"reviewThreads": {
						"nodes": [
							{
								"isResolved": false,
								"comments": {"nodes": [{"url": "https://github.com/picatz/flowstate/pull/488#discussion_r1", "body": "This CEL macro name is wrong, it binds the wrong variable."}]}
							},
							{
								"isResolved": true,
								"comments": {"nodes": [{"url": "https://github.com/picatz/flowstate/pull/488#discussion_r2", "body": "nit: typo"}]}
							}
						]
					}
				}
			}
		}
	}`)
	threads, err := unresolvedThreads(context.Background(), srv.Client(), srv.URL, "tok", "picatz", "flowstate", 488)
	if err != nil {
		t.Fatalf("unresolvedThreads: %v", err)
	}
	if len(threads) != 1 {
		t.Fatalf("got %d unresolved threads, want 1: %+v", len(threads), threads)
	}
	if threads[0].URL != "https://github.com/picatz/flowstate/pull/488#discussion_r1" {
		t.Errorf("unresolved thread URL = %q", threads[0].URL)
	}

	msg := denyMessage("picatz", "flowstate", 488, threads)
	if !strings.Contains(msg, "1 unresolved review thread") {
		t.Errorf("deny message missing count: %s", msg)
	}
	if !strings.Contains(msg, "discussion_r1") {
		t.Errorf("deny message does not name the unresolved thread: %s", msg)
	}
	if strings.Contains(msg, "discussion_r2") {
		t.Errorf("deny message names the resolved thread too: %s", msg)
	}
	if !strings.Contains(msg, "wrong variable") {
		t.Errorf("deny message does not quote the thread's comment: %s", msg)
	}
}

// TestUnresolvedThreadsAllowsWhenAllResolved is case 2: every thread
// resolved reports no unresolved threads, which main() treats as an allow.
func TestUnresolvedThreadsAllowsWhenAllResolved(t *testing.T) {
	t.Parallel()

	srv := reviewThreadsServer(t, `{
		"data": {
			"repository": {
				"pullRequest": {
					"reviewThreads": {
						"nodes": [
							{"isResolved": true, "comments": {"nodes": [{"url": "https://github.com/picatz/flowstate/pull/500#discussion_r9", "body": "done"}]}}
						]
					}
				}
			}
		}
	}`)
	threads, err := unresolvedThreads(context.Background(), srv.Client(), srv.URL, "tok", "picatz", "flowstate", 500)
	if err != nil {
		t.Fatalf("unresolvedThreads: %v", err)
	}
	if len(threads) != 0 {
		t.Fatalf("got %d unresolved threads, want 0: %+v", len(threads), threads)
	}
}

// TestUnresolvedThreadsReportsAPIError is case 3: a genuine network failure
// (a closed listener, not a stub) surfaces as an error rather than a panic or
// a false clean result, so main()'s caller can deny the merge.
func TestUnresolvedThreadsReportsAPIError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := srv.URL
	srv.Close() // the listener is now refusing connections: a real network error

	client := &http.Client{Timeout: 2 * time.Second}
	threads, err := unresolvedThreads(context.Background(), client, deadURL, "tok", "picatz", "flowstate", 501)
	if err == nil {
		t.Fatalf("unresolvedThreads against a dead server returned no error (threads=%v)", threads)
	}
	if threads != nil {
		t.Errorf("unresolvedThreads returned threads alongside an error: %+v", threads)
	}
}

// TestUnresolvedThreadsSurfacesGraphQLErrors covers the other API-error
// shape: a 200 response carrying a GraphQL errors array (bad auth, unknown
// PR, ...) must not be read as "zero unresolved threads".
func TestUnresolvedThreadsSurfacesGraphQLErrors(t *testing.T) {
	t.Parallel()

	srv := reviewThreadsServerStatus(t, http.StatusOK, `{"errors": [{"message": "Bad credentials"}]}`)
	_, err := unresolvedThreads(context.Background(), srv.Client(), srv.URL, "bad-tok", "picatz", "flowstate", 502)
	if err == nil {
		t.Fatal("unresolvedThreads did not error on a GraphQL errors payload")
	}
	if !strings.Contains(err.Error(), "Bad credentials") {
		t.Errorf("error does not name the GraphQL error: %v", err)
	}
}

// TestUnresolvedThreadsWalksToExhaustion is the traversal test CLAUDE.md's
// "Test the traversal, not just the step" calls for: two full pages of
// resolved threads followed by a third page holding one unresolved thread
// must still surface that thread. A version that reads only the first page
// (the P2 finding) would report this PR clean.
func TestUnresolvedThreadsWalksToExhaustion(t *testing.T) {
	t.Parallel()

	page1 := reviewThreadsPageBody(t, resolvedNodes(reviewThreadsPageSize), true, "cursor-1")
	page2 := reviewThreadsPageBody(t, resolvedNodes(reviewThreadsPageSize), true, "cursor-2")
	page3 := reviewThreadsPageBody(t, []threadNode{
		{Resolved: false, URL: "https://github.com/picatz/flowstate/pull/509#discussion_r3", Body: "unresolved, three pages in"},
	}, false, "")

	srv := sequencedServer(t, []string{page1, page2, page3})
	threads, err := unresolvedThreads(context.Background(), srv.Client(), srv.URL, "tok", "picatz", "flowstate", 509)
	if err != nil {
		t.Fatalf("unresolvedThreads: %v", err)
	}
	if len(threads) != 1 || threads[0].URL != "https://github.com/picatz/flowstate/pull/509#discussion_r3" {
		t.Fatalf("walk did not reach the unresolved thread on page 3: %+v", threads)
	}
}

// TestUnresolvedThreadsIncompleteAtBoundIsAnError is the other half of the
// same fix: a PR whose review threads never stop paginating (or legitimately
// exceed the bound) must not be walked forever, and must not be reported as
// "all resolved" when the walk gives up. Reaching the bound with hasNextPage
// still true is an incomplete check, which the caller's fail-open path
// treats the same as any other API failure.
func TestUnresolvedThreadsIncompleteAtBoundIsAnError(t *testing.T) {
	t.Parallel()

	requests := 0
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		body := reviewThreadsPageBody(t, resolvedNodes(reviewThreadsPageSize), true, "always-more")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	client := srv.Client()

	threads, err := unresolvedThreads(context.Background(), client, srv.URL, "tok", "picatz", "flowstate", 510)
	if err == nil {
		t.Fatalf("unresolvedThreads did not error on a walk that never terminates (threads=%v)", threads)
	}
	if !strings.Contains(err.Error(), "incomplete") {
		t.Errorf("error does not say the check is incomplete: %v", err)
	}
	if requests != maxReviewThreadRequests {
		t.Errorf("made %d requests, want exactly the bound %d (not fewer, not unbounded)", requests, maxReviewThreadRequests)
	}
}

// threadNode is the test-side description of one review-thread node.
type threadNode struct {
	Resolved bool
	URL      string
	Body     string
}

// resolvedNodes builds n resolved threads with no comment, the shape of an
// uneventful page.
func resolvedNodes(n int) []threadNode {
	nodes := make([]threadNode, n)
	for i := range nodes {
		nodes[i] = threadNode{Resolved: true}
	}
	return nodes
}

// reviewThreadsPageBody marshals a full GraphQL response envelope for one
// page, matching the shape unresolvedThreads decodes.
func reviewThreadsPageBody(t *testing.T, nodes []threadNode, hasNextPage bool, endCursor string) string {
	t.Helper()

	type comment struct {
		URL  string `json:"url"`
		Body string `json:"body"`
	}
	type node struct {
		IsResolved bool `json:"isResolved"`
		Comments   struct {
			Nodes []comment `json:"nodes"`
		} `json:"comments"`
	}
	var respNodes []node
	for _, n := range nodes {
		rn := node{IsResolved: n.Resolved}
		if n.URL != "" || n.Body != "" {
			rn.Comments.Nodes = []comment{{URL: n.URL, Body: n.Body}}
		}
		respNodes = append(respNodes, rn)
	}

	envelope := map[string]any{
		"data": map[string]any{
			"repository": map[string]any{
				"pullRequest": map[string]any{
					"reviewThreads": map[string]any{
						"pageInfo": map[string]any{
							"hasNextPage": hasNextPage,
							"endCursor":   endCursor,
						},
						"nodes": respNodes,
					},
				},
			},
		},
	}
	b, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal test page: %v", err)
	}
	return string(b)
}

// sequencedServer returns responses[i] to the i-th request it receives, and
// fails the test if it is called more times than there are responses (which
// would mean the walk under test did not stop where expected).
func sequencedServer(t *testing.T, responses []string) *httptest.Server {
	t.Helper()
	i := 0
	server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Errorf("request carried no Authorization header")
		}
		if i >= len(responses) {
			t.Fatalf("server received a %d-th request beyond the %d prepared pages", i+1, len(responses))
		}
		body := responses[i]
		i++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	server.Client() // Initialize URL before returning it to callers.
	return server
}

func reviewThreadsServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return reviewThreadsServerStatus(t, http.StatusOK, body)
}

func reviewThreadsServerStatus(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Errorf("request carried no Authorization header")
		}
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}
		if req.Query == "" {
			t.Errorf("request carried no GraphQL query")
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	server.Client() // Initialize URL before returning it to callers.
	return server
}

// TestMCPMergeTarget pins the exact-identification path: owner, repo and
// pullNumber come straight from the tool's structured arguments.
func TestMCPMergeTarget(t *testing.T) {
	t.Parallel()

	in := &hook.Input{
		ToolName: "mcp__github__merge_pull_request",
		ToolInput: map[string]any{
			"owner":      "picatz",
			"repo":       "flowstate",
			"pullNumber": float64(498),
		},
	}
	owner, repo, number, ok := mergeTarget(in)
	if !ok || owner != "picatz" || repo != "flowstate" || number != 498 {
		t.Errorf("mergeTarget(mcp) = %q, %q, %d, %v", owner, repo, number, ok)
	}

	missing := &hook.Input{
		ToolName:  "mcp__github__merge_pull_request",
		ToolInput: map[string]any{"owner": "picatz"},
	}
	if _, _, _, ok := mergeTarget(missing); ok {
		t.Error("mergeTarget(mcp) identified a call missing repo/pullNumber")
	}
}

// TestGHCLIMergeTarget covers both directions of the Bash path: a full PR
// URL or a number with an explicit -R/--repo identifies the PR, and a bare
// invocation (which gh would resolve from the current branch) does not,
// because that resolution is not this hook's to guess.
func TestGHCLIMergeTarget(t *testing.T) {
	t.Parallel()

	identify := []struct {
		cmd         string
		owner, repo string
		number      int
	}{
		{`gh pr merge https://github.com/picatz/flowstate/pull/498`, "picatz", "flowstate", 498},
		{`gh pr merge 498 -R picatz/flowstate`, "picatz", "flowstate", 498},
		{`gh pr merge --repo picatz/flowstate 498 --squash`, "picatz", "flowstate", 498},
		{`gh pr merge --repo=picatz/flowstate 498`, "picatz", "flowstate", 498},
		{`gh -R picatz/flowstate pr merge 498`, "picatz", "flowstate", 498},
		{`gh -Rpicatz/flowstate pr merge 498`, "picatz", "flowstate", 498},
		{`gh -R=picatz/flowstate pr merge 498`, "picatz", "flowstate", 498},
		{`gh --repo=picatz/flowstate pr merge 498`, "picatz", "flowstate", 498},
		{"gh pr \\" + "\n" + "merge 498 -R picatz/flowstate", "picatz", "flowstate", 498},
		{`(gh pr merge 498 -R picatz/flowstate)`, "picatz", "flowstate", 498},
		{`cd /repo && gh pr merge 498 -R picatz/flowstate`, "picatz", "flowstate", 498},
		// A value-taking flag ahead of the positional target: without
		// consuming its argument, "$sha" is mistaken for the target and the
		// real target (498) is never found. Covers the P2 finding on #503.
		{`gh pr merge --match-head-commit "$sha" 498 -R picatz/flowstate`, "picatz", "flowstate", 498},
		// Both single- and multi-word flag values remain one shell word and
		// are consumed without becoming the positional target.
		{`gh pr merge --body release-notes --author-email a@b.com -t subject 498 -R picatz/flowstate`, "picatz", "flowstate", 498},
		{`echo "gh pr merge"; gh pr merge 999 -R owner/repo`, "owner", "repo", 999},
		{`gh pr merge --body "note 1" 999 -R owner/repo`, "owner", "repo", 999},
		{`gh pr merge --subject 'release 42' 999 --repo 'owner/repo'`, "owner", "repo", 999},
	}
	for _, tt := range identify {
		owner, repo, number, ok := ghCLIMergeTarget(tt.cmd)
		if !ok || owner != tt.owner || repo != tt.repo || number != tt.number {
			t.Errorf("ghCLIMergeTarget(%q) = %q, %q, %d, %v; want %q, %q, %d, true",
				tt.cmd, owner, repo, number, ok, tt.owner, tt.repo, tt.number)
		}
	}

	allow := []string{
		``,
		`gh pr list`,
		`gh pr merge`,
		`gh pr merge --squash`,
		`gh pr merge 498`,                      // no repo named: would resolve from the branch
		`git commit -m "docs: gh pr merge 42"`, // prose, not an invocation
	}
	for _, cmd := range allow {
		if owner, repo, number, ok := ghCLIMergeTarget(cmd); ok {
			t.Errorf("ghCLIMergeTarget(%q) identified a PR (%q, %q, %d) it should not have", cmd, owner, repo, number)
		}
	}
}

// TestUnidentifiedMergeCallWarns is the gap the coordinator flagged: a
// merge attempt this hook recognizes but cannot identify a PR for must warn
// the same way an API failure does, not allow silently. A call that is not
// a merge attempt at all gets no warning, because there is nothing to warn
// about.
func TestUnidentifiedMergeCallWarns(t *testing.T) {
	t.Parallel()

	warns := []struct {
		name string
		in   *hook.Input
	}{
		{"bare gh pr merge", &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": "gh pr merge"}}},
		{"gh pr merge with flags but no repo", &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": "gh pr merge --squash"}}},
		{"gh pr merge with a number but no -R", &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": "gh pr merge 498"}}},
		{"mcp call missing pullNumber", &hook.Input{ToolName: "mcp__github__merge_pull_request", ToolInput: map[string]any{"owner": "picatz", "repo": "flowstate"}}},
		{"mcp call missing repo", &hook.Input{ToolName: "mcp__github__merge_pull_request", ToolInput: map[string]any{"owner": "picatz", "pullNumber": float64(498)}}},
	}
	for _, tt := range warns {
		if _, _, _, ok := mergeTarget(tt.in); ok {
			t.Fatalf("%s: mergeTarget unexpectedly identified a PR", tt.name)
		}
		reason := unidentifiedMergeReason(tt.in)
		if reason == "" {
			t.Errorf("%s: unidentifiedMergeReason returned no reason for a merge attempt", tt.name)
			continue
		}
		if !strings.Contains(reason, "Merge blocked") {
			t.Errorf("%s: reason does not say the merge is blocked: %s", tt.name, reason)
		}
	}
	if reason := unidentifiedMergeReason(warns[2].in); !strings.Contains(reason, "-R") {
		t.Errorf("bare-number case does not say how to make the PR identifiable: %s", reason)
	}

	silent := []struct {
		name string
		in   *hook.Input
	}{
		{"not a merge call", &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": "gh pr list"}}},
		{"unrelated MCP tool", &hook.Input{ToolName: "mcp__github__get_pull_request", ToolInput: map[string]any{}}},
		{"prose mentioning gh pr merge", &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": `git commit -m "docs: gh pr merge 42"`}}},
	}
	for _, tt := range silent {
		if reason := unidentifiedMergeReason(tt.in); reason != "" {
			t.Errorf("%s: warned on a call that is not an identifiable merge attempt: %s", tt.name, reason)
		}
	}
}

func TestAutoMergeIsRejected(t *testing.T) {
	t.Parallel()
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --auto",
		"gh>/tmp/merge-output pr merge 498 -R picatz/flowstate --auto",
		"gh 2> /tmp/merge-error pr merge 498 -R picatz/flowstate --auto",
		"gh 2>&1 pr merge 498 -R picatz/flowstate --auto",
		"gh 2>& 1 pr merge 498 -R picatz/flowstate --auto",
		"gh &>/dev/null pr merge 498 -R picatz/flowstate --auto",
		"gh &>>/tmp/log pr merge 498 -R picatz/flowstate --auto",
		"gh >|/tmp/log pr merge 498 -R picatz/flowstate --auto",
		"true 2>&1&& gh pr merge 498 -R picatz/flowstate --auto",
		"true 2>& 1&& gh pr merge 498 -R picatz/flowstate --auto",
		"true &>/dev/null&& gh pr merge 498 -R picatz/flowstate --auto",
		"true &>>/tmp/log&& gh pr merge 498 -R picatz/flowstate --auto",
		"true >|/tmp/log&& gh pr merge 498 -R picatz/flowstate --auto",
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + strings.Repeat("a", 40) + " 2>&1 --auto",
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + strings.Repeat("a", 40) + " &>/dev/null --auto",
		"gh pr merge 498 -R picatz/flowstate --auto=true",
		"gh pr merge --auto https://github.com/picatz/flowstate/pull/498",
		"gh --help=false pr merge 498 -R picatz/flowstate --auto",
		"gh pr --help=false merge 498 -R picatz/flowstate --auto",
		"gh --help=0 pr merge 498 -R picatz/flowstate --auto",
		"gh pr --help=False merge 498 -R picatz/flowstate --auto",
		"gh --help --help=false pr merge 498 -R picatz/flowstate --auto",
		"gh pr -R picatz/flowstate merge 498 --auto",
		"gh -R picatz/flowstate pr merge 498 --auto",
		"gh -Rpicatz/flowstate pr merge 498 --auto",
		"gh -R=picatz/flowstate pr merge 498 --auto",
		"gh --repo=picatz/flowstate pr merge 498 --auto=true",
		"gh pr merge 498 -R picatz/flowstate --squash && gh pr merge 499 -R picatz/flowstate --auto",
		"gh pr \\" + "\n" + "merge 498 -R picatz/flowstate --auto",
		"(gh pr merge 498 -R picatz/flowstate --auto)",
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if !autoMergeRequested(in) {
			t.Errorf("autoMergeRequested(%q) = false", command)
		}
	}
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --squash",
		"gh pr merge 498 -R picatz/flowstate --body '--auto'",
		"git commit -m 'never run gh pr merge --auto'",
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if autoMergeRequested(in) {
			t.Errorf("autoMergeRequested(%q) = true", command)
		}
	}
}

func TestMergeHelpOnlyIsAllowed(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"gh pr merge --help", "gh pr merge 498 -R picatz/flowstate -h"} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if !mergeHelpOnly(in) {
			t.Errorf("mergeHelpOnly(%q) = false", command)
		}
	}
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --help --auto",
		"gh pr merge --help && gh pr merge 498 -R picatz/flowstate --squash",
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if mergeHelpOnly(in) {
			t.Errorf("mergeHelpOnly(%q) = true", command)
		}
	}
}

func TestAdminMergeIsRejected(t *testing.T) {
	t.Parallel()
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --admin",
		"gh pr merge 498 -R picatz/flowstate --admin=true",
		"gh pr merge 498 -R picatz/flowstate --admin=1",
		"gh pr merge 498 -R picatz/flowstate --admin=TRUE",
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if !adminMergeRequested(in) {
			t.Errorf("adminMergeRequested(%q) = false", command)
		}
	}
}

func TestDisableAutoOnlyIsNotTreatedAsAMerge(t *testing.T) {
	t.Parallel()
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --disable-auto",
		"gh pr merge 498 -R picatz/flowstate --disable-auto=true",
		"gh pr merge 498 -R picatz/flowstate --body '--disable-auto' --disable-auto",
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if !disableAutoOnly(in) {
			t.Errorf("disableAutoOnly(%q) = false", command)
		}
	}
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --disable-auto=false",
		"gh pr merge 498 -R picatz/flowstate --body '--disable-auto'",
		"gh pr merge 498 -R picatz/flowstate --squash --disable-auto --disable-auto=false",
		"gh pr merge 498 -R picatz/flowstate --squash --disable-auto --disable-auto=0",
		"gh pr merge 498 -R picatz/flowstate --squash --disable-auto --disable-auto=f",
		"gh pr merge 498 -R picatz/flowstate --squash --disable-auto --disable-auto=F",
		"gh pr merge 498 -R picatz/flowstate --squash --disable-auto --disable-auto=False",
		"gh pr merge 498 -R picatz/flowstate --squash --disable-auto --disable-auto=FALSE",
		"gh pr merge 498 -R picatz/flowstate -sb '--disable-auto'",
		"gh pr merge 498 -R picatz/flowstate -- --disable-auto",
		"gh pr merge 498 -R picatz/flowstate --disable-auto && gh pr merge 499 -R picatz/flowstate --squash",
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if disableAutoOnly(in) {
			t.Errorf("disableAutoOnly(%q) = true", command)
		}
	}
}

func TestMergeShellExpansionIsRejected(t *testing.T) {
	t.Parallel()
	sha := strings.Repeat("a", 40)
	for _, command := range []string{
		"flag=--auto; gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + ` "$flag"`,
		"gh pr merge 498 -R picatz/flowstate --match-head-commit $HEAD",
		"gh pr merge 498 -R picatz/flowstate --body \"$(payload)\" --match-head-commit " + sha,
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + " > >(cat) --auto",
		"gh > >(cat) pr merge 498 -R picatz/flowstate --auto",
		"gh > >(cat; :) pr merge 498 -R picatz/flowstate --auto",
		"gh > >(cat\n:) pr merge 498 -R picatz/flowstate --auto",
		`gh pr merge 498 -R picatz/flowstate --body "owner's notes" "$flag"`,
		`flags='--help=false --auto'; gh pr merge 498 -R picatz/flowstate --help $flags`,
		`cmd=gh; "$cmd" pr merge 498 -R picatz/flowstate --auto`,
		`cmd=gh; "$cmd" -R picatz/flowstate pr merge 498 --auto`,
		`cmd=gh; "$cmd" -Rpicatz/flowstate pr merge 498 --auto`,
		`empty=; gh p${empty}r merge 498 -R picatz/flowstate --auto`,
		`empty=; gh pr mer${empty}ge 498 -R picatz/flowstate --auto`,
		`empty=; g${empty}h pr merge 498 -R picatz/flowstate --auto`,
		`gh pr merge 498 -R picatz/flowstate --disable-auto{,=false}`,
		`gh pr merge 498 -R picatz/flowstate --disable-auto{=true,=false}`,
		`eval 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`bash -c 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`bash -lc 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`bash -cu 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`bash -cl 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`bash --noprofile --norc -c 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`bash -e -c 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`sh -c 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`/bin/sh -c 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`dash -c 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`/bin/dash -c 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`${MERGE_CLI} pr merge 498 -R picatz/flowstate --auto`,
		`$(which gh) pr merge 498 -R picatz/flowstate --auto`,
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if !mergeUsesShellExpansion(in) {
			t.Errorf("mergeUsesShellExpansion(%q) = false", command)
		}
	}
	in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{
		"command": "gh pr merge 498 -R picatz/flowstate --body '$literal' --match-head-commit " + sha,
	}}
	if mergeUsesShellExpansion(in) {
		t.Error("single-quoted literal was treated as shell expansion")
	}
	for _, command := range []string{`echo "$cmd pr merge"`, `git commit -m "$msg about pr merge"`} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if mergeUsesShellExpansion(in) {
			t.Errorf("quoted prose %q was treated as an expanded merge executable", command)
		}
	}
}

// TestShellExpansionDenialSaysWhatToDo covers #1972's other half. The refusal is
// intentional and unchanged — text cannot separate an obfuscated merge from prose
// about merging once expansion is in play — but the message has to tell the
// caller which of the two cases they are in, because the remedies are opposite.
func TestShellExpansionDenialSaysWhatToDo(t *testing.T) {
	t.Parallel()
	sha := strings.Repeat("a", 40)

	// A recognized merge keeps the advice that fits it: make the invocation
	// explicit and pin the head.
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --match-head-commit $HEAD",
		`cmd=gh; "$cmd" pr merge 498 -R picatz/flowstate --auto`,
	} {
		reason := shellExpansionDenial(command)
		if !strings.Contains(reason, "shell expansion in a merge invocation") {
			t.Errorf("a recognized merge lost its own advice: %q -> %q", command, reason)
		}
		if strings.Contains(reason, "not a recognized merge invocation") {
			t.Errorf("a recognized merge was described as not one: %q", command)
		}
		// Recognized is recognized by text, so this branch is also where prose
		// quoting a merge invocation lands. It has to offer that caller a way out
		// rather than only the advice for merging.
		if !strings.Contains(reason, "only quotes or documents a merge invocation") ||
			!strings.Contains(reason, "git commit -F FILE") {
			t.Errorf("the recognized branch offers no remedy for prose: %q", reason)
		}
	}

	// The #1972 reproduction: a status query. The message must not assert this is
	// a merge invocation, must name what actually matched, and must give the
	// remedy for a command that is not merging anything.
	repro := `cd /home/user/flowstate && echo "=== PR 1960 state ===" && ` +
		`gh api repos/picatz/flowstate/pulls/1960 --jq '"merged=\(.merged)"'; ` +
		`echo "=== any comment/review after the merge? ==="; ` +
		`gh api repos/picatz/flowstate/commits/$(git rev-parse origin/main)/check-runs --jq .total_count`
	reason := shellExpansionDenial(repro)
	// The ingredient clause is asserted whole, in order, and anchored to the text
	// that follows it. Matching the ingredient names one at a time does not work:
	// "the standalone word `merge`" also appears in the remedy sentence below, so
	// a per-name check passes even when the ingredient list is empty.
	const clause = "it carries a `gh` token, the standalone word `merge` and an expansion character (`$` or a backtick). This hook matches text"
	if !strings.Contains(reason, clause) {
		t.Errorf("denial for the status query does not name what matched:\n got %q\nwant substring %q", reason, clause)
	}
	for _, want := range []string{
		"not a recognized merge invocation",
		"quoting is not the remedy",
		"no single remedy is promised here",
		"#1972",
	} {
		if !strings.Contains(reason, want) {
			t.Errorf("denial for the status query does not mention %q: %q", want, reason)
		}
	}

	// The names describe syntax the predicates matched, never a behavior they
	// proved. A quoted jq selector is the case that makes the difference: its
	// braces and `$` are literal to the shell, so a message calling them an
	// expansion would be false and would imply quoting as a fix.
	quoted := `gh api repos/o/r/pulls/1 --jq '{merge: .mergeable}' && echo $?`
	if in := (&hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": quoted}}); !mergeUsesShellExpansion(in) {
		t.Fatalf("test corpus drifted: %q is no longer denied", quoted)
	}
	quotedReason := shellExpansionDenial(quoted)
	for _, want := range []string{
		"brace characters (`{` with `}`)",
		"an expansion character (`$` or a backtick)",
		"a quoted occurrence counts the same as a live one",
	} {
		if !strings.Contains(quotedReason, want) {
			t.Errorf("denial for a quoted selector does not mention %q: %q", want, quotedReason)
		}
	}
	for _, unwanted := range []string{"a brace expansion", "a shell expansion", "process substitution syntax"} {
		if strings.Contains(quotedReason, unwanted) {
			t.Errorf("denial asserts %q for text the shell treats literally: %q", unwanted, quotedReason)
		}
	}

	// A commit message discussing the tooling is the other shape that reaches
	// here, and the file remedy is the one that works for it. The message has to
	// carry the standalone word *after* a gh token to reach the heuristic at all:
	// `\bgh\b.*\bmerge\b` is ordered, and `mergeguard` is not `\bmerge\b`, so a
	// subject naming only this tool does not trigger it.
	message := `git commit -m "a gh api read is not a merge, $reason"`
	messageIn := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": message}}
	if !mergeUsesShellExpansion(messageIn) {
		t.Fatalf("the commit-message case is unreachable, so its assertion proves nothing: %q", message)
	}
	if reason := shellExpansionDenial(message); !strings.Contains(reason, "git commit -F FILE") {
		t.Errorf("denial for a commit message does not name the file remedy: %q", reason)
	}

	// Process substitution is the one indicator with no other case behind it. It
	// is reachable through an evaluator: the single-quoted body is one word to the
	// tokenizer, so the command is not a recognized merge and the heuristic branch
	// renders the list.
	procSub := `eval 'gh pr merge 498 -R picatz/flowstate --auto > >(cat)'`
	procSubIn := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": procSub}}
	if !mergeUsesShellExpansion(procSubIn) {
		t.Fatalf("test corpus drifted: %q is no longer denied", procSub)
	}
	procSubReason := shellExpansionDenial(procSub)
	for _, want := range []string{
		"process-substitution syntax (`>(` or `<(`)",
		"an `eval` or shell `-c` spelling",
	} {
		if !strings.Contains(procSubReason, want) {
			t.Errorf("denial does not name %q for %q: %q", want, procSub, procSubReason)
		}
	}

	// The negative direction for every name, which is what the positive cases
	// above cannot establish: a name must appear only when its own syntax is
	// present. Without this, stubbing an indicator's condition to true survives
	// the suite, because the anchored clause only pins the repro's three names.
	for _, tc := range []struct {
		name    string
		command string
		absent  []string
	}{
		{
			// dynamicGHExecutable fires here while `\bgh\b` does not match the
			// obfuscated executable, so the denial must not claim a `gh` token.
			name:    "obfuscated executable carries no gh token",
			command: `empty=; g${empty}h pr merge 498 -R picatz/flowstate --auto`,
			absent:  []string{"a `gh` token"},
		},
		{
			name:    "status query has no evaluator or process substitution",
			command: repro,
			absent: []string{
				"an `eval` or shell `-c` spelling",
				"process-substitution syntax (`>(` or `<(`)",
				"brace characters (`{` with `}`)",
			},
		},
		{
			name:    "brace-only selector has no process substitution",
			command: quoted,
			absent:  []string{"process-substitution syntax (`>(` or `<(`)"},
		},
		{
			// ghDynamicPRMergeText matches a `p…` `m…` adjacency and needs no
			// standalone `merge`, so this reaches the heuristic branch without the
			// word and the denial must not claim it.
			name:    "p-m adjacency carries no standalone merge",
			command: `gh api pulls m$X`,
			absent:  []string{"the standalone word `merge`"},
		},
		{
			// Braces are the trigger here, so the command reaches the heuristic
			// branch with no `$` or backtick anywhere in it.
			name:    "brace trigger carries no expansion character",
			command: `gh api repos/o/r/pulls/1/merge --jq '{x: .y}'`,
			absent:  []string{"an expansion character (`$` or a backtick)"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": tc.command}}
			if !mergeUsesShellExpansion(in) {
				t.Fatalf("test corpus drifted: %q is no longer denied", tc.command)
			}
			got := shellExpansionDenial(tc.command)
			if strings.Contains(got, "not a recognized merge invocation") == isGHPRMergeInvocation(tc.command) {
				t.Fatalf("branch and message disagree for %q: %q", tc.command, got)
			}
			for _, absent := range tc.absent {
				if strings.Contains(got, absent) {
					t.Errorf("denial names %q for a command that does not carry it: %q", absent, got)
				}
			}
		})
	}

	// Every indicator the decision can fire on has a name in the message, so a
	// denial never reports an empty or generic shape. The fallback exists for
	// unreachable input and must not be what a real denial says.
	for _, command := range []string{
		repro,
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + " > >(cat) --auto",
		`eval 'gh pr merge 498 -R picatz/flowstate --auto'`,
		`gh pr merge 498 -R picatz/flowstate --disable-auto{,=false}`,
		`empty=; g${empty}h pr merge 498 -R picatz/flowstate --auto`,
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if !mergeUsesShellExpansion(in) {
			t.Fatalf("test corpus drifted: %q is no longer denied", command)
		}
		if reason := shellExpansionDenial(command); strings.Contains(reason, "a shape this hook refuses") {
			t.Errorf("denial fell back to the generic shape for %q: %q", command, reason)
		}
	}

	for _, tc := range []struct {
		items []string
		want  string
	}{
		{items: nil, want: "a shape this hook refuses"},
		{items: []string{"one"}, want: "one"},
		{items: []string{"one", "two"}, want: "one and two"},
		{items: []string{"one", "two", "three"}, want: "one, two and three"},
	} {
		if got := joinAnd(tc.items); got != tc.want {
			t.Errorf("joinAnd(%q) = %q, want %q", tc.items, got, tc.want)
		}
	}
}

func TestManualMergeRequiresOneExactHeadPrecondition(t *testing.T) {
	t.Parallel()
	sha := strings.Repeat("a", 40)
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --squash --match-head-commit " + sha,
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + " # reviewed head",
		"gh>/tmp/merge-output pr merge 498 -R picatz/flowstate --squash --match-head-commit " + sha,
		"gh -R picatz/flowstate pr merge 498 --match-head-commit=" + sha,
		"/usr/bin/gh pr merge 498 -R picatz/flowstate --match-head-commit=" + sha,
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if !mergeHeadPinned(in) {
			t.Errorf("mergeHeadPinned(%q) = false", command)
		}
	}
	for _, command := range []string{
		"gh pr merge 498 -R picatz/flowstate --squash",
		"gh pr merge 498 -R picatz/flowstate --match-head-commit abc1234",
		"gh pr merge 498 -R picatz/flowstate --body '--match-head-commit=" + sha + "'",
		"gh pr merge 498 -R picatz/flowstate -b '--match-head-commit=" + sha + "'",
		"gh pr merge 498 -R picatz/flowstate -sb '--match-head-commit=" + sha + "'",
		"gh pr merge 498 -R picatz/flowstate --squash # --match-head-commit=" + sha,
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + " --match-head-commit=",
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + " 2>&1 --match-head-commit=",
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + " &>/dev/null --match-head-commit=",
		"gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + " && gh pr merge 499 -R picatz/flowstate --match-head-commit " + sha,
	} {
		in := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": command}}
		if mergeHeadPinned(in) {
			t.Errorf("mergeHeadPinned(%q) = true", command)
		}
	}
	multiple := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": "gh pr merge 498 -R picatz/flowstate --match-head-commit " + sha + " && gh pr merge 499 -R picatz/flowstate --match-head-commit " + sha}}
	if !isMergeInvocation(multiple) {
		t.Error("multiple merge invocations were treated as a non-merge command")
	}
	mcp := &hook.Input{ToolName: "mcp__github__merge_pull_request", ToolInput: map[string]any{"expectedHeadSha": sha}}
	if !mergeHeadPinned(mcp) {
		t.Error("structured exact-head precondition was rejected")
	}
	mcp.ToolInput = map[string]any{"expectedHeadOid": sha}
	if mergeHeadPinned(mcp) {
		t.Error("unsupported expectedHeadOid was accepted as the MCP head precondition")
	}
}

// TestWarnReachesStderrAndPermissionReason confirms a neutral warning reaches
// a human on stderr and as systemMessage, and confirms the P1 fix directly:
// it must never carry a permissionDecision at all. permissionDecision:
// "allow" bypasses the normal permission prompt outright under the hook
// contract, which would make a blind mergeguard grant more than its own
// absence would have; "ask" or no field at all preserves the prompt. Only
// the second is a neutral fail-open.
func TestWarnReachesStderrAndPermissionReason(t *testing.T) {
	origStdout, origStderr := os.Stdout, os.Stderr
	outR, outW, _ := os.Pipe()
	errR, errW, _ := os.Pipe()
	os.Stdout, os.Stderr = outW, errW
	defer func() { os.Stdout, os.Stderr = origStdout, origStderr }()

	const reason = "mergeguard: test warning reaches a human"
	warn(reason)

	outW.Close()
	errW.Close()
	stdout, _ := io.ReadAll(outR)
	stderr, _ := io.ReadAll(errR)

	if !strings.Contains(string(stderr), reason) {
		t.Errorf("warning did not reach stderr: %q", stderr)
	}

	var raw map[string]any
	if err := json.Unmarshal(stdout, &raw); err != nil {
		t.Fatalf("stdout is not the expected JSON: %v (%q)", err, stdout)
	}
	if _, present := raw["hookSpecificOutput"]; present {
		t.Errorf("warn emitted hookSpecificOutput at all, which can carry a permissionDecision: %q", stdout)
	}
	if _, present := raw["permissionDecision"]; present {
		t.Errorf("warn emitted a top-level permissionDecision: %q", stdout)
	}
	msg, _ := raw["systemMessage"].(string)
	if msg != reason {
		t.Errorf("systemMessage = %q, want %q", msg, reason)
	}
}

// TestDenyMessageNamesEveryThread pins the format the operator reads: a
// count and one line per unresolved thread.
func TestDenyMessageNamesEveryThread(t *testing.T) {
	t.Parallel()

	msg := denyMessage("picatz", "flowstate", 488, []thread{
		{URL: "https://github.com/picatz/flowstate/pull/488#discussion_r1", Body: "first finding"},
		{URL: "https://github.com/picatz/flowstate/pull/488#discussion_r2", Body: "second finding"},
	})
	if !strings.Contains(msg, "2 unresolved review thread") {
		t.Errorf("deny message missing count: %s", msg)
	}
	for _, want := range []string{"discussion_r1", "first finding", "discussion_r2", "second finding"} {
		if !strings.Contains(msg, want) {
			t.Errorf("deny message missing %q: %s", want, msg)
		}
	}
}

// TestGithubTokenFallsBackToGHAuthToken is the P1 fix on #503: an operator
// authenticated only via `gh auth login`, with no GH_TOKEN or GITHUB_TOKEN
// exported, must still get the review-thread check. The seam is ghBinary,
// pointed at a real, freshly written shell script that a real
// exec.CommandContext runs as a real subprocess and whose real stdout is
// parsed — not a stubbed function standing in for gh.
func TestGithubTokenFallsBackToGHAuthToken(t *testing.T) {
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	restore := setGHBinary(t, fakeGH(t, 0, "stored-credential-token\n"))
	defer restore()

	tok, ok := githubToken(context.Background())
	if !ok || tok != "stored-credential-token" {
		t.Errorf("githubToken() = %q, %v; want the token gh auth token printed", tok, ok)
	}
}

// TestGithubTokenPrefersEnvironment pins gh's own precedence, named in the
// finding: GH_TOKEN/GITHUB_TOKEN win over a stored credential, so a fake gh
// that would return a different token must never be consulted, let alone
// win.
func TestGithubTokenPrefersEnvironment(t *testing.T) {
	restore := setGHBinary(t, fakeGH(t, 0, "should-not-be-used\n"))
	defer restore()
	t.Setenv("GH_TOKEN", "env-token")

	tok, ok := githubToken(context.Background())
	if !ok || tok != "env-token" {
		t.Errorf("githubToken() = %q, %v; want the environment token, not gh auth token's", tok, ok)
	}
}

// TestGithubTokenFailsOpenWhenGHHasNoCredential covers gh installed but not
// logged in (a real nonzero exit from a real subprocess): no token, ok=false,
// which the caller treats as fail-open-and-warn exactly like a missing
// environment variable.
func TestGithubTokenFailsOpenWhenGHHasNoCredential(t *testing.T) {
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	restore := setGHBinary(t, fakeGH(t, 1, "gh: not logged in\n"))
	defer restore()

	if tok, ok := githubToken(context.Background()); ok {
		t.Errorf("githubToken() = %q, true; want ok=false when gh auth token fails", tok)
	}
}

// TestGithubTokenFailsOpenWhenGHMissing covers no gh binary on PATH at all.
func TestGithubTokenFailsOpenWhenGHMissing(t *testing.T) {
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	restore := setGHBinary(t, filepath.Join(t.TempDir(), "no-such-gh-binary"))
	defer restore()

	if tok, ok := githubToken(context.Background()); ok {
		t.Errorf("githubToken() = %q, true; want ok=false when gh is not installed", tok)
	}
}

// fakeGH writes a real, executable script at a fresh path that prints body
// to stdout and exits with code, standing in for `gh auth token`.
func fakeGH(t *testing.T, code int, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake gh script is a POSIX shell script")
	}
	path := filepath.Join(t.TempDir(), "gh")
	script := "#!/bin/sh\nprintf %s " + shellQuote(body) + "\nexit " + strconv.Itoa(code) + "\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	return path
}

func setGHBinary(t *testing.T, path string) func() {
	t.Helper()
	orig := ghBinary
	ghBinary = path
	return func() { ghBinary = orig }
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// TestConventionNoteReadsOnlyAMessageTheCallCarries pins the two paths: the
// MCP merge tool's commit_title is checked, and a Bash `gh pr merge`, which
// carries its message in flags this hook does not parse, is not read as an
// empty message that fails every rule.
func TestConventionNoteReadsOnlyAMessageTheCallCarries(t *testing.T) {
	mcp := &hook.Input{ToolName: "mcp__github__merge_pull_request", ToolInput: map[string]any{
		"owner": "o", "repo": "r", "pullNumber": float64(7),
		"commit_title": "Fix bug", "commit_message": "",
	}}
	note := conventionNote(mcp, "o", "r", 7)
	if !strings.Contains(note, "subject:") || !strings.Contains(note, "issue:") || !strings.Contains(note, "verification:") {
		t.Errorf("a bare title and an empty body named fewer than three rules: %q", note)
	}

	mcp.ToolInput["commit_title"] = "tools: hold the merge message to the conventions"
	mcp.ToolInput["commit_message"] = "Why.\n\nVerification: go test ./tools/hooks/... ok\n\nRefs #1728\n"
	if note := conventionNote(mcp, "o", "r", 7); note != "" {
		t.Errorf("a conforming message drew a note: %q", note)
	}

	bash := &hook.Input{ToolName: "Bash", ToolInput: map[string]any{"command": "gh pr merge 7 --squash"}}
	if note := conventionNote(bash, "o", "r", 7); note != "" {
		t.Errorf("a Bash merge, whose message is in flags this hook does not parse, was read as an empty one: %q", note)
	}
}

func TestJoinNotesSkipsTheEmptyOnes(t *testing.T) {
	if got := joinNotes("", "a", "", "b"); got != "a\n\nb" {
		t.Errorf("joinNotes = %q", got)
	}
	if got := joinNotes("", ""); got != "" {
		t.Errorf("joinNotes of nothing = %q", got)
	}
}
