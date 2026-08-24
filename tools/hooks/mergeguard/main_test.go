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
	defer srv.Close()

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
	defer srv.Close()

	threads, err := unresolvedThreads(context.Background(), srv.Client(), srv.URL, "tok", "picatz", "flowstate", 500)
	if err != nil {
		t.Fatalf("unresolvedThreads: %v", err)
	}
	if len(threads) != 0 {
		t.Fatalf("got %d unresolved threads, want 0: %+v", len(threads), threads)
	}
}

// TestUnresolvedThreadsFailsOpenOnAPIError is case 3: a genuine network
// failure (a closed listener, not a stub) surfaces as an error rather than
// a panic or a false deny, so main()'s caller allows and warns.
func TestUnresolvedThreadsFailsOpenOnAPIError(t *testing.T) {
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
	defer srv.Close()

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
	defer srv.Close()

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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		body := reviewThreadsPageBody(t, resolvedNodes(reviewThreadsPageSize), true, "always-more")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	defer srv.Close()

	threads, err := unresolvedThreads(context.Background(), srv.Client(), srv.URL, "tok", "picatz", "flowstate", 510)
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
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
}

func reviewThreadsServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return reviewThreadsServerStatus(t, http.StatusOK, body)
}

func reviewThreadsServerStatus(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		reason := unidentifiedMergeWarning(tt.in)
		if reason == "" {
			t.Errorf("%s: unidentifiedMergeWarning returned no warning for a merge attempt", tt.name)
			continue
		}
		if !strings.Contains(reason, "MERGING WITHOUT THE CHECK") {
			t.Errorf("%s: warning does not say the merge proceeded unchecked: %s", tt.name, reason)
		}
	}
	if reason := unidentifiedMergeWarning(warns[2].in); !strings.Contains(reason, "-R") {
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
		if reason := unidentifiedMergeWarning(tt.in); reason != "" {
			t.Errorf("%s: warned on a call that is not an identifiable merge attempt: %s", tt.name, reason)
		}
	}
}

// TestWarnReachesStderrAndPermissionReason confirms the fail-open warning
// (shared by the API-failure and unidentified-PR paths) actually reaches a
// human on stderr and as systemMessage, and confirms the P1 fix directly:
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
