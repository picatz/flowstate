package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
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
// human: stderr, and the tool call's permission-decision reason.
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
	var decision struct {
		HookSpecificOutput struct {
			PermissionDecision       string `json:"permissionDecision"`
			PermissionDecisionReason string `json:"permissionDecisionReason"`
		} `json:"hookSpecificOutput"`
	}
	if err := json.Unmarshal(stdout, &decision); err != nil {
		t.Fatalf("stdout is not the expected JSON: %v (%q)", err, stdout)
	}
	if decision.HookSpecificOutput.PermissionDecision != "allow" {
		t.Errorf("permissionDecision = %q, want allow", decision.HookSpecificOutput.PermissionDecision)
	}
	if decision.HookSpecificOutput.PermissionDecisionReason != reason {
		t.Errorf("permissionDecisionReason = %q, want %q", decision.HookSpecificOutput.PermissionDecisionReason, reason)
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
