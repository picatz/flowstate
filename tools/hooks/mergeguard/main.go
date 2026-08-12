// Command mergeguard is a Claude Code PreToolUse hook on the merge path
// (#498): before a merge proceeds, it queries the target pull request's
// review threads and denies the merge when any is unresolved, naming the
// threads so the operator knows what to clear. The rules it enforces live
// in .claude/skills/comms-review (#494); #488 was merged with its review
// unread and four threads unresolved, three of them pointing at factual
// errors in that very guidance. A rule that depends on remembering is not a
// mechanism — this hook is the mechanism.
//
// Wired in .claude/settings.json as:
//
//	go -C "${CLAUDE_PROJECT_DIR}" run ./tools/hooks/mergeguard
//
// # Identifying the PR
//
// Identification is exact, never guessed, per two paths:
//
//   - The mcp__github__merge_pull_request tool call carries owner, repo and
//     pullNumber as structured tool_input fields — the primary path, and the
//     one every merge through the GitHub MCP server takes.
//   - A Bash `gh pr merge` invocation is recognized only when it names the
//     PR explicitly: a full pull-request URL (self-identifying), or a bare
//     number together with an explicit `-R`/`--repo owner/repo` flag. A bare
//     `gh pr merge` with neither resolves the current branch's PR through
//     gh's own lookup, which this hook cannot reproduce without an API call
//     of its own — recognizing that would be guessing, so it allows instead.
//
// Any other tool call is not a merge and returns immediately.
//
// # Fail open, loudly
//
// Review threads live behind GitHub's GraphQL API, not REST, and GraphQL and
// REST exhaust independently — CLAUDE.md, and the outage that motivated this
// hook (two API failures in one session on 2026-08-12). A session that has
// burned its GraphQL budget can still merge through REST while this hook is
// blind to threads. On any failure to query GraphQL — network, auth, rate
// limit, or a malformed response — the hook allows the merge and prints a
// visible warning, on stderr and as the permission-decision reason, that the
// check did not run and why. Blocking every merge because GitHub is slow
// would be worse than the problem this hook solves, and failing silently
// would be worse still.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/picatz/flowstate/tools/hooks/internal/hook"
)

// requestTimeout bounds the GraphQL round trip. CLAUDE.md's discipline is to
// bound every reader of input an outside party controls; GitHub is a
// trusted peer but a slow or hanging one still must not hang the merge
// indefinitely, so a client-side timeout applies here.
const requestTimeout = 10 * time.Second

// maxResponseBytes bounds the GraphQL response read into memory, the same
// discipline CLAUDE.md applies to any HTTP response before it is parsed.
const maxResponseBytes = 4 << 20

const graphQLEndpoint = "https://api.github.com/graphql"

func main() {
	in, err := hook.Read(os.Stdin)
	if err != nil {
		return // lenient: unrecognized input allows
	}

	owner, repo, number, ok := mergeTarget(in)
	if !ok {
		return // not a merge call this hook can identify
	}

	tok, ok := githubToken()
	if !ok {
		warn(fmt.Sprintf(
			"mergeguard: no GH_TOKEN or GITHUB_TOKEN in the environment, so the review-thread check on %s/%s#%d did not run. MERGING WITHOUT THE CHECK.",
			owner, repo, number))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	client := &http.Client{Timeout: requestTimeout}
	threads, err := unresolvedThreads(ctx, client, graphQLEndpoint, tok, owner, repo, number)
	if err != nil {
		warn(fmt.Sprintf(
			"mergeguard: could not query review threads on %s/%s#%d (%v). GraphQL and REST exhaust independently, so this can happen even when the merge call itself would succeed. MERGING WITHOUT THE CHECK.",
			owner, repo, number, err))
		return
	}

	if len(threads) == 0 {
		return
	}
	hook.Deny(denyMessage(owner, repo, number, threads))
}

// warn surfaces reason both on stderr and as the tool call's (allowing)
// permission-decision reason, so a fail-open check is never silent.
func warn(reason string) {
	hook.Warn(reason)
}

// githubToken reads the token gh itself would use, GH_TOKEN taking
// precedence over GITHUB_TOKEN to match gh's own precedence.
func githubToken() (string, bool) {
	for _, key := range []string{"GH_TOKEN", "GITHUB_TOKEN"} {
		if v := os.Getenv(key); v != "" {
			return v, true
		}
	}
	return "", false
}

// mergeTarget identifies the owner, repo and PR number a tool call would
// merge, or reports ok=false when the call is not a merge this hook can
// identify without guessing.
func mergeTarget(in *hook.Input) (owner, repo string, number int, ok bool) {
	switch in.ToolName {
	case "mcp__github__merge_pull_request":
		return mcpMergeTarget(in)
	case "Bash":
		return ghCLIMergeTarget(in.Command())
	default:
		return "", "", 0, false
	}
}

// mcpMergeTarget reads owner, repo and pullNumber directly from the MCP
// tool's structured arguments — the exact path, no inference.
func mcpMergeTarget(in *hook.Input) (owner, repo string, number int, ok bool) {
	if in == nil || in.ToolInput == nil {
		return "", "", 0, false
	}
	owner, _ = in.ToolInput["owner"].(string)
	repo, _ = in.ToolInput["repo"].(string)
	n, isNum := in.ToolInput["pullNumber"].(float64)
	if owner == "" || repo == "" || !isNum || n <= 0 {
		return "", "", 0, false
	}
	return owner, repo, int(n), true
}

// ghPRMerge matches a `gh pr merge` invocation and captures everything
// after it, up to the next control operator or the end of the string.
var ghPRMerge = regexp.MustCompile(`\bgh\s+pr\s+merge\b(.*)`)

// prURL matches a full pull-request URL, which is self-identifying.
var prURL = regexp.MustCompile(`^https://github\.com/([^/\s]+)/([^/\s]+)/pull/(\d+)$`)

// repoFlagValue matches an explicit -R/--repo owner/repo flag value.
var repoFlagValue = regexp.MustCompile(`^([^/\s]+)/([^/\s]+)$`)

// ghCLIMergeTarget recognizes `gh pr merge` only when the command names the
// PR explicitly: a full PR URL, or a bare number alongside an explicit
// -R/--repo owner/repo. A bare `gh pr merge` (or one with only a number and
// no repo flag) resolves against the current branch through gh's own
// lookup, which this hook does not reproduce; it returns ok=false rather
// than guess.
func ghCLIMergeTarget(cmd string) (owner, repo string, number int, ok bool) {
	m := ghPRMerge.FindStringSubmatch(cmd)
	if m == nil {
		return "", "", 0, false
	}
	rest := m[1]
	if i := strings.IndexAny(rest, ";&|\n`"); i >= 0 {
		rest = rest[:i]
	}

	var flagOwner, flagRepo, target string
	fields := strings.Fields(unquote(rest))
	skipNext := false
	for _, f := range fields {
		if skipNext {
			if sub := repoFlagValue.FindStringSubmatch(f); sub != nil {
				flagOwner, flagRepo = sub[1], sub[2]
			}
			skipNext = false
			continue
		}
		switch {
		case f == "-R" || f == "--repo":
			skipNext = true
		case strings.HasPrefix(f, "--repo="):
			if sub := repoFlagValue.FindStringSubmatch(strings.TrimPrefix(f, "--repo=")); sub != nil {
				flagOwner, flagRepo = sub[1], sub[2]
			}
		case strings.HasPrefix(f, "-"):
			// Other flags (--auto, --squash, --body, ...) are not targets.
		case target == "":
			target = f
		}
	}

	if sub := prURL.FindStringSubmatch(target); sub != nil {
		n, err := strconv.Atoi(sub[3])
		if err != nil {
			return "", "", 0, false
		}
		return sub[1], sub[2], n, true
	}
	if n, err := strconv.Atoi(target); err == nil && n > 0 && flagOwner != "" && flagRepo != "" {
		return flagOwner, flagRepo, n, true
	}
	return "", "", 0, false
}

// unquote strips single and double quote characters. It is not a shell
// parser; it exists only so `gh pr merge "42"` and `-R 'owner/repo'` still
// match the plain forms above. Anything it mishandles simply fails to
// identify the PR (ok=false), which is the safe direction for this guard.
func unquote(s string) string {
	return strings.NewReplacer(`"`, "", `'`, "").Replace(s)
}

// thread is the part of an unresolved review thread this guard names in its
// denial: the URL and body of its first comment, when GitHub returns one.
type thread struct {
	URL  string
	Body string
}

const reviewThreadsQuery = `query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      reviewThreads(first: 100) {
        nodes {
          isResolved
          comments(first: 1) {
            nodes {
              url
              body
            }
          }
        }
      }
    }
  }
}`

type graphQLResponse struct {
	Data struct {
		Repository struct {
			PullRequest struct {
				ReviewThreads struct {
					Nodes []struct {
						IsResolved bool `json:"isResolved"`
						Comments   struct {
							Nodes []struct {
								URL  string `json:"url"`
								Body string `json:"body"`
							} `json:"nodes"`
						} `json:"comments"`
					} `json:"nodes"`
				} `json:"reviewThreads"`
			} `json:"pullRequest"`
		} `json:"repository"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// unresolvedThreads queries endpoint for owner/repo#number's review threads
// and returns the ones that are not resolved. It is bounded to the first
// 100 threads and the first comment of each: this guard exists to name what
// to clear, not to be a complete review-thread browser, and a PR carrying
// more than 100 open threads has bigger problems than this hook.
func unresolvedThreads(ctx context.Context, client *http.Client, endpoint, token, owner, repo string, number int) ([]thread, error) {
	body, err := json.Marshal(map[string]any{
		"query": reviewThreadsQuery,
		"variables": map[string]any{
			"owner":  owner,
			"repo":   repo,
			"number": number,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(respBody), 300))
	}

	var gr graphQLResponse
	if err := json.Unmarshal(respBody, &gr); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if len(gr.Errors) > 0 {
		return nil, fmt.Errorf("graphql error: %s", gr.Errors[0].Message)
	}

	var threads []thread
	for _, n := range gr.Data.Repository.PullRequest.ReviewThreads.Nodes {
		if n.IsResolved {
			continue
		}
		t := thread{}
		if len(n.Comments.Nodes) > 0 {
			t.URL = n.Comments.Nodes[0].URL
			t.Body = n.Comments.Nodes[0].Body
		}
		threads = append(threads, t)
	}
	return threads, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// denyMessage names every unresolved thread, so the operator knows exactly
// what to clear rather than just that something is unresolved.
func denyMessage(owner, repo string, number int, threads []thread) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s/%s#%d has %d unresolved review thread(s); resolve them (or explicitly decide and record why not) before merging:\n", owner, repo, number, len(threads))
	for _, t := range threads {
		line := strings.ReplaceAll(strings.TrimSpace(t.Body), "\n", " ")
		line = truncate(line, 140)
		if line == "" {
			line = "(no comment body)"
		}
		if t.URL != "" {
			fmt.Fprintf(&b, "- %s: %s\n", t.URL, line)
		} else {
			fmt.Fprintf(&b, "- %s\n", line)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}
