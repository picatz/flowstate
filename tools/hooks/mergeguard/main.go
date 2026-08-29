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
	"os/exec"
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
		if reason := unidentifiedMergeWarning(in); reason != "" {
			warn(reason)
		}
		return // not a merge call, or a merge call this hook could not identify
	}

	tokCtx, tokCancel := context.WithTimeout(context.Background(), tokenLookupTimeout)
	tok, ok := githubToken(tokCtx)
	tokCancel()
	if !ok {
		warn(fmt.Sprintf(
			"mergeguard: no GH_TOKEN or GITHUB_TOKEN in the environment, and `gh auth token` returned none either, so the review-thread check on %s/%s#%d did not run. MERGING WITHOUT THE CHECK.",
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

// tokenLookupTimeout bounds the `gh auth token` subprocess. It is a
// separate, shorter budget from requestTimeout: a hung or missing gh binary
// must not eat into the time available for the actual GraphQL call.
const tokenLookupTimeout = 5 * time.Second

// ghBinary is the executable githubTokenFromGH runs, overridable in tests so
// the fallback exercises a real subprocess without depending on a real gh
// installation or real stored credentials.
var ghBinary = "gh"

// githubToken reads the token gh itself would use, in gh's own documented
// precedence: GH_TOKEN or GITHUB_TOKEN from the environment first, and only
// when neither is set, the credential `gh auth login` stored, via
// `gh auth token`. Stopping at "no environment variable" treats an operator
// authenticated only through `gh auth login` as unauthenticated and skips
// the check while the `gh pr merge` that follows succeeds anyway.
func githubToken(ctx context.Context) (string, bool) {
	for _, key := range []string{"GH_TOKEN", "GITHUB_TOKEN"} {
		if v := os.Getenv(key); v != "" {
			return v, true
		}
	}
	return githubTokenFromGH(ctx)
}

// githubTokenFromGH shells out to `gh auth token` for the credential gh has
// stored, bounded by ctx. Any failure (gh not installed, not logged in,
// timeout) returns ok=false; the caller's fail-open path handles it the
// same as a missing environment variable.
func githubTokenFromGH(ctx context.Context) (string, bool) {
	out, err := exec.CommandContext(ctx, ghBinary, "auth", "token").Output()
	if err != nil {
		return "", false
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return "", false
	}
	return tok, true
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

// unidentifiedMergeWarning reports the fail-open warning for a tool call
// that is a merge attempt this hook recognizes but could not identify a PR
// for, or "" when the call is not a merge attempt at all (in which case
// main stays silent, correctly: there is nothing to warn about). Silence on
// an unidentified merge attempt is the same failure mode as silence on an
// API error — a check that reports nothing looks identical to a check that
// passed — so both paths warn the same way: on stderr and as the visible
// permission-decision reason.
func unidentifiedMergeWarning(in *hook.Input) string {
	switch in.ToolName {
	case "mcp__github__merge_pull_request":
		return "mergeguard: this merge_pull_request call did not carry a usable owner, repo and pullNumber, so unresolved review threads were not checked. MERGING WITHOUT THE CHECK."
	case "Bash":
		if isGHPRMergeInvocation(in.Command()) {
			return "mergeguard: could not identify the pull request from this `gh pr merge` invocation, so unresolved review threads were not checked. MERGING WITHOUT THE CHECK. Name the PR explicitly (a full PR URL, or a number together with -R/--repo owner/repo) to make it checkable."
		}
		return ""
	default:
		return ""
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

// prURL matches a full pull-request URL, which is self-identifying.
var prURL = regexp.MustCompile(`^https://github\.com/([^/\s]+)/([^/\s]+)/pull/(\d+)$`)

// repoFlagValue matches an explicit -R/--repo owner/repo flag value.
var repoFlagValue = regexp.MustCompile(`^([^/\s]+)/([^/\s]+)$`)

// ghMergeValueFlags are `gh pr merge` flags (per `gh pr merge --help`) that
// take a separate argument that is not the PR target: --author-email,
// --body, --body-file, --match-head-commit and -t/--subject. -R/--repo also
// takes a value but is handled separately, since its value identifies the
// repo rather than being skipped. Without consuming these, a value that
// happens to precede the target (`gh pr merge --match-head-commit "$sha"
// 498 -R owner/repo`) is mistaken for the target itself.
var ghMergeValueFlags = map[string]bool{
	"--author-email":      true,
	"--body":              true,
	"--body-file":         true,
	"--match-head-commit": true,
	"--subject":           true,
	"-t":                  true,
}

// isGHPRMergeInvocation reports whether cmd actually invokes `gh pr merge`,
// as opposed to merely mentioning the words — a commit message or a grep
// pattern quoting them must never trigger this guard, the same false-alarm
// concern pidguard's package doc names. ghPRMergeArgs preserves shell-word
// boundaries, so a trigger can only be formed by three unquoted words.
func isGHPRMergeInvocation(cmd string) bool {
	_, ok := ghPRMergeArgs(cmd)
	return ok
}

// ghCLIMergeTarget recognizes `gh pr merge` only when the command names the
// PR explicitly: a full PR URL, or a bare number alongside an explicit
// -R/--repo owner/repo. A bare `gh pr merge` (or one with only a number and
// no repo flag) resolves against the current branch through gh's own
// lookup, which this hook does not reproduce; it returns ok=false rather
// than guess.
func ghCLIMergeTarget(cmd string) (owner, repo string, number int, ok bool) {
	if !isGHPRMergeInvocation(cmd) {
		return "", "", 0, false
	}
	fields, ok := ghPRMergeArgs(cmd)
	if !ok {
		return "", "", 0, false
	}

	var flagOwner, flagRepo, target string
	skipNext := false
	skipIsRepo := false
	for _, f := range fields {
		if skipNext {
			if skipIsRepo {
				if sub := repoFlagValue.FindStringSubmatch(f); sub != nil {
					flagOwner, flagRepo = sub[1], sub[2]
				}
			}
			skipNext = false
			skipIsRepo = false
			continue
		}
		switch {
		case f == "-R" || f == "--repo":
			skipNext = true
			skipIsRepo = true
		case ghMergeValueFlags[f]:
			// A value-taking flag whose argument is not the PR target:
			// --author-email, --body, --body-file, --match-head-commit,
			// -t/--subject. Consume its value so it is never mistaken for
			// the positional target.
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

// ghPRMergeArgs finds the first unquoted `gh pr merge` command and returns
// its arguments as shell words. Quoted whitespace stays within one word,
// and quoted mentions of the command stay ordinary words, so neither can
// redirect the guard to a different pull request. Control operators bound
// each simple command; this intentionally remains a small recognizer, not a
// shell evaluator.
func ghPRMergeArgs(s string) ([]string, bool) {
	var commands [][]string
	var words []string
	var word strings.Builder
	var inSingle, inDouble, escaped, started bool
	flushWord := func() {
		if started {
			words = append(words, word.String())
			word.Reset()
			started = false
		}
	}
	flushCommand := func() {
		flushWord()
		if len(words) > 0 {
			commands = append(commands, words)
			words = nil
		}
	}
	for _, r := range s {
		switch {
		case escaped:
			word.WriteRune(r)
			started, escaped = true, false
		case inSingle:
			if r == '\'' {
				inSingle = false
			} else {
				word.WriteRune(r)
			}
		case inDouble:
			switch r {
			case '\\':
				escaped = true
			case '"':
				inDouble = false
			default:
				word.WriteRune(r)
			}
		case r == '\\':
			escaped, started = true, true
		case r == '\'':
			inSingle, started = true, true
		case r == '"':
			inDouble, started = true, true
		case r == ' ' || r == '\t' || r == '\r':
			flushWord()
		case strings.ContainsRune(";&|\n`", r):
			flushCommand()
		default:
			word.WriteRune(r)
			started = true
		}
	}
	flushCommand()

	for _, command := range commands {
		for i := 0; i+2 < len(command); i++ {
			if command[i] == "gh" && command[i+1] == "pr" && command[i+2] == "merge" {
				return command[i+3:], true
			}
		}
	}
	return nil, false
}

// thread is the part of an unresolved review thread this guard names in its
// denial: the URL and body of its first comment, when GitHub returns one.
type thread struct {
	URL  string
	Body string
}

const reviewThreadsQuery = `query($owner: String!, $repo: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      reviewThreads(first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
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

// reviewThreadsPageSize is the page size requested per call; 100 is
// GraphQL's own maximum for a `first` argument on this connection.
const reviewThreadsPageSize = 100

// maxReviewThreadRequests and maxReviewThreadsScanned bound the walk over a
// PR's review threads the same way CLAUDE.md's List bounds a paged listing:
// by requests made *and* by items read, because the peer (GitHub) controls
// how many threads come back per page. Five pages of 100 is enough for any
// PR with a functioning review process; one that has more open threads than
// that has bigger problems than this hook, but the walk must say so rather
// than quietly read as clean.
const (
	maxReviewThreadRequests = 5
	maxReviewThreadsScanned = maxReviewThreadRequests * reviewThreadsPageSize
)

// reviewThreadsPage is one page of review threads, and exactly the shape
// the GraphQL query's reviewThreads field returns.
type reviewThreadsPage struct {
	PageInfo struct {
		HasNextPage bool   `json:"hasNextPage"`
		EndCursor   string `json:"endCursor"`
	} `json:"pageInfo"`
	Nodes []struct {
		IsResolved bool `json:"isResolved"`
		Comments   struct {
			Nodes []struct {
				URL  string `json:"url"`
				Body string `json:"body"`
			} `json:"nodes"`
		} `json:"comments"`
	} `json:"nodes"`
}

type graphQLResponse struct {
	Data struct {
		Repository struct {
			PullRequest struct {
				ReviewThreads reviewThreadsPage `json:"reviewThreads"`
			} `json:"pullRequest"`
		} `json:"repository"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// unresolvedThreads queries endpoint for owner/repo#number's review threads
// and returns the ones that are not resolved, walking every page rather
// than trusting the first one: a PR with its first 100 threads resolved and
// an unresolved thread on page two must not read as clean just because page
// one did. The walk is bounded by both requests made and threads scanned;
// if it hits either bound while GitHub still reports more pages, it returns
// an error rather than concluding "all resolved" from a partial view, and
// the caller's fail-open path treats that exactly like any other API
// failure — a check that could not finish is not a check that passed.
func unresolvedThreads(ctx context.Context, client *http.Client, endpoint, token, owner, repo string, number int) ([]thread, error) {
	var (
		threads []thread
		cursor  string
		scanned int
	)
	for requests := 0; ; requests++ {
		if requests >= maxReviewThreadRequests {
			return nil, fmt.Errorf("exceeded %d requests walking review threads (%d seen so far) with more pages remaining; treating the check as incomplete rather than resolved", maxReviewThreadRequests, scanned)
		}

		page, err := fetchReviewThreadsPage(ctx, client, endpoint, token, owner, repo, number, cursor)
		if err != nil {
			return nil, err
		}

		for _, n := range page.Nodes {
			scanned++
			if scanned > maxReviewThreadsScanned {
				return nil, fmt.Errorf("exceeded %d review threads scanned with more pages remaining; treating the check as incomplete rather than resolved", maxReviewThreadsScanned)
			}
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

		if !page.PageInfo.HasNextPage {
			return threads, nil
		}
		cursor = page.PageInfo.EndCursor
	}
}

// fetchReviewThreadsPage performs one bounded GraphQL request for a single
// page of review threads, starting after cursor ("" for the first page).
func fetchReviewThreadsPage(ctx context.Context, client *http.Client, endpoint, token, owner, repo string, number int, cursor string) (reviewThreadsPage, error) {
	var zero reviewThreadsPage

	variables := map[string]any{
		"owner":  owner,
		"repo":   repo,
		"number": number,
	}
	if cursor != "" {
		variables["cursor"] = cursor
	}
	body, err := json.Marshal(map[string]any{
		"query":     reviewThreadsQuery,
		"variables": variables,
	})
	if err != nil {
		return zero, fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return zero, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := client.Do(req)
	if err != nil {
		return zero, fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return zero, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return zero, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(respBody), 300))
	}

	var gr graphQLResponse
	if err := json.Unmarshal(respBody, &gr); err != nil {
		return zero, fmt.Errorf("decode response: %w", err)
	}
	if len(gr.Errors) > 0 {
		return zero, fmt.Errorf("graphql error: %s", gr.Errors[0].Message)
	}

	return gr.Data.Repository.PullRequest.ReviewThreads, nil
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
