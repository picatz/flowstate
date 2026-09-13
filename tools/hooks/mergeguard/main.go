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
//	bash "${CLAUDE_PROJECT_DIR}/.claude/hooks/run-hook.sh" mergeguard
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
//     of its own — recognizing that would be guessing, so it is denied.
//
// Any other tool call is not a merge and returns immediately.
//
// # Fail closed on merge evidence
//
// Review threads live behind GitHub's GraphQL API, and GraphQL and REST
// exhaust independently — CLAUDE.md, and the outage that motivated this
// hook (two API failures in one session on 2026-08-12). A session that has
// burned its GraphQL budget can still merge through REST while this hook is
// blind to threads. GraphQL is asked first; when it fails for any reason
// (that outage, or the Claude Code proxy, which refuses GraphQL outright
// and serves a REST review-thread route instead) the same evidence is read
// over REST. On any failure to identify the target, or of both transports —
// network, auth, rate limit, or a malformed response — the hook denies the
// merge. A reviewer can retry after evidence is available; absence of evidence
// is not approval. Auto-merge is denied because it can execute later without a
// final-head review immediately preceding it.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/picatz/flowstate/internal/commitcheck"
	"github.com/picatz/flowstate/internal/textbound"
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

// restEndpoint is the REST API root the fallback below reads when GraphQL
// is refused. A Claude Code session reaches GitHub through a proxy that
// answers every GraphQL request with 403 and serves REST only; the proxy's
// CCR review-thread route is the REST spelling of the same evidence.
const restEndpoint = "https://api.github.com"

func main() {
	in, err := hook.Read(os.Stdin)
	if err != nil {
		hook.Deny("mergeguard: hook input could not be read or parsed, so merge evidence was not checked. Merge blocked.")
		return
	}
	if autoMergeRequested(in) {
		hook.Deny("mergeguard: auto-merge is disabled for Flowstate; wait for exact-final-head reviews and every applicable check, run `go run ./tools/shipcheck --repo picatz/flowstate --pr NUMBER`, then merge manually")
		return
	}
	if adminMergeRequested(in) {
		hook.Deny("mergeguard: --admin can bypass repository requirements and is not permitted by the autonomous shipping policy")
		return
	}
	if mergeUsesShellExpansion(in) {
		hook.Deny(shellExpansionDenial(in.Command()))
		return
	}
	if mergeHelpOnly(in) {
		return
	}
	if disableAutoOnly(in) {
		return
	}
	if isMergeInvocation(in) && !mergeHeadPinned(in) {
		hook.Deny("mergeguard: a manual merge must be pinned to the reviewed final head. Use one explicit `gh pr merge ... --match-head-commit FULL_SHA` invocation after shipcheck passes; merge tools without an exact-head precondition are blocked.")
		return
	}

	owner, repo, number, ok := mergeTarget(in)
	if !ok {
		if reason := unidentifiedMergeReason(in); reason != "" {
			hook.Deny(reason)
		}
		return // not a merge call, or a merge call this hook could not identify
	}

	// The squash message the merge would write, held to the conventions
	// before the merge rather than found wanting in `git log` afterwards
	// (#1728). A note rather than a denial, matching the plan job's posture
	// until 2026-09-21, and folded into whichever single document this hook
	// ends with: a PreToolUse hook answers with one JSON object, so a warning
	// written here and a denial written below would be two, and the second
	// would be the one ignored (Codex, #1848).
	conventions := conventionNote(in, owner, repo, number)

	tokCtx, tokCancel := context.WithTimeout(context.Background(), tokenLookupTimeout)
	tok, ok := githubToken(tokCtx)
	tokCancel()
	if !ok {
		hook.Deny(joinNotes(conventions, fmt.Sprintf(
			"mergeguard: no GH_TOKEN or GITHUB_TOKEN in the environment, and `gh auth token` returned none either, so the review-thread check on %s/%s#%d did not run. Merge blocked until the evidence is available.",
			owner, repo, number)))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	client := &http.Client{Timeout: requestTimeout}
	threads, err := reviewThreadEvidence(ctx, client, graphQLEndpoint, restEndpoint, tok, owner, repo, number)
	if err != nil {
		hook.Deny(joinNotes(conventions, fmt.Sprintf(
			"mergeguard: could not query review threads on %s/%s#%d (%v). Merge blocked until the evidence is available.",
			owner, repo, number, err)))
		return
	}

	if len(threads) == 0 {
		if conventions != "" {
			warn(conventions)
		}
		return
	}
	hook.Deny(joinNotes(denyMessage(owner, repo, number, threads), conventions))
}

// conventionNote is what the merge message owes the conventions, or "" when
// it owes nothing or the call carries no message to read.
//
// Only the MCP merge tool hands its squash message over as commit_title and
// commit_message. A `gh pr merge` in a Bash call carries it in flags this
// hook does not parse (-t/--subject, -b/--body, -F/--body-file), and a title
// the call does not set is GitHub's default rather than an empty one, so
// that path is left to the plan job, which holds the pull request's own
// title and body (Codex, #1848).
func conventionNote(in *hook.Input, owner, repo string, number int) string {
	title, ok := in.ToolInput["commit_title"].(string)
	if !ok {
		return ""
	}
	findings := commitcheck.Check(title, stringOf(in.ToolInput["commit_message"]))
	if len(findings) == 0 {
		return ""
	}
	lines := make([]string, 0, len(findings))
	for _, f := range findings {
		lines = append(lines, f.String())
	}
	return fmt.Sprintf("mergeguard: the merge message for %s/%s#%d does not follow the conventions:\n  %s",
		owner, repo, number, strings.Join(lines, "\n  "))
}

// joinNotes is the notes that are not empty, one paragraph each.
func joinNotes(notes ...string) string {
	var out []string
	for _, n := range notes {
		if n != "" {
			out = append(out, n)
		}
	}
	return strings.Join(out, "\n\n")
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

// stringOf is a tool-input field as a string, or "" when it is absent or
// something else.
func stringOf(v any) string {
	s, _ := v.(string)
	return s
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

// unidentifiedMergeReason reports why a merge attempt whose target cannot be
// identified must be denied, or "" when the call is not a merge attempt.
func unidentifiedMergeReason(in *hook.Input) string {
	switch in.ToolName {
	case "mcp__github__merge_pull_request":
		return "mergeguard: this merge_pull_request call did not carry a usable owner, repo and pullNumber, so unresolved review threads were not checked. Merge blocked until the target is explicit."
	case "Bash":
		if isGHPRMergeInvocation(in.Command()) {
			return "mergeguard: could not identify the pull request from this `gh pr merge` invocation, so unresolved review threads were not checked. Merge blocked. Name the PR explicitly (a full PR URL, or a number together with -R/--repo owner/repo) to make it checkable."
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
	"-A":                  true,
	"-b":                  true,
	"-F":                  true,
	"--author-email":      true,
	"--body":              true,
	"--body-file":         true,
	"--match-head-commit": true,
	"--subject":           true,
	"-t":                  true,
}

// ghInheritedValueFlags are gh-wide flags that may precede the `pr merge`
// subcommand. Preserve them in the returned arguments so repository selection
// is still available to mergeTarget and auto-merge cannot hide before `pr`.
var ghInheritedValueFlags = map[string]bool{
	"--hostname": true,
	"--repo":     true,
	"-R":         true,
}

// isGHPRMergeInvocation reports whether cmd actually invokes `gh pr merge`,
// as opposed to merely mentioning the words — a commit message or a grep
// pattern quoting them must never trigger this guard, the same false-alarm
// concern pidguard's package doc names. ghPRMergeArgs preserves shell-word
// boundaries, so a trigger can only be formed by three unquoted words.
func isGHPRMergeInvocation(cmd string) bool {
	return len(ghPRMergeInvocations(cmd)) > 0
}

// autoMergeRequested reports whether a recognized gh merge invocation asks
// GitHub to merge later. The strict Ship procedure requires a manual merge
// immediately after exact-head evidence passes.
func autoMergeRequested(in *hook.Input) bool {
	if in == nil || in.ToolName != "Bash" {
		return false
	}
	for _, args := range ghPRMergeInvocations(in.Command()) {
		for i := 0; i < len(args); i++ {
			arg := args[i]
			if ghMergeValueFlags[arg] || arg == "-R" || arg == "--repo" {
				i++
				continue
			}
			if arg == "--auto" || strings.HasPrefix(arg, "--auto=") {
				return true
			}
		}
	}
	return false
}

func mergeHelpOnly(in *hook.Input) bool {
	if in == nil || in.ToolName != "Bash" {
		return false
	}
	invocations := ghPRMergeInvocations(in.Command())
	if len(invocations) != 1 {
		return false
	}
	help := false
	helpSeen := false
	for i := 0; i < len(invocations[0]); i++ {
		arg := invocations[0][i]
		switch {
		case arg == "-R" || arg == "--repo":
			i++
		case strings.HasPrefix(arg, "-R") || strings.HasPrefix(arg, "--repo="):
		case arg == "-h" || arg == "--help":
			help = true
			helpSeen = true
		case strings.HasPrefix(arg, "--help="):
			value, err := strconv.ParseBool(strings.TrimPrefix(arg, "--help="))
			if err != nil {
				return false
			}
			help = value
			helpSeen = true
		case strings.HasPrefix(arg, "-"):
			return false
		}
	}
	return helpSeen && help
}

func adminMergeRequested(in *hook.Input) bool {
	if in == nil || in.ToolName != "Bash" {
		return false
	}
	for _, args := range ghPRMergeInvocations(in.Command()) {
		for i := 0; i < len(args); i++ {
			arg := args[i]
			if ghMergeValueFlags[arg] || arg == "-R" || arg == "--repo" {
				i++
				continue
			}
			if arg == "--admin" || strings.HasPrefix(arg, "--admin=") {
				return true
			}
		}
	}
	return false
}

// disableAutoOnly recognizes the gh subcommand's corrective early-return path.
// It changes no code and performs no merge, so requiring merge evidence would
// obstruct disabling the state that shipcheck rejects. Compound invocations do
// not qualify: every actual merge in the command must still be checked.
func disableAutoOnly(in *hook.Input) bool {
	if in == nil || in.ToolName != "Bash" {
		return false
	}
	invocations := ghPRMergeInvocations(in.Command())
	if len(invocations) != 1 {
		return false
	}
	args := invocations[0]
	if hasUnsupportedShortOptionGroup(args) {
		return false
	}
	var disableCount int
	var disable bool
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if ghMergeValueFlags[arg] || arg == "-R" || arg == "--repo" {
			i++
			continue
		}
		switch arg {
		case "--disable-auto", "--disable-auto=true":
			disableCount++
			disable = true
		case "--disable-auto=false":
			disableCount++
			disable = false
		case "--":
			return false
		default:
			if strings.HasPrefix(arg, "--disable-auto=") {
				return false
			}
		}
	}
	return disableCount == 1 && disable
}

func mergeUsesShellExpansion(in *hook.Input) bool {
	if in == nil || in.ToolName != "Bash" {
		return false
	}
	cmd := in.Command()
	recognized := isGHPRMergeInvocation(cmd)
	processSubstitution := (strings.Contains(cmd, ">(") || strings.Contains(cmd, "<(")) &&
		ghMergeText.MatchString(cmd)
	braceExpansion := ghExpansionMergeText.MatchString(cmd) && strings.Contains(cmd, "{") && strings.Contains(cmd, "}")
	mergeExpansion := !recognized && (ghExpansionMergeText.MatchString(cmd) || ghDynamicPRMergeText.MatchString(cmd) || dynamicGHExecutable.MatchString(cmd)) && (strings.ContainsAny(cmd, "$`") ||
		strings.Contains(cmd, "{") && strings.Contains(cmd, "}"))
	nestedEvaluation := ghMergeText.MatchString(cmd) && (strings.Contains(cmd, "eval ") || bashCommandEvaluation.MatchString(cmd))
	if !recognized && !expandedMergeExecutable.MatchString(cmd) && !processSubstitution && !braceExpansion && !mergeExpansion && !nestedEvaluation {
		return false
	}
	if processSubstitution || braceExpansion || mergeExpansion || nestedEvaluation {
		return true
	}
	var inSingle, inDouble, inComment, escaped, wordStarted bool
	for _, r := range in.Command() {
		switch {
		case inComment:
			if r == '\n' {
				inComment = false
				wordStarted = false
			}
		case escaped:
			escaped = false
			wordStarted = true
		case r == '\\' && !inSingle:
			escaped = true
		case r == '\'' && !inDouble:
			inSingle = !inSingle
			wordStarted = true
		case r == '"' && !inSingle:
			inDouble = !inDouble
			wordStarted = true
		case r == '#' && !inSingle && !inDouble && !wordStarted:
			inComment = true
		case !inSingle && (r == '$' || r == '`'):
			return true
		case !inSingle && !inDouble && strings.ContainsRune(" \t\r\n;&|(){}", r):
			wordStarted = false
		default:
			wordStarted = true
		}
	}
	return false
}

// expandedMergeExecutable catches a merge whose executable is supplied by
// parameter or command expansion, before the literal gh recognizer can see it.
// It permits inherited gh flags between that executable and `pr merge` so a
// dynamic `gh -R owner/repo pr merge` is denied too.
var expandedMergeExecutable = regexp.MustCompile(`(?:^|[;&|(){}\n])\s*(?:[A-Za-z_][A-Za-z0-9_]*=[^;\n]+;\s*)?(?:"?\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[^}\n]+\}|\([^\n)]*\))"?|` + "`[^`\n]+`" + `)(?:\s+(?:-R\S+|(?:-R|--repo|--hostname)(?:=\S+|\s+\S+)))*\s+pr(?:\s+(?:-R\S+|(?:-R|--repo|--hostname)(?:=\S+|\s+\S+)))*\s+merge(?:\s|$)`)

// Process substitution can contain shell separators that the deliberately
// small command lexer does not nest. Match across them and deny conservatively
// rather than letting their contents split a merge into unrecognized commands.
var ghMergeText = regexp.MustCompile(`(?s)\bgh\b.*\bpr\b.*\bmerge\b`)
var ghExpansionMergeText = regexp.MustCompile(`(?s)\bgh\b.*\bmerge\b`)
var ghDynamicPRMergeText = regexp.MustCompile(`(?s)\bgh\b[^;\n]*\bp\S*\s+m\S*`)
var dynamicGHExecutable = regexp.MustCompile("\\bg\\S*(?:\\$|`)\\S*h\\b[^;\\n]*\\bpr\\b[^;\\n]*\\bmerge\\b")
var bashCommandEvaluation = regexp.MustCompile(`\b(?:ba|da)?sh(?:\s+(?:--[A-Za-z-]+|-[A-Za-z]*))*\s+-[A-Za-z]*c[A-Za-z]*\b`)

var fullCommitOID = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)

// ghToken and standaloneMergeWord are the two ingredients the loose heuristics
// above look for, named separately so a denial can report which of them it saw.
// `merged`, `mergeable` and `merge_commit` are not standaloneMergeWord: `_`, `d`
// and `a` are word characters, so a status query reading those fields does not
// carry the word in the sense that matters here.
var (
	ghToken             = regexp.MustCompile(`\bgh\b`)
	standaloneMergeWord = regexp.MustCompile(`\bmerge\b`)
)

// shellExpansionDenial explains one mergeUsesShellExpansion denial. That
// function owns the decision and is not consulted here for anything but the
// reason; this only chooses the wording, because a PreToolUse hook may write one
// document and the reason is the whole of what a caller can act on.
//
// The distinction worth drawing is whether the command was recognized as a merge
// at all. When it was, the original advice is the right advice: make the
// invocation explicit and pin it. When it was not, a loose text heuristic
// matched — a gh token, a standalone `merge`, and an expansion, in any three
// places in the command — and the caller may well have written a status query, or
// a commit message that discusses merging, with no merge anywhere in it (#1972).
// Telling that caller to use one fully explicit merge invocation after shipcheck
// passes is advice for a task they are not doing, and the only thing it teaches
// is to stop running the command rather than to separate it.
//
// The refusal itself is deliberate and unchanged. Text cannot tell an obfuscated
// merge from prose about merging once expansion is in play, so this hook fails
// closed and says so, rather than guessing from what the words look like.
func shellExpansionDenial(cmd string) string {
	const explicit = "If it is a merge, use one fully explicit `gh pr merge ... --match-head-commit FULL_SHA` invocation after shipcheck passes."

	// Recognized means recognized by text, not proven to be a merge: the words are
	// matched wherever they appear, a quoted heredoc body included, so prose that
	// merely documents a merge invocation lands here too and needs a remedy of its
	// own rather than only the advice for merging.
	if isGHPRMergeInvocation(cmd) || expandedMergeExecutable.MatchString(cmd) {
		return "mergeguard: shell expansion in a merge invocation can hide auto-merge or change its target and head. " + explicit +
			" If this command only quotes or documents a merge invocation, the words were still matched: pass that text through a file, the way `git commit -F FILE` does, rather than through the command line."
	}

	// Each name describes the syntax the heuristic matched, not a behavior it
	// proved. The predicates run over raw text, so a `$` inside single quotes and
	// a `{` in a jq selector count exactly as much as a live expansion does;
	// calling those "a shell expansion" would send the caller looking for an
	// expansion that is not there, and, worse, would suggest quoting as the
	// remedy when quoting changes nothing here.
	//
	// The list reports what the command carries, not why it was refused, and is
	// deliberately not claimed to be either exhaustive or sufficient. Two earlier
	// drafts of this message asserted remedies and both were falsified by
	// measurement: `dynamicGHExecutable` can fire on an obfuscated executable that
	// `\bgh\b` does not match, so a named ingredient is not always the one that
	// matched; and `gh api "repos/o/r/pulls/$PR/merge"`, GitHub's own endpoint for
	// checking whether a pull request is merged, is refused with no remedy
	// available short of inlining the value. Stating what matched is checkable.
	// Stating what clears it is a claim about a five-way disjunction, which this
	// text is the wrong place to make; #1972 carries it instead.
	var saw []string
	if ghToken.MatchString(cmd) {
		saw = append(saw, "a `gh` token")
	}
	if standaloneMergeWord.MatchString(cmd) {
		saw = append(saw, "the standalone word `merge`")
	}
	if strings.Contains(cmd, ">(") || strings.Contains(cmd, "<(") {
		saw = append(saw, "process-substitution syntax (`>(` or `<(`)")
	}
	if strings.Contains(cmd, "eval ") || bashCommandEvaluation.MatchString(cmd) {
		saw = append(saw, "an `eval` or shell `-c` spelling")
	}
	if strings.Contains(cmd, "{") && strings.Contains(cmd, "}") {
		saw = append(saw, "brace characters (`{` with `}`)")
	}
	if strings.ContainsAny(cmd, "$`") {
		saw = append(saw, "an expansion character (`$` or a backtick)")
	}

	return "mergeguard: this is not a recognized merge invocation, but it carries " + joinAnd(saw) +
		". This hook matches text rather than parsed shell, so a quoted occurrence counts the same as a live one and quoting is not the remedy; " +
		"together they are how an obfuscated merge looks to it, so the command is refused rather than guessed at. " +
		"Which change clears it depends on which pattern matched, so no single remedy is promised here: #1972 records what has " +
		"worked, and the reads these patterns cannot tell from a merge. If the command only discusses merging, a commit message " +
		"say, passing that text through a file the way `git commit -F FILE` does keeps it off the command line. " + explicit
}

// joinAnd renders a short list as prose. The lists here are the handful of
// indicator names above, so the simple form is enough and an empty list cannot
// arise: mergeUsesShellExpansion denies only when at least one of them is set.
func joinAnd(items []string) string {
	switch len(items) {
	case 0:
		return "a shape this hook refuses"
	case 1:
		return items[0]
	case 2:
		return items[0] + " and " + items[1]
	default:
		return strings.Join(items[:len(items)-1], ", ") + " and " + items[len(items)-1]
	}
}

func isMergeInvocation(in *hook.Input) bool {
	if in == nil {
		return false
	}
	return in.ToolName == "mcp__github__merge_pull_request" ||
		(in.ToolName == "Bash" && isGHPRMergeInvocation(in.Command()))
}

func mergeHeadPinned(in *hook.Input) bool {
	if in == nil {
		return false
	}
	if in.ToolName == "mcp__github__merge_pull_request" {
		// The official GitHub MCP server forwards expectedHeadSha to the REST
		// merge API's SHA precondition. expectedHeadOid is not part of that
		// contract and must not make an unpinned merge appear pinned.
		return fullCommitOID.MatchString(stringOf(in.ToolInput["expectedHeadSha"]))
	}
	args, ok := ghPRMergeArgs(in.Command())
	if !ok {
		return false
	}
	if hasUnsupportedShortOptionGroup(args) {
		return false
	}
	var heads []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--match-head-commit" {
			if i+1 >= len(args) {
				return false
			}
			heads = append(heads, args[i+1])
			i++
			continue
		}
		if after, ok0 := strings.CutPrefix(arg, "--match-head-commit="); ok0 {
			heads = append(heads, after)
			continue
		}
		if ghMergeValueFlags[arg] || arg == "-R" || arg == "--repo" {
			i++
		}
	}
	return len(heads) == 1 && fullCommitOID.MatchString(heads[0])
}

// hasUnsupportedShortOptionGroup rejects bundled short options rather than
// guessing which rune owns a following value. The documented shipping command
// uses long options, and a false negative here could mistake body text for the
// exact-head precondition.
func hasUnsupportedShortOptionGroup(args []string) bool {
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") && len(arg) > 2 &&
			!strings.HasPrefix(arg, "-R") {
			return true
		}
	}
	return false
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
		case strings.HasPrefix(f, "-R="):
			if sub := repoFlagValue.FindStringSubmatch(strings.TrimPrefix(f, "-R=")); sub != nil {
				flagOwner, flagRepo = sub[1], sub[2]
			}
		case strings.HasPrefix(f, "-R") && len(f) > 2:
			if sub := repoFlagValue.FindStringSubmatch(strings.TrimPrefix(f, "-R")); sub != nil {
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
func ghPRMergeInvocations(s string) [][]string {
	// Normalize Bash's combined output-redirection spellings before tokenizing.
	// Their leading ampersand is not a command separator, and >| is one
	// redirection operator rather than a redirect followed by a pipeline.
	s = strings.NewReplacer("&>>", ">>", "&>", ">", ">|", ">").Replace(s)
	var commands [][]string
	var words []string
	var word strings.Builder
	var inSingle, inDouble, inComment, escaped, started bool
	redirection := 0 // 1 awaits a target; 2 consumes one
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
		case inComment:
			if r == '\n' {
				inComment = false
				flushCommand()
			}
		case escaped:
			if redirection != 0 {
				redirection = 2
			} else if r != '\n' {
				word.WriteRune(r)
				started = true
			}
			escaped = false
		case inSingle:
			if r == '\'' {
				inSingle = false
			} else if redirection == 0 {
				word.WriteRune(r)
			}
		case inDouble:
			switch r {
			case '\\':
				escaped = true
			case '"':
				inDouble = false
			default:
				if redirection == 0 {
					word.WriteRune(r)
				}
			}
		case r == '\\':
			escaped = true
		case r == '\'':
			inSingle, started = true, true
			if redirection != 0 {
				redirection, started = 2, false
			}
		case r == '"':
			inDouble, started = true, true
			if redirection != 0 {
				redirection, started = 2, false
			}
		case r == '#' && !started && redirection == 0:
			inComment = true
		case r == ' ' || r == '\t' || r == '\r':
			if redirection == 2 {
				redirection = 0
			} else if redirection == 0 {
				flushWord()
			}
		case r == '<' || r == '>':
			if started && strings.Trim(word.String(), "0123456789") == "" {
				word.Reset()
				started = false
			} else {
				flushWord()
			}
			redirection = 1
		case r == '&' && redirection == 1:
			// Descriptor duplication (for example 2>&1) is part of the
			// redirection, not a command separator. Keep the following merge
			// arguments in this simple command.
			redirection = 1
		case strings.ContainsRune(";&|\n`(){}", r):
			redirection = 0
			flushCommand()
		default:
			if redirection != 0 {
				redirection = 2
			} else {
				word.WriteRune(r)
				started = true
			}
		}
	}
	flushCommand()

	var invocations [][]string
	for _, command := range commands {
		for i := range command {
			if command[i] != "gh" && !strings.HasSuffix(command[i], "/gh") {
				continue
			}
			var inherited []string
			j := i + 1
			for j < len(command) {
				arg := command[j]
				switch {
				case arg == "--help" || strings.HasPrefix(arg, "--help="):
					inherited = append(inherited, arg)
					j++
				case ghInheritedValueFlags[arg] && j+1 < len(command):
					inherited = append(inherited, arg, command[j+1])
					j += 2
				case strings.HasPrefix(arg, "--repo=") || strings.HasPrefix(arg, "--hostname="):
					inherited = append(inherited, arg)
					j++
				case strings.HasPrefix(arg, "-R=") || strings.HasPrefix(arg, "-R") && len(arg) > 2:
					inherited = append(inherited, arg)
					j++
				default:
					if arg == "pr" {
						j++
						for j < len(command) {
							arg = command[j]
							switch {
							case arg == "--help" || strings.HasPrefix(arg, "--help="):
								inherited = append(inherited, arg)
								j++
							case ghInheritedValueFlags[arg] && j+1 < len(command):
								inherited = append(inherited, arg, command[j+1])
								j += 2
							case strings.HasPrefix(arg, "--repo=") || strings.HasPrefix(arg, "--hostname=") ||
								strings.HasPrefix(arg, "-R=") || strings.HasPrefix(arg, "-R") && len(arg) > 2:
								inherited = append(inherited, arg)
								j++
							default:
								if arg == "merge" {
									invocations = append(invocations, append(inherited, command[j+1:]...))
								}
								j = len(command)
							}
						}
					}
					j = len(command)
				}
			}
		}
	}
	return invocations
}

func ghPRMergeArgs(s string) ([]string, bool) {
	invocations := ghPRMergeInvocations(s)
	if len(invocations) != 1 {
		return nil, false
	}
	return invocations[0], true
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
		return zero, fmt.Errorf("status %d: %s", resp.StatusCode, textbound.Truncate(string(respBody), 300))
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

// reviewThreadEvidence is the unresolved-thread check with two transports:
// GraphQL first, and when that fails for any reason (a proxy that refuses
// GraphQL, an exhausted GraphQL budget while REST still answers), the REST
// review-thread route at rest. It errors only when both are unavailable, so
// the fallback adds a way for the check to run without adding a way for a
// failed check to read as clean.
func reviewThreadEvidence(ctx context.Context, client *http.Client, graphql, rest, token, owner, repo string, number int) ([]thread, error) {
	threads, graphQLErr := unresolvedThreads(ctx, client, graphql, token, owner, repo, number)
	if graphQLErr == nil {
		return threads, nil
	}
	// A GraphQL failure that spent the whole budget (a hang, not a 403) must
	// not leave REST with none: the fallback gets its own requestTimeout.
	restCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), requestTimeout)
	defer cancel()
	threads, restErr := unresolvedThreadsREST(restCtx, client, rest, token, owner, repo, number)
	if restErr != nil {
		return nil, fmt.Errorf("GraphQL: %v; REST: %v", graphQLErr, restErr)
	}
	return threads, nil
}

// restReviewThread is one thread as the REST review-thread route reports
// it: resolution state and location, but no comment body.
type restReviewThread struct {
	Resolved   bool    `json:"resolved"`
	Path       string  `json:"path"`
	Line       *int    `json:"line"`
	CommentIDs []int64 `json:"comment_ids"`
}

// unresolvedThreadsREST reads owner/repo#number's review threads from the
// REST route GET {rest}/repos/{owner}/{repo}/pulls/{number}/ccr/review_threads
// and returns the unresolved ones. The route returns every thread in one
// response, so the walk bound is on threads read rather than pages; more
// than maxReviewThreadsScanned is reported as incomplete rather than read
// as clean, matching the GraphQL walk. Each thread is named by the URL of
// its first comment and its path:line, which is what the route exposes.
func unresolvedThreadsREST(ctx context.Context, client *http.Client, rest, token, owner, repo string, number int) ([]thread, error) {
	// owner and repo come from the tool call; mergeTarget keeps them free of
	// slashes and whitespace, and escaping keeps every other byte a path
	// segment rather than a query or fragment.
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/ccr/review_threads", strings.TrimRight(rest, "/"), neturl.PathEscape(owner), neturl.PathEscape(repo), number)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
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
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, textbound.Truncate(string(respBody), 300))
	}

	var page []restReviewThread
	if err := json.Unmarshal(respBody, &page); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	// A JSON null decodes into a nil slice without error; it is a missing
	// thread list, not an empty one, and must not read as "all resolved".
	if page == nil {
		return nil, fmt.Errorf("decode response: the route returned no thread list")
	}
	if len(page) > maxReviewThreadsScanned {
		return nil, fmt.Errorf("the route returned %d review threads, more than the %d this check reads; treating the check as incomplete rather than resolved", len(page), maxReviewThreadsScanned)
	}

	var threads []thread
	for _, rt := range page {
		if rt.Resolved {
			continue
		}
		t := thread{Body: rt.Path}
		if rt.Line != nil {
			t.Body = fmt.Sprintf("%s:%d", rt.Path, *rt.Line)
		}
		if len(rt.CommentIDs) > 0 {
			t.URL = fmt.Sprintf("https://github.com/%s/%s/pull/%d#discussion_r%d", owner, repo, number, rt.CommentIDs[0])
		}
		threads = append(threads, t)
	}
	return threads, nil
}

// denyMessage names every unresolved thread, so the operator knows exactly
// what to clear rather than just that something is unresolved.
func denyMessage(owner, repo string, number int, threads []thread) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s/%s#%d has %d unresolved review thread(s); resolve them (or explicitly decide and record why not) before merging:\n", owner, repo, number, len(threads))
	for _, t := range threads {
		line := strings.ReplaceAll(strings.TrimSpace(t.Body), "\n", " ")
		line = textbound.Truncate(line, 140)
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
