package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/go-git/go-git/v5/plumbing"
)

// Bounds this plugin enforces on attacker-chosen input. See doc.go for the
// reasoning that is specific to a write task rather than a bound; this file
// is where the numbers live, for the same reason plugins/vcs keeps its own
// bounds in one place: every one of these has to match the shape of what an
// attacker actually controls, and a bound nobody can find is a bound nobody
// can review.
const (
	// maxURLBytes and maxRevisionBytes mirror plugins/vcs's own bounds -
	// generous relative to anything real, small enough that a workflow
	// author cannot make this plugin buffer megabytes of text before the
	// first real check runs.
	maxURLBytes      = 2048
	maxRevisionBytes = 512

	// maxBranchBytes bounds a ref's own component length before it ever
	// reaches [plumbing.ReferenceName.Validate].
	maxBranchBytes = 255

	// defaultCloneDepth and maxCloneDepth bound how much history a task asks
	// go-git to fetch when resolving base_ref - see clone.go.
	defaultCloneDepth = 50
	maxCloneDepth     = 500

	// maxResumeCloneDepth bounds how deep a cursor-driven resume's own
	// progressive clone-depth retries may go - see resumeCloneDepthSteps
	// and doLog's own comment on why a linear history longer than
	// maxCloneDepth needs more than one shallow-clone attempt to keep
	// paging to exhaustion: go-git supports neither fetching an arbitrary
	// commit sha directly nor incremental --deepen (checked against its
	// own source - see doLog's comment), so the only way to reach a commit
	// a shallow clone missed is a fresh, deeper one. Larger than
	// maxCloneDepth - every OTHER caller's own ceiling, unchanged, since
	// this is the one path that deliberately widens the fetch as
	// pagination goes deeper into history - but still a fixed, small
	// multiple of it, not "keep doubling forever": the bound
	// cloneBoundedWithInflationCap enforces on every clone this plugin
	// ever makes, including this one.
	maxResumeCloneDepth = maxCloneDepth * 4

	// maxMessageBytes bounds a commit message this plugin will write. A
	// message this plugin constructs becomes durable history on the remote,
	// same reasoning as maxCommitMessageBytes in plugins/vcs applies to one
	// it merely reads.
	maxMessageBytes = 4096

	// maxFiles bounds how many entries commit_push's files map may hold, and
	// maxFileBytes bounds any one entry's content. maxTotalFileBytes bounds
	// the sum, independent of maxFiles*maxFileBytes, so a workflow cannot
	// reach the same total by many small files instead of a few large ones.
	maxFiles          = 200
	maxFileBytes      = 4 << 20  // 4 MiB
	maxTotalFileBytes = 16 << 20 // 16 MiB

	// maxPatchBytes bounds the unified diff text before it is parsed at all -
	// the same "bound before the real parser sees it" reasoning
	// validateRevision documents in plugins/vcs.
	maxPatchBytes = 1 << 20 // 1 MiB
	maxPatchFiles = 200

	// maxRemoteRefs bounds git.ls_remote's own output, independent of how
	// many refs the remote actually advertised.
	maxRemoteRefs = 1000

	// defaultMaxCommits and maxMaxCommits bound git.log's own output size,
	// independent of clone depth: a repository fetched to a shallow depth
	// but asked to report only 10 commits should report 10, not spend the
	// request budget serializing however many the clone happened to reach.
	// maxMaxCommits is the ceiling max_commits cannot cross - the resource
	// an attacker-chosen repository's own history length controls, bounded
	// so a workflow author cannot ask this task to walk and serialize an
	// unbounded amount of it.
	defaultMaxCommits = 20
	maxMaxCommits     = 200

	// maxLogMessageBytes bounds one commit's message in git.log's output,
	// and maxTotalLogMessageBytes bounds the sum across every commit
	// returned, independent of maxMaxCommits*maxLogMessageBytes: a commit
	// message has no natural size limit, and the repository this task reads
	// is attacker-chosen input the same way base_ref's own tree is for
	// commit_push - see maxMessageBytes's own doc comment for the write
	// side of that same reasoning. The total bound is what makes many
	// merely-large messages (each under the per-entry cap, several hundred
	// of them) refused the same way one pathologically large one is,
	// stopping collection early and reporting truncated: true rather than
	// serializing an unbounded response one entry at a time.
	maxLogMessageBytes      = 4096
	maxTotalLogMessageBytes = 256 << 10 // 256 KiB

	// maxLogPathBytes bounds git.log's optional path filter before it is
	// used to build a PathFilter closure - the same "bound before the real
	// use sees it" reasoning validateRevision documents.
	maxLogPathBytes = 4096

	// readFileCloneDepth is the depth git.read_file asks go-git to fetch:
	// exactly the tip of every ref/branch/tag the clone requests, since
	// reading one file at one ref never needs history - only the tree that
	// commit points to. The cheapest clone go-git can do for this
	// operation, per CLAUDE.md's "massive scale repos" guidance.
	readFileCloneDepth = 1

	// maxReadFileBytes bounds a file git.read_file returns. Refused, never
	// truncated, when a blob exceeds it - a truncated file that looks whole
	// is a worse failure mode than a clear refusal naming the actual size,
	// since a workflow (or a human) reading a silently truncated file has
	// no way to tell it apart from the real, complete content.
	maxReadFileBytes = 8 << 20 // 8 MiB

	// bytesToSniffForBinary bounds how much of a file's content
	// isLikelyBinary examines before giving up and calling it text - the
	// same window (the first 8000 bytes) git itself uses for this exact
	// heuristic (buffer_is_binary in git's own convert.c), so this task's
	// "binary" judgement agrees with plain `git` on the same content.
	bytesToSniffForBinary = 8000

	// maxResponseBytes and requestTimeout mirror plugins/vcs's own clone.go -
	// see there for the full argument, including the gap CLAUDE.md's own
	// connect-go example names: this bounds the transport every byte
	// crosses, on every status code, which is the layer a library's non-2xx
	// error path cannot bypass.
	maxResponseBytes = 128 << 20 // 128 MiB
	requestTimeout   = 2 * time.Minute

	// maxInflatedBytes bounds the sum of every object's decompressed size
	// that go-git's packfile parser materializes while parsing one clone's
	// pack stream - see packbound.go for the full argument, including what
	// this bound does and does not close. Mirrors plugins/vcs's own value.
	maxInflatedBytes = 512 << 20 // 512 MiB

	// maxUsernameBytes bounds the username input before it ever reaches
	// net/http.Request.SetBasicAuth - generous relative to any real forge
	// username or literal (Bitbucket's own documented alternative,
	// "x-bitbucket-api-token-auth", is 26 bytes), small enough that a
	// workflow cannot make this plugin build an oversized Authorization
	// header.
	maxUsernameBytes = 256

	// defaultBasicAuthUsername is what this plugin has always sent as the
	// HTTP Basic-auth username - every version before the username input
	// existed, unconditionally - and is what an unset username input still
	// resolves to, so a Flowfile written against an earlier version of this
	// schema behaves byte-identically today. See README.md, "Choosing the
	// username," for why most providers never look at this value at all,
	// and the one verified case (Bitbucket Cloud) that does.
	defaultBasicAuthUsername = "x-access-token"
)

// validateRepositoryURL refuses anything but a plain https:// URL with no
// embedded credential. See doc.go, "URL schemes: an allowlist, and the one
// still missing from it," for why ssh:// - which git itself speaks - is not
// in this allowlist yet, and why that is a reported gap rather than an
// oversight.
func validateRepositoryURL(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("url is required")
	}
	if len(raw) > maxURLBytes {
		return nil, fmt.Errorf("url is %d bytes, over the %d byte limit", len(raw), maxURLBytes)
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("url does not parse: %w", err)
	}

	if !strings.EqualFold(u.Scheme, "https") {
		return nil, fmt.Errorf(
			"url has scheme %q; this task only accepts https:// - an explicit allowlist, not "+
				"a blocklist, so a scheme this plugin has not reasoned about is refused by default "+
				"rather than admitted by omission", u.Scheme)
	}

	if u.Host == "" {
		return nil, fmt.Errorf("url has no host")
	}

	if u.User != nil {
		return nil, fmt.Errorf(
			"url must not carry a userinfo component; pass a credential through the token " +
				"input instead, as a secret reference - never in the url itself, where it would " +
				"travel as a literal and appear in any error message that echoes the url back")
	}

	return u, nil
}

// validateRevision bounds and lightly sanity-checks a ref, branch, tag, or
// commit-ish before it reaches go-git's own revision parser. See
// plugins/vcs's own validateRevision for why this is deliberately not a
// strict grammar check.
func validateRevision(field, raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if len(raw) > maxRevisionBytes {
		return "", fmt.Errorf("%s is %d bytes, over the %d byte limit", field, len(raw), maxRevisionBytes)
	}
	for _, r := range raw {
		if r == 0 || unicode.IsControl(r) {
			return "", fmt.Errorf("%s contains a control character, which no git revision does", field)
		}
	}
	return raw, nil
}

// validateOptionalRevision is validateRevision with raw == "" accepted -
// git.log's and git.read_file's own ref input, where empty means "the
// remote's HEAD" rather than a mistake, unlike commit_push's base_ref
// (always explicit; see CommitPushInputs.base_ref's own doc comment for why
// that field never defaults).
func validateOptionalRevision(field, raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	return validateRevision(field, raw)
}

// fullShaHexLen is the length of a full git commit sha in hex - always 40
// for the sha-1 object ids every repository this plugin clones actually
// uses (go-git does not yet support sha-256 repositories).
const fullShaHexLen = 40

// maxCursorEntries bounds how many commit shas a single cursor may encode
// in total (frontier entries plus already-emitted entries combined) - see
// cursor.go's own doc comment for why a cursor now carries more than one
// sha, and doLog's own comment on where this bound is actually enforced
// (at the point a NEXT cursor would be constructed, not merely when one
// arrives - an incoming cursor that itself already exceeds this bound is
// refused here too, since this task never emits one that does).
//
// Set equal to maxCloneDepth: nothing a cursor-driven clone ever fetches
// has more commits reachable from a single starting point than that, in
// the overwhelmingly common case of one dominant branch, and once a
// paginated walk has legitimately emitted that many commits this task
// stops promising a further cursor rather than encode one this bound
// cannot vouch for - see doLog's own comment on that refusal, and
// log_test.go's octopus-merge test for the case that actually reaches it
// (a single merge wide enough on its own, not merely a long walk).
const maxCursorEntries = maxCloneDepth

// validateCursor bounds git.log's optional resume position. Unlike
// validateOptionalRevision (ref's own check, which deliberately accepts
// anything go-git's revision parser does - a branch, a tag, "HEAD~3"), this
// is deliberately narrow: a cursor is never something a workflow author or
// coding agent composes by hand, only ever something this task itself
// emitted as a previous call's next_cursor - so the one shape it accepts is
// the one shape this task ever produces: cursor.go's own frontier|emitted
// encoding, every element a full 40-character lowercase hex commit sha.
// Refusing anything else (a short sha, a branch name, "HEAD", a single bare
// sha the way this field's first version accepted) closes off a second,
// differently-validated spelling of ref that would otherwise invite the two
// to be confused with one another - see LogInputs.cursor's own doc comment
// for the full argument, and cursor.go's for why a bare single sha stopped
// being enough to resume correctly at all.
func validateCursor(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	state, err := decodeCursor(raw)
	if err != nil {
		return "", fmt.Errorf("cursor is not a value this task ever emitted: %w", err)
	}
	if n := len(state.frontier) + len(state.emitted); n > maxCursorEntries {
		return "", fmt.Errorf(
			"cursor names %d commits total, over the %d this task will track in one resume - "+
				"see LogOutputs.next_cursor's own doc comment for what to do once a walk reaches this",
			n, maxCursorEntries)
	}
	return raw, nil
}

// isLowerHexDigit reports whether r is one of the 16 characters
// [plumbing.Hash.String] ever produces - lowercase only, since that is what
// every sha this task ever emits as next_cursor looks like, and a cursor
// this plugin did not itself emit is refused rather than case-normalized
// (see validateCursor's own doc comment on why cursor is narrower than
// ref).
func isLowerHexDigit(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')
}

// clampMaxCommits applies git.log's default and ceiling to a requested
// count - the same refuse-rather-than-clamp reasoning plugins/vcs's own
// clampMaxCommits documents: a silently reduced bound looks like a working
// request that quietly returned less than it was asked for.
func clampMaxCommits(requested int32) (int, error) {
	if requested == 0 {
		return defaultMaxCommits, nil
	}
	if requested < 0 {
		return 0, fmt.Errorf("max_commits must not be negative")
	}
	if requested > maxMaxCommits {
		return 0, fmt.Errorf("max_commits is %d, over the %d ceiling this task enforces", requested, maxMaxCommits)
	}
	return int(requested), nil
}

// validateLogPath bounds and lightly sanity-checks git.log's optional path
// filter. Unlike validateTreePath (commit_push's own path checks), this is
// deliberately not a full tree-path grammar: a log path filter is matched
// against history, never used to write anywhere, so the traversal and
// ".git"-segment refusals that matter for a write have no equivalent risk
// here - only size and control characters, the same two properties
// validateRevision bounds for the same reason.
func validateLogPath(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if len(raw) > maxLogPathBytes {
		return "", fmt.Errorf("path is %d bytes, over the %d byte limit", len(raw), maxLogPathBytes)
	}
	for _, r := range raw {
		if r == 0 || unicode.IsControl(r) {
			return "", fmt.Errorf("path contains a control character, which no git tree path does")
		}
	}
	return raw, nil
}

// parseSince parses git.log's optional since filter, refusing rather than
// ignoring a value that does not parse - the same reasoning parseTimestamp
// documents for commit_push's own timestamp input.
func parseSince(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("since %q is not RFC 3339: %w", raw, err)
	}
	return t, nil
}

// resolveUsername turns a task's raw username input into the value this
// plugin actually pairs with token as HTTP Basic-auth credentials: raw
// unchanged when non-empty (validated below), or [defaultBasicAuthUsername]
// when raw is empty - which is what makes leaving username unset in a
// Flowfile behave identically to a file written before this input existed.
//
// The value this returns ends up in an HTTP Authorization header
// (net/http.Request.SetBasicAuth base64-encodes "username:password" and
// sets it verbatim), so it is bound and checked for exactly what that
// header format cannot tolerate. A colon is refused outright, not treated
// as the password's problem: net/http's own SetBasicAuth documentation
// states plainly that "the provided username and password... may not
// contain a colon", and the standard says why - Basic-auth parsing splits
// the decoded "username:password" pair at the *first* colon it finds, so a
// username of "alice:admin" paired with a token "secret" does not become
// three fields, it becomes exactly two: username "alice", password
// "admin:secret". Authentication then fails against whatever real
// credential the workflow author meant to send, silently and confusingly -
// the request still reaches the server, still carries a syntactically
// valid Basic-auth header, and is refused as a bad credential rather than
// as a malformed one, which is a much harder failure to diagnose than a
// clear refusal at this layer. A literal CR or LF is refused for the
// separate reason that either could inject a second header or split the
// request into something else entirely, and every other control character
// is refused alongside them, none of which any real forge username or
// documented literal (GitHub's convention, GitLab's "oauth2", Bitbucket's
// "x-bitbucket-api-token-auth") ever contains. Refused outright, not
// stripped - see validateTreePath's own doc comment for why refusing
// rather than sanitising is this plugin's rule for every field that is
// attacker-adjacent input, which a task input a coding agent could compute
// always is.
func resolveUsername(raw string) (string, error) {
	if raw == "" {
		return defaultBasicAuthUsername, nil
	}
	if len(raw) > maxUsernameBytes {
		return "", fmt.Errorf("username is %d bytes, over the %d byte limit", len(raw), maxUsernameBytes)
	}
	if strings.Contains(raw, ":") {
		return "", fmt.Errorf(
			"username must not contain \":\" - net/http's SetBasicAuth encodes \"username:password\" " +
				"and Basic-auth parsing splits on the *first* colon, so a colon in username would " +
				"silently absorb part of token into the username instead, splitting the credential " +
				"pair wrong rather than merely rearranging it")
	}
	for _, r := range raw {
		if r == 0 || unicode.IsControl(r) {
			return "", fmt.Errorf(
				"username contains a control character (byte %#x) - it reaches an HTTP "+
					"Authorization header, where a CR or LF could inject a second header or split "+
					"the request; refused rather than stripped", r)
		}
	}
	return raw, nil
}

// validateBranchName refuses anything that is not a plain branch component:
// no "refs/heads/" prefix (this task always supplies that itself), and
// nothing plumbing.ReferenceName's own git-compatible grammar check refuses -
// including a leading "-", which [plumbing.ReferenceName.Validate] refuses
// for exactly the reason a security review of this plugin named even though
// nothing here ever builds an argv: a ref name is attacker-adjacent input
// that reaches a workflow's later steps as this task's own output, and a
// name beginning with "-" is the classic shape that confuses a naive
// argument parser somewhere further down the line, even when this plugin
// itself never passes one to anything but a typed API call.
func validateBranchName(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("branch is required")
	}
	if len(raw) > maxBranchBytes {
		return "", fmt.Errorf("branch is %d bytes, over the %d byte limit", len(raw), maxBranchBytes)
	}
	if strings.HasPrefix(raw, "refs/") {
		return "", fmt.Errorf(
			"branch must be a plain branch name, not a full ref - this task always writes to " +
				"refs/heads/<branch> itself")
	}

	full := plumbing.ReferenceName("refs/heads/" + raw)
	if err := full.Validate(); err != nil {
		return "", fmt.Errorf("branch %q is not a valid git ref component: %w", raw, err)
	}
	if strings.HasPrefix(raw, "-") {
		// full.Validate's own leading-"-" rule only fires on the third
		// slash-separated component (i==2), which "refs/heads/<raw>" always
		// makes true for a plain branch name unless raw itself contains a
		// slash and pushes the check past the branch's own first segment -
		// checked again here, directly, so a nested name like "-x/y" cannot
		// slip past on that technicality.
		return "", fmt.Errorf("branch %q must not begin with \"-\"", raw)
	}

	return raw, nil
}

// validateTreePath refuses a path this plugin will not write into a tree:
// absolute, empty, a ".." segment, anything under a literal ".git" segment,
// or a Windows-style backslash. See doc.go for why refusing (never
// sanitising) is the only sound answer for a path a patch or a files map
// names - a workflow author's own choice, or a coding agent's, and either
// way attacker-adjacent input this plugin does not get to guess about.
//
// This is deliberately not the only check standing between a hostile path
// and a written object: go-git's own object.Tree.Encode calls
// internal/pathutil.ValidTreePath on every entry name and independently
// refuses "..", ".", and a ".git" segment at any position (with HFS+/NTFS
// disguise variants), mirroring upstream git's own read-cache.c check. That
// was found while testing this function, not assumed - see tree.go and
// commit_push_test.go for where the two layers were told apart: go-git's
// check still refuses a path even with this function's own ".." case
// deliberately disabled, which is a genuine belt-and-suspenders property
// rather than redundant code, since this layer runs first, with a clearer
// diagnostic, before any object is written at all - and it is the *only*
// layer that catches writing through an already-existing symlink or
// submodule (see rebuildTree in tree.go), which is a property of where a
// path sits in base_ref's tree, not of the path string alone, and which
// go-git's own check has no way to know about.
func validateTreePath(field, raw string) (string, error) {
	// field names the input that carried this path, so a diagnostic can say
	// *which* one is wrong when more than one exists: commit_push's files map
	// ("files") and its patch ("patch") each carry paths, and each prefixes its
	// messages accordingly. read_file's sole input *is* the path, so there is no
	// second path to distinguish it from - it passes an empty field and the
	// prefix is omitted, rather than doubling the word into "path: path ...".
	// The templates below own the noun "path"; field only qualifies it.
	label := ""
	if field != "" {
		label = field + ": "
	}

	if raw == "" {
		return "", fmt.Errorf("%spath is empty", label)
	}
	if len(raw) > 4096 {
		return "", fmt.Errorf("%spath is %d bytes, over the 4096 byte limit", label, len(raw))
	}
	if strings.HasPrefix(raw, "/") {
		return "", fmt.Errorf("%spath %q is absolute; every path in a git tree is relative", label, raw)
	}
	if strings.Contains(raw, "\\") {
		return "", fmt.Errorf("%spath %q contains a backslash, which no git tree path does", label, raw)
	}
	for _, r := range raw {
		if r == 0 || unicode.IsControl(r) {
			return "", fmt.Errorf("%spath %q contains a control character", label, raw)
		}
	}

	segments := strings.Split(raw, "/")
	for _, seg := range segments {
		switch {
		case seg == "":
			return "", fmt.Errorf("%spath %q has an empty segment (a leading, trailing, or doubled \"/\")", label, raw)
		case seg == ".":
			return "", fmt.Errorf("%spath %q has a \".\" segment", label, raw)
		case seg == "..":
			return "", fmt.Errorf("%spath %q escapes the tree with \"..\"", label, raw)
		case seg == ".git":
			return "", fmt.Errorf("%spath %q writes under a \".git\" segment, which this task refuses regardless of position", label, raw)
		}
	}

	return raw, nil
}

// clampTimestamp parses an RFC 3339 timestamp, refusing rather than
// defaulting a value that does not parse - a silently ignored bad timestamp
// would fall back to the wall clock and quietly lose the determinism the
// caller asked for.
func parseTimestamp(raw string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("timestamp %q is not RFC 3339: %w", raw, err)
	}
	return t, nil
}
