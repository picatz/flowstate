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
	if raw == "" {
		return "", fmt.Errorf("%s: path is empty", field)
	}
	if len(raw) > 4096 {
		return "", fmt.Errorf("%s: path is %d bytes, over the 4096 byte limit", field, len(raw))
	}
	if strings.HasPrefix(raw, "/") {
		return "", fmt.Errorf("%s: path %q is absolute; every path in a git tree is relative", field, raw)
	}
	if strings.Contains(raw, "\\") {
		return "", fmt.Errorf("%s: path %q contains a backslash, which no git tree path does", field, raw)
	}
	for _, r := range raw {
		if r == 0 || unicode.IsControl(r) {
			return "", fmt.Errorf("%s: path %q contains a control character", field, raw)
		}
	}

	segments := strings.Split(raw, "/")
	for _, seg := range segments {
		switch {
		case seg == "":
			return "", fmt.Errorf("%s: path %q has an empty segment (a leading, trailing, or doubled \"/\")", field, raw)
		case seg == ".":
			return "", fmt.Errorf("%s: path %q has a \".\" segment", field, raw)
		case seg == "..":
			return "", fmt.Errorf("%s: path %q escapes the tree with \"..\"", field, raw)
		case seg == ".git":
			return "", fmt.Errorf("%s: path %q writes under a \".git\" segment, which this task refuses regardless of position", field, raw)
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
