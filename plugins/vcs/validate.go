package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"
)

// Bounds this plugin enforces on attacker-chosen input: a repository URL, a
// revision string, and the shape of history or a diff it is asked to return.
// Each is here, in one file, for the same reason CLAUDE.md's guidance on
// bounding untrusted input gives - every one of these has to match the shape
// of what an attacker actually controls, and a bound nobody can find is a
// bound nobody can review.
const (
	// maxURLBytes and maxRevisionBytes bound the two strings this plugin
	// never builds a shell command or a filesystem path from, but still has
	// to hold in memory and pass to go-git's own parser. Generous relative to
	// anything real - a git ref name over a few hundred bytes is already
	// unusual - and small enough that a workflow author cannot make this
	// plugin buffer megabytes of text before the first real check runs.
	maxURLBytes      = 2048
	maxRevisionBytes = 512

	// defaultCloneDepth and maxCloneDepth bound how much history a task asks
	// go-git to fetch. Depth bounds the *commit* graph, not the size of any
	// one blob in it - see clone.go for the byte-level bound that covers
	// that gap.
	defaultCloneDepth = 50
	maxCloneDepth     = 500

	// defaultMaxCommits and maxMaxCommits bound vcs.log's own output size,
	// independent of clone depth: a repository fetched to depth 500 but
	// asked to report only 10 commits should report 10, not spend the
	// request budget serializing 500.
	defaultMaxCommits = 20
	maxMaxCommits     = 200

	// maxCommitMessageBytes bounds one commit's message in the output. A
	// commit message is written by whoever had push access to the
	// repository, which for this task is an attacker-controlled remote -
	// the same reasoning the log task in the core engine applies to a
	// workflow author's own field values.
	maxCommitMessageBytes = 4096

	// maxPatchBytes bounds the whole unified diff vcs.diff returns, and
	// maxDiffFiles bounds the per-file summary independently of it - a
	// rename-heavy commit can have thousands of file entries and a two-line
	// patch, so one bound cannot stand in for the other.
	maxPatchBytes = 1 << 20 // 1 MiB
	maxDiffFiles  = 500

	// maxResponseBytes bounds any single HTTP response go-git's transport
	// reads while cloning or fetching - see clone.go for why this, and not
	// a "maximum repository size," is the honest bound this plugin can make.
	maxResponseBytes = 128 << 20 // 128 MiB

	// requestTimeout backstops a clone or fetch that hangs, and overrides
	// netpolicy's own [netpolicy.DefaultTimeout] (30s), which is sized for a
	// single ordinary HTTP request and too tight for a packfile transfer. A
	// step's own `timeout:` is still the primary bound - it is the workflow
	// author's own budget for the step, enforced by the engine regardless of
	// what a task does internally - but a task that installed no bound of
	// its own would be relying entirely on the workflow having set one, and
	// an author who forgot is not a reason for this process to hang forever.
	requestTimeout = 2 * time.Minute
)

// validateRepositoryURL refuses anything that is not an https:// URL to a
// plain host and path.
//
// This is a scheme allowlist, not a content filter: go-git's URL handling
// recognizes several transports this plugin never wants reachable from a
// Flowfile, most importantly file:// (arbitrary local file read - the
// worker's own filesystem, including anything else on it) and the git:// and
// ssh:// forms this build has no credential story for. Refusing by allowlist
// rather than trying to deny a list of dangerous schemes is the fail-closed
// direction: a scheme go-git adds support for tomorrow that this list has
// never heard of is refused by default, not admitted by omission.
//
// http:// (unencrypted) is refused too, for the same reason the core http
// task's egress policy denies plaintext by default - a token sent to
// authenticate a private repository over http is a token sent in the clear.
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
			"url has scheme %q; this task fetches over https only, so that a repository's "+
				"remote is reached through the same governed, egress-policed transport every "+
				"other network task in this repository uses", u.Scheme)
	}

	if u.Host == "" {
		return nil, fmt.Errorf("url has no host")
	}

	if u.User != nil {
		// A credential embedded in the URL itself - https://user:pass@host/... -
		// would travel through this task's inputs as a literal, which is
		// exactly what the token field exists to avoid. Refusing it here
		// closes the one path around that field rather than leaving it as
		// an unreviewed way to smuggle a secret into a workflow file.
		return nil, fmt.Errorf(
			"url must not carry a userinfo component; pass a credential through the token " +
				"input instead, as a secret reference")
	}

	return u, nil
}

// validateRevision bounds and lightly sanity-checks a ref, branch, tag, or
// commit-ish before it reaches go-git's own revision parser.
//
// This is deliberately not a strict grammar check. go-git parses the string
// itself, safely - it is a library call, not a shell, so there is no
// injection class a stricter regex here would be closing - and refusing a
// revision go-git would have accepted just because this function's guess at
// git's grammar was too narrow is its own kind of bug. What this function
// bounds is size (so a workflow cannot make this plugin hold an arbitrarily
// large string before the real parser ever sees it) and the presence of
// control characters and embedded NULs, which no legitimate ref name
// contains and which are exactly the kind of thing worth refusing outright
// rather than trying to strip.
func validateRevision(field, raw string) (string, error) {
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

// clampMaxCommits applies vcs.log's default and ceiling to a requested count.
//
// A request over the ceiling is refused rather than silently reduced - the
// same reasoning DecodeInputs applies to a value it cannot represent: a
// silently clamped bound looks like a working request that quietly returns
// less than it was asked for, and an author who wanted 1000 commits and
// bound one page of a review to it. deserves to be told the ceiling, not
// handed a wrong-shaped answer that happens to work today.
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
