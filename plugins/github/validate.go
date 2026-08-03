package main

import (
	"fmt"
	"regexp"
	"strings"
)

// Bounds and validation this plugin applies to attacker-chosen input before
// it reaches GitHub's API - see plugins/vcs/validate.go's own doc comment
// for the reasoning this mirrors.
const (
	// maxResponseBytes bounds any single HTTP response this plugin reads,
	// installed on the transport itself (see client.go) so it applies to
	// every response go-github's own client reads, including an error body
	// on a non-2xx status - the layer CLAUDE.md's connect-go lesson names as
	// the only one a library's own error-path handling cannot bypass.
	maxResponseBytes = 8 << 20 // 8 MiB - generous for an issue/PR body and comment thread, nowhere near a repository's own size

	// maxCommentBodyBytes bounds a comment this plugin is asked to post.
	// GitHub's own limit is 65536 characters; this is tighter, since a
	// workflow step's own comment is normally short text, not a document,
	// and a bound matching this task's real use is worth more than one that
	// only repeats GitHub's own ceiling.
	maxCommentBodyBytes = 32 << 10 // 32 KiB

	requestTimeout = 30 // seconds; see client.go
)

// ownerPattern and repoPattern are validated because both are later placed
// into a URL path via go-github's own request builder (never
// string-concatenated by this plugin - go-github's PullRequests.Get and
// Issues.CreateComment take owner/repo as separate parameters and build the
// request path themselves), so refusing a malformed value here is a
// courtesy to the author, not a defense the request-building code needs.
//
// The two are deliberately different patterns, not one shared one. A single
// pattern requiring an alphanumeric first character rejects GitHub's own
// `.github` community-health repository before this task ever makes a
// request - GitHub's owner rules and repository-name rules are not the
// same, and treating them as one was itself the bug.
//
// Owner (a user or organization login): alphanumeric characters or
// hyphens, max 39 characters, and may not begin or end with a hyphen -
// GitHub's own documented username rules
// (https://docs.github.com/en/actions/reference/workflows-and-actions/limits#usernames,
// mirrored by github.com/shinnn/github-username-regex). Consecutive hyphens
// are refused too, checked separately below since Go's RE2 engine has no
// lookahead to express "no --" in the pattern itself.
var ownerPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$`)

// repoPattern is GitHub's actual repository-name rule: alphanumeric
// characters, hyphens, underscores, and periods, up to 100 characters -
// which is why a leading `.` or `_` (as in `.github` or `_internal-tools`)
// is accepted here where it is refused for an owner. `.` and `..` are
// refused explicitly below rather than by the pattern, and refused outright
// rather than sanitized: a name meaningful to path traversal in anything
// filesystem-adjacent is exactly the shape CLAUDE.md says to reject, not
// clean up, because a "cleaned" `..` silently becomes a different,
// unintended value instead of failing loudly.
var repoPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)

func validateOwner(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !ownerPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a name GitHub allows", field, value)
	}
	if strings.Contains(value, "--") {
		return fmt.Errorf("%s %q is not a name GitHub allows: consecutive hyphens", field, value)
	}
	return nil
}

func validateRepo(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	// Rejected outright, not stripped or normalized: `.` and `..` are the
	// two repository names that mean something other than a name wherever
	// this string might later reach a filesystem path, and refusing them
	// here is cheaper and more honest than trusting every future caller of
	// this value to also treat them as path-meaningful.
	if value == "." || value == ".." {
		return fmt.Errorf("%s %q is not a name GitHub allows", field, value)
	}
	if !repoPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a name GitHub allows", field, value)
	}
	return nil
}

func validateNumber(field string, n int64) error {
	if n <= 0 {
		return fmt.Errorf("%s must be a positive number", field)
	}
	return nil
}

func validateCommentBody(body string) error {
	if body == "" {
		return fmt.Errorf("body is required")
	}
	if len(body) > maxCommentBodyBytes {
		return fmt.Errorf("body is %d bytes, over the %d byte limit this task enforces", len(body), maxCommentBodyBytes)
	}
	return nil
}
