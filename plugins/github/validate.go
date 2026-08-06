package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
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

	// defaultMaxResults and maxMaxResults bound every read/audit-tier list
	// task's own output size (pull_request_list, pull_request_files,
	// issue_list) - the same "ceiling, refused rather than silently
	// clamped" reasoning plugins/git's own defaultMaxCommits/maxMaxCommits
	// document (see that package's validate.go) - independent of how many
	// pull requests, files, or issues the repository actually has.
	defaultMaxResults = 30
	maxMaxResults     = 200

	// maxPerPage bounds this plugin's own request to GitHub per page -
	// GitHub's own ceiling for every List endpoint this plugin calls, so
	// asking for more would not get more.
	maxPerPage = 100

	// maxListRequests bounds how many page requests one listing task may
	// make - the second, independent bound CLAUDE.md's own "Bound anything
	// that consumes untrusted input" section requires for any paged
	// listing (see pkg/flowstate/v1/server/list.go's maxListScan /
	// maxListRequests, the lesson this mirrors): GitHub decides how many
	// items land in a page - Response.NextPage can legitimately stay
	// non-zero across a page carrying zero items while a large result set
	// is computed - so a loop bounded only by items collected does not
	// terminate against a peer that always pages forward with an empty
	// page. See paginate.go's paginateBounded and
	// paginate_test.go's own peer that does exactly this.
	maxListRequests = 20

	// maxLabels and maxLabelBytes bound issue_list's optional labels filter
	// before it is sent to GitHub at all - the same "bound before the real
	// use sees it" reasoning validateCommentBody documents for a comment's
	// own body.
	maxLabels     = 20
	maxLabelBytes = 100
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

// validateState normalizes and checks the read/audit tier's optional state
// filter (pull_request_list, issue_list): empty means this task's own
// explicit default, "open" - named rather than left to whatever GitHub's
// API defaults to unasked, so a Flowfile reading state: ${vars.something}
// with nothing set behaves the same today and after any future change to
// GitHub's own default.
func validateState(field, value string) (string, error) {
	if value == "" {
		return "open", nil
	}
	switch value {
	case "open", "closed", "all":
		return value, nil
	default:
		return "", fmt.Errorf("%s %q is not one of \"open\", \"closed\", \"all\"", field, value)
	}
}

// clampMaxResults applies the read/audit tier's shared default and ceiling
// to a requested count - the same refuse-rather-than-clamp reasoning
// plugins/git's own clampMaxCommits documents: a silently reduced bound
// looks like a working request that quietly returned less than it was
// asked for.
func clampMaxResults(requested int32) (int, error) {
	if requested == 0 {
		return defaultMaxResults, nil
	}
	if requested < 0 {
		return 0, fmt.Errorf("max_results must not be negative")
	}
	if requested > maxMaxResults {
		return 0, fmt.Errorf("max_results is %d, over the %d ceiling this task enforces", requested, maxMaxResults)
	}
	return int(requested), nil
}

// validateLabels bounds issue_list's optional labels filter before it is
// ever sent to GitHub - count and per-entry length, the same "bound before
// the real use sees it" reasoning every other attacker-adjacent input in
// this plugin follows.
func validateLabels(labels []string) error {
	if len(labels) > maxLabels {
		return fmt.Errorf("labels has %d entries, over the %d limit this task enforces", len(labels), maxLabels)
	}
	for _, l := range labels {
		if l == "" {
			return fmt.Errorf("labels must not contain an empty entry")
		}
		if len(l) > maxLabelBytes {
			return fmt.Errorf("label %q is %d bytes, over the %d byte limit this task enforces", l, len(l), maxLabelBytes)
		}
	}
	return nil
}

// parseSince parses issue_list's optional since filter, refusing rather
// than ignoring a value that does not parse - the same reasoning
// plugins/git's own parseSince (git.log's since input) documents.
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
