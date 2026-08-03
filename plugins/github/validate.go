package main

import (
	"fmt"
	"regexp"
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

// ownerRepoPattern matches what GitHub actually permits in an owner or
// repository name: this is validated because it is later placed into a URL
// path via go-github's own request builder (never string-concatenated by
// this plugin - go-github's PullRequests.Get and Issues.CreateComment take
// owner/repo as separate parameters and build the request path themselves),
// so refusing a malformed value here is a courtesy to the author, not a
// defense the request-building code needs.
var ownerRepoPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,98}[A-Za-z0-9])?$`)

func validateOwnerRepo(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !ownerRepoPattern.MatchString(value) {
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
