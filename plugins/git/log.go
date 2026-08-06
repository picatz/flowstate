package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	gitv1 "github.com/picatz/flowstate/plugins/git/gen/git/v1"
)

// gitLog implements git.log: a bounded slice of a repository's commit
// history reachable from ref, oldest boundary reported so a workflow can
// tell truncation from "that is all of it." This is the read tier's audit
// primitive - the "including messages" question a security engineer or an
// agent asks about a repository without writing to it.
func gitLog(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in gitv1.LogInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	repoURL, err := validateRepositoryURL(in.GetUrl())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	ref, err := validateOptionalRevision("ref", in.GetRef())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	maxCommits, err := clampMaxCommits(in.GetMaxCommits())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	path, err := validateLogPath(in.GetPath())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	since, err := parseSince(in.GetSince())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	cursor, err := validateCursor(in.GetCursor())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	if cursor != "" && ref != "" {
		return nil, sdk.InvalidInput(
			"ref and cursor must not both be set - cursor already names a position in the same " +
				"history a resumed call would otherwise use ref to reach; a call sets one or the other")
	}

	token, err := tokenFromValue(ctx, in.GetToken())
	if err != nil {
		return nil, err
	}
	username, err := resolveUsername(in.GetUsername())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	out, err := doLog(ctx, logParams{
		url:        repoURL,
		ref:        ref,
		maxCommits: maxCommits,
		path:       path,
		since:      since,
		cursor:     cursor,
		token:      func() string { return token },
		username:   username,
	})
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// logParams is gitLog's already-validated, already-typed input - split out
// from the task function for the same reason commitPushParams is in
// commit_push.go: tests can drive the actual git mechanics against a local
// repository fixture without going through validateRepositoryURL's
// https-only gate. gitLog is the only production caller.
type logParams struct {
	url        *url.URL
	ref        string
	maxCommits int
	path       string
	since      time.Time
	cursor     string // full 40-hex commit sha, or "" - see LogInputs.cursor
	token      func() string
	username   string
}

// doLog is the actual clone -> resolve -> walk mechanics, taking
// already-validated parameters.
func doLog(ctx context.Context, p logParams) (*gitv1.LogOutputs, error) {
	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	// The fetch depth is derived from what was asked for, not fixed - see
	// plugins/vcs's own vcsLog for the full argument on why maxCommits+1,
	// not a bare maxCommits: fetching one more than what was asked for is
	// what tells "there was more history" apart from "that was genuinely
	// all of it" once a shallow clone's own boundary, rather than
	// max_commits, is what actually ran out first.
	//
	// A path filter does not change this depth: it can only ever narrow the
	// commits this task considers within whatever window it fetched, never
	// widen it. A commit older than that window that touched path is not
	// found, and this is exactly what Truncated exists to report honestly -
	// see README.md, "Operational scale," for the equivalent limit
	// git.commit_push's own base_ref resolution already has.
	//
	// An explicit ref is the one case that widens this: fetchDepthForMaxCommits
	// only reasons about walking history forward from the default branch's own
	// tip, but a named ref can be arbitrarily older than that window - the
	// commit a previous git.log call itself returned, for instance. Rather
	// than fetch that ref specifically (which go-git's clone/fetch API has no
	// clean way to do for an arbitrary commit-ish, only a named branch/tag),
	// this deepens to the plugin's own existing clone-depth ceiling
	// (maxCloneDepth, the same bound git.commit_push's base_ref resolution
	// already uses) whenever a caller names a ref at all - never for the
	// common empty-ref call, so the shallow default stays shallow. A cursor
	// is an explicit ref for exactly this purpose (gitLog already refuses a
	// call that sets both ref and cursor): resuming from a sha that can sit
	// arbitrarily far back in history needs the same widened fetch a named
	// ref does, for the same reason.
	fetchDepth := fetchDepthForMaxCommits(p.maxCommits)
	if p.ref != "" || p.cursor != "" {
		fetchDepth = maxCloneDepth
	}

	repo, err := cloneBounded(ctx, cloneOptions{url: p.url, depth: fetchDepth, token: p.token, username: p.username})
	if err != nil {
		return nil, err
	}

	// startHash is where the walk actually begins descending from, and
	// resolvedHash is what ResolvedRef reports - equal for an ordinary call,
	// but distinct when p.cursor is set: the walk begins one commit past the
	// cursor (its parent), while ResolvedRef still names the position this
	// call resolved (the cursor itself), the same way an ordinary call's
	// ResolvedRef names the ref it resolved rather than that commit's
	// parent.
	var startHash, resolvedHash plumbing.Hash
	if p.cursor != "" {
		resolvedHash, err = resolve(repo, p.cursor)
		if err != nil {
			return nil, err
		}
		cursorCommit, err := repo.CommitObject(resolvedHash)
		if err != nil {
			return nil, classifyGitError(err)
		}
		if len(cursorCommit.ParentHashes) == 0 {
			// cursor named the root commit: there is nothing older to
			// resume into. Report the walk as complete rather than
			// attempting a From with no commit behind it - go-git's own
			// LogOptions has no "start from nothing" spelling, and there is
			// nothing this call could honestly find anyway.
			return &gitv1.LogOutputs{
				ResolvedRef: resolvedHash.String(),
				Truncated:   false,
			}, nil
		}
		startHash = cursorCommit.ParentHashes[0]
	} else {
		startHash, err = resolveOptionalRef(repo, p.ref)
		if err != nil {
			return nil, err
		}
		resolvedHash = startHash
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)

	logOpts := &git.LogOptions{From: startHash}
	if p.path != "" {
		path := p.path
		logOpts.PathFilter = func(candidate string) bool { return pathMatchesFilter(candidate, path) }
	}
	if !p.since.IsZero() {
		since := p.since
		logOpts.Since = &since
	}

	commitIter, err := repo.Log(logOpts)
	if err != nil {
		return nil, classifyGitError(err)
	}
	defer commitIter.Close()

	commits, truncated, err := collectLogCommits(repo, commitIter, p.maxCommits)
	if err != nil {
		return nil, classifyGitError(err)
	}

	var nextCursor string
	if truncated && len(commits) > 0 {
		// The natural git cursor: the oldest sha this call actually
		// reached, Commits' own last entry (most-recent-first order). Left
		// empty when Commits is empty even though Truncated is true (the
		// shallow-boundary case, collectLogCommits's own doc comment) -
		// there is no last-returned commit to resume from in that case, so
		// no cursor exists to hand back; see LogOutputs.next_cursor's own
		// doc comment.
		nextCursor = commits[len(commits)-1].Sha
	}

	return &gitv1.LogOutputs{
		Commits:     commits,
		ResolvedRef: resolvedHash.String(),
		Truncated:   truncated,
		NextCursor:  nextCursor,
	}, nil
}

// fetchDepthForMaxCommits derives a shallow-clone depth from a requested
// commit count - see doLog's own comment on why it is maxCommits+1 rather
// than a fixed depth. clampMaxCommits already ceilings maxCommits at
// maxMaxCommits (200), well under maxCloneDepth (500), so this never needs
// its own clamp to stay in bounds.
func fetchDepthForMaxCommits(maxCommits int) int {
	return maxCommits + 1
}

// resolveOptionalRef resolves ref within repo, treating an empty ref as
// "the remote's HEAD" - the default git.log's and git.read_file's own
// schema advertises, unlike commit_push's base_ref, which is always
// explicit (see CommitPushInputs.base_ref's own doc comment for why). This
// plugin's own resolve (clone.go) has no such fallback, since base_ref
// never needs one; this is the read tier's counterpart, mirroring
// plugins/vcs/clone.go's own resolve.
func resolveOptionalRef(repo *git.Repository, ref string) (plumbing.Hash, error) {
	if ref == "" {
		head, err := repo.Head()
		if err != nil {
			return plumbing.ZeroHash, classifyGitError(err)
		}
		return head.Hash(), nil
	}
	return resolve(repo, ref)
}

// collectLogCommits walks iter, keeping at most maxCommits entries and
// stopping early - reporting truncated: true - the moment either bound is
// reached: maxCommits entries collected, or maxTotalLogMessageBytes worth of
// message text collected across them, whichever comes first. Both bounds
// exist independently (see validate.go's own doc comment on
// maxTotalLogMessageBytes): a repository with few, enormous commit messages
// can exceed the byte budget long before it exceeds the count.
//
// A third way collection can stop is the shallow clone's own fetch boundary,
// rather than either bound above: with a sparse path filter, the walk can run
// out of commits to consider before it ever reaches maxCommits or the byte
// budget, simply because the shallow window it fetched ran out. go-git's own
// commit walker surfaces that as plumbing.ErrObjectNotFound the moment it
// tries to step past the boundary commit into a parent this clone never
// fetched - see repoHasShallowBoundary's own doc comment for why that error,
// specifically, is the honest signal to convert into truncated: true rather
// than either a hard failure or a false truncated: false. Passed repo, not
// merely iter, so that check can consult repo.Storer directly.
func collectLogCommits(repo *git.Repository, iter object.CommitIter, maxCommits int) ([]*gitv1.Commit, bool, error) {
	var (
		commits    []*gitv1.Commit
		truncated  bool
		totalBytes int
	)
	err := iter.ForEach(func(c *object.Commit) error {
		if len(commits) >= maxCommits {
			truncated = true
			return storer.ErrStop
		}

		message, messageBytes := truncateLogMessage(c.Message, maxLogMessageBytes)
		if totalBytes+messageBytes > maxTotalLogMessageBytes {
			// The total message budget, not the per-entry cap or
			// maxCommits, is what ran out here - stopped with the same
			// honest signal: there was more history this call could have
			// described, but describing it would have meant exceeding the
			// bound on how much of it this task will ever serialize in one
			// response.
			truncated = true
			return storer.ErrStop
		}
		totalBytes += messageBytes

		commits = append(commits, &gitv1.Commit{
			Sha:          c.Hash.String(),
			Author:       signatureOf(c.Author),
			Committer:    signatureOf(c.Committer),
			Message:      message,
			ParentHashes: parentHashesOf(c),
		})
		return nil
	})
	if err == nil || err == storer.ErrStop {
		return commits, truncated, nil
	}
	if errors.Is(err, plumbing.ErrObjectNotFound) && repoHasShallowBoundary(repo) {
		// The walk did not run out of history - it ran out of what this
		// shallow clone fetched. A commit at the true end of history has no
		// parent hashes at all (iter.ForEach would then reach a clean EOF,
		// the first branch above); a commit whose parent hashes exist but
		// whose parent objects do not is exactly what a shallow fetch
		// boundary looks like, and repoHasShallowBoundary confirms this
		// clone actually has one, rather than assuming from this error
		// shape alone. Report honestly: there was more history a deeper
		// fetch could have found, so truncated is true even though neither
		// maxCommits nor the byte budget was what stopped this call.
		return commits, true, nil
	}
	return nil, false, err
}

// repoHasShallowBoundary reports whether repo's own object store recorded any
// shallow-boundary commits - go-git's own bookkeeping (equivalent to git's
// .git/shallow) for exactly the commits a shallow fetch cut off from their
// real parents. Not every storer.Storer implementation tracks this (the
// interface embeds only EncodedObjectStorer and ReferenceStorer), but
// cloneBounded's own packBoundedStorer always unwraps to a plain
// *memory.Storage (see clone.go), which does - so this holds for every
// repository this plugin ever builds.
func repoHasShallowBoundary(repo *git.Repository) bool {
	ss, ok := repo.Storer.(storer.ShallowStorer)
	if !ok {
		return false
	}
	shallow, err := ss.Shallow()
	return err == nil && len(shallow) > 0
}

// pathMatchesFilter reports whether candidate is path itself, or a path
// beneath the directory path names - the same semantics `git log -- <path>`
// has, where naming a directory includes everything under it. The separator
// is checked explicitly, not merely a prefix: strings.HasPrefix(candidate,
// path) alone would make path "auth" also match "authz/token.go", a
// different path this filter must not silently report on.
func pathMatchesFilter(candidate, path string) bool {
	return candidate == path || strings.HasPrefix(candidate, path+"/")
}

// signatureOf formats sig.When in the zone git itself recorded, never
// normalized to UTC - Signature.when's own schema doc promises RFC 3339 "in
// the recorded zone," and go-git's own decoder (object.Signature.Decode)
// already parses the raw "+hhmm"/"-hhmm" offset into sig.When's Location via
// time.FixedZone, so formatting it directly (not sig.When.UTC()) is what
// keeps that promise: a commit authored at -07:00 comes back as -07:00, not
// silently normalized to Z.
func signatureOf(sig object.Signature) *gitv1.Signature {
	return &gitv1.Signature{
		Name:  sig.Name,
		Email: sig.Email,
		When:  sig.When.Format("2006-01-02T15:04:05Z07:00"),
	}
}

func parentHashesOf(c *object.Commit) []string {
	if len(c.ParentHashes) == 0 {
		return nil
	}
	out := make([]string, len(c.ParentHashes))
	for i, h := range c.ParentHashes {
		out[i] = h.String()
	}
	return out
}

// truncateLogMessage bounds one commit message to n bytes, cutting on a
// rune boundary so it never ends mid-codepoint, and returns the byte length
// actually charged against maxTotalLogMessageBytes - the truncated result's
// own length, not the original's, since what was cut off never counts
// against the total budget.
func truncateLogMessage(s string, n int) (string, int) {
	if len(s) <= n {
		return s, len(s)
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	out := s[:n] + fmt.Sprintf("... (truncated at %d bytes)", n)
	return out, len(out)
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
