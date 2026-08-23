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
	// Checked against the raw input, before validateCursor, deliberately:
	// a call that sets both gets this diagnostic regardless of whether its
	// own cursor value happens to be well-formed - the conflict is the
	// actual problem, not incidentally which of the two checks would have
	// fired first.
	if in.GetCursor() != "" && ref != "" {
		return nil, sdk.InvalidInput(
			"ref and cursor must not both be set - cursor already names a position in the same " +
				"history a resumed call would otherwise use ref to reach; a call sets one or the other")
	}
	cursor, err := validateCursor(in.GetCursor())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
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

// doLog is doLogWithBounds using this task's real ceilings - the only
// production caller. See doLogWithBounds for why the ceilings are
// parameters at all.
func doLog(ctx context.Context, p logParams) (*gitv1.LogOutputs, error) {
	return doLogWithBounds(ctx, p, resumeCloneDepthSteps, maxCursorEntries)
}

// resumeCloneDepthSteps are the shallow-clone depths a cursor-driven
// resume attempts, in increasing order, before giving up.
//
// A linear history longer than one clone's own depth cannot be reached by
// a single fixed-depth attempt: the resumed call's frontier can sit
// arbitrarily far behind whatever the remote's tips currently are, and
// go-git has no way to fetch an arbitrary commit sha directly (no
// uploadpack.allowReachableSHA1InWant support - that capability is
// transport- and server-config-dependent even where git itself supports
// it) and no incremental --deepen (checked against go-git v5.19.2's own
// source: FetchOptions and CloneOptions both take a single, one-shot
// Depth, nothing that "deepens" an existing shallow clone). The only way
// to reach a commit a shallow clone missed is a fresh, deeper clone -
// so a resume retries at increasing depth rather than failing on the
// first attempt, which is what lets a history longer than one
// maxCloneDepth window still page all the way to exhaustion.
//
// Bounded to this small, fixed sequence - not "keep doubling forever" -
// so a resume can never become an unbounded fetch loop against a hostile
// or merely enormous remote: maxResumeCloneDepth (cloneBounded's own
// ceiling) is the hard stop, and doLogWithBounds returns a distinct,
// actionable error once even the largest attempt here still cannot
// resolve anything in the cursor's own frontier.
var resumeCloneDepthSteps = []int{maxCloneDepth, maxCloneDepth * 2, maxResumeCloneDepth}

// doLogWithBounds is doLog's actual clone -> resolve -> walk mechanics,
// taking already-validated parameters and its two ceilings
// (cloneDepthSteps, maxCursor) as arguments rather than reading the
// package's own resumeCloneDepthSteps/maxCursorEntries constants directly -
// the same seam doReadFileWithMax uses for maxReadFileBytes: it lets tests
// drive the progressive-deepening retry and the cursor-size bound with a
// small, fast fixture instead of needing genuinely hundreds of commits or
// an octopus merge wide enough to reach maxCursorEntries for real.
func doLogWithBounds(ctx context.Context, p logParams, cloneDepthSteps []int, maxCursor int) (*gitv1.LogOutputs, error) {
	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	var cur cursorState
	if p.cursor != "" {
		var err error
		cur, err = decodeCursor(p.cursor)
		if err != nil {
			// gitLog's own validateCursor already refused anything not
			// shaped like this before doLog was ever reached in
			// production - reaching here means logParams was built
			// directly (a test, most likely) with a cursor that bypassed
			// that check, so this is refused on the same grounds
			// validateCursor itself would have refused it on.
			return nil, sdk.InvalidInput("cursor: %v", err)
		}
	}

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
	// tip, but a named ref can be arbitrarily older than that window. Rather
	// than fetch that ref specifically (which go-git's clone/fetch API has no
	// clean way to do for an arbitrary commit-ish, only a named branch/tag),
	// this deepens to the plugin's own existing clone-depth ceiling
	// (maxCloneDepth, the same bound git.commit_push's base_ref resolution
	// already uses) whenever a caller names a ref at all - never for the
	// common empty-ref call, so the shallow default stays shallow.
	//
	// A cursor takes an entirely different path below (progressive
	// deepening, cloneDepthSteps), since its own frontier can sit even
	// further back than a single maxCloneDepth widening reaches.
	var (
		commits     []*gitv1.Commit
		truncated   bool
		baseIter    *multiRootCommitIter
		resolvedRef string
	)

	if p.cursor != "" {
		// The retry unit is the WHOLE clone-plus-walk attempt, not merely
		// "does the frontier resolve": a shallow-enough clone can let SOME
		// frontier entries resolve (frontierReachable's own quick check,
		// used below purely to skip a doomed walk early) while the walk
		// itself still runs face-first into the boundary on its very
		// first pop, if THAT happens to be the deepest entry - producing
		// the same "truncated: true, zero commits" shallow-boundary signal
		// collectLogCommits already reports honestly for a single-root
		// walk. Escalating only the reachability PROBE and then trying to
		// walk at that same depth would leave that case stuck forever
		// (this is exactly what TestGitLogCursorResumesLinearHistoryLongerThanTheFirstCloneDepth
		// caught) - so a degenerate zero-commit result retries the entire
		// attempt at the next, larger depth too, and only the LAST step's
		// own degenerate result becomes the honest ceiling error below.
		var lastErr error
		for i, depth := range cloneDepthSteps {
			r, err := cloneBounded(ctx, cloneOptions{url: p.url, depth: depth, token: p.token, username: p.username})
			if err != nil {
				return nil, err
			}
			if !frontierReachable(r, cur.frontier) {
				lastErr = fmt.Errorf("none of the cursor's own frontier resolved within a %d-commit-deep clone", depth)
				continue
			}

			flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)
			c, tr, bi, werr := walkPage(r, cur.frontier, cur.emittedSet(), p)
			if werr != nil {
				return nil, werr
			}
			if tr && len(c) == 0 {
				// The walk resolved at least one frontier entry but hit the
				// shallow boundary immediately, before ever emitting a
				// commit - the same degenerate shape a too-shallow clone
				// produces regardless of which specific check surfaces it.
				// Never an acceptable final answer, even on the last step:
				// falling through here with baseIter still nil is what
				// turns this into the honest ceiling error below, rather
				// than silently handing back a truncated: true page that
				// can never be resumed.
				lastErr = fmt.Errorf("the walk immediately hit a shallow boundary within a %d-commit-deep clone", depth)
				if i < len(cloneDepthSteps)-1 {
					continue
				}
				break
			}

			commits, truncated, baseIter = c, tr, bi
			break
		}
		if baseIter == nil {
			return nil, sdk.InvalidInput(
				"this cursor names history more than %d commits behind the branch tips this task "+
					"fetched, even after retrying at increasing depth - git.log cannot resume this far "+
					"back in one call; narrow the walk with since or path so fewer pages are needed "+
					"to get this deep, or treat the walk as complete at the last page that succeeded: %v",
				cloneDepthSteps[len(cloneDepthSteps)-1], lastErr)
		}
	} else {
		fetchDepth := fetchDepthForMaxCommits(p.maxCommits)
		if p.ref != "" {
			fetchDepth = maxCloneDepth
		}
		repo, err := cloneBounded(ctx, cloneOptions{url: p.url, depth: fetchDepth, token: p.token, username: p.username})
		if err != nil {
			return nil, err
		}

		startHash, err := resolveOptionalRef(repo, p.ref)
		if err != nil {
			return nil, err
		}
		resolvedRef = startHash.String()

		flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)

		c, tr, bi, werr := walkPage(repo, []plumbing.Hash{startHash}, map[plumbing.Hash]bool{}, p)
		if werr != nil {
			return nil, werr
		}
		commits, truncated, baseIter = c, tr, bi
	}

	var nextCursor string
	if truncated && len(commits) > 0 {
		newFrontier := baseIter.Frontier()

		// The count-based stop above sets truncated: true the moment
		// max_commits or the message-byte budget is reached, without
		// peeking whether anything genuinely remains beyond it - a
		// pre-existing, harmless imprecision for a single-root walk (see
		// fetchDepthForMaxCommits's own "+1" trick, which mostly avoids
		// it there). It is not harmless here: the frontier redesign can
		// legitimately consume its last pending entry as a duplicate skip
		// (an already-emitted commit reached a second time through
		// reconverging history - see cursor.go's own doc comment) in the
		// same call that also happens to hit max_commits, leaving nothing
		// left to explore even though truncated still says true. Since
		// Frontier() gives a PROVEN answer (not a guess) about what
		// remains, correct the flag whenever it says nothing does -
		// strictly more precise than the count-based signal alone, never
		// less.
		if len(newFrontier) == 0 {
			truncated = false
		} else {
			newEmitted := make([]plumbing.Hash, 0, len(cur.emitted)+len(commits))
			newEmitted = append(newEmitted, cur.emitted...)
			for _, c := range commits {
				newEmitted = append(newEmitted, plumbing.NewHash(c.Sha))
			}
			if len(newFrontier)+len(newEmitted) <= maxCursor {
				nextCursor = encodeCursor(newFrontier, newEmitted)
			}
			// else: resuming further would need a cursor bigger than this
			// task will ever track - truncated stays true (there
			// genuinely is more), but nextCursor stays empty, the same
			// honest "hit a wall, nothing this call can hand back" signal
			// the shallow-boundary case (collectLogCommits, zero commits)
			// already uses. See maxCursorEntries's own doc comment for
			// what a caller does next.
		}
	}

	return &gitv1.LogOutputs{
		Commits:     commits,
		ResolvedRef: resolvedRef,
		Truncated:   truncated,
		NextCursor:  nextCursor,
	}, nil
}

// walkPage builds the multi-root iterator over roots (already-cloned into
// repo), applies p's own path/since filters, and collects up to
// p.maxCommits results - the one walking mechanics both doLogWithBounds
// branches share (a fresh single-root walk, and each attempt of a
// cursor-driven resume's own retry loop). Returns the built
// multiRootCommitIter itself (not merely its Frontier()) because the
// caller may need to call PushBack again after seeing the result (see
// doLogWithBounds's own retry loop, which does not otherwise touch it) and
// because Frontier() itself is computed later, after the caller decides
// whether the result is usable.
func walkPage(repo *git.Repository, roots []plumbing.Hash, emittedSet map[plumbing.Hash]bool, p logParams) ([]*gitv1.Commit, bool, *multiRootCommitIter, error) {
	// since is passed into the iterator itself, not applied by a wrapping
	// object.NewCommitLimitIterFromIter, so the walk can decline to expand
	// a too-old commit's parents rather than merely discard the commit
	// after fully resolving it - see multiRootCommitIter.Next's own
	// comment, and issue #717, for why applying since only as an outer
	// filter lets an octopus merge past maxLogParents fail the walk even
	// when since's own cutoff would have excluded it.
	var since *time.Time
	if !p.since.IsZero() {
		s := p.since
		since = &s
	}
	baseIter := newMultiRootCommitIter(repo, roots, emittedSet, since)
	var iter object.CommitIter = baseIter
	if p.path != "" {
		path := p.path
		iter = newPathFilteringCommitIter(iter, baseIter.PushBack, func(candidate string) bool { return pathMatchesFilter(candidate, path) })
	}
	defer iter.Close()

	commits, truncated, discarded, err := collectLogCommits(repo, iter, p.maxCommits)
	if err != nil {
		return nil, false, nil, classifyGitError(err)
	}
	if discarded != nil {
		// See collectLogCommits's own doc comment on discarded: this
		// commit was fully resolved by baseIter before collectLogCommits
		// refused it (maxCommits or the byte budget already full), so it
		// is not "pending" from baseIter's own perspective any more - push
		// it back so a later Frontier() call reports it anyway.
		baseIter.PushBack(*discarded)
	}
	return commits, truncated, baseIter, nil
}

// frontierReachable reports whether at least one hash in frontier resolves
// to a commit object repo actually has - the progressive-deepening probe:
// a clone at some depth either lets the walk make progress (at least one
// frontier entry reachable - any others are handled by the ordinary
// shallow-boundary path in collectLogCommits, exactly as a single-root
// call's own frontier running out mid-page already is) or it does not,
// in which case doLogWithBounds retries at the next, larger depth.
func frontierReachable(repo *git.Repository, frontier []plumbing.Hash) bool {
	for _, h := range frontier {
		if _, err := repo.CommitObject(h); err == nil {
			return true
		}
	}
	return false
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

// errCommitMetadataTooLarge marks a commit refused for its own metadata, as
// against a page that stopped because a budget ran out.
//
// The distinction decides what may be handed back. A budget refills on the next
// call, so the commit that exhausted one is pushed back, named in next_cursor,
// and collected by the page after - the cursor makes progress. A commit larger
// than a per-commit limit does not become smaller on the next call: a cursor
// naming it would refuse it again forever, and doLogWithBounds would read the
// resulting truncated zero-commit page as a shallow boundary, retry every clone
// depth, and finally report that the cursor is too far behind the tips, which is
// not what happened. So this leaves as an error rather than as a page.
var errCommitMetadataTooLarge = errors.New("commit metadata exceeds what git.log reads")

// collectLogCommits walks iter, keeping at most maxCommits entries and
// stopping early - reporting truncated: true - the moment any budget is
// reached: maxCommits entries collected, maxTotalLogMessageBytes worth of
// message text, or maxTotalLogMetadataBytes worth of identities and parent
// hashes, whichever comes first. A per-commit identity limit is checked
// before that attacker-controlled value is copied into the response, and
// refuses the walk rather than truncating it - see
// [errCommitMetadataTooLarge]. The per-commit parent limit is enforced
// earlier still, in [multiRootCommitIter.Next].
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
// collectLogCommits's own third return value, discarded, is the reason its
// signature grew past the single-root implementation's own: fetchDepth's
// "+1" trick (doLogWithBounds's own comment) works by letting iter yield
// one commit MORE than maxCommits allows, so the moment len(commits) or
// the byte budget is already at its ceiling, THIS callback invocation's
// own commit is refused and iteration stops - proving "there really was
// more" rather than guessing. That refused commit was still fully
// resolved by iter before this callback ever saw it - multiRootCommitIter
// (cursor.go) has already marked it visited and pushed its own children -
// so simply discarding it here would lose it exactly the way a lookup
// failure would (see pathFilteringCommitIter's own preserve callback for
// that half of the same problem): neither returned to the caller nor
// present in Frontier(), since Frontier only reports what is still
// genuinely un-popped. discarded carries that commit's hash back to
// doLogWithBounds, which pushes it back onto the frontier (baseIter's own
// PushBack) the same way a lookup failure already does - proven by
// TestGitLogCursorPagesReachEveryCommitExactlyOnce, which under-counted by
// exactly the commits lost this way before discarded existed.
func collectLogCommits(repo *git.Repository, iter object.CommitIter, maxCommits int) (commits []*gitv1.Commit, truncated bool, discarded *plumbing.Hash, err error) {
	var totalMessageBytes, totalMetadataBytes int
	err = iter.ForEach(func(c *object.Commit) error {
		if len(commits) >= maxCommits {
			truncated = true
			h := c.Hash
			discarded = &h
			return storer.ErrStop
		}

		message, messageBytes := truncateLogMessage(c.Message, maxLogMessageBytes)
		if totalMessageBytes+messageBytes > maxTotalLogMessageBytes {
			// The total message budget, not the per-entry cap or
			// maxCommits, is what ran out here - stopped with the same
			// honest signal: there was more history this call could have
			// described, but describing it would have meant exceeding the
			// bound on how much of it this task will ever serialize in one
			// response.
			truncated = true
			h := c.Hash
			discarded = &h
			return storer.ErrStop
		}

		metadataBytes, metadataErr := logMetadataBytes(c)
		if metadataErr != nil {
			// Not a budget running out, and so not a truncation: this
			// commit is refused for what it is rather than for what is
			// left, and it will be refused identically on the next call.
			// Pushing it back and naming it in next_cursor would hand the
			// caller a cursor whose only effect is to fail again - and
			// worse than merely failing, doLogWithBounds reads the
			// resulting truncated zero-commit page as a shallow boundary,
			// retries every clone depth, and reports that the cursor is
			// too far behind, which is not what happened.
			return metadataErr
		}
		if totalMetadataBytes+metadataBytes > maxTotalLogMetadataBytes {
			// The aggregate metadata budget, which *is* a budget: the next
			// page starts with a fresh one and this commit fits in it, so
			// the cursor below makes progress exactly as the message
			// budget's does.
			truncated = true
			h := c.Hash
			discarded = &h
			return storer.ErrStop
		}
		totalMessageBytes += messageBytes
		totalMetadataBytes += metadataBytes

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
		return commits, truncated, discarded, nil
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
		// maxCommits nor the byte budget was what stopped this call. No
		// discarded here - the commit that hit the boundary already pushed
		// itself back via pathFilteringCommitIter's own preserve callback
		// (or, with no path filter, never left multiRootCommitIter at all -
		// see Next's own doc comment on why a lookup failure there
		// restores the stack before returning).
		return commits, true, nil, nil
	}
	return nil, false, nil, err
}

// logMetadataBytes validates and charges the commit metadata whose size the
// repository controls, before collectLogCommits copies it: the four identity
// strings, and the parent hashes, whose encoded width is fixed but whose count
// is not.
//
// What it deliberately does not charge is the per-commit fields of fixed size -
// the sha, and the timestamps in each signature. Those cannot be made larger by
// a repository, so they are bounded by maxCommits alone and adding them would
// only shift the budget by a constant per entry.
//
// The parent count is charged here and bounded in [multiRootCommitIter.Next],
// which is the last point before the walk expands it - by the time a commit
// reaches this function its parents are already on the iterator's stack, so a
// refusal here would be a bound on the copy rather than on the allocation.
func logMetadataBytes(c *object.Commit) (int, error) {
	identities := [...]string{c.Author.Name, c.Author.Email, c.Committer.Name, c.Committer.Email}
	total := 0
	for _, identity := range identities {
		if len(identity) > maxLogIdentityBytes {
			return 0, fmt.Errorf("%w: commit %s carries a %d byte identity field, and at most %d are read",
				errCommitMetadataTooLarge, c.Hash, len(identity), maxLogIdentityBytes)
		}
		total += len(identity)
	}
	return total + len(c.ParentHashes)*hashTextBytes, nil
}

// hashTextBytes is the width of a hash once [plumbing.Hash.String] has encoded
// it: two hex characters per byte. A constant rather than
// `len(plumbing.ZeroHash.String())`, which builds and throws away a string on
// every commit of every page to learn a number that cannot change.
const hashTextBytes = 2 * len(plumbing.ZeroHash)

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
