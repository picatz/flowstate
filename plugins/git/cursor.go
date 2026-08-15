package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
)

// cursorState is a decoded LogInputs.cursor: the exact information a
// resumed git.log call needs to continue a truncated walk correctly,
// against any commit graph shape - not just a linear chain.
//
// # Why the frontier is not just the last commit's parents
//
// The first version of this cursor carried a single sha - the last commit
// a truncated call returned - and resumed by walking from that commit's
// PARENT (parents[0] only). That is correct for a linear chain, but wrong
// the moment a page boundary lands on or after a merge: a merge commit's
// history fans out into every one of its parents, and "parents[0]"
// (first-parent only) silently drops everything reachable only through
// parents[1] and beyond - commits this task would then never return, ever.
// That is a MISS, not a duplicate, which is exactly the direction a purely
// linear test fixture cannot expose (see log_test.go's own merge-topology
// test).
//
// Frontier fixes this by tracking every not-yet-explored commit a resumed
// walk still owes an answer for - every parent of an already-returned
// commit that is not itself already returned - not merely the most recent
// entry's own parents. A merge with N parents puts all N in the frontier
// when the walk reaches it, and every one of them gets explored on
// resumption, via multiRootCommitIter below.
//
// # Why frontier alone is not enough either
//
// A frontier without also tracking what has already been emitted still
// duplicates on a RECONVERGING history - the ordinary shape two branches
// merged together have, sharing history further back (a common ancestor
// both branches descend from). Once that shared ancestor is reachable from
// more than one still-pending frontier entry, a fresh walk that only knows
// "start exploring from here" has no way to know one of those paths
// already reached it in an earlier page, and would return it a second
// time.
//
// Emitted is the accumulated set of every commit this walk has already
// returned, across every earlier page - not merely this page's own
// entries. A commit reached again through a second path is looked up in
// emitted, found already returned, and skipped without being returned
// again AND without its own parents being pushed onto the frontier a
// second time (see multiRootCommitIter.Next) - nothing is lost by not
// re-exploring it, because its own children were already pushed onto some
// earlier page's frontier the first time it was visited.
//
// Both lists are ordinary full commit shas - nothing this task cannot
// already vouch for, and nothing a caller could compose by hand and have
// accepted: decodeCursor's own structural check (and validateCursor, which
// calls it before this ever reaches doLog) refuses anything that does not
// parse into exactly this shape.
type cursorState struct {
	frontier []plumbing.Hash
	emitted  []plumbing.Hash // in the order first added; also the encode order
}

// emittedSet returns state's own emitted list as a lookup set - built once
// per decoded cursor, not once per commit examined during the walk.
func (state cursorState) emittedSet() map[plumbing.Hash]bool {
	set := make(map[plumbing.Hash]bool, len(state.emitted))
	for _, h := range state.emitted {
		set[h] = true
	}
	return set
}

// encodeCursor packs frontier and emitted into the string
// LogOutputs.next_cursor carries: two comma-joined hash lists separated by
// "|". Every hash is rendered through plumbing.Hash.String(), which always
// produces 40 lowercase hex characters, so anything this function writes
// is exactly the shape decodeCursor (and validateCursor) accepts back.
func encodeCursor(frontier, emitted []plumbing.Hash) string {
	return joinHashes(frontier) + "|" + joinHashes(emitted)
}

func joinHashes(hs []plumbing.Hash) string {
	parts := make([]string, len(hs))
	for i, h := range hs {
		parts[i] = h.String()
	}
	return strings.Join(parts, ",")
}

// decodeCursor is encodeCursor's inverse, and the one place that decides
// whether a string is shaped like a value this task could ever have
// emitted - checked structurally (exactly two "|"-separated sections, each
// a non-empty comma-separated list of full lowercase hex shas) before a
// single byte of it is trusted for anything else. validateCursor
// (validate.go) is the production entry point; doLog calls decodeCursor
// again once validation has already passed - a cheap, auditable
// re-derivation rather than threading a decoded value through logParams,
// the same double-parse shape this plugin's other validators accept
// elsewhere for the same reason.
func decodeCursor(raw string) (cursorState, error) {
	sections := strings.Split(raw, "|")
	if len(sections) != 2 {
		return cursorState{}, fmt.Errorf("expected 2 \"|\"-separated sections (frontier, emitted), got %d", len(sections))
	}
	frontier, err := parseHashList(sections[0])
	if err != nil {
		return cursorState{}, fmt.Errorf("frontier section: %w", err)
	}
	if len(frontier) == 0 {
		return cursorState{}, fmt.Errorf("frontier section is empty - this task never emits a cursor with nothing left to resume")
	}
	emitted, err := parseHashList(sections[1])
	if err != nil {
		return cursorState{}, fmt.Errorf("emitted section: %w", err)
	}
	if len(emitted) == 0 {
		return cursorState{}, fmt.Errorf("emitted section is empty - this task never emits a cursor before returning at least one commit")
	}
	return cursorState{frontier: frontier, emitted: emitted}, nil
}

// parseHashList splits s on "," and requires every piece to be a full
// 40-character lowercase hex commit sha - the same shape the single-sha
// cursor's own validateCursor required, applied to each element of a list
// instead of one bare value.
func parseHashList(s string) ([]plumbing.Hash, error) {
	parts := strings.Split(s, ",")
	out := make([]plumbing.Hash, 0, len(parts))
	for _, p := range parts {
		if len(p) != fullShaHexLen {
			return nil, fmt.Errorf("%q is %d characters, want exactly %d", p, len(p), fullShaHexLen)
		}
		for _, r := range p {
			if !isLowerHexDigit(r) {
				return nil, fmt.Errorf("%q is not lowercase hex", p)
			}
		}
		out = append(out, plumbing.NewHash(p))
	}
	return out, nil
}

// multiRootCommitIter walks a DFS preorder over every hash in roots,
// exploring each one's parents in turn - the frontier-resume primitive
// cursorState exists to carry.
//
// Deliberately not built on object.NewCommitPreorderIter's own "ignore"
// parameter: ignore treats a hash as already fully explored (skipped
// outright, AND its own children never pushed), which is exactly wrong for
// an already-emitted commit resuming into a brand-new iterator instance
// that never itself visited it in this call. This type visits every root
// fresh, pushing its children normally, and only skips a hash once it
// discovers it is already in emitted, without re-exploring past it - see
// cursorState's own doc comment, "why frontier alone is not enough
// either," for why that skip-without-re-explore is still correct: an
// emitted commit's own children were already pushed during whichever
// earlier page first visited it, so nothing is lost by not pushing them
// again from a second, later-discovered path to the same commit.
type multiRootCommitIter struct {
	repo    *git.Repository
	emitted map[plumbing.Hash]bool // already returned in an earlier page
	seen    map[plumbing.Hash]bool // visited already during this call
	stack   []plumbing.Hash        // DFS stack; last element pops next
}

// newMultiRootCommitIter builds the iterator, pushing roots so the FIRST
// entry of roots is the FIRST one explored - a plain slice-as-stack pops
// its last element first, so roots are pushed in reverse order.
func newMultiRootCommitIter(repo *git.Repository, roots []plumbing.Hash, emitted map[plumbing.Hash]bool) *multiRootCommitIter {
	stack := make([]plumbing.Hash, 0, len(roots))
	for i := len(roots) - 1; i >= 0; i-- {
		stack = append(stack, roots[i])
	}
	return &multiRootCommitIter{
		repo:    repo,
		emitted: emitted,
		seen:    make(map[plumbing.Hash]bool),
		stack:   stack,
	}
}

// Next returns the next not-yet-emitted, not-yet-seen-this-call commit,
// or io.EOF once nothing remains reachable from roots that emitted did not
// already cover.
//
// On a lookup failure (repo.CommitObject returning an error - the shape a
// shallow clone's own fetch boundary takes, see collectLogCommits's own
// doc comment on plumbing.ErrObjectNotFound), the hash that failed is
// pushed back onto the stack before the error is returned, rather than
// left popped-and-discarded - so Frontier(), called after this error by a
// caller that converts it into an honest truncated: true (as
// collectLogCommits does), still reports that hash as part of what remains
// to resume. Losing it here would mean a commit this shallow window
// could not reach becomes permanently unreachable by any future,
// deeper-fetched resume - exactly the kind of silent miss this whole
// redesign exists to close.
func (it *multiRootCommitIter) Next() (*object.Commit, error) {
	for {
		if len(it.stack) == 0 {
			return nil, io.EOF
		}
		h := it.stack[len(it.stack)-1]
		it.stack = it.stack[:len(it.stack)-1]

		if it.emitted[h] || it.seen[h] {
			continue
		}

		c, err := it.repo.CommitObject(h)
		if err != nil {
			it.stack = append(it.stack, h) // preserve for a future, deeper resume
			return nil, err
		}
		it.seen[h] = true

		// The parent bound is enforced here, before the list below expands
		// it, because expanding it *is* the allocation the bound exists to
		// refuse: every parent is appended to it.stack, and a later
		// Frontier() copies the whole stack again. Checked downstream, in
		// collectLogCommits, it reads a list the walk has already paid for
		// - and with a path filter in front it never reads it at all,
		// since a commit the filter discards is one collectLogCommits
		// never sees while its parents went onto the stack regardless.
		//
		// Refused rather than truncated, for the reason
		// [errCommitMetadataTooLarge] gives: this is a property of the
		// history, identical on every retry, so no resumable page could
		// hand back a cursor that makes progress past it.
		if len(c.ParentHashes) > maxLogParents {
			return nil, fmt.Errorf("%w: commit %s has %d parents, and at most %d are read",
				errCommitMetadataTooLarge, h, len(c.ParentHashes), maxLogParents)
		}

		// Parents pushed in reverse so the first parent is popped (and
		// thus visited) first - matching the same "first parent explored
		// before second" convention object.NewCommitPreorderIter's own
		// filteredParentIter uses, so a linear (no-merge) walk's ordering
		// is unchanged from before this type existed.
		for i := len(c.ParentHashes) - 1; i >= 0; i-- {
			p := c.ParentHashes[i]
			if !it.emitted[p] && !it.seen[p] {
				it.stack = append(it.stack, p)
			}
		}

		return c, nil
	}
}

func (it *multiRootCommitIter) ForEach(cb func(*object.Commit) error) error {
	for {
		c, err := it.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := cb(c); err != nil {
			if err == storer.ErrStop {
				return nil
			}
			return err
		}
	}
}

func (it *multiRootCommitIter) Close() {}

// Frontier returns whatever remains on the stack once collection stops -
// the exact set doLog needs to build the next cursor's own frontier
// section, in "will be explored first" order (the inverse of the reversal
// newMultiRootCommitIter applies on construction, so a later
// newMultiRootCommitIter call fed this slice reproduces the identical
// continuation).
func (it *multiRootCommitIter) Frontier() []plumbing.Hash {
	out := make([]plumbing.Hash, len(it.stack))
	for i := range it.stack {
		out[i] = it.stack[len(it.stack)-1-i]
	}
	return out
}

// PushBack re-adds h to the top of the stack, to be popped (and thus
// explored again) first on any further Next() call - undoing, for
// Frontier's purposes, a hash this iterator itself already successfully
// resolved (its own children are already correctly sitting on the stack)
// but whose disposition a WRAPPING iterator (pathFilteringCommitIter)
// could not finish deciding, because a shallow clone did not reach far
// enough to check it.
//
// Without this, that commit would be lost entirely: this iterator already
// marked it visited and pushed its children when it first resolved h, so
// it is not "returned" (never reached the caller, since the wrapping
// iterator errored before handing it back) and not "pending" (Frontier()
// alone would not include it, since it is no longer on the stack) - the
// exact silent-miss shape this whole cursor redesign exists to close. A
// caller that pushes h back and later re-derives Frontier() gets h back
// in the result, ready to be resolved and explored again by a future,
// possibly deeper resume.
func (it *multiRootCommitIter) PushBack(h plumbing.Hash) {
	it.stack = append(it.stack, h)
}

// pathFilteringCommitIter wraps source, returning only commits that
// touched path - determined by diffing each commit's own tree against its
// own actual parents' trees directly (commitTouchesPath), never by
// comparing a commit against whatever the wrapped iterator happens to
// return next the way go-git's own object.NewCommitPathIterFromIter does.
//
// That distinction is not cosmetic once source can be a
// multiRootCommitIter. object.NewCommitPathIterFromIter diffs a commit
// against whatever commit its own source iterator returns immediately
// after it - correct only when that really is the commit's own parent,
// which a strictly linear, single-root DFS always happens to produce
// (checkParent: true exists to catch the case where it is not, but this
// task has never set it). A multi-root frontier walk can - and does, the
// moment a page boundary sits where two frontier entries interleave -
// return two genealogically unrelated commits back to back, which would
// make that comparison diff two unrelated trees, silently producing a
// wrong match or a wrong non-match. Worse: to make that comparison at
// all, object.NewCommitPathIterFromIter buffers exactly one extra,
// already-consumed commit inside its own private state while it decides -
// invisible to multiRootCommitIter.Frontier() once collection stops, so
// that buffered commit is silently dropped: neither returned to the
// caller nor reported as part of what remains to resume. Diffing a
// commit against its own parents, looked up directly, needs no lookahead
// into source at all, so nothing this type does can ever be invisible to
// Frontier() when collection stops.
//
// TestGitLogCursorPagesReachEveryCommitExactlyOnce failed with exactly
// this loss (a short union, commits missing with no error) when this type
// was still object.NewCommitPathIterFromIter, even though that test's own
// fixture is a plain linear chain with no merge in it at all - the bug
// was in the buffering, not in anything specific to branching history.
//
// A second, distinct loss lives here even with the buffering gone:
// commitTouchesPath needs a commit's own PARENT to decide whether that
// commit touched path, and a parent sitting right at a shallow clone's own
// fetch boundary is exactly the kind of object that is not there. By the
// time that lookup fails, source (multiRootCommitIter) has already fully
// resolved the commit being checked - its own children are already
// sitting on source's stack - so it is not "pending" either; without
// preserve, it would be lost the same way, just one layer further along.
// preserve is source's own PushBack, so that commit goes back onto the
// frontier a future, possibly deeper resume can pick it up and finish
// deciding.
type pathFilteringCommitIter struct {
	source   object.CommitIter
	preserve func(plumbing.Hash) // multiRootCommitIter.PushBack, or nil
	matches  func(string) bool
}

func newPathFilteringCommitIter(source object.CommitIter, preserve func(plumbing.Hash), matches func(string) bool) *pathFilteringCommitIter {
	return &pathFilteringCommitIter{source: source, preserve: preserve, matches: matches}
}

func (it *pathFilteringCommitIter) Next() (*object.Commit, error) {
	for {
		c, err := it.source.Next()
		if err != nil {
			return nil, err
		}
		touched, err := commitTouchesPath(c, it.matches)
		if err != nil {
			if it.preserve != nil {
				it.preserve(c.Hash)
			}
			return nil, err
		}
		if touched {
			return c, nil
		}
	}
}

func (it *pathFilteringCommitIter) ForEach(cb func(*object.Commit) error) error {
	for {
		c, err := it.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := cb(c); err != nil {
			if err == storer.ErrStop {
				return nil
			}
			return err
		}
	}
}

func (it *pathFilteringCommitIter) Close() { it.source.Close() }

// commitTouchesPath reports whether c is the commit that changed something
// matches accepts, diffing c's own tree against each of its own parents'
// trees directly - a root commit (no parents) is diffed against an empty
// tree (object.DiffTree accepts nil for this, the same convention go-git's
// own commit_walker_path.go uses for the final commit of an ordinary
// traversal), and a merge commit touches path if the diff against ANY one
// of its parents shows a change there, matching how a plain (not
// first-parent-only) path filter treats a merge.
func commitTouchesPath(c *object.Commit, matches func(string) bool) (bool, error) {
	currentTree, err := c.Tree()
	if err != nil {
		return false, err
	}
	if c.NumParents() == 0 {
		return treeTouchesPath(nil, currentTree, matches)
	}
	for i := 0; i < c.NumParents(); i++ {
		parent, err := c.Parent(i)
		if err != nil {
			return false, err
		}
		parentTree, err := parent.Tree()
		if err != nil {
			return false, err
		}
		touched, err := treeTouchesPath(parentTree, currentTree, matches)
		if err != nil {
			return false, err
		}
		if touched {
			return true, nil
		}
	}
	return false, nil
}

func treeTouchesPath(from, to *object.Tree, matches func(string) bool) (bool, error) {
	changes, err := object.DiffTree(from, to)
	if err != nil {
		return false, err
	}
	for _, change := range changes {
		if matches(changeName(change)) {
			return true, nil
		}
	}
	return false, nil
}

// changeName is object.Change's own private name() method, reimplemented
// here from its exported fields (From.Name, To.Name) since this package
// cannot call an unexported method on a type it does not define.
func changeName(c *object.Change) string {
	if c.From.Name != "" {
		return c.From.Name
	}
	return c.To.Name
}
