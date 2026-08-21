package main

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The tool that caused the staleness is the tool that should report it.
//
// `flow fix` leaves a `digest:` pin exactly as it found it, which is pinned by
// byte-for-byte tests (#639). What it cannot leave alone is the *callee*: a
// directory-wide run that legitimately rewrites one file — an edition migration,
// say — changes that file's bytes, and every pin naming those bytes is stale the
// moment it is written. Nothing about that is silent corruption, because the next
// command to compile the caller refuses it. It is a diagnostics gap: the author
// learns about it at one remove, from a command they ran afterwards, rather than
// from the run that did it (#640).
//
// So a run that rewrote anything reads the pins of every file it was given and
// says which of them its own rewrite invalidated.
//
// # Reported, not re-stamped
//
// The tempting other half is for `flow fix` to write the new digest itself, and
// it is the wrong half. A pin is the caller saying *I have read these bytes*.
// Re-stamping it would authorize bytes nobody looked at, in the course of running
// a migration — a rewriter turning a security check off while doing something
// else, which is precisely the shape #339 and #639 exist to prevent. What the
// report does instead is name the digest to adopt, so adopting one is a paste
// once a human has read the diff, and never a side effect of a migration.
//
// # What it can and cannot see
//
// The scope is the files the invocation named. A caller outside them, pinning a
// callee inside them, is not read — `flow fix one/dir` has not been told about
// the rest of the tree, and walking out to find callers would be a command going
// looking for files it was not pointed at. The message says which file was
// rewritten, so a reader with pins elsewhere knows what to grep for.
//
// It reports only staleness *this run caused*: the pin has to have named the
// callee's bytes as they were before the rewrite. A pin that was already stale
// when the run started is somebody else's news, and repeating it here would
// attach every long-broken pin in a tree to whoever next ran `flow fix`.

// A stalePin is one `digest:` this run invalidated.
type stalePin struct {
	// caller is the file holding the pin, as it was named on the command line.
	caller string

	// pin is what the caller wrote, read back from its own source.
	pin flowfile.CallPin

	// now is the digest the callee has after the rewrite: the value to adopt.
	now string

	// unreadable is set instead of pin and now when the caller's own pins could
	// not be read at all, which is not the same as knowing it has none. See
	// [findStalePins].
	unreadable error
}

// diagnostic renders one stale pin the way every other line `flow fix` prints is
// rendered — positioned at the pin, and in the tense of what actually happened,
// since `--check` writes nothing and so invalidates nothing yet.
func (s stalePin) diagnostic(applied bool) flowfile.Diagnostic {
	rewrote, longer := "rewrote", "no longer"
	if !applied {
		rewrote, longer = "would rewrite", "would no longer"
	}

	if s.unreadable != nil {
		return flowfile.Diagnostic{
			Message: fmt.Sprintf(
				"this run %s another file in this tree, and this one could not be read to check "+
					"the `digest:` pins it holds against it: %s; a file whose pins cannot be read "+
					"is not a file known to hold none",
				rewrote, s.unreadable),
		}
	}

	where := "the `digest:` pin here"
	if s.pin.Step != "" {
		where = fmt.Sprintf("the `digest:` pin on step %q", s.pin.Step)
	}

	return flowfile.Diagnostic{
		Line:   s.pin.Line,
		Column: s.pin.Column,
		Message: fmt.Sprintf(
			"this run %s %s, so %s %s names the bytes it pins; read what the rewrite did to that "+
				"file and then write `digest: %s` to adopt it — `flow fix` reports a pin it "+
				"invalidated rather than re-stamping one, because a pin is the caller saying it "+
				"read those bytes",
			rewrote, s.pin.Call, where, longer, s.now),
	}
}

// findStalePins reports every pin among these files that this run's own rewrites
// invalidated, in the order the files were given and by position within a file.
//
// outcomes is keyed by the path each file was given as, and carries the bytes
// before and after for the ones that changed. A file that did not change is still
// read for its pins: it is a caller like any other, and the callee it pins may be
// one of the files that did.
//
// A caller whose pins could not be read at all is reported too, rather than
// passed over. [flowfile.CallPins] fails on a document that is not YAML or one
// that expands past what a Flowfile may hold, and neither answer is "this file
// holds no pins" — the same fail-closed reading the formatter applies when it
// cannot see a pin it might otherwise drop. Reporting it costs a line in the one
// case where a rewrite happened *and* a sibling file cannot be read, and saying
// nothing there would be this scan quietly not doing the job it exists for.
func findStalePins(files []string, outcomes map[string]fixOutcome) []stalePin {
	rewritten := map[string]fixOutcome{}
	for path, outcome := range outcomes {
		if outcome.changed {
			rewritten[canonicalPath(path)] = outcome
		}
	}
	if len(rewritten) == 0 {
		// Nothing was rewritten, so nothing this run did could have invalidated
		// anything. Checked before any file is read, so the overwhelmingly
		// common `flow fix` over a current tree costs nothing at all.
		return nil
	}

	var out []stalePin
	for _, caller := range files {
		outcome, ok := outcomes[caller]
		if !ok {
			continue
		}
		// The bytes this run produces for the file, which is where its pins are
		// read from — [flowfile.Fix] carries a pin across verbatim, so these are
		// the same pins the file had before, read from the document the run
		// arrived at rather than from the one it started with.
		pins, err := flowfile.CallPins(outcome.after)
		if err != nil {
			out = append(out, stalePin{caller: caller, unreadable: err})
			continue
		}
		for _, pin := range pins {
			target := flowfile.ResolveCallTarget(caller, pin.Call)
			if target.Refusal != flowfile.CallTargetResolved {
				// Not a path this caller may read, which is the compiler's
				// refusal to report and not this scan's.
				continue
			}
			callee, ok := rewritten[canonicalPath(target.Path)]
			if !ok {
				continue
			}

			// Case-insensitively, the way the compiler compares a pin: hex has
			// no case, and a pin copied out of a tool that renders upper-case
			// names the same bytes.
			written := strings.ToLower(pin.Digest)
			now := v1.ContentDigest(callee.after)
			if written == now {
				// The rewrite happened to leave the pin naming the bytes it
				// names, which cannot happen for a real content change and can
				// for a caller pinning a file this run only reformatted back to
				// what it was.
				continue
			}
			if written != v1.ContentDigest(callee.before) {
				// Already stale before this run touched anything: somebody
				// else's news. See this file's header.
				continue
			}

			out = append(out, stalePin{caller: caller, pin: pin, now: now})
		}
	}

	slices.SortStableFunc(out, func(a, b stalePin) int {
		if a.pin.Line != b.pin.Line {
			return a.pin.Line - b.pin.Line
		}
		return a.pin.Column - b.pin.Column
	})
	return out
}

// canonicalPath names a file the one way two paths to the same file compare
// equal: absolute, with every symlink resolved.
//
// Best-effort in both steps, because a path that cannot be resolved is still a
// key — it simply only matches itself, which is the same answer a scan over
// unrelated files would have given anyway.
// [flowfile.ResolveCallTarget] resolves a callee's path the same way, so the two
// sides of the comparison are built by one rule.
func canonicalPath(path string) string {
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real
	}
	return path
}
