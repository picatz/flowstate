package flowtest

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"

	"github.com/goccy/go-yaml/parser"
)

// `testdefaults.yaml` (#1072, slice 3): the values every suite in one
// directory shares, stated once beside them. The merge chain reads the way an
// author expects: directory → file `defaults:` → entry → row, each level
// filling only what the level below did not state — the same one direction
// every merge in this package takes, applied one level further out.
//
// Three properties are load-bearing:
//
//   - The name deliberately does not match the `*.test.yaml` discovery glob,
//     so the file can never be run as a suite.
//   - Exactly the suite's own directory is consulted — no upward walk. A
//     suite's behaviour depends on at most two files, both visible in one
//     `ls`, and "where did this value come from" has two possible answers
//     rather than a path-length's worth.
//   - It is read where a suite is loaded *from a path* ([Load], and so
//     [RunPath], the CLI, and flowtesting.RunFile). A suite born from bytes
//     ([LoadSource]) or built in Go has no directory to consult, and gets
//     none — stated here rather than discovered, since the same rule already
//     governs how those doors resolve a `workflow:` path.

// DirDefaultsName is the file a directory states its shared fixture in.
const DirDefaultsName = "testdefaults.yaml"

// dirDefaults is what that file may hold: vars and defaults, nothing else. A
// `tests:` key here is almost certainly a suite saved under the wrong name,
// and the strict decode refuses it with the field named.
type dirDefaults struct {
	// Edition is accepted and otherwise unused, for [File.Edition]'s exact
	// reason (#203): `flow fix` stamps `edition:` into documents this repo's
	// tooling migrates forward, and a strict decode without the field would
	// refuse the file the moment a migration touched it.
	Edition string `yaml:"edition"`

	Vars     map[string]any `yaml:"vars"`
	Defaults *Defaults      `yaml:"defaults"`

	// path is the file this was read from, kept so a diagnostic about a value
	// the fold below contributed can name the document that holds it rather
	// than the suite that inherited it. Unexported, so the strict decode never
	// sees a key for it, and read only through [contribution.file] — the fold
	// answers "which document" for the whole of its work at once, so nothing
	// downstream has to ask a possibly-nil directory the same question twice.
	path string
	doc  *document
}

// DirDefaultsError reports that the thing that could not be read was a
// directory's [DirDefaultsName], and names which one.
//
// It exists so a caller can ask *where an error came from* instead of reading
// its prose for a filename. The editor's suite diagnostics have to know: a
// refusal originating in the sibling defaults file carries that file's
// positions, so it is anchored at the suite's document start and shown whole
// rather than mapped onto lines it does not describe. Deciding that by looking
// for "testdefaults.yaml" in the message text misfiled two ordinary things as
// defaults errors — a case *named* `testdefaults.yaml`, and any suite under a
// directory whose name contains the string (Codex, #1109).
//
// The rendered message is unchanged — `<path>: <what went wrong>`, the form a
// terminal reader already sees — because the point is to make provenance
// answerable, not to reword anything.
type DirDefaultsError struct {
	// Path is the defaults file, as it would be opened.
	Path string

	// Err is what went wrong with it: unreadable, over a bound, or invalid.
	Err error
}

func (e *DirDefaultsError) Error() string { return e.Path + ": " + e.Err.Error() }

func (e *DirDefaultsError) Unwrap() error { return e.Err }

// loadDirDefaults reads dir's testdefaults.yaml, or reports nil when the
// directory states none. The file is untrusted input exactly as a suite is:
// size-bounded before reading, alias-expansion-bounded before parsing.
func loadDirDefaults(dir string) (*dirDefaults, error) {
	path := filepath.Join(dir, DirDefaultsName)
	data, err := readBounded(path, MaxTestFileBytes, "directory defaults file")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, &DirDefaultsError{Path: path, Err: err}
	}
	return parseDirDefaults(data, path)
}

// parseDirDefaults is the byte seam behind [loadDirDefaults]. Keeping the
// strict decode and expansion checks here lets an editor validate an unsaved
// testdefaults.yaml through the same loader as the suite beside it, without
// writing the live buffer to disk or implementing the defaults grammar again.
func parseDirDefaults(data []byte, path string) (*dirDefaults, error) {
	refuse := func(err error) error { return &DirDefaultsError{Path: path, Err: err} }
	if len(data) > MaxTestFileBytes {
		return nil, refuse(fmt.Errorf("%d bytes exceeds the %d byte limit for a directory defaults file",
			len(data), MaxTestFileBytes))
	}
	parsed, err := parser.ParseBytes(data, 0)
	if err != nil {
		return nil, refuse(err)
	}
	if err := checkExpansionBoundsIn(parsed); err != nil {
		return nil, refuse(err)
	}

	var dd dirDefaults
	// Through the same contained decode the suite's own bytes go through: this
	// file is untrusted for the identical reasons, and a panic here would take
	// the process down over a directory's shared fixture.
	if err := decodeStrict(data, &dd); err != nil {
		return nil, refuse(err)
	}
	dd.path = path
	dd.doc = newDocument(parsed)

	return &dd, nil
}

// A contribution is what one directory fold moved into a suite, in the shapes a
// diagnostic needs to name the document a value was written in.
//
// Two mechanisms, and which one a field gets is decided by whether the fold
// moves its value *unchanged*. `fold_internal_test.go` holds the classification
// for every field either struct has, and a walk that fails when a field exists
// the table does not classify — so the next field added to a `defaults:` block
// is classified before it can quietly take the wrong one.
type contribution struct {
	// file is the document the directory's values were written in, empty when
	// the directory stated none.
	file string
	doc  *document

	// paths are the values the fold moved unchanged, addressable in file at the
	// very path they have here. [problems.wrote] takes these, and a problem at
	// or under one of them is attributed to file and positioned in doc.
	paths []loc

	// ownChecks and ownStubs are how many claims and stubs the *suite's* own
	// `defaults:` block wrote, counted before the fold. After it, index i of
	// `defaults.check` is no longer entry i of the suite's list — the
	// directory's claims are prepended — and any index at or past ownStubs
	// addresses a stub the directory wrote, since its stubs are appended.
	ownChecks int
	ownStubs  int

	// stubSource is the index each appended stub has in the directory's own
	// list, which is the one fact no path can carry: the fold both renumbers
	// them and drops the ones the suite already targets, so entry ownStubs+k of
	// the combined list is neither entry k nor entry ownStubs+k of file.
	stubSource []int
}

// stubWrittenElsewhere answers whether stub i of the combined `defaults.stubs`
// was appended from the directory's file, and the index that file writes it at.
//
// A function rather than an expression at the two report sites, because the
// suite's own stubs and the directory's agree about index i whenever the suite
// wrote none — so a check written inline against real data is one no fixture can
// drive (CLAUDE.md, "assert where the answers differ").
func (c contribution) stubWrittenElsewhere(i int) (int, bool) {
	if c.file == "" || i < c.ownStubs {
		return i, false
	}
	if k := i - c.ownStubs; k < len(c.stubSource) {
		return c.stubSource[k], true
	}

	return i, false
}

// combineInto folds the directory's contribution into a parsed suite, before
// vars resolve and before anything validates — so a directory-stated
// `${vars.x}` resolves against the combined vars exactly once, and every
// check below sees what the suite effectively declares.
//
// One direction, per field: the file wins where both speak. Checks are the
// accumulating exception, directory first, so a failure lists claims in the
// order a reader meets the files in.
//
// It answers with every path it contributed, because after this fold the
// combined value no longer says which file each part of it came from — and a
// diagnostic about a directory-stated value that named the *suite* would send a
// reader to a file that does not contain the text being refused (Codex, #1179).
// The loader hands these to [problems], which attributes a problem at or under
// one of them to [DirDefaultsName] instead, and declines to look for a position
// it could only find by accident.
//
// A path answers "which document wrote this" only where the fold moves the value
// unchanged. Where it **renumbers** — `check:` prepends, `stubs:` appends — no
// path is recorded at all, because one string would name an entry in each
// document at once; the check that knows is told the file and the source index
// instead. See [contribution].
//
// A collection the suite states *no part of* is recorded as the collection
// itself rather than entry by entry, because a refusal about the collection —
// a bound on how many entries it may hold — reports at the collection's own
// path, which is an ancestor of every leaf and therefore matches none of them
// (Codex, #1185). That is this function's existing rule for a `defaults:` block
// the suite states nothing of, applied one level down. Where both files write
// into a collection, its entries are recorded one by one and its *size* stays
// the suite's: the overrun is a joint property, and the suite is the document
// that can stop inheriting.
func (dd *dirDefaults) combineInto(file *File) contribution {
	// Counted here rather than by the caller, so the count and the fold that
	// invalidates it cannot come to be done in two places that disagree.
	moved := contribution{}
	if file.Defaults != nil {
		moved.ownChecks = len(file.Defaults.Check)
		moved.ownStubs = len(file.Defaults.Stubs)
	}
	if dd == nil {
		return moved
	}
	moved.file = dd.path
	moved.doc = dd.doc

	var contributed []loc
	if len(dd.Vars) > 0 {
		combined := make(map[string]any, len(dd.Vars)+len(file.Vars))
		maps.Copy(combined, dd.Vars)
		maps.Copy(combined, file.Vars)
		if len(file.Vars) == 0 {
			contributed = append(contributed, at("vars"))
		} else {
			for _, name := range slices.Sorted(maps.Keys(dd.Vars)) {
				if _, stated := file.Vars[name]; !stated {
					contributed = append(contributed, at("vars").field(name))
				}
			}
		}
		file.Vars = combined
	}

	outer := dd.Defaults
	if outer == nil {
		moved.paths = contributed

		return moved
	}

	base := at("defaults")
	if file.Defaults == nil {
		// The suite states no defaults of its own: the directory's are its
		// defaults, copied so two suites sharing the file cannot append into
		// each other's slices through the shared struct. One path covers the
		// whole block, since every part of it came from the directory.
		copied := *outer
		copied.Stubs = append([]Stub(nil), outer.Stubs...)
		copied.Check = append([]CheckClaim(nil), outer.Check...)
		file.Defaults = &copied

		// Every stub here is the directory's, at the index it has there —
		// recorded even though it is the identity, so the one rule that decides
		// a stub's provenance is the same rule in both directions.
		moved.stubSource = make([]int, len(copied.Stubs))
		for i := range moved.stubSource {
			moved.stubSource[i] = i
		}
		moved.paths = append(contributed, base)

		return moved
	}

	inner := file.Defaults
	if inner.Workflow == "" {
		if outer.Workflow != "" {
			contributed = append(contributed, base.field("workflow"))
		}
		inner.Workflow = outer.Workflow
	}
	if len(outer.Inputs) > 0 {
		combined := make(map[string]any, len(outer.Inputs)+len(inner.Inputs))
		maps.Copy(combined, outer.Inputs)
		maps.Copy(combined, inner.Inputs)
		if len(inner.Inputs) == 0 {
			contributed = append(contributed, base.field("inputs"))
		} else {
			for _, name := range slices.Sorted(maps.Keys(outer.Inputs)) {
				if _, stated := inner.Inputs[name]; !stated {
					contributed = append(contributed, base.field("inputs").field(name))
				}
			}
		}
		inner.Inputs = combined
	}
	if len(outer.Stubs) > 0 {
		// The file's stub for a target replaces the directory's — the
		// identity rule [mergeDefaults] already applies one level down.
		wholly := len(inner.Stubs) == 0
		taken := make(map[string]bool, len(inner.Stubs))
		for i := range inner.Stubs {
			taken[stubTargetKey(&inner.Stubs[i])] = true
		}
		combined := append([]Stub(nil), inner.Stubs...)
		for i := range outer.Stubs {
			if !taken[stubTargetKey(&outer.Stubs[i])] {
				// No `defaults.stubs[i]` path is recorded for these,
				// deliberately, and for the reason no `defaults.check[i]` one
				// is: the fold *appends* them, so the index they land on is
				// the suite's numbering rather than the directory's, and a
				// diagnostic attributed by that path sent a reader to an entry
				// the named file does not have (Codex, #1185). The index the
				// directory writes it at travels instead, to the check that
				// reports it.
				moved.stubSource = append(moved.stubSource, i)
				combined = append(combined, outer.Stubs[i])
			}
		}
		if wholly {
			// The collection itself, for the refusal that is about the
			// collection: its bound. Sound because the suite states no part of
			// it, so no entry under this path is the suite's.
			contributed = append(contributed, base.field("stubs"))
		}
		inner.Stubs = combined
	}
	if inner.Sender == nil {
		if outer.Sender != nil {
			contributed = append(contributed, base.field("sender"))
		}
		inner.Sender = outer.Sender
	}
	if len(outer.Check) > 0 {
		// No path is recorded for these, deliberately. They are *prepended*,
		// so `defaults.check[0]` names the directory's first claim and the
		// suite's first claim at once, and a set keyed on that string cannot
		// tell them apart — it sent a suite-written claim to the wrong file and
		// dropped its real position (Codex, #1185). [checkCheckClaims] is told
		// which document the inherited ones came from instead, which is a fact
		// it has and a string cannot carry.
		inner.Check = append(append([]CheckClaim(nil), outer.Check...), inner.Check...)
	}
	moved.paths = contributed

	return moved
}
