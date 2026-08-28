package flowtest

import (
	"errors"
	"io/fs"
	"maps"
	"path/filepath"

	"github.com/goccy/go-yaml"
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
	// sees a key for it.
	path string
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

// sourcePath is the file a directory's defaults were read from, or empty when
// the directory stated none. Nil-safe, because a suite with no sibling file
// asks the same question.
func (dd *dirDefaults) sourcePath() string {
	if dd == nil {
		return ""
	}

	return dd.path
}

func (e *DirDefaultsError) Error() string { return e.Path + ": " + e.Err.Error() }

func (e *DirDefaultsError) Unwrap() error { return e.Err }

// loadDirDefaults reads dir's testdefaults.yaml, or reports nil when the
// directory states none. The file is untrusted input exactly as a suite is:
// size-bounded before reading, alias-expansion-bounded before parsing.
func loadDirDefaults(dir string) (*dirDefaults, error) {
	path := filepath.Join(dir, DirDefaultsName)
	refuse := func(err error) error { return &DirDefaultsError{Path: path, Err: err} }

	data, err := readBounded(path, MaxTestFileBytes, "directory defaults file")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, refuse(err)
	}
	if err := checkExpansionBounds(data); err != nil {
		return nil, refuse(err)
	}

	var dd dirDefaults
	if err := yaml.UnmarshalWithOptions(data, &dd, yaml.Strict()); err != nil {
		return nil, refuse(err)
	}
	dd.path = path

	return &dd, nil
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
func (dd *dirDefaults) combineInto(file *File) []loc {
	if dd == nil {
		return nil
	}

	var contributed []loc
	if len(dd.Vars) > 0 {
		combined := make(map[string]any, len(dd.Vars)+len(file.Vars))
		maps.Copy(combined, dd.Vars)
		maps.Copy(combined, file.Vars)
		for name := range dd.Vars {
			if _, stated := file.Vars[name]; !stated {
				contributed = append(contributed, at("vars").field(name))
			}
		}
		file.Vars = combined
	}

	outer := dd.Defaults
	if outer == nil {
		return contributed
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

		return append(contributed, base)
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
		for name := range outer.Inputs {
			if _, stated := inner.Inputs[name]; !stated {
				contributed = append(contributed, base.field("inputs").field(name))
			}
		}
		inner.Inputs = combined
	}
	if len(outer.Stubs) > 0 {
		// The file's stub for a target replaces the directory's — the
		// identity rule [mergeDefaults] already applies one level down.
		taken := make(map[string]bool, len(inner.Stubs))
		for i := range inner.Stubs {
			taken[stubTargetKey(&inner.Stubs[i])] = true
		}
		combined := append([]Stub(nil), inner.Stubs...)
		for i := range outer.Stubs {
			if !taken[stubTargetKey(&outer.Stubs[i])] {
				// Appended, so the directory's stubs occupy the indices past
				// the ones this suite wrote.
				contributed = append(contributed, base.field("stubs").item(len(combined)))
				combined = append(combined, outer.Stubs[i])
			}
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
		// Prepended, so the directory's claims occupy the front of the
		// combined list — which is the index [checkCheckClaims] addresses an
		// inherited claim by.
		for i := range outer.Check {
			contributed = append(contributed, base.field("check").item(i))
		}
		inner.Check = append(append([]CheckClaim(nil), outer.Check...), inner.Check...)
	}

	return contributed
}
