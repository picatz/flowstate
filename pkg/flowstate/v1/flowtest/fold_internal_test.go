package flowtest

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The classification every field of a directory fold is held to, and the walk
// that makes a written-out list trustworthy rather than a thing somebody
// remembered to update.
//
// Three review rounds on #1185 found the same defect three times, once per field
// whose provenance nobody had classified: a `defaults:` sender judged at a path
// no document holds, a wholly inherited collection recorded leaf by leaf under a
// bound that reports at the collection, and two documents sharing one index
// namespace. Each was fixed on its own. This is the part that closes the family:
// the next field added to a `defaults:` block cannot pass without saying which
// class it is in, because the walk below fails on a field the table does not
// name.
//
// Same mechanism as `tools/fuzztargets` and `internal/conformance`'s
// `corpora_test.go` — a hand-written table, a walk over the real thing, and a
// test that fails when the two disagree — for the same reason: Go cannot
// enumerate this at run time from anything but the type, and a list nothing
// checks is a list that rots.

// A foldClass is how a diagnostic about a value the directory fold moved finds
// the document that wrote it.
type foldClass string

const (
	// movedUnchanged: the fold puts the value at the very path it has in the
	// directory's file, so [dirDefaults.combineInto] records that path and
	// [problems.writtenElsewhere] is a sound lookup.
	movedUnchanged foldClass = "moved unchanged"

	// renumbered: the fold changes the index an entry sits at — `check:`
	// prepends, `stubs:` appends — so one path string names an entry in each
	// document at once. No path is recorded; the check that knows names the
	// document through [site.file] and counts from *its* list.
	renumbered foldClass = "renumbered"

	// perField: not a value but a block the fold descends into, classified one
	// field at a time below. Recorded as a single path only when the suite
	// states no block at all, which is when every part of it came from the
	// directory.
	perField foldClass = "folded field by field"

	// notFolded: the fold does not move it, and the reason is written down.
	notFolded foldClass = "not folded"
)

// A foldedField is one field the fold could move, its class, and the fixtures
// that drive the fold rather than describe it.
type foldedField struct {
	class foldClass

	// why is required, for the reason `//lint:ignore` and `//vacuity:ignore`
	// require one: a classification nobody was willing to write a sentence for
	// is a classification nobody checked.
	why string

	// sibling states this field in the directory's file, and suite states the
	// same field in the loaded suite. Both nil exactly for [notFolded].
	sibling func(*dirDefaults)
	suite   func(*File)

	// alone is every path the fold must record when only the directory states
	// the field, and shared every path when both do. Written out rather than
	// derived: the table is a second opinion about what the fold does, and a
	// second opinion computed from the first is not one.
	alone  []loc
	shared []loc
}

// foldClassification is every field either side of the fold has. A field
// missing here fails the walk below; a name here that is no longer a field
// fails it too.
var foldClassification = map[string]foldedField{
	"dirDefaults.edition": {
		class: notFolded,
		why: "accepted so a `flow fix` stamp cannot make the strict decode refuse the file, " +
			"and read by nothing — there is no value to attribute",
	},
	"dirDefaults.path": {
		class: notFolded,
		why: "the fold's own bookkeeping: it *is* the answer to which document wrote a value, " +
			"so it is not itself a value the fold moves",
	},
	"dirDefaults.doc": {
		class: notFolded,
		why: "the sibling's parsed source tree is diagnostic bookkeeping retained for exact positions, " +
			"not a value folded into the suite",
	},
	"dirDefaults.vars": {
		class: movedUnchanged,
		why: "keyed, and the fold copies each name to the same name — `vars.region` addresses " +
			"one value in both documents, so a lookup on the path is sound",
		sibling: func(dd *dirDefaults) { dd.Vars = map[string]any{"fromDir": 1} },
		suite:   func(f *File) { f.Vars = map[string]any{"fromSuite": 1} },
		alone:   []loc{at("vars")},
		shared:  []loc{at("vars").field("fromDir")},
	},
	"dirDefaults.defaults": {
		class: perField,
		why: "a block rather than a value; its fields are classified one by one, and only a " +
			"suite that states no block at all inherits the whole of it",
		sibling: func(dd *dirDefaults) { dd.Defaults = &Defaults{Workflow: "./from-dir.yaml"} },
		suite:   func(f *File) { f.Defaults = &Defaults{Workflow: "./from-suite.yaml"} },
		alone:   []loc{at("defaults")},
		shared:  nil,
	},
	"Defaults.workflow": {
		class:   movedUnchanged,
		why:     "one scalar at one path, the same path in both documents",
		sibling: func(dd *dirDefaults) { dd.Defaults.Workflow = "./from-dir.yaml" },
		suite:   func(f *File) { f.Defaults.Workflow = "./from-suite.yaml" },
		alone:   []loc{at("defaults").field("workflow")},
		shared:  nil,
	},
	"Defaults.inputs": {
		class:   movedUnchanged,
		why:     "keyed, exactly as `vars:` is",
		sibling: func(dd *dirDefaults) { dd.Defaults.Inputs = map[string]any{"fromDir": 1} },
		suite:   func(f *File) { f.Defaults.Inputs = map[string]any{"fromSuite": 1} },
		alone:   []loc{at("defaults").field("inputs")},
		shared:  []loc{at("defaults").field("inputs").field("fromDir")},
	},
	"Defaults.stubs": {
		class: renumbered,
		why: "appended, so the directory's first stub lands past the suite's and " +
			"`defaults.stubs[1]` would name an entry the directory's file does not have. " +
			"[checkDefaults] is given the index that file uses instead. The collection's own " +
			"path is still recorded when the suite states no stub, because the refusal that " +
			"is about the collection — its bound — reports there",
		sibling: func(dd *dirDefaults) { dd.Defaults.Stubs = []Stub{{Task: "fromDir"}} },
		suite:   func(f *File) { f.Defaults.Stubs = []Stub{{Task: "fromSuite"}} },
		alone:   []loc{at("defaults").field("stubs")},
		shared:  nil,
	},
	"Defaults.sender": {
		class:   movedUnchanged,
		why:     "one value at one path, judged at the block by [checkDefaults]",
		sibling: func(dd *dirDefaults) { dd.Defaults.Sender = &ScriptedIdentity{Subject: "dir"} },
		suite:   func(f *File) { f.Defaults.Sender = &ScriptedIdentity{Subject: "suite"} },
		alone:   []loc{at("defaults").field("sender")},
		shared:  nil,
	},
	"Defaults.check": {
		class: renumbered,
		why: "prepended, so `defaults.check[0]` is the directory's first claim and the suite's " +
			"first claim at once — the collision that cost a suite-written claim its own " +
			"position. [checkCheckClaims] is told the document instead. No collection path " +
			"either: nothing reports at `defaults.check` itself",
		sibling: func(dd *dirDefaults) { dd.Defaults.Check = []CheckClaim{{That: "fromDir"}} },
		suite:   func(f *File) { f.Defaults.Check = []CheckClaim{{That: "fromSuite"}} },
		alone:   nil,
		shared:  nil,
	},
}

// foldedNames is every field name reflection finds on the two structs the fold
// reads, in the `Type.yamlkey` spelling the table is keyed by.
//
// Read off the types rather than listed, because a list of a struct's fields
// beside the struct is the duplicate this whole file exists to refuse.
func foldedNames() []string {
	var names []string
	for _, spec := range []struct {
		label string
		typ   reflect.Type
	}{
		{"dirDefaults", reflect.TypeFor[dirDefaults]()},
		{"Defaults", reflect.TypeFor[Defaults]()},
	} {
		for i := range spec.typ.NumField() {
			field := spec.typ.Field(i)
			name := field.Tag.Get("yaml")
			if name == "" {
				// Unexported bookkeeping carries no tag, and still has to be
				// classified: `path` is a field of this struct that a reader
				// could reasonably expect the fold to move.
				name = field.Name
			}
			names = append(names, spec.label+"."+name)
		}
	}

	return names
}

// TestEveryFieldTheFoldTouchesIsClassified is the walk. A field the table does
// not name is a field whose provenance nobody decided, which is how the three
// defects this file follows all arrived.
func TestEveryFieldTheFoldTouchesIsClassified(t *testing.T) {
	t.Parallel()

	names := foldedNames()
	require.NotEmpty(t, names, "reflection found no fields, so this walk proves nothing")

	for _, name := range names {
		field, classified := foldClassification[name]
		require.True(t, classified,
			"%s is a field of a fold this package performs and nothing says how a diagnostic "+
				"about it names the document that wrote it. Classify it in foldClassification: "+
				"moved unchanged (a path is recorded), renumbered (no path; the check names the "+
				"file), or not folded (say why).", name)
		assert.NotEmpty(t, field.why, "%s is classified with no reason", name)
	}
	for name := range foldClassification {
		assert.Contains(t, names, name,
			"foldClassification names %s, which is no longer a field of either struct", name)
	}
}

// foldOf runs one field's fixtures through the real fold and answers with what
// it recorded. withSuite decides whether the suite states the field too, which
// is the only input that tells the classes apart: a scalar the suite also writes
// contributes nothing at all, and a collection both documents write into is
// recorded entry by entry rather than whole.
func (f foldedField) foldOf(name string, withSuite bool) contribution {
	dd := &dirDefaults{path: "testdefaults.yaml"}
	file := &File{}
	if strings.HasPrefix(name, "Defaults.") {
		// A field *inside* the block, so both documents state a block and this
		// stays a test of that field's own rule rather than of the whole-block
		// rule one level up.
		dd.Defaults = &Defaults{}
		file.Defaults = &Defaults{}
	}
	f.sibling(dd)
	if withSuite {
		f.suite(file)
	}

	return dd.combineInto(file)
}

// TestTheFoldRecordsWhatTheClassificationSays drives every classified field
// through the real fold, in both directions that matter: the directory alone,
// and both documents writing the same field.
func TestTheFoldRecordsWhatTheClassificationSays(t *testing.T) {
	t.Parallel()

	driven := 0
	for name, field := range foldClassification {
		if field.class == notFolded {
			require.Nil(t, field.sibling, "%s is not folded, so it has no fold to drive", name)

			continue
		}
		driven++
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.NotNil(t, field.sibling, "%s is folded and states nothing to fold", name)
			require.NotNil(t, field.suite, "%s is folded and the suite states nothing to collide with", name)

			assert.Equal(t, rendered(field.alone), rendered(field.foldOf(name, false).paths),
				"what the fold records when only the directory states %s", name)
			assert.Equal(t, rendered(field.shared), rendered(field.foldOf(name, true).paths),
				"what the fold records when both documents state %s", name)
		})
	}
	require.NotZero(t, driven, "no field was driven, so this table asserts nothing")
}

// TestTheFoldRecordsNoIndexedPath is the classification stated once, as the
// invariant it comes down to: an index is exactly the part of a path a fold
// invalidates when it prepends or appends, and both of the collections this fold
// merges do one of those.
//
// A future field could be a list the fold moves *unchanged*, and an indexed path
// would be sound for it — at which point this test is the thing that makes
// somebody say so out loud rather than the thing that was quietly wrong.
func TestTheFoldRecordsNoIndexedPath(t *testing.T) {
	t.Parallel()

	checked := 0
	for name, field := range foldClassification {
		if field.class == notFolded {
			continue
		}
		for _, withSuite := range []bool{false, true} {
			for _, path := range field.foldOf(name, withSuite).paths {
				checked++
				assert.False(t, slices.ContainsFunc(path, func(s pathStep) bool { return s.indexed }),
					"the fold recorded %s for %s, and an index means one string names an entry "+
						"in each document", path, name)
			}
		}
	}
	require.NotZero(t, checked, "the fold recorded nothing, so this invariant was not tested")
}

// TestAnAppendedStubIsNumberedTheWayItsOwnFileNumbersIt drives the one decision
// the renumbering class turns on, directly.
//
// Extracted to a function for the reason CLAUDE.md gives: in a real document the
// suite's numbering and the directory's agree whenever the suite wrote no stubs,
// so a check written inline against real data is one no fixture can reach.
func TestAnAppendedStubIsNumberedTheWayItsOwnFileNumbersIt(t *testing.T) {
	t.Parallel()

	// Two stubs the suite wrote, then the directory's second and fourth — its
	// first and third having been replaced by a suite stub for the same target.
	moved := contribution{file: "testdefaults.yaml", ownStubs: 2, stubSource: []int{1, 3}}

	for _, tc := range []struct {
		merged    int
		source    int
		elsewhere bool
	}{
		{merged: 0, source: 0, elsewhere: false},
		{merged: 1, source: 1, elsewhere: false},
		{merged: 2, source: 1, elsewhere: true},
		{merged: 3, source: 3, elsewhere: true},
	} {
		source, elsewhere := moved.stubWrittenElsewhere(tc.merged)
		assert.Equal(t, tc.elsewhere, elsewhere, "stub %d", tc.merged)
		assert.Equal(t, tc.source, source, "stub %d", tc.merged)
	}

	// With no directory file there is nothing to attribute, whatever the counts
	// happen to say: a suite loaded on its own wrote every stub it has.
	alone := contribution{ownStubs: 0, stubSource: []int{0}}
	source, elsewhere := alone.stubWrittenElsewhere(0)
	assert.False(t, elsewhere, "a suite with no sibling file inherited a stub from nowhere")
	assert.Equal(t, 0, source)
}

// rendered is a path set as strings, so a failure names the paths rather than
// printing two slices of structs nobody can read.
func rendered(paths []loc) []string {
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		out = append(out, path.String())
	}
	slices.Sort(out)

	return out
}
