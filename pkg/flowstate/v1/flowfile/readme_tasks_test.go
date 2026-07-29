package flowfile_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The README's task table is a second source of truth, and it had drifted.
//
// `docs/ARCHITECTURE.md` names this exact table when it explains why capability
// lives in one place: "the hand-written task table in the README drifting from the
// code is exactly the failure this prevents". It drifted anyway — six of `http`'s
// eleven inputs and one of its four outputs were missing — while the sentence
// directly beneath it told the reader that a listing derived from the registry
// cannot drift. Both claims were on the same screen, and the true one was
// describing `flow tasks` rather than the table above it.
//
// Deleting the table was the other option. It is kept because someone landing on
// the README should see what the engine can do without running a command, and a
// table something checks is worth more than no table. What makes that safe is this
// file: the registry is asked rather than remembered, so a task or an input added
// tomorrow fails here rather than on the day a reader notices.

// taskTableRow matches one row of the README's task table: a backticked task name,
// then the inputs cell, then the outputs cell.
var taskTableRow = regexp.MustCompile("(?m)^\\| `([a-z_]+)`\\s*\\|([^|]*)\\|([^|]*)\\|")

// celLibraryClaim isolates the README sentence that enumerates the extension
// libraries, so the assertion is about that list rather than about the words in
// it appearing somewhere in the file.
var celLibraryClaim = regexp.MustCompile("CEL extension libraries: ([^.]*)\\.")

// backtickedName finds each `name` inside a table cell.
var backtickedName = regexp.MustCompile("`([a-z_]+)`")

// TestREADMETaskTableMatchesTheRegistry checks the documented catalog against the
// engine's own, in both directions.
func TestREADMETaskTableMatchesTheRegistry(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "README.md"))
	require.NoError(t, err)

	type cells struct{ inputs, outputs string }
	rows := map[string]cells{}
	for _, m := range taskTableRow.FindAllStringSubmatch(string(data), -1) {
		rows[m[1]] = cells{inputs: m[2], outputs: m[3]}
	}
	require.NotEmpty(t, rows,
		"no task rows found in the README; either the table moved or this pattern stopped matching it")

	registered := v1.DefaultRegistry().All()
	require.NotEmpty(t, registered, "the registry is empty; this test is checking nothing")

	// Everything the engine can do is documented.
	for _, def := range registered {
		row, documented := rows[def.Name]
		if !assert.True(t, documented,
			"task %q is registered and has no row in the README's task table\n"+
				"  add one, or the table tells a reader the engine does less than it does", def.Name) {
			continue
		}

		for _, field := range fieldNames(def.Inputs) {
			assert.Contains(t, row.inputs, "`"+field+"`",
				"task %q accepts input %q and the README's row does not name it", def.Name, field)
		}
		for _, field := range fieldNames(def.Outputs) {
			assert.Contains(t, row.outputs, "`"+field+"`",
				"task %q produces output %q and the README's row does not name it", def.Name, field)
		}
	}

	// And nothing documented is invented. This direction matters as much: a row
	// naming an input the task does not accept sends an author to write a key the
	// validator refuses, and a test that only looked for omissions would pass on it.
	for name, row := range rows {
		def, known := v1.LookupTask(name)
		if !assert.True(t, known,
			"the README's task table has a row for %q, which no task is registered under", name) {
			continue
		}
		assertNamesAreReal(t, name, "input", row.inputs, fieldNames(def.Inputs))
		assertNamesAreReal(t, name, "output", row.outputs, fieldNames(def.Outputs))
	}
}

// assertNamesAreReal reports a backticked name in a cell that the task lacks.
func assertNamesAreReal(t *testing.T, task, kind, cell string, real []string) {
	t.Helper()

	for _, match := range backtickedName.FindAllStringSubmatch(cell, -1) {
		if name := match[1]; !slices.Contains(real, name) {
			t.Errorf("the README says task %q has %s %q, and it does not\n  it has: %s",
				task, kind, name, strings.Join(real, ", "))
		}
	}
}

// fieldNames lists the field names a task's input or output message declares.
func fieldNames(md protoreflect.MessageDescriptor) []string {
	if md == nil {
		return nil
	}
	names := make([]string, 0, md.Fields().Len())
	for i := range md.Fields().Len() {
		names = append(names, string(md.Fields().Get(i).Name()))
	}
	return names
}

// TestREADMENamesEveryCELLibrary is the same rule for the other list the README
// keeps.
//
// Three places named the CEL extension libraries and two of them were wrong. The
// evaluator accepts eleven; the README listed nine and the schema comment on
// `libs` listed eight, both missing `json` — which `examples/http-json-via-cel`
// uses, so the documentation omitted a library a shipped example depends on.
//
// The language server was the one that was right, and it is worth saying why,
// because it is not diligence: `celLibraries` iterates `v1.ExtensionLibraries()`
// and diffs environments to find what each provides, so it cannot name a library
// the evaluator would refuse. Derived beats maintained, again.
//
// The schema comment stopped enumerating rather than being corrected — a list in
// prose beside the thing it describes is a second source of truth, and that one
// had already drifted. The README keeps its list, because a reader should see what
// is available without running a command, and pays for it by being checked here.
func TestREADMENamesEveryCELLibrary(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "README.md"))
	require.NoError(t, err)

	libraries := v1.ExtensionLibraries()
	require.NotEmpty(t, libraries, "the evaluator offers no libraries; this test is checking nothing")

	// Scoped to the sentence making the claim, not searched across the whole file.
	//
	// The first version of this asserted the README *contains* each name, and it
	// passed while `json` was missing from the list — because `json` appears in
	// `--output json`, in `parse_json`, and in the http task's row. It was an
	// assertion about a word that legitimately appears elsewhere, which is an
	// assertion about that word's presence rather than about the list.
	//
	// Caught by deleting `json` from the list and watching the test stay green,
	// which is the only way that class of mistake announces itself.
	claim := celLibraryClaim.FindStringSubmatch(string(data))
	require.NotNil(t, claim,
		"the README no longer has a sentence listing the CEL extension libraries;\n"+
			"  either it moved or this pattern stopped matching it, and either way this test covers nothing")

	for _, name := range libraries {
		assert.Contains(t, claim[1], "`"+name+"`",
			"the evaluator accepts CEL library %q and the README's list does not name it\n"+
				"  a library nobody documents is one nobody enables", name)
	}

	// And the other direction, which this test was missing while the task table
	// test above had it — an inconsistency inside one change.
	//
	// A library removed or renamed in the evaluator but left in this sentence keeps
	// every assertion above green, because they only ask whether each real name is
	// present. What the reader gets is documentation recommending a value that
	// `flow validate` refuses, which is worse than an omission: an omission costs
	// them a feature they never knew about, and this costs them a file that does
	// not work and a search for why.
	for _, match := range backtickedName.FindAllStringSubmatch(claim[1], -1) {
		name := match[1]
		assert.Contains(t, libraries, name,
			"the README's list names CEL library %q, which the evaluator does not accept\n"+
				"  an author enabling it gets a validation error, not the library", name)
	}
}
