package flowfile_test

import (
	"io/fs"
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
var celLibraryClaim = regexp.MustCompile(`CEL extension libraries: ([^.]*)\.`)

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
// The catalog was the one that was right, and it is worth saying why, because it
// is not diligence: `celLibraries` is `v1.ExtensionLibraries()`, which is the key
// set `buildEnv` matches against, so it cannot name a library the evaluator would
// refuse. Derived beats maintained, again.
//
// An earlier version of this comment also said it "diffs environments to find what
// each provides", which was not true of anything: the catalog carried library names
// and nothing about their contents. It is true now — `cel_functions` is built by
// asking each library's environment what it declares and subtracting cel-go's own —
// which is the direction a comment describing something aspirational should be
// resolved in.
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

// exampleLink matches a link to an example directory in examples/README.md.
//
// The whole markdown link, not just a parenthesised word. It used to be the
// latter, which meant any ordinary prose in a round bracket was read as a link to
// an example — describing a task as built on `(go-git)`, or a verb as `(read)`,
// failed this test with a complaint about a stale link to a directory nobody had
// ever mentioned. The diagnostic was confidently wrong, which is worse than
// silence, and it punished writing rather than catching a mistake.
//
// Nested targets are matched too. They did not used to be: the pattern stopped at
// the first slash, on the reasoning that `[plugins/greet](plugins/greet/)` sits a
// directory deeper on purpose and the glob only walked one level. Both halves of
// that were true and the conclusion was still wrong — it meant the index's own
// completeness check could not see the deeper half of the index, and
// `plugins/codex` and `plugins/sql` were absent from the table for exactly as
// long as nobody read it by hand. A guard that walks one level of a two-level
// tree reports on a page rather than on the walk.
var exampleLink = regexp.MustCompile(`\[[^\]]+\]\(([^)\s#]+)\)`)

// exampleLinkTarget normalizes a matched link into the example path it names, or
// "" for a link that names something else.
//
// Links out of the directory (`../docs/USE_CASES.md`) and to the web are not
// claims about an example and are dropped. A link to an example's README
// (`[embedding](embedding/README.md)`) is a link to that example, because that is
// the file a reader is being sent to read.
func exampleLinkTarget(target string) string {
	if strings.HasPrefix(target, "../") || strings.Contains(target, "://") {
		return ""
	}

	target = strings.TrimSuffix(target, "/")
	target = strings.TrimSuffix(target, "/README.md")

	if target == "" || strings.HasSuffix(target, ".md") {
		return ""
	}

	return target
}

// TestExamplesREADMEListsEveryExample keeps the examples index complete.
//
// Nothing is missing from it today. This is the unusual case where the audit item
// was already stale — a note said two examples were absent and both are listed —
// so there is nothing to fix, and a test is the only thing worth adding.
//
// It earns its place because the index is exactly the kind of file that goes
// wrong quietly. Adding an example is a directory and a workflow; updating a
// README is a separate act nothing forces, and the failure is invisible from
// inside the change that causes it. An example nobody links is an example nobody
// runs, which is the same defect as a capability with no example, one surface
// further out.
//
// Both directions, per the lesson from the two tests above. A stale row survives a
// directory being renamed or removed, and sends a reader to a 404 — which is worse
// than an omission, because the omission merely hides something while the stale
// link actively wastes their time.
func TestExamplesREADMEListsEveryExample(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "..", "..")

	data, err := os.ReadFile(filepath.Join(root, "examples", "README.md"))
	require.NoError(t, err, "examples/README.md moved and this test did not")

	linked := map[string]bool{}
	for _, m := range exampleLink.FindAllStringSubmatch(string(data), -1) {
		if target := exampleLinkTarget(m[1]); target != "" {
			linked[target] = true
		}
	}
	require.NotEmpty(t, linked,
		"no example links found; either the index changed shape or this pattern stopped matching it")

	// Every example, at whatever depth it sits — `plugins/sql` and
	// `operations/worker-versioning` are examples in exactly the sense
	// `hello-world` is, and the reason they were ever exempt was that the
	// pattern above could not name them.
	var onDisk []string
	examplesDir := filepath.Join(root, "examples")
	require.NoError(t, filepath.WalkDir(examplesDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != "workflow.yaml" {
			return nil
		}
		rel, err := filepath.Rel(examplesDir, filepath.Dir(path))
		if err != nil {
			return err
		}
		onDisk = append(onDisk, filepath.ToSlash(rel))
		return nil
	}))
	require.NotEmpty(t, onDisk, "no examples found; the walk is wrong")

	for _, name := range onDisk {
		// Its own row, or an ancestor's. `embedding/flowfile` is the Flowfile
		// belonging to the `embedding` example rather than an example in its
		// own right, and a reader sent to `embedding` has been sent to it —
		// demanding a separate row would be asking the index to list parts.
		covered := false
		for path := name; ; {
			if linked[path] {
				covered = true
				break
			}
			parent := filepath.ToSlash(filepath.Dir(path))
			if parent == path || parent == "." {
				break
			}
			path = parent
		}

		assert.True(t, covered,
			"examples/%s exists and examples/README.md links neither it nor a directory above it\n"+
				"  an example nobody links is an example nobody runs", name)
	}

	for name := range linked {
		info, err := os.Stat(filepath.Join(examplesDir, name))
		assert.NoError(t, err,
			"examples/README.md links %q, which is not there\n"+
				"  a stale link sends a reader somewhere that is not", name)
		if err == nil {
			assert.True(t, info.IsDir(),
				"examples/README.md links %q, which is not a directory", name)
		}
	}
}
