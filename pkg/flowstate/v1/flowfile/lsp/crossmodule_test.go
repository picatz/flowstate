package lsp

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A second importer, so a rename has more than the file the cursor is in to reach.
const otherImporterSource = `edition: ` + flowfile.CurrentEdition + `
name: other
use:
  shared:
    path: ./lib/ids.yaml
inputs:
  ref:
    type: shared.Uuid
    required: true
steps:
  - id: noop
    log:
      message: hi
`

// moduleTree writes lib/ids.yaml, bill.yaml and other.yaml under a fresh directory
// and returns it.
func moduleTree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "lib"), 0o755))
	for name, text := range map[string]string{
		"lib/ids.yaml": usedModuleSource,
		"bill.yaml":    useSource,
		"other.yaml":   otherImporterSource,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644))
	}

	return dir
}

func docAt(dir, name, text string) *document {
	return newDocument(fileURI(filepath.Join(dir, name)), 1, text, nil)
}

// applyAll applies one file's edits, last first so earlier offsets hold.
func applyAll(t *testing.T, text string, edits []lsp.TextEdit) string {
	t.Helper()
	ix := newLineIndex(text)
	sorted := slices.Clone(edits)
	slices.SortFunc(sorted, func(a, b lsp.TextEdit) int {
		return ix.offsetOfPosition(b.Range.Start) - ix.offsetOfPosition(a.Range.Start)
	})
	for _, e := range sorted {
		text = text[:ix.offsetOfPosition(e.Range.Start)] + e.NewText + text[ix.offsetOfPosition(e.Range.End):]
	}

	return text
}

// TestDefinitionFollowsAQualifiedNameIntoTheModule: a type, a function and an error
// each jump to the key that declares them in the module, and a name the module does
// not declare, or a module the compiler would refuse, jumps nowhere.
func TestDefinitionFollowsAQualifiedNameIntoTheModule(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	moduleURI := fileURI(filepath.Join(dir, "lib", "ids.yaml"))
	doc := docAt(dir, "bill.yaml", useSource)

	for _, tc := range []struct{ at, key string }{
		{"ids.Uuid", "Uuid"},
		{"ids.isUuid(", "isUuid"},
		{"ids.NotFound", "NotFound"},
	} {
		// On the name and on the alias: the same declaration.
		for _, off := range []int{1, len("ids.") + 1} {
			got := definitionAt(doc, positionOf(t, useSource, tc.at, off))
			require.Len(t, got, 1, "%s at +%d", tc.at, off)
			assert.Equal(t, moduleURI, got[0].URI)
			start := newLineIndex(usedModuleSource).offsetOfPosition(got[0].Range.Start)
			assert.Equal(t, tc.key, usedModuleSource[start:start+len(tc.key)], "lands on the declaring key")
		}
	}

	missing := strings.Replace(useSource, "ids.NotFound", "ids.Missing", 1)
	assert.Empty(t, definitionAt(docAt(dir, "bill.yaml", missing), positionOf(t, missing, "ids.Missing", 1)),
		"a module that does not declare the name is not navigated to")

	climbing := strings.Replace(useSource, "./lib/ids.yaml", "../ids.yaml", 1)
	assert.Empty(t, definitionAt(docAt(dir, "bill.yaml", climbing), positionOf(t, climbing, "ids.Uuid", 1)),
		"a path the compiler refuses names no module")

	unused := strings.Replace(useSource, "use:\n  ids:\n    path: ./lib/ids.yaml\n", "", 1)
	assert.Empty(t, definitionAt(docAt(dir, "bill.yaml", unused), positionOf(t, unused, "ids.Uuid", 1)),
		"an alias the file does not use names nothing")
}

// TestHoverDescribesAModuleTypeAndError: hover on a qualified type or error says
// what it is, where it comes from, and pins the bytes read.
func TestHoverDescribesAModuleTypeAndError(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	doc := docAt(dir, "bill.yaml", useSource)

	typ := hoverText(hoverAt(doc, positionOf(t, useSource, "ids.Uuid", 5)))
	assert.Contains(t, typ, "`ids.Uuid`")
	assert.Contains(t, typ, "a type declared by the module `ids`")
	assert.Contains(t, typ, "./lib/ids.yaml@"+v1.ContentDigest([]byte(usedModuleSource)))
	assert.Contains(t, typ, "Base type `string`")
	assert.Contains(t, typ, "`isUuid(this)`")
	assert.Contains(t, typ, "A lowercase UUID.")

	errText := hoverText(hoverAt(doc, positionOf(t, useSource, "ids.NotFound", 5)))
	assert.Contains(t, errText, "an error declared by the module `ids`")

	// A function keeps the hover that already describes every declared function.
	fn := hoverText(hoverAt(doc, positionOf(t, useSource, "ids.isUuid(", 5)))
	assert.Contains(t, fn, "ids.isUuid(s: string) -> bool")

	assert.Empty(t, hoverText(hoverAt(docAt(dir, "bill.yaml", strings.Replace(useSource, "ids.NotFound", "ids.Nope", 1)),
		positionOf(t, strings.Replace(useSource, "ids.NotFound", "ids.Nope", 1), "ids.Nope", 5))),
		"a name the module does not declare is described by nothing")
}

// TestCompletionOffersAModulesTypesAndErrors: after `alias.` in a type or an error
// position the module's declarations of that kind are offered, and nothing is for
// an alias the file does not use.
func TestCompletionOffersAModulesTypesAndErrors(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	head := "edition: " + flowfile.CurrentEdition + "\nname: bill\nuse:\n  ids:\n    path: ./lib/ids.yaml\n"
	for _, tc := range []struct {
		name, body string
		want       []string
	}{
		{"input type", "inputs:\n  ref:\n    type: ids.|\n", []string{"Uuid"}},
		{"input type prefix", "inputs:\n  ref:\n    type: ids.Uu|\n", []string{"Uuid"}},
		{"error", "steps:\n  - id: a\n    fail:\n      error: ids.|\n", []string{"NotFound"}},
		{"function result", "functions:\n  f:\n    returns: ids.|\n    body: ${true}\n", []string{"Uuid"}},
		{"unused alias", "inputs:\n  ref:\n    type: nope.|\n", nil},
	} {
		text, cursor := splitCursor(t, head+tc.body)
		doc := docAt(dir, "menu.yaml", text)
		list := completeAt(doc, cursor)
		if tc.want == nil {
			assert.Empty(t, labels(list.Items), tc.name)
			continue
		}
		assert.Equal(t, tc.want, labels(list.Items), tc.name)
		edit := list.Items[0].TextEdit
		require.NotNil(t, edit, tc.name)
		assert.Equal(t, tc.want[0], edit.NewText)
		assert.Equal(t, cursor.Line, edit.Range.Start.Line)
		assert.LessOrEqual(t, edit.Range.Start.Character, cursor.Character)
	}
}

// TestRenameReachesTheModuleAndEveryImporter: renaming from a qualified name edits
// the declaring key and every importer in the workspace, under whatever alias each
// uses.
func TestRenameReachesTheModuleAndEveryImporter(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	ws := workspace{roots: []string{dir}}
	doc := docAt(dir, "bill.yaml", useSource)

	rng, placeholder, ok := prepareRenameQualified(doc, positionOf(t, useSource, "ids.Uuid", 1))
	require.True(t, ok)
	assert.Equal(t, "Uuid", placeholder)
	assert.Equal(t, rng.Start.Line, rng.End.Line)
	assert.Equal(t, len("Uuid"), rng.End.Character-rng.Start.Character, "the alias is the importer's own word")

	edit, handled, err := renameQualified(doc, ws, positionOf(t, useSource, "ids.Uuid", 5), "Id")
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, edit.Changes, 3, "the module and both importers")

	moduleKey := string(fileURI(filepath.Join(dir, "lib", "ids.yaml")))
	assert.Contains(t, applyAll(t, usedModuleSource, edit.Changes[moduleKey]), "types:\n  Id:\n")
	assert.Contains(t, applyAll(t, useSource, edit.Changes[string(doc.uri)]), "type: ids.Id\n")
	assert.Contains(t, applyAll(t, otherImporterSource, edit.Changes[string(fileURI(filepath.Join(dir, "other.yaml")))]), "type: shared.Id\n")

	// A function renames through the fence that calls it.
	fn, handled, err := renameQualified(doc, ws, positionOf(t, useSource, "ids.NotFound", 5), "Gone")
	require.NoError(t, err)
	require.True(t, handled)
	assert.Contains(t, applyAll(t, useSource, fn.Changes[string(doc.uri)]), "error: ids.Gone\n")
}

// TestRenameRefusesWhatItCannotCompleteExactly: every case where an edit could
// leave a file stale is a reason, not a partial edit.
func TestRenameRefusesWhatItCannotCompleteExactly(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	ws := workspace{roots: []string{dir}}
	doc := docAt(dir, "bill.yaml", useSource)
	at := positionOf(t, useSource, "ids.Uuid", 5)

	for _, tc := range []struct {
		name, newName, want string
		ws                  workspace
		doc                 *document
		at                  lsp.Position
	}{
		{"no workspace folder", "Id", "no workspace folder", workspace{}, doc, at},
		{"not a name", "id lower", "not a valid type name", ws, doc, at},
		{"lower case type", "id", "not a valid type name", ws, doc, at},
		{"already declared", "NotFound", "already declares", ws, doc, at},
		{"same name", "Uuid", "already the name", ws, doc, at},
	} {
		edit, handled, err := renameQualified(tc.doc, tc.ws, tc.at, tc.newName)
		assert.True(t, handled, tc.name)
		assert.Nil(t, edit, tc.name)
		require.Error(t, err, tc.name)
		assert.Contains(t, err.Error(), tc.want, tc.name)
	}

	// The module's own body calls the function by its bare name; that is not tracked.
	_, handled, err := renameQualified(doc, ws, positionOf(t, useSource, "ids.isUuid(", 5), "isId")
	assert.True(t, handled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the module itself mentions")

	// A spelling beside code that the model does not place.
	stale := strings.Replace(otherImporterSource, "type: shared.Uuid", "type: shared.Uuid # was shared.Uuid", 1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "other.yaml"), []byte(stale), 0o644))
	_, handled, err = renameQualified(doc, ws, at, "Id")
	assert.True(t, handled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not track")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "other.yaml"), []byte(otherImporterSource), 0o644))

	// Importers the walk could not see make a rename unsafe.
	for i := range maxWorkspaceFiles + 1 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "pad"+string(rune('a'+i%26))+strings.Repeat("x", i/26)+".yaml"), []byte("name: pad\n"), 0o644))
	}
	_, handled, err = renameQualified(doc, ws, at, "Id")
	assert.True(t, handled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "narrower folder")

	// Not a qualified name: left to the single-file renames.
	_, handled, err = renameQualified(doc, ws, positionOf(t, useSource, "inputs.ref", 1), "x")
	assert.False(t, handled)
	assert.NoError(t, err)
}

// TestAddUseQuickFixForAnUnresolvedQualifiedName: a name under a diagnostic whose
// alias the file does not use is offered the `use:` of a module beside it that
// declares the name, and applying it makes the file compile.
func TestAddUseQuickFixForAnUnresolvedQualifiedName(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	src := "edition: " + flowfile.CurrentEdition + "\nname: bill\ninputs:\n  c:\n    type: ids.Uuid\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	doc := docAt(dir, "bill.yaml", src)
	require.NotEmpty(t, diagnose(doc), "premise: the unresolved name is diagnosed")

	actions := codeActions(doc, codeActionParams{Range: lsp.Range{Start: positionOf(t, src, "ids.Uuid", 1), End: positionOf(t, src, "ids.Uuid", 3)}})
	var add *codeAction
	for i := range actions {
		if strings.HasPrefix(actions[i].Title, "Add `use:") {
			add = &actions[i]
		}
	}
	require.NotNil(t, add, "titles: %v", actions)
	assert.Equal(t, "Add `use: {ids: ./lib/ids.yaml}` for `ids.Uuid`", add.Title)

	fixed := applyAll(t, src, add.Edit.Changes[string(doc.uri)])
	assert.Contains(t, fixed, "name: bill\nuse:\n  ids:\n    path: ./lib/ids.yaml\ninputs:")
	path := filepath.Join(dir, "bill.yaml")
	_, _, err := flowfile.ParseAt([]byte(fixed), path)
	require.NoError(t, err, "the fixed file compiles")

	// Into an existing block, indented as its siblings are.
	withUse := "edition: " + flowfile.CurrentEdition + "\nname: bill\nuse:\n  other:\n    path: ./lib/ids.yaml\ninputs:\n  c:\n    type: ids.Uuid\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	d2 := docAt(dir, "bill.yaml", withUse)
	got := codeActions(d2, codeActionParams{Range: lsp.Range{Start: positionOf(t, withUse, "ids.Uuid", 1), End: positionOf(t, withUse, "ids.Uuid", 3)}})
	require.NotEmpty(t, got)
	appended := applyAll(t, withUse, got[len(got)-1].Edit.Changes[string(d2.uri)])
	assert.Contains(t, appended, "use:\n  other:\n    path: ./lib/ids.yaml\n  ids:\n    path: ./lib/ids.yaml\ninputs:")
	_, _, err = flowfile.ParseAt([]byte(appended), path)
	require.NoError(t, err)

	// Nothing declares the name: nothing is offered.
	none := strings.Replace(src, "ids.Uuid", "ids.Nowhere", 1)
	for _, a := range codeActions(docAt(dir, "bill.yaml", none), codeActionParams{Range: wholeOf(none)}) {
		assert.False(t, strings.HasPrefix(a.Title, "Add `use:"), a.Title)
	}

	// A CEL root is not an alias to import.
	root := strings.Replace(src, "type: ids.Uuid", "type: steps.Uuid", 1)
	for _, a := range codeActions(docAt(dir, "bill.yaml", root), codeActionParams{Range: wholeOf(root)}) {
		assert.False(t, strings.HasPrefix(a.Title, "Add `use:"), a.Title)
	}

	// A module that sits above the file cannot be reached by a `use:` and is not offered.
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.MkdirAll(sub, 0o755))
	subDoc := docAt(sub, "bill.yaml", src)
	for _, a := range codeActions(subDoc, codeActionParams{Range: wholeOf(src)}) {
		assert.False(t, strings.HasPrefix(a.Title, "Add `use:"), a.Title)
	}
}

// TestRepinQuickFixStampsThroughRepinUses: a pin that no longer matches the module
// is offered a repin that is exactly what `flow fix --repin` writes, and a current
// pin is offered nothing.
func TestRepinQuickFixStampsThroughRepinUses(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	stale := "sha256:" + strings.Repeat("0", 64)
	src := strings.Replace(useSource, "    path: ./lib/ids.yaml\n", "    path: ./lib/ids.yaml\n    digest: "+stale+"\n", 1)
	path := filepath.Join(dir, "bill.yaml")
	doc := docAt(dir, "bill.yaml", src)

	var covered lsp.Range
	for _, c := range diagnoseCarried(doc) {
		if c.published.Code == string(v1.DiagnosticCodeModulePinMismatch) {
			covered = c.published.Range
		}
	}
	require.NotEqual(t, lsp.Range{}, covered, "premise: the stale pin is diagnosed")

	actions := codeActions(doc, codeActionParams{Range: covered})
	var repin *codeAction
	for i := range actions {
		if strings.HasPrefix(actions[i].Title, "Repin") {
			repin = &actions[i]
		}
	}
	require.NotNil(t, repin)
	require.Len(t, repin.Edit.Changes[string(doc.uri)], 1)
	want, err := flowfile.RepinUses(path, []byte(src))
	require.NoError(t, err)
	assert.Equal(t, string(want.Source), repin.Edit.Changes[string(doc.uri)][0].NewText)
	assert.Contains(t, repin.Edit.Changes[string(doc.uri)][0].NewText, v1.ContentDigest([]byte(usedModuleSource)))

	// Never a fix-all: a pin is read, not saved.
	for _, a := range codeActions(doc, codeActionParams{Range: covered, Context: codeActionContext{Only: []lsp.CodeActionKind{codeActionKindSourceFixAll}}}) {
		assert.NotContains(t, a.Title, "Repin")
	}

	current := strings.Replace(src, stale, v1.ContentDigest([]byte(usedModuleSource)), 1)
	for _, a := range codeActions(docAt(dir, "bill.yaml", current), codeActionParams{Range: wholeOf(current)}) {
		assert.NotContains(t, a.Title, "Repin", "a current pin has nothing to repin")
	}
}

// TestWorkspaceSymbolsListModuleDeclarations: workspace/symbol answers over the
// modules under the folders the client opened, filters by the query, and reads
// nothing outside them or through a symlink.
func TestWorkspaceSymbolsListModuleDeclarations(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.yaml"), []byte(strings.Replace(usedModuleSource, "Uuid", "Hidden", 1)), 0o644))
	require.NoError(t, os.Symlink(filepath.Join(outside, "secret.yaml"), filepath.Join(dir, "linked.yaml")))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".git"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".git", "hidden.yaml"), []byte(usedModuleSource), 0o644))

	ws := workspace{roots: []string{dir}}
	names := func(query string) []string {
		var out []string
		for _, s := range workspaceSymbols(ws, query) {
			out = append(out, s.Name)
		}
		return out
	}

	assert.ElementsMatch(t, []string{"Uuid", "NotFound", "isUuid"}, names(""))
	assert.Equal(t, []string{"Uuid", "isUuid"}, names("uuid"), "case-insensitive substring")
	assert.Empty(t, names("Hidden"), "a symlink out of the workspace is not followed")
	assert.Empty(t, workspaceSymbols(workspace{}, ""), "no folder, no symbols")

	got := workspaceSymbols(ws, "NotFound")
	require.Len(t, got, 1)
	assert.Equal(t, fileURI(filepath.Join(dir, "lib", "ids.yaml")), got[0].Location.URI)
	assert.Contains(t, got[0].ContainerName, "ids")
}

// TestCrossModuleRequestsOverTheWire: the folder the client opens at initialize is
// the one workspace/symbol and rename read, and the capability is advertised.
func TestCrossModuleRequestsOverTheWire(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	c := newClient(t)
	var result initializeResult
	require.NoError(t, c.conn.Call(t.Context(), "initialize", lsp.InitializeParams{RootURI: fileURI(dir)}, &result))
	require.NoError(t, c.conn.Notify(t.Context(), "initialized", struct{}{}))
	assert.True(t, result.Capabilities.WorkspaceSymbolProvider)

	var symbols []lsp.SymbolInformation
	require.NoError(t, c.conn.Call(t.Context(), "workspace/symbol", lsp.WorkspaceSymbolParams{Query: "notfound"}, &symbols))
	require.Len(t, symbols, 1)

	uri := string(fileURI(filepath.Join(dir, "bill.yaml")))
	c.open(uri, useSource)
	at := positionOf(t, useSource, "ids.Uuid", 5)
	var edit lsp.WorkspaceEdit
	require.NoError(t, c.conn.Call(t.Context(), "textDocument/rename", lsp.RenameParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)}, Position: at, NewName: "Id",
	}, &edit))
	assert.Len(t, edit.Changes, 3)
}
