package flowfile_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const idsModule = `edition: ` + flowfile.CurrentEdition + `
name: ids
types:
  Uuid:
    type: string
    must: isUuid(this)
  Customer:
    fields:
      id:
        type: Uuid
        required: true
errors:
  NotFound:
    description: No customer has this id.
functions:
  isUuid:
    params:
      s: string
    returns: bool
    body: ${s.matches("^[0-9a-f]{8}$")}
`

// workflowUsing is a workflow with one step that uses alias, spelling the use block
// as given.
func workflowUsing(use string, extra string) string {
	return "edition: " + flowfile.CurrentEdition + "\nname: bill\n" + use + extra + `steps:
  - id: check
    log:
      message: hi
`
}

const useIds = "use:\n  ids:\n    path: ./lib/ids.yaml\n"

// tree writes files (relative path to content) under a fresh directory and returns it.
func tree(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	for name, content := range files {
		full := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, []byte(content), 0o644))
	}

	return dir
}

// compileAt compiles dir/name from disk.
func compileAt(t *testing.T, dir, name string) (*v1.Workflow, error) {
	t.Helper()

	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	wf, _, err := flowfile.ParseAt(data, path)

	return wf, err
}

// validateAt compiles and validates dir/name, returning every problem as one error.
func validateAt(t *testing.T, dir, name string) error {
	t.Helper()

	_, _, ds, err := flowfile.ParseAndValidateFileAt(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	if len(ds) > 0 {
		return ds
	}

	return nil
}

func declaredNames[T interface{ GetName() string }](decls []T) []string {
	names := make([]string, 0, len(decls))
	for _, d := range decls {
		names = append(names, d.GetName())
	}

	return names
}

func TestUseCarriesDeclarationsUnderTheAlias(t *testing.T) {
	t.Parallel()

	src := workflowUsing(useIds, `inputs:
  customer:
    type: ids.Customer
    required: true
`)
	src = strings.Replace(src, "message: hi", `message: '${ids.isUuid(inputs.customer.id) ? "ok" : "bad"}'`, 1)
	dir := tree(t, map[string]string{"bill.yaml": src, "lib/ids.yaml": idsModule})

	wf, err := compileAt(t, dir, "bill.yaml")
	require.NoError(t, err)

	assert.Contains(t, declaredNames(wf.GetDeclaredTypes()), "ids.Uuid")
	assert.Contains(t, declaredNames(wf.GetDeclaredTypes()), "ids.Customer")
	assert.Equal(t, []string{"ids.isUuid"}, declaredNames(wf.GetDeclaredFunctions()))
	assert.Equal(t, []string{"ids.NotFound"}, declaredNames(wf.GetDeclaredErrors()))
	require.Len(t, wf.GetModules(), 1)
	assert.Equal(t, "ids", wf.GetModules()[0].GetAlias())
	assert.Equal(t, "./lib/ids.yaml", wf.GetModules()[0].GetSource())
	assert.NoError(t, v1.ValidateContentDigest(wf.GetModules()[0].GetSourceDigest()))
	assert.NoError(t, v1.CheckModules(wf), "what the compiler produced is what the server accepts")
}

func TestUseIsDeterministic(t *testing.T) {
	t.Parallel()

	src := workflowUsing(useIds, "")
	dir := tree(t, map[string]string{"bill.yaml": src, "lib/ids.yaml": idsModule})

	first, err := compileAt(t, dir, "bill.yaml")
	require.NoError(t, err)
	want, err := flowfile.Marshal(first)
	require.NoError(t, err)

	for range 5 {
		again, err := compileAt(t, dir, "bill.yaml")
		require.NoError(t, err)
		got, err := flowfile.Marshal(again)
		require.NoError(t, err)
		assert.Equal(t, string(want), string(got))
		assert.Equal(t, first.GetModules()[0].GetSourceDigest(), again.GetModules()[0].GetSourceDigest())
		assert.Equal(t, declaredNames(first.GetDeclaredTypes()), declaredNames(again.GetDeclaredTypes()))
	}
}

func TestUseRoundTripsThroughMarshalWithoutTheCarriedDeclarations(t *testing.T) {
	t.Parallel()

	src := workflowUsing(useIds, "")
	dir := tree(t, map[string]string{"bill.yaml": src, "lib/ids.yaml": idsModule})
	wf, err := compileAt(t, dir, "bill.yaml")
	require.NoError(t, err)

	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	text := string(out)
	assert.Contains(t, text, "use:\n  ids:\n    path: ./lib/ids.yaml\n")
	assert.NotContains(t, text, "Uuid", "a module's declarations are written by the module, not here")
	assert.NotContains(t, text, "isUuid")
}

func TestUseRefusesAPathOutsideWhatItMayRead(t *testing.T) {
	t.Parallel()

	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "ids.yaml"), []byte(idsModule), 0o644))

	cases := map[string]string{
		"climb":            "../ids.yaml",
		"climb in the way": "./lib/../../ids.yaml",
		"absolute":         filepath.Join(outside, "ids.yaml"),
		"url":              "https://example.com/ids.yaml",
		"empty":            "",
	}
	for name, target := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := tree(t, map[string]string{"bill.yaml": workflowUsing(fmt.Sprintf("use:\n  ids:\n    path: %q\n", target), "")})
			wf, err := compileAt(t, dir, "bill.yaml")
			require.Error(t, err, "fail closed")
			assert.Nil(t, wf)
		})
	}
}

func TestUseRefusesASymlinkThatEscapes(t *testing.T) {
	t.Parallel()

	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "ids.yaml"), []byte(idsModule), 0o644))
	dir := tree(t, map[string]string{"bill.yaml": workflowUsing(useIds, "")})
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "lib"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join(outside, "ids.yaml"), filepath.Join(dir, "lib", "ids.yaml")))

	_, err := compileAt(t, dir, "bill.yaml")
	require.Error(t, err)

	// A directory symlink is the same escape one component earlier.
	dir = tree(t, map[string]string{"bill.yaml": workflowUsing(useIds, "")})
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "lib")))
	_, err = compileAt(t, dir, "bill.yaml")
	require.Error(t, err)
}

func TestUseRefusesWhatIsNotAReadableModule(t *testing.T) {
	t.Parallel()

	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		dir := tree(t, map[string]string{"bill.yaml": workflowUsing(useIds, "")})
		_, err := compileAt(t, dir, "bill.yaml")
		require.Error(t, err)
	})
	t.Run("a directory", func(t *testing.T) {
		t.Parallel()
		dir := tree(t, map[string]string{"bill.yaml": workflowUsing(useIds, ""), "lib/ids.yaml/x.yaml": "x: 1\n"})
		_, err := compileAt(t, dir, "bill.yaml")
		require.Error(t, err)
	})
	t.Run("empty file", func(t *testing.T) {
		t.Parallel()
		dir := tree(t, map[string]string{"bill.yaml": workflowUsing(useIds, ""), "lib/ids.yaml": ""})
		_, err := compileAt(t, dir, "bill.yaml")
		require.Error(t, err)
	})
	t.Run("a workflow says call", func(t *testing.T) {
		t.Parallel()
		dir := tree(t, map[string]string{
			"bill.yaml":    workflowUsing(useIds, ""),
			"lib/ids.yaml": workflowUsing("", ""),
		})
		_, err := compileAt(t, dir, "bill.yaml")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "call:")
		assert.Contains(t, err.Error(), "not a module")
	})
	t.Run("oversized", func(t *testing.T) {
		t.Parallel()
		huge := idsModule + "# " + strings.Repeat("x", 8<<20) + "\n"
		dir := tree(t, map[string]string{"bill.yaml": workflowUsing(useIds, ""), "lib/ids.yaml": huge})
		_, err := compileAt(t, dir, "bill.yaml")
		require.Error(t, err)
	})
}

func TestUseHasNoPathWithoutALocation(t *testing.T) {
	t.Parallel()

	_, err := flowfile.Unmarshal([]byte(workflowUsing(useIds, "")))
	require.Error(t, err, "bytes with no file cannot resolve a relative path")
}

func TestUseRefusesACycle(t *testing.T) {
	t.Parallel()

	t.Run("itself", func(t *testing.T) {
		t.Parallel()
		dir := tree(t, map[string]string{
			"a.yaml": "edition: " + flowfile.CurrentEdition + "\nname: a\nuse:\n  me:\n    path: ./a.yaml\ntypes:\n  T:\n    type: string\n    must: this != \"\"\n",
			"b.yaml": workflowUsing("use:\n  a:\n    path: ./a.yaml\n", ""),
		})
		_, err := compileAt(t, dir, "b.yaml")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "leads back")
	})
	t.Run("through another", func(t *testing.T) {
		t.Parallel()
		dir := tree(t, map[string]string{
			"a.yaml": "edition: " + flowfile.CurrentEdition + "\nname: a\nuse:\n  b:\n    path: ./b.yaml\ntypes:\n  A:\n    type: string\n    must: this != \"\"\n",
			"b.yaml": "edition: " + flowfile.CurrentEdition + "\nname: b\nuse:\n  a:\n    path: ./a.yaml\ntypes:\n  B:\n    type: string\n    must: this != \"\"\n",
			"w.yaml": workflowUsing("use:\n  a:\n    path: ./a.yaml\n", ""),
		})
		_, err := compileAt(t, dir, "w.yaml")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "leads back")
	})
}

// chain writes modules m0..m{n-1} where m{i} uses m{i+1} and the last declares a
// type, and a workflow that uses m0.
func chain(t *testing.T, n int) string {
	t.Helper()

	files := map[string]string{"w.yaml": workflowUsing("use:\n  m:\n    path: ./m0.yaml\n", "")}
	for i := range n {
		body := "edition: " + flowfile.CurrentEdition + fmt.Sprintf("\nname: m%d\n", i)
		if i+1 < n {
			body += fmt.Sprintf("use:\n  m:\n    path: ./m%d.yaml\n", i+1)
		}
		body += fmt.Sprintf("types:\n  T%d:\n    type: string\n    must: this != \"\"\n", i)
		files[fmt.Sprintf("m%d.yaml", i)] = body
	}

	return tree(t, files)
}

func TestUseDepthIsBounded(t *testing.T) {
	t.Parallel()

	wf, err := compileAt(t, chain(t, v1.MaxUseDepth), "w.yaml")
	require.NoError(t, err, "four deep is allowed")
	assert.Len(t, wf.GetModules(), v1.MaxUseDepth)
	assert.NoError(t, v1.CheckModules(wf))

	_, err = compileAt(t, chain(t, v1.MaxUseDepth+1), "w.yaml")
	require.Error(t, err, "five deep is refused")
}

func TestUsesPerFileAreBounded(t *testing.T) {
	t.Parallel()

	build := func(n int) string {
		files := map[string]string{}
		var use strings.Builder
		use.WriteString("use:\n")
		for i := range n {
			fmt.Fprintf(&use, "  x%c%c:\n    path: ./m%d.yaml\n", 'a'+i/26, 'a'+i%26, i)
			files[fmt.Sprintf("m%d.yaml", i)] = "edition: " + flowfile.CurrentEdition + fmt.Sprintf("\nname: m%d\ntypes:\n  T:\n    type: string\n    must: this != \"\"\n", i)
		}
		files["w.yaml"] = workflowUsing(use.String(), "")

		return tree(t, files)
	}

	_, err := compileAt(t, build(v1.MaxUsesPerFile), "w.yaml")
	require.NoError(t, err)
	_, err = compileAt(t, build(v1.MaxUsesPerFile+1), "w.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the most a file uses")
}

func TestADiamondIsCompiledOnce(t *testing.T) {
	t.Parallel()

	leaf := "edition: " + flowfile.CurrentEdition + "\nname: leaf\ntypes:\n  Id:\n    type: string\n    must: this != \"\"\n"
	mid := func(name string) string {
		return "edition: " + flowfile.CurrentEdition + "\nname: " + name + "\nuse:\n  leaf:\n    path: ./leaf.yaml\ntypes:\n  X:\n    fields:\n      id: {type: leaf.Id}\n"
	}
	dir := tree(t, map[string]string{
		"leaf.yaml":  leaf,
		"left.yaml":  mid("left"),
		"right.yaml": mid("right"),
		"w.yaml":     workflowUsing("use:\n  left:\n    path: ./left.yaml\n  right:\n    path: ./right.yaml\n", ""),
	})
	wf, err := compileAt(t, dir, "w.yaml")
	require.NoError(t, err)
	assert.Contains(t, declaredNames(wf.GetDeclaredTypes()), "left.X")
	assert.Contains(t, declaredNames(wf.GetDeclaredTypes()), "right.X")
	assert.NoError(t, v1.CheckModules(wf))
}

func TestAnAliasIsAWordOfItsOwn(t *testing.T) {
	t.Parallel()

	for _, alias := range []string{"inputs", "steps", "math", "Ids", "my_ids", "now", "string", strings.Repeat("a", 40), "1ids"} {
		t.Run(alias, func(t *testing.T) {
			t.Parallel()
			dir := tree(t, map[string]string{
				"w.yaml":       workflowUsing(fmt.Sprintf("use:\n  %s:\n    path: ./lib/ids.yaml\n", alias), ""),
				"lib/ids.yaml": idsModule,
			})
			_, err := compileAt(t, dir, "w.yaml")
			require.Error(t, err)
		})
	}
}

func TestAnAliasMayNotBeAStepInputVarOrOutputOrFunction(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"step":     strings.Replace(workflowUsing(useIds, ""), "id: check", "id: ids", 1),
		"input":    workflowUsing(useIds, "inputs:\n  ids:\n    type: string\n"),
		"var":      workflowUsing(useIds, "vars:\n  ids: 1\n"),
		"function": workflowUsing(useIds, "functions:\n  ids:\n    params: {s: string}\n    returns: bool\n    body: ${true}\n"),
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})
			err := validateAt(t, dir, "w.yaml")
			require.Error(t, err)
		})
	}
}

func TestQualifiedNamesAreTheOnlyWayInAndMustResolve(t *testing.T) {
	t.Parallel()

	cases := map[string]struct{ extra, message string }{
		"bare type":        {"inputs:\n  c:\n    type: Customer\n", ""},
		"unresolved type":  {"inputs:\n  c:\n    type: ids.Custmer\n", "ids.Customer"},
		"unused alias":     {"inputs:\n  c:\n    type: other.Customer\n", ""},
		"own dotted type":  {"types:\n  ids.Mine:\n    type: string\n    must: this != \"\"\n", ""},
		"own dotted error": {"errors:\n  ids.Mine: {}\n", ""},
		"transitive name":  {"inputs:\n  c:\n    type: ids_x.Foo\n", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := tree(t, map[string]string{"w.yaml": workflowUsing(useIds, tc.extra), "lib/ids.yaml": idsModule})
			_, err := compileAt(t, dir, "w.yaml")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.message)
		})
	}

	t.Run("unresolved function suggests", func(t *testing.T) {
		t.Parallel()
		src := strings.Replace(workflowUsing(useIds, ""), "message: hi", "message: ${ids.isUuidd('x')}", 1)
		dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})
		_, err := compileAt(t, dir, "w.yaml")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ids.isUuid")
	})
	t.Run("unresolved error suggests", func(t *testing.T) {
		t.Parallel()
		src := strings.Replace(workflowUsing(useIds, ""), "    log:\n      message: hi\n", "    fail:\n      error: ids.NotFund\n", 1)
		dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})
		err := validateAt(t, dir, "w.yaml")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ids.NotFound")
	})
}

func TestTheSameBareNameUnderTwoAliasesIsTwoNames(t *testing.T) {
	t.Parallel()

	mod := func(name string) string {
		return "edition: " + flowfile.CurrentEdition + "\nname: " + name + "\ntypes:\n  Id:\n    type: string\n    must: this != \"\"\nerrors:\n  Bad: {}\nfunctions:\n  f:\n    params: {s: string}\n    returns: string\n    body: ${s + \"" + name + "\"}\n"
	}
	src := workflowUsing("use:\n  a:\n    path: ./a.yaml\n  b:\n    path: ./b.yaml\n", "inputs:\n  x:\n    type: a.Id\n  y:\n    type: b.Id\n")
	src = strings.Replace(src, "message: hi", `message: ${a.f("1") + b.f("2")}`, 1)
	dir := tree(t, map[string]string{"w.yaml": src, "a.yaml": mod("a"), "b.yaml": mod("b")})

	wf, err := compileAt(t, dir, "w.yaml")
	require.NoError(t, err)
	assert.Equal(t, []string{"a.f", "b.f"}, declaredNames(wf.GetDeclaredFunctions()))
	assert.Equal(t, []string{"a.Bad", "b.Bad"}, declaredNames(wf.GetDeclaredErrors()))
}

func TestAModulesOwnProblemsAreReportedAtTheUse(t *testing.T) {
	t.Parallel()

	bad := strings.Replace(idsModule, "returns: bool", "returns: bogus", 1)
	dir := tree(t, map[string]string{"w.yaml": workflowUsing(useIds, ""), "lib/ids.yaml": bad})
	_, err := compileAt(t, dir, "w.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ids.yaml")
}

func TestAHandBuiltSpecWithAnUnresolvedQualifiedNameIsRefused(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		DeclaredTypes: []*v1.TypeDeclaration{{Name: "ids.Uuid"}},
	}
	err := v1.CheckModules(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ids")

	wf = &v1.Workflow{
		Modules: []*v1.Module{{Alias: "ids", Source: "./x.yaml", SourceDigest: "not-a-digest"}},
	}
	require.Error(t, v1.CheckModules(wf))

	wf = &v1.Workflow{
		Modules: []*v1.Module{{Alias: "ids", Source: "./x.yaml"}, {Alias: "ids", Source: "./y.yaml"}},
	}
	require.Error(t, v1.CheckModules(wf), "an alias is recorded once")

	assert.NoError(t, v1.CheckModules(&v1.Workflow{}))
}

func TestModuleNames(t *testing.T) {
	t.Parallel()

	assert.True(t, v1.IsModuleName("ids.Uuid"))
	assert.True(t, v1.IsModuleName("a_b.Foo"))
	assert.False(t, v1.IsModuleName("Uuid"))
	assert.False(t, v1.IsModuleName("a.b.C"))
	assert.False(t, v1.IsModuleName(".Uuid"))
	assert.False(t, v1.IsModuleName("ids."))

	assert.NoError(t, v1.ValidModuleAlias("ids"))
	assert.NoError(t, v1.ValidModuleAlias("billing2"))
	for _, bad := range []string{"", "Ids", "a_b", "inputs", "math", "9a", "run", "outputs", "item", "state", "error", "failure", "payload", "event", "response", "this", "sender"} {
		assert.Error(t, v1.ValidModuleAlias(bad), bad)
	}
}
