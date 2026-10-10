package flowfile_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// coreModule and chainModule make a three-deep chain: bill.yaml uses lib/chain.yaml,
// which uses lib/core.yaml.
const coreModule = `edition: ` + flowfile.CurrentEdition + `
name: core
types:
  Id:
    type: string
    must: this != ""
`

const chainModule = `edition: ` + flowfile.CurrentEdition + `
name: chain
use:
  core:
    path: ./core.yaml
types:
  Order:
    fields:
      id:
        type: core.Id
        required: true
`

const useChain = "use:\n  chain:\n    path: ./lib/chain.yaml\n"

func chainTree(t *testing.T) string {
	t.Helper()

	return tree(t, map[string]string{
		"bill.yaml":      workflowUsing(useChain, "inputs:\n  order:\n    type: chain.Order\n    required: true\n"),
		"lib/chain.yaml": chainModule,
		"lib/core.yaml":  coreModule,
	})
}

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644))
}

// problems is what validating a file reports, as one comparable string.
func reported(ds flowfile.Diagnostics, err error) string {
	if err != nil {
		return "error: " + err.Error()
	}

	return ds.Error()
}

// The cache may only ever save work: after every edit in a sequence that adds and
// removes comments, reformats, changes an interface, breaks and repairs a module,
// pins and unpins, a validation through one long-lived cache reports exactly what a
// validation with none does.
func TestModuleCacheReportsWhatNoCacheReports(t *testing.T) {
	t.Parallel()

	pinned := func(digest string) string {
		return workflowUsing("use:\n  chain:\n    path: ./lib/chain.yaml\n    digest: "+digest+"\n", "")
	}
	steps := []struct {
		name  string
		files map[string]string
	}{
		{"as written", nil},
		{"a comment in the leaf", map[string]string{"lib/core.yaml": "# a note\n" + coreModule}},
		{"the leaf reformatted", map[string]string{"lib/core.yaml": strings.Replace(coreModule, "    type: string\n", "    type:   string\n", 1)}},
		{"the leaf's interface changes", map[string]string{"lib/core.yaml": strings.Replace(coreModule, `this != ""`, `size(this) > 3`, 1)}},
		{"the leaf breaks", map[string]string{"lib/core.yaml": strings.Replace(coreModule, "type: string", "type: bogus", 1)}},
		{"the leaf is repaired", map[string]string{"lib/core.yaml": coreModule}},
		{"the middle drops a type", map[string]string{"lib/chain.yaml": strings.Replace(chainModule, "core.Id", "string", 1)}},
		{"the middle is restored", map[string]string{"lib/chain.yaml": chainModule}},
		{"a wrong pin", map[string]string{"bill.yaml": pinned("sha256:" + strings.Repeat("0", 64))}},
		{"the right pin", map[string]string{"bill.yaml": pinned(v1.ContentDigest([]byte(chainModule)))}},
		{"the pinned module is reformatted", map[string]string{"lib/chain.yaml": "# moved\n" + chainModule}},
		{"the leaf uses the workflow", map[string]string{"lib/core.yaml": "edition: " + flowfile.CurrentEdition + "\nname: core\nuse:\n  chain:\n    path: ../lib/chain.yaml\n"}},
		{"the leaf is gone", map[string]string{"lib/core.yaml": ""}},
	}

	dir := chainTree(t)
	cache := flowfile.NewModuleCache(0, 0)
	for _, step := range steps {
		for name, content := range step.files {
			write(t, dir, name, content)
		}
		for _, file := range []string{"bill.yaml", "lib/chain.yaml", "lib/core.yaml"} {
			path := filepath.Join(dir, file)
			want := reported(flowfile.ValidateSourceFile(path))
			// Twice, so the second call is the warm one.
			assert.Equal(t, want, reported(cache.ValidateSourceFile(path)), "%s: %s (cold)", step.name, file)
			assert.Equal(t, want, reported(cache.ValidateSourceFile(path)), "%s: %s (warm)", step.name, file)
		}
	}
	assert.Positive(t, cache.Stats().Hits, "the sequence never used the cache, so it proved nothing")
}

func TestModuleCacheDoesNotFanOutAFormattingEdit(t *testing.T) {
	t.Parallel()

	dir := chainTree(t)
	path := filepath.Join(dir, "bill.yaml")
	cache := flowfile.NewModuleCache(0, 0)

	ds, err := cache.ValidateSourceFile(path)
	require.NoError(t, err)
	require.Empty(t, ds)
	assert.Equal(t, 2, cache.Stats().Compiles, "chain and core, each once")

	_, err = cache.ValidateSourceFile(path)
	require.NoError(t, err)
	assert.Equal(t, 2, cache.Stats().Compiles, "nothing changed")

	// Comments and layout: core's bytes change and what it declares does not.
	before := cache.Stats().Compiles
	write(t, dir, "lib/core.yaml", "# who owns ids\n"+strings.Replace(coreModule, "    type: string\n", "    type:   string  # text\n", 1))
	ds, err = cache.ValidateSourceFile(path)
	require.NoError(t, err)
	require.Empty(t, ds)
	assert.Equal(t, 1, cache.Stats().Compiles-before, "only the module that was edited is compiled; its dependent is not")

	// What it declares: both are compiled, and the dependent now sees the change.
	before = cache.Stats().Compiles
	write(t, dir, "lib/core.yaml", strings.Replace(coreModule, `this != ""`, `size(this) > 3`, 1))
	_, err = cache.ValidateSourceFile(path)
	require.NoError(t, err)
	assert.Equal(t, 2, cache.Stats().Compiles-before, "an interface edit reaches the module that uses it")
}

func TestModuleCacheInterfaceIgnoresTheFileAndKeepsTheDeclarations(t *testing.T) {
	t.Parallel()

	dir := chainTree(t)
	core := filepath.Join(dir, "lib/core.yaml")
	cache := flowfile.NewModuleCache(0, 0)

	first := cache.Interface(core)
	require.NotEmpty(t, first)
	assert.Equal(t, first, cache.Interface(core))

	write(t, dir, "lib/core.yaml", "# note\n"+strings.Replace(coreModule, "name: core", "name:   core", 1))
	assert.Equal(t, first, cache.Interface(core), "comments and layout are not the interface")

	write(t, dir, "lib/core.yaml", strings.Replace(coreModule, `this != ""`, `size(this) > 3`, 1))
	changed := cache.Interface(core)
	require.NotEmpty(t, changed)
	assert.NotEqual(t, first, changed)

	write(t, dir, "lib/core.yaml", strings.Replace(coreModule, "type: string", "type: bogus", 1))
	assert.Empty(t, cache.Interface(core), "a module that does not compile has no interface, which a caller reads as changed")

	var none *flowfile.ModuleCache
	assert.Empty(t, none.Interface(core))
}

func TestModuleCacheReportsAModulesErrorsOnceInEachImporter(t *testing.T) {
	t.Parallel()

	broken := strings.Replace(idsModule, "type: Uuid", "type: Missing", 1)
	broken = strings.Replace(broken, "returns: bool", "returns: bogus", 1)
	dir := tree(t, map[string]string{
		"a.yaml":       workflowUsing(useIds, "inputs:\n  c:\n    type: ids.Customer\n"),
		"b.yaml":       workflowUsing(useIds, ""),
		"lib/ids.yaml": broken,
	})
	cache := flowfile.NewModuleCache(0, 0)

	_, err := cache.ValidateSourceFile(filepath.Join(dir, "lib/ids.yaml"))
	own, isDiagnostics := errors.AsType[flowfile.Diagnostics](err)
	require.True(t, isDiagnostics)
	require.Len(t, own, 2, "the module reports its own errors in the module")

	for _, file := range []string{"a.yaml", "b.yaml", "a.yaml"} {
		_, err := cache.ValidateSourceFile(filepath.Join(dir, file))
		require.Error(t, err)
		text := err.Error()
		assert.Equal(t, 1, strings.Count(text, "which has"), "%s: once, at the use: line, not once per error", file)
		assert.Contains(t, text, `uses "./lib/ids.yaml", which has 2 errors; first: `)
	}

	// The cache holds no failure: it is repaired the moment the file is.
	write(t, dir, "lib/ids.yaml", idsModule)
	ds, err := cache.ValidateSourceFile(filepath.Join(dir, "a.yaml"))
	require.NoError(t, err)
	assert.Empty(t, ds)
}

func TestModuleCacheIsBounded(t *testing.T) {
	t.Parallel()

	files := map[string]string{}
	var uses strings.Builder
	uses.WriteString("use:\n")
	for i := range 6 {
		files[fmt.Sprintf("lib/m%d.yaml", i)] = strings.Replace(coreModule, "name: core", fmt.Sprintf("name: m%d", i), 1)
		fmt.Fprintf(&uses, "  m%d:\n    path: ./lib/m%d.yaml\n", i, i)
	}
	files["bill.yaml"] = workflowUsing(uses.String(), "")
	dir := tree(t, files)
	path := filepath.Join(dir, "bill.yaml")

	t.Run("entries", func(t *testing.T) {
		t.Parallel()
		cache := flowfile.NewModuleCache(2, 0)
		ds, err := cache.ValidateSourceFile(path)
		require.NoError(t, err)
		assert.Empty(t, ds)
		stats := cache.Stats()
		assert.Equal(t, 2, stats.Entries)
		assert.Equal(t, 4, stats.Evictions)
		assert.Equal(t, reported(flowfile.ValidateSourceFile(path)), reported(cache.ValidateSourceFile(path)), "a cache too small to help still reports the same")
	})
	t.Run("bytes", func(t *testing.T) {
		t.Parallel()
		unbounded := flowfile.NewModuleCache(0, 0)
		_, err := unbounded.ValidateSourceFile(path)
		require.NoError(t, err)
		each := unbounded.Stats().Bytes / unbounded.Stats().Entries

		cache := flowfile.NewModuleCache(0, 3*each)
		_, err = cache.ValidateSourceFile(path)
		require.NoError(t, err)
		assert.LessOrEqual(t, cache.Stats().Bytes, 3*each)
		assert.LessOrEqual(t, cache.Stats().Entries, 3)
		assert.Positive(t, cache.Stats().Evictions)
	})
	t.Run("one module larger than the cache", func(t *testing.T) {
		t.Parallel()
		cache := flowfile.NewModuleCache(0, 1)
		ds, err := cache.ValidateSourceFile(path)
		require.NoError(t, err)
		assert.Empty(t, ds)
		assert.Zero(t, cache.Stats().Entries)
		assert.Zero(t, cache.Stats().Bytes)
	})
}

func TestModuleCacheIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	dir := chainTree(t)
	path := filepath.Join(dir, "bill.yaml")
	cache := flowfile.NewModuleCache(2, 0)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for range 10 {
				ds, err := cache.ValidateSourceFile(path)
				assert.NoError(t, err)
				assert.Empty(t, ds)
				assert.NotEmpty(t, cache.Interface(filepath.Join(dir, "lib/core.yaml")), "worker %d", i)
				_ = cache.Stats()
			}
		})
	}
	wg.Wait()
	assert.LessOrEqual(t, cache.Stats().Entries, 2)
}

// A cycle through a module the cache already holds is still a cycle.
func TestModuleCacheStillFindsACycle(t *testing.T) {
	t.Parallel()

	dir := chainTree(t)
	path := filepath.Join(dir, "bill.yaml")
	cache := flowfile.NewModuleCache(0, 0)
	_, err := cache.ValidateSourceFile(path)
	require.NoError(t, err)

	write(t, dir, "lib/core.yaml", coreModule+"use:\n  chain:\n    path: ./chain.yaml\n")
	want := reported(flowfile.ValidateSourceFile(path))
	require.Contains(t, want, "leads back")
	assert.Equal(t, want, reported(cache.ValidateSourceFile(path)))
}
