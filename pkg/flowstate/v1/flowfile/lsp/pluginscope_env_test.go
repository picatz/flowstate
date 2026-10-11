package lsp

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// registryFingerprint is everything observable about the process default
// registry that a document's plugin lock could disturb: every task's name,
// summary, input and output descriptors and the identity of its function.
func registryFingerprint() string {
	var b strings.Builder
	for _, def := range v1.DefaultRegistry().All() {
		fmt.Fprintf(&b, "%s|%s|%t|%t|%x\n", def.Name, def.Summary, def.Inputs == nil, def.Outputs == nil, reflect.ValueOf(def.Fn).Pointer())
	}

	return b.String()
}

// Two documents under different locks, hammered through every feature at once,
// never put a lock's task into the process default registry, at any moment and
// afterwards, and each is still answered from its own lock. Not parallel, so no
// other test registers into the default registry while it is compared; run it
// under -race.
func TestLockedFeaturesNeverTouchTheDefaultRegistry(t *testing.T) {
	l := newLocks(t)
	l.set("a", "1")
	l.set("b", "1")
	l.by["a"].Tasks = []v1.TaskDef{lockTaskLike(t, "alpha.run", "http")}
	l.by["b"].Tasks = []v1.TaskDef{lockTaskLike(t, "beta.run", "http")}
	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), PluginCatalog: l.hook})
	c.initialize()

	before := registryFingerprint()
	require.NotContains(t, before, "alpha.run")
	require.NotContains(t, before, "beta.run")
	probe, ok := v1.LookupTask("log")
	require.True(t, ok)

	sources := map[string]string{
		uriIn("a"): lockFeaturesSource,
		uriIn("b"): strings.ReplaceAll(lockFeaturesSource, "alpha.run", "beta.run"),
	}
	for uri, src := range sources {
		own := "alpha.run"
		if strings.Contains(uri, "/b/") {
			own = "beta.run"
		}
		got := c.open(uri, src)
		require.False(t, unknownTask(got, own), "premise: %s knows its own lock's task: %v", uri, got.Diagnostics)
	}

	var (
		stop    atomic.Bool
		drifted atomic.Value
		sampler sync.WaitGroup
	)
	sampler.Go(func() {
		for !stop.Load() {
			if now := registryFingerprint(); now != before {
				drifted.Store(now)

				return
			}
			for _, name := range []string{"alpha.run", "beta.run"} {
				if _, leaked := v1.LookupTask(name); leaked {
					drifted.Store(name + " is in the default registry")

					return
				}
			}
			time.Sleep(time.Millisecond)
		}
	})

	var workers sync.WaitGroup
	for uri, src := range sources {
		doc, ok := c.rawServerDoc(uri)
		require.True(t, ok)
		for range 2 {
			workers.Go(func() {
				for range 6 {
					idAt := positionOf(t, src, "id: pre", len("id: "))
					loopItem := positionOf(t, src, "${n}", 2)
					entry := positionOf(t, src, "r.greet.status_code", 0)
					c.hover(uri, loopItem.Line, loopItem.Character)
					c.hover(uri, entry.Line, entry.Character)
					c.complete(uri, entry.Line, entry.Character)
					c.format(uri)
					c.symbols(uri)
					c.definition(uri, idAt.Line, idAt.Character)
					c.codeAction(uri, wholeOf(src), nil, nil)
					_, _ = renameAt(doc, idAt, "renamed")
					inlayHints(doc, wholeDocument())
					diagnoseCarried(doc)
				}
			})
		}
	}
	workers.Wait()
	stop.Store(true)
	sampler.Wait()

	assert.Nil(t, drifted.Load(), "the default registry changed while locked documents were answered")
	assert.Equal(t, before, registryFingerprint(), "the default registry changed after locked documents were answered")
	after, ok := v1.LookupTask("log")
	require.True(t, ok)
	assert.Equal(t, probe.Summary, after.Summary)
	assert.Equal(t, reflect.ValueOf(probe.Fn).Pointer(), reflect.ValueOf(after.Fn).Pointer())

	// And the answers were each document's own.
	a, _ := c.rawServerDoc(uriIn("a"))
	b, _ := c.rawServerDoc(uriIn("b"))
	_, aHasOwn := a.tasks.Lookup("alpha.run")
	_, aHasForeign := a.tasks.Lookup("beta.run")
	_, bHasOwn := b.tasks.Lookup("beta.run")
	_, bHasForeign := b.tasks.Lookup("alpha.run")
	assert.True(t, aHasOwn && bHasOwn)
	assert.False(t, aHasForeign || bHasForeign, "a document's registry holds another lock's task")
	assert.NotSame(t, a.tasks, b.tasks)
}

// The shared module cache is keyed by the registry a module was compiled
// against: the same bytes under another lock, or under none, are compiled again
// rather than served from a registry that is not the document's, and a second
// document under the same registry does share. Not parallel: it reads the
// process-wide cache's counters.
func TestTheModuleCacheDoesNotShareAcrossRegistries(t *testing.T) {
	dir := moduleTree(t)
	// Bytes unique to this test, so the cache holds nothing for them yet.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib", "ids.yaml"), []byte(usedModuleSource+"# registry key "+dir+"\n"), 0o644))

	base := (&FlowfileServer{}).tasks()
	regA, err := scopedRegistry(base, []v1.TaskDef{lockTask(t, "alpha.run")})
	require.NoError(t, err)
	regB, err := scopedRegistry(base, []v1.TaskDef{lockTask(t, "beta.run")})
	require.NoError(t, err)

	compile := func(name, text string, registry *v1.Registry) (compiles, hits int) {
		t.Helper()
		before := moduleCache.Stats()
		doc := newDocument(fileURI(filepath.Join(dir, name)), 1, text, registry)
		diagnoseCarried(doc)
		after := moduleCache.Stats()

		return after.Compiles - before.Compiles, after.Hits - before.Hits
	}

	compiles, _ := compile("bill.yaml", useSource, regA)
	require.Positive(t, compiles, "premise: the first compile of these bytes goes through the cache")

	// Same registry, same bytes: shared.
	compiles, hits := compile("other.yaml", otherImporterSource, regA)
	assert.Zero(t, compiles, "a second document under the same registry recompiled the module")
	assert.Positive(t, hits, "a second document under the same registry did not share the module")

	// Other registries, same bytes: each compiles for itself.
	compiles, _ = compile("bill.yaml", useSource, regB)
	assert.Positive(t, compiles, "a module compiled against one lock's registry was served to another's")
	compiles, _ = compile("bill.yaml", useSource, nil)
	assert.Positive(t, compiles, "a module compiled against a lock's registry was served to the base registry")

	// And now each of them is a hit for itself.
	for _, registry := range []*v1.Registry{regA, regB, nil} {
		compiles, hits = compile("bill.yaml", useSource, registry)
		assert.Zero(t, compiles)
		assert.Positive(t, hits)
	}
}
