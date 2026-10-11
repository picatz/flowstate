package lsp

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	lsp "github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// These tests are about which registry a document is answered from when
// several workspaces, each with its own plugins.lock.json, are open in one
// server (#2656). The hook is faked: reading a lock is the command's, and the
// registry a lock's tasks end up in is this package's.

// lockTask is a plugin task borrowed from the log task's shape, named name.
func lockTask(t *testing.T, name string) v1.TaskDef {
	t.Helper()

	return lockTaskLike(t, name, "log")
}

// lockTaskLike is a plugin task with the inputs and outputs of the built-in
// task like.
func lockTaskLike(t *testing.T, name, like string) v1.TaskDef {
	t.Helper()

	shape, ok := v1.LookupTask(like)
	require.True(t, ok)

	return v1.TaskDef{Name: name, Summary: "Task " + name + ".", Inputs: shape.Inputs, Outputs: shape.Outputs, Fn: shape.Fn}
}

// locks is a fake lock directory: a document under /work/<repo>/ is governed by
// that repo's lock, whose tasks and version a test changes between opens.
type locks struct {
	t  *testing.T
	mu sync.Mutex
	by map[string]*PluginLock
}

func newLocks(t *testing.T) *locks { return &locks{t: t, by: map[string]*PluginLock{}} }

func (l *locks) set(repo, version string, tasks ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lock := &PluginLock{Path: "/work/" + repo + "/plugins.lock.json", Version: version}
	for _, name := range tasks {
		lock.Tasks = append(lock.Tasks, lockTask(l.t, name))
	}
	l.by[repo] = lock
}

func (l *locks) hook(path string) (*PluginLock, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	repo, _, _ := strings.Cut(strings.TrimPrefix(path, "/work/"), "/")
	if repo == "broken" {
		return nil, fmt.Errorf("%s: not a plugin catalog", path)
	}

	return l.by[repo], nil
}

// closeAndWait closes uri and waits for the empty diagnostics a close publishes,
// so the next publish a test reads is its own.
func closeAndWait(c *client, uri string) {
	wait := c.expectPublish()
	c.closeNoWait(uri)
	c.await(wait)
}

func uriIn(repo string) string { return "file:///work/" + repo + "/flow.yaml" }

func usesTask(task string) string {
	return "name: uses\nsteps:\n  - id: s\n    " + task + ":\n      message: hi\nedition: v2026.4\n"
}

func unknownTask(p lsp.PublishDiagnosticsParams, task string) bool {
	for _, d := range p.Diagnostics {
		if strings.Contains(d.Message, `"`+task+`"`) {
			return true
		}
	}

	return false
}

var offerVersion = 1000

func offers(t *testing.T, c *client, uri, task string) bool {
	t.Helper()
	src, pos := splitCursor(t, "name: c\nsteps:\n  - |\n")
	offerVersion++
	c.changeNoWait(uri, src, offerVersion)

	return slices.Contains(labels(c.complete(uri, pos.Line, pos.Character).Items), task)
}

// Each document is diagnosed, completed and hovered against its own lock, in
// both directions: A's task is absent from B and B's from A.
func TestADocumentSeesOnlyItsOwnLocksPluginTasks(t *testing.T) {
	t.Parallel()

	l := newLocks(t)
	l.set("a", "1", "alpha.run")
	l.set("b", "1", "beta.run")
	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), PluginCatalog: l.hook})
	c.initialize()

	a, b := uriIn("a"), uriIn("b")

	own := c.open(a, usesTask("alpha.run"))
	assert.False(t, unknownTask(own, "alpha.run"), "a document's own lock task is unknown: %v", own.Diagnostics)
	foreign := c.open(b, usesTask("alpha.run"))
	assert.True(t, unknownTask(foreign, "alpha.run"), "a task from another workspace's lock was accepted: %v", foreign.Diagnostics)

	ownB := c.open(uriIn("b")+"2", usesTask("beta.run"))
	assert.False(t, unknownTask(ownB, "beta.run"), "%v", ownB.Diagnostics)
	foreignA := c.open(uriIn("a")+"2", usesTask("beta.run"))
	assert.True(t, unknownTask(foreignA, "beta.run"), "B's task leaked into A: %v", foreignA.Diagnostics)

	assert.True(t, offers(t, c, a, "alpha.run"))
	assert.False(t, offers(t, c, a, "beta.run"), "completion in A offered B's task")
	assert.True(t, offers(t, c, b, "beta.run"))
	assert.False(t, offers(t, c, b, "alpha.run"), "completion in B offered A's task")

	c.changeNoWait(a, usesTask("alpha.run"), 5000)
	c.changeNoWait(b, usesTask("alpha.run"), 5000)
	assert.Contains(t, hoverText(c.hover(a, 3, 6)), "Task alpha.run.", "hover in A does not describe its own task")
	assert.NotContains(t, hoverText(c.hover(b, 3, 6)), "Task alpha.run.", "hover in B described A's task")
}

// What happens to A (it is closed, its lock edited and reopened) does not change
// B, and a task removed from a lock is unknown to the document reopened under it
// without restarting the server.
func TestClosingOrChangingOneLocksDoesNotChangeAnotherDocument(t *testing.T) {
	t.Parallel()

	l := newLocks(t)
	l.set("a", "1", "alpha.run")
	l.set("b", "1", "beta.run")
	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), PluginCatalog: l.hook})
	c.initialize()

	a, b := uriIn("a"), uriIn("b")
	require.False(t, unknownTask(c.open(a, usesTask("alpha.run")), "alpha.run"))
	require.False(t, unknownTask(c.open(b, usesTask("beta.run")), "beta.run"))

	// A closes; B is untouched.
	closeAndWait(c, a)
	after := c.change(b, usesTask("beta.run"), 2)
	assert.False(t, unknownTask(after, "beta.run"), "closing A changed B: %v", after.Diagnostics)

	// A's lock loses the task and A is reopened: the task is unknown there, B
	// still has its own, and an edit to B (a fresh compile) still does.
	l.set("a", "2")
	reopened := c.open(a, usesTask("alpha.run"))
	assert.True(t, unknownTask(reopened, "alpha.run"), "a task removed from the lock survived a reopen: %v", reopened.Diagnostics)
	edited := c.change(b, usesTask("beta.run")+"# edit\n", 3)
	assert.False(t, unknownTask(edited, "beta.run"), "changing A's lock changed B: %v", edited.Diagnostics)

	// A lock edited under a document still open does not change that document
	// until it is reopened: the lock is read when a document opens.
	l.set("b", "2", "gamma.run")
	stale := c.change(b, usesTask("beta.run"), 4)
	assert.False(t, unknownTask(stale, "beta.run"), "an open document followed a lock edit without a reopen: %v", stale.Diagnostics)

	// A lock's tasks reach no one through the process: the default registry is
	// exactly what it was.
	for _, name := range []string{"alpha.run", "beta.run", "gamma.run"} {
		compileMu.RLock() // another test's compile may hold its overlay right now
		_, known := v1.DefaultRegistry().Lookup(name)
		compileMu.RUnlock()
		assert.False(t, known, "%s was left in the default registry", name)
	}
}

// A lock that cannot be used fails closed: the document is on the base registry,
// where plugin tasks are unknown, and the person is told.
func TestAnUnusableLockLeavesTheDocumentOnTheBaseRegistry(t *testing.T) {
	t.Parallel()

	l := newLocks(t)
	l.set("a", "1", "alpha.run")
	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), PluginCatalog: l.hook})
	c.initialize()

	_ = c.open(uriIn("a"), usesTask("alpha.run"))
	got := c.open(uriIn("broken"), usesTask("alpha.run"))
	assert.True(t, unknownTask(got, "alpha.run"), "a document with a bad lock inherited another's tasks: %v", got.Diagnostics)

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Equal(t, 1, c.notified["window/showMessage"])
}

// The registries kept are bounded, per distinct lock in use, and a closed
// document frees its place.
func TestTheNumberOfRetainedLockRegistriesIsBounded(t *testing.T) {
	t.Parallel()

	l := newLocks(t)
	for i := range MaxPluginLocks + 1 {
		l.set(fmt.Sprint("r", i), "1", fmt.Sprintf("p%d.run", i))
	}
	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), PluginCatalog: l.hook})
	c.initialize()

	for i := range MaxPluginLocks {
		got := c.open(uriIn(fmt.Sprint("r", i)), usesTask(fmt.Sprintf("p%d.run", i)))
		require.False(t, unknownTask(got, fmt.Sprintf("p%d.run", i)), "lock %d: %v", i, got.Diagnostics)
	}
	// A second document under a lock already held costs no new registry.
	shared := c.open(uriIn("r0")+"2", usesTask("p0.run"))
	assert.False(t, unknownTask(shared, "p0.run"), "%v", shared.Diagnostics)

	over := fmt.Sprint("r", MaxPluginLocks)
	task := fmt.Sprintf("p%d.run", MaxPluginLocks)
	got := c.open(uriIn(over), usesTask(task))
	assert.True(t, unknownTask(got, task), "a lock past the bound was used: %v", got.Diagnostics)
	c.mu.Lock()
	assert.Equal(t, 1, c.notified["window/showMessage"], "a lock past the bound was refused silently")
	c.mu.Unlock()

	// r0 is still held by its second document, so closing one frees nothing...
	closeAndWait(c, uriIn(over))
	closeAndWait(c, uriIn("r0"))
	got = c.open(uriIn(over), usesTask(task))
	assert.True(t, unknownTask(got, task), "a registry still in use was released: %v", got.Diagnostics)
	// ...and closing the last user of one does.
	closeAndWait(c, uriIn(over))
	closeAndWait(c, uriIn("r0")+"2")
	got = c.open(uriIn(over), usesTask(task))
	assert.False(t, unknownTask(got, task), "a released registry kept its place: %v", got.Diagnostics)
}
