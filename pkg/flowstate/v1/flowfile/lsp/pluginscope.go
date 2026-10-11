package lsp

import (
	"context"
	"fmt"
	"sync"

	lsp "github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// PluginLock is the plugin tasks one plugins.lock.json declares, as the
// [FlowfileServer.PluginCatalog] hook reads them. It carries descriptors only.
type PluginLock struct {
	// Path identifies the lock file, and Version its content (a digest of the
	// bytes the Tasks were read from). Two documents under the same Path and
	// Version share one registry; a lock edited between two opens is a new
	// version, so opening a document after the edit does not change the answers
	// of one opened before it.
	Path    string
	Version string

	// Tasks are the tasks the lock declares.
	Tasks []v1.TaskDef
}

// MaxPluginLocks bounds how many distinct (lock, version) registries one server
// retains at once. A registry is released when the last open document using it
// closes, so the bound is on what is open together, not on what was ever seen.
const MaxPluginLocks = 8

// pluginScopes owns the per-lock registries of one server.
//
// A document is diagnosed, completed and hovered against the registry of the lock
// that governs it: the server's base registry plus that lock's tasks, built with
// the one [v1.Registry] type and handed to the document the way the base is. No
// document's lock is registered anywhere another document can read it.
type pluginScopes struct {
	mu      sync.Mutex
	entries map[string]*pluginScope
	byDoc   map[lsp.DocumentURI]string
}

type pluginScope struct {
	registry *v1.Registry
	refs     int
}

// acquire returns the registry for lock on behalf of uri, building it from base
// the first time. It fails when the registry cannot be built or when the bound
// is reached, and the caller keeps uri on base.
func (p *pluginScopes) acquire(uri lsp.DocumentURI, base *v1.Registry, lock *PluginLock) (*v1.Registry, error) {
	key := lock.Path + "\x00" + lock.Version

	p.mu.Lock()
	defer p.mu.Unlock()
	p.releaseLocked(uri)

	entry := p.entries[key]
	if entry == nil {
		if len(p.entries) >= MaxPluginLocks {
			return nil, fmt.Errorf("%s is not used: %d plugin locks are already in use by open documents, and "+
				"each is held in its own registry; close documents under another lock first",
				lock.Path, MaxPluginLocks)
		}
		registry, err := scopedRegistry(base, lock.Tasks)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", lock.Path, err)
		}
		entry = &pluginScope{registry: registry}
		if p.entries == nil {
			p.entries = map[string]*pluginScope{}
			p.byDoc = map[lsp.DocumentURI]string{}
		}
		p.entries[key] = entry
		setOverlay(registry, lock.Tasks)
	}
	entry.refs++
	p.byDoc[uri] = key

	return entry.registry, nil
}

// registryOf is the registry uri was last given, or nil.
func (p *pluginScopes) registryOf(uri lsp.DocumentURI) *v1.Registry {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.entries[p.byDoc[uri]]; entry != nil {
		return entry.registry
	}

	return nil
}

func (p *pluginScopes) release(uri lsp.DocumentURI) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.releaseLocked(uri)
}

func (p *pluginScopes) releaseLocked(uri lsp.DocumentURI) {
	key, ok := p.byDoc[uri]
	if !ok {
		return
	}
	delete(p.byDoc, uri)
	entry := p.entries[key]
	if entry == nil {
		return
	}
	if entry.refs--; entry.refs <= 0 {
		delete(p.entries, key)
		clearOverlay(entry.registry)
	}
}

// scopedRegistry is base plus tasks, in a new registry. A lock may not replace a
// task base already holds: [v1.Registry.Register] refuses a duplicate, so a lock
// cannot redefine a built-in or a task the host launched.
func scopedRegistry(base *v1.Registry, tasks []v1.TaskDef) (*v1.Registry, error) {
	registry := v1.NewRegistry()
	for _, def := range base.All() {
		if err := registry.Register(def); err != nil {
			return nil, err
		}
	}
	for _, def := range tasks {
		if err := registry.Register(def); err != nil {
			return nil, fmt.Errorf("registering task %q: %w", def.Name, err)
		}
	}

	return registry, nil
}

// tasksFor is the registry uri's answers come from: its lock's, or the base.
func (s *FlowfileServer) tasksFor(uri lsp.DocumentURI) *v1.Registry {
	if registry := s.scopes.registryOf(uri); registry != nil {
		return registry
	}

	return s.tasks()
}

// discoverPluginCatalog runs the [FlowfileServer.PluginCatalog] hook for one
// document, gives the document the registry of the lock it found, and says so
// loudly when that fails. It returns the registry the document is built from.
//
// Failure closes: a lock that is unreadable, invalid, or past the bound leaves
// the document on the base registry, where its plugin tasks read as unknown,
// rather than on a registry some other document's lock taught.
func (s *FlowfileServer) discoverPluginCatalog(ctx context.Context, conn *jsonrpc2.Conn, uri lsp.DocumentURI) *v1.Registry {
	s.scopes.release(uri)
	base := s.tasks()
	if s.PluginCatalog == nil {
		return base
	}
	path, ok := (&document{uri: uri}).filesystemPath()
	if !ok {
		return base
	}
	lock, err := s.PluginCatalog(path)
	if err == nil && lock != nil {
		var registry *v1.Registry
		if registry, err = s.scopes.acquire(uri, base, lock); err == nil {
			return registry
		}
	}
	if err == nil {
		return base
	}
	s.logger().Error("plugin catalog next to a document could not be used", "uri", uri, "error", err)
	if nerr := conn.Notify(ctx, "window/showMessage", lsp.ShowMessageParams{
		Type:    lsp.MTWarning,
		Message: "Plugin catalog not used, so plugin tasks may read as unknown: " + err.Error(),
	}); nerr != nil {
		s.logger().Warn("showing plugin catalog message failed", "error", nerr)
	}

	return base
}

// The compiler (package flowfile) resolves task names against
// [v1.DefaultRegistry]; it takes no registry. A document whose registry holds
// lock tasks therefore compiles with those tasks added to the default registry
// for the duration of the call and removed after, under a lock that excludes
// every other compile in this package. Documents with no lock share the lock for
// reading, so they see only what the default registry holds.
//
// Nothing else in `flow lsp` reads the default registry outside these calls, so
// the added tasks are visible to this document's compile alone.
var (
	compileMu sync.RWMutex
	overlays  sync.Map // *v1.Registry -> []v1.TaskDef
)

func setOverlay(registry *v1.Registry, tasks []v1.TaskDef) {
	if len(tasks) > 0 {
		overlays.Store(registry, tasks)
	}
}

func clearOverlay(registry *v1.Registry) { overlays.Delete(registry) }

// overlayOf is the lock tasks registry adds to the compiler's view.
func overlayOf(registry *v1.Registry) []v1.TaskDef {
	tasks, _ := overlays.Load(registry)
	defs, _ := tasks.([]v1.TaskDef)

	return defs
}

// underDoc runs compile with doc's lock tasks visible to the compiler. It, and
// [underBase], are the only way this package reaches the compiler or any other
// reader of the default registry; TestNoCompilerCallEscapesTheRegistryScope
// holds the rest of the package to that.
//
// The tasks are the document's own (see [document.lock]), copied from the scope
// when the document was built, so a request holding a document that has since
// been closed still compiles it as it was opened.
//
// Not re-entrant: compile must not call underDoc or underBase.
func underDoc[T any](doc *document, compile func() T) T {
	return underLock(doc.lock, compile)
}

// underBase is [underDoc] for work that belongs to no document (a module's
// interface, read through the shared byte-keyed cache): it sees the default
// registry as the host left it and never an overlay.
func underBase[T any](compile func() T) T {
	return underLock(nil, compile)
}

func underLock[T any](defs []v1.TaskDef, compile func() T) T {
	if len(defs) == 0 {
		compileMu.RLock()
		defer compileMu.RUnlock()

		return compile()
	}

	// compileMu first, then the registry's own compound-mutation lock, always in
	// that order.
	compileMu.Lock()
	defer compileMu.Unlock()
	defer v1.LockDefaultRegistry()()
	defaults := v1.DefaultRegistry()
	for _, def := range defs {
		prior, had := defaults.Lookup(def.Name)
		if err := defaults.Replace(def); err != nil {
			// Registered into this document's registry already, so the same
			// validation passed once; a failure leaves the task unknown.
			continue
		}
		defer func() {
			if had {
				_ = defaults.Replace(prior)
			} else {
				defaults.Unregister(def.Name)
			}
		}()
	}

	return compile()
}
