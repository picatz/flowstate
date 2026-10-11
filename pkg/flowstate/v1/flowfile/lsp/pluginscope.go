package lsp

import (
	"context"
	"fmt"
	"sync"

	lsp "github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
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
// the one [v1.Registry] type and handed to the document the way the base is. The
// document turns it into its own [flowfile.Env] (see [document.env]), so the
// compiler resolves task names against that registry and no other. No document's
// lock is registered anywhere another document can read it, and nothing here
// touches [v1.DefaultRegistry].
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

// defaultTasks is the one place this package names the process default registry:
// the registry a server answers from when [FlowfileServer.Tasks] is nil, and a
// document built with no registry. TestNoRegistryReaderEscapesTheDocumentEnv holds
// every other file to that.
func defaultTasks() *v1.Registry { return v1.DefaultRegistry() }

// baseEnv is the [flowfile.Env] for work that belongs to no document: a module's
// interface, which the server remembers per path. It resolves against the
// server's base registry, never a document's lock, so a module that uses a
// lock-only task has no interface here, which a caller takes as changed.
func (s *FlowfileServer) baseEnv() flowfile.Env { return flowfile.WithRegistry(s.tasks()) }

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
