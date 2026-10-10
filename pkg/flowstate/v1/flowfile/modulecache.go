package flowfile

import (
	"container/list"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The module cache: one compile unit per module, kept in this process for the
// callers that check many files over the same modules (the language server, and
// `flow validate` over a directory).
//
// # What is cached, and what is not
//
// What is kept is the result of compiling and validating a module that came out
// clean: its declarations ([loadedModule]), under the module's resolved path, the
// digest of its bytes, and the profile and edition this build compiles. A module
// that does not compile is never kept, so a problem is always found by compiling
// and the cache cannot lag a fix or hold a stale refusal; the `use:` that names it
// reports it as it always did. Workflows are not kept either: a file being checked
// is compiled every time, so what the cache saves is the modules under it.
//
// A cached module is only ever a way to avoid compiling bytes that were compiled
// before. It is used by the validating entry points ([ModuleCache.ValidateSourceAt],
// [ModuleCache.ValidateSourceFile]) and never by a compile whose workflow is run or
// submitted: a workflow recorded from a cached module carries the digests the
// module's own dependencies had when it was compiled, which is right for a
// diagnostic and not for a record of what a run read.
//
// # When an entry may be used
//
// A module's bytes are read and hashed every time, so an edit is seen at once. An
// entry is then used only if every module it was compiled against is still what it
// was: each direct dependency is loaded again through [compiler.loadModule], the
// one resolver, and must resolve to the same file with the same interface digest
// ([interfaceDigest]) and, when the entry pinned it with `digest:`, the same bytes.
// Anything else, including any refusal, bound or read error met on the way, is a
// miss and the module is compiled as it would be with no cache. A comment or a
// reformatting of a dependency therefore changes its bytes and not its interface,
// and nothing that uses it is compiled again; an edit to what it declares does.
//
// The depth the module sits at is part of the key because the bound on how deeply
// modules use one another is judged while a module is compiled, and a module
// compiled with room to spare must not be reused where there is none.

const (
	// DefaultModuleCacheEntries is how many compiled modules a [ModuleCache] holds
	// when it is not told: a few workspaces' worth, since one workflow uses at
	// most [v1.MaxModules].
	DefaultModuleCacheEntries = 4 * v1.MaxModules

	// DefaultModuleCacheBytes is the most the encoded declarations of the modules
	// in a [ModuleCache] may add up to when it is not told.
	DefaultModuleCacheBytes = 32 << 20
)

// A moduleKey names the inputs a module's compile depends on besides its own
// dependencies: where it is, what its bytes are, the language this build
// compiles, and how deep it sits.
type moduleKey struct {
	path    string
	digest  string
	profile string
	edition string
	depth   int
}

// A moduleDep is one `use:` a cached module was compiled with: the path as the
// module wrote it, where that resolved, and what it declared.
type moduleDep struct {
	target string
	path   string
	iface  string

	// pinned is the digest of the dependency's bytes when the entry carried a
	// `digest:`, and empty otherwise: a pin is a statement about bytes, so it is
	// the one place a dependency's formatting matters.
	pinned string
}

type moduleEntry struct {
	key    moduleKey
	loaded *loadedModule
	deps   []moduleDep
	size   int
}

// ModuleCacheStats is what a [ModuleCache] has done, for tests and for an
// operator asking why a save was slow.
type ModuleCacheStats struct {
	Entries   int
	Bytes     int
	Hits      int // modules answered from an entry
	Misses    int // modules with no entry, or an entry whose dependencies had changed
	Compiles  int // modules compiled because of a miss
	Evictions int
}

// A ModuleCache holds compiled modules, bounded in entries and in bytes, and is
// safe for concurrent use. The nil *ModuleCache is a valid cache that holds
// nothing, so a caller with no cache is the caller with the old behaviour.
//
// Nothing blocks under its lock but map and list operations: a module is compiled,
// and a dependency re-checked, with the lock released.
type ModuleCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int
	bytes      int
	order      *list.List // front is the most recently used
	entries    map[moduleKey]*list.Element
	stats      ModuleCacheStats
}

// NewModuleCache makes a cache of at most maxEntries modules and maxBytes of
// encoded declarations. A bound that is not positive is its default
// ([DefaultModuleCacheEntries], [DefaultModuleCacheBytes]).
func NewModuleCache(maxEntries, maxBytes int) *ModuleCache {
	if maxEntries <= 0 {
		maxEntries = DefaultModuleCacheEntries
	}
	if maxBytes <= 0 {
		maxBytes = DefaultModuleCacheBytes
	}

	return &ModuleCache{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		order:      list.New(),
		entries:    map[moduleKey]*list.Element{},
	}
}

// Stats reports what the cache holds and has done.
func (m *ModuleCache) Stats() ModuleCacheStats {
	if m == nil {
		return ModuleCacheStats{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stats
	s.Entries, s.Bytes = len(m.entries), m.bytes

	return s
}

// ValidateSourceAt is [ValidateSourceAt] with the modules the file uses taken
// from the cache when they have not changed. The diagnostics are the ones an
// uncached call reports.
func (m *ModuleCache) ValidateSourceAt(data []byte, path string) (Diagnostics, error) {
	return validateThroughEdition(data, path, m)
}

// ValidateSourceFile is [ValidateSourceFile] with the modules the file uses taken
// from the cache when they have not changed.
func (m *ModuleCache) ValidateSourceFile(path string) (Diagnostics, error) {
	data, err := readBoundedSource(path)
	if err != nil {
		return nil, err
	}

	return validateThroughEdition(data, path, m)
}

// Interface is the interface digest of the module in the file at path, as the
// modules in a workspace see it: the digest of what it declares and nothing else, so
// it is the same before and after an edit that only moves or comments the file.
// It is empty when the file is not a module that compiles, which a caller must take
// as "changed". The file is compiled, or found in the cache, by the same code a
// `use:` reaches it with.
func (m *ModuleCache) Interface(path string) string {
	if m == nil {
		return ""
	}
	c := &compiler{
		// A name that is the target's plus a suffix is never the target itself,
		// which would be a cycle.
		filePath:   path + ".importer",
		callBudget: new(int),
		session:    m.moduleSession(),
	}
	loaded, ok := c.loadModule(nil, ref{}, "./"+filepath.Base(path), nil)
	if !ok || len(c.diags) > 0 {
		return ""
	}

	return loaded.iface
}

// moduleSession starts the account of one compile that uses the cache. A nil cache
// has none, and [parse] starts the ordinary one.
func (m *ModuleCache) moduleSession() *moduleSession {
	if m == nil {
		return nil
	}
	s := newModuleSession()
	s.cache = m

	return s
}

func (m *ModuleCache) keyFor(path, digest string, depth int) moduleKey {
	return moduleKey{path: path, digest: digest, profile: v1.CurrentProfile, edition: CurrentEdition, depth: depth}
}

// lookup finds the entry for key and marks it used. It does not judge it: the
// caller must re-check the entry's dependencies before relying on it.
func (m *ModuleCache) lookup(key moduleKey) (*moduleEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	m.order.MoveToFront(el)

	return el.Value.(*moduleEntry), true
}

// store keeps a module that compiled clean, unless it alone is larger than the
// cache may hold, and drops the least recently used entries until the bounds hold.
func (m *ModuleCache) store(key moduleKey, loaded *loadedModule, deps []moduleDep) {
	if loaded.iface == "" {
		// No interface digest is no way to tell later whether a dependent may reuse
		// this: not kept.
		return
	}
	size := proto.Size(loaded.workflow) + len(key.path) + len(key.digest) + len(loaded.iface)
	for _, d := range deps {
		size += len(d.target) + len(d.path) + len(d.iface) + len(d.pinned)
	}
	if size > m.maxBytes {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.entries[key]; ok {
		m.bytes -= el.Value.(*moduleEntry).size
		m.order.Remove(el)
		delete(m.entries, key)
	}
	m.entries[key] = m.order.PushFront(&moduleEntry{key: key, loaded: loaded, deps: deps, size: size})
	m.bytes += size
	for len(m.entries) > m.maxEntries || m.bytes > m.maxBytes {
		oldest := m.order.Back()
		evicted := oldest.Value.(*moduleEntry)
		m.order.Remove(oldest)
		delete(m.entries, evicted.key)
		m.bytes -= evicted.size
		m.stats.Evictions++
	}
}

func (m *ModuleCache) count(f func(*ModuleCacheStats)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f(&m.stats)
}

// reuseModule answers the module at resolved, whose bytes digest to digest and
// which the caller has already pinned and bounded, from the cache, or reports
// false and leaves the module to be compiled.
//
// The dependencies of the entry are loaded again through [compiler.loadModule] as
// the module itself would load them, in a compiler standing where the module
// would, so a cycle, a bound, a read error or a refusal on the way is met here as
// it would be there and is a miss. Whatever that loads is what the module would have
// loaded, so the session ends up holding the same modules compiling it would have.
func (c *compiler) reuseModule(resolved string, ancestors []string, digest string) (*loadedModule, bool) {
	cache := c.session.cache
	if cache == nil || c.session.ignorePins {
		return nil, false
	}
	entry, ok := cache.lookup(cache.keyFor(resolved, digest, c.session.depth+1))
	if !ok {
		cache.count(func(s *ModuleCacheStats) { s.Misses++ })

		return nil, false
	}

	module := &compiler{
		filePath:   resolved,
		callStack:  ancestors,
		callBudget: c.callBudget,
		session:    c.session.child(),
	}
	for _, dep := range entry.deps {
		got, ok := module.loadModule(nil, ref{}, dep.target, nil)
		if !ok || len(module.diags) > 0 || got.path != dep.path || got.iface != dep.iface ||
			(dep.pinned != "" && got.digest != dep.pinned) {
			cache.count(func(s *ModuleCacheStats) { s.Misses++ })

			return nil, false
		}
	}
	cache.count(func(s *ModuleCacheStats) { s.Hits++ })

	return entry.loaded, true
}

// child is the session of a module this one uses: one deeper, sharing the account
// of what has been loaded, and the cache.
func (s *moduleSession) child() *moduleSession {
	return &moduleSession{depth: s.depth + 1, loaded: s.loaded, failed: s.failed, reads: s.reads, ignorePins: s.ignorePins, cache: s.cache}
}

// interfaceDigest names what a module gives the files that use it: the types,
// functions and errors it declares, as [compiler.carry] hands them on, hashed with
// [v1.CanonicalDigest] like any other program. The call-form text a declaration
// was written with is dropped (carrying drops it too), so it is the declarations
// that decide, never where they sit or how they are spelled. It is empty if the
// digest cannot be made.
func interfaceDigest(module *v1.Workflow) string {
	declared := proto.CloneOf(&v1.Workflow{
		DeclaredTypes:     module.GetDeclaredTypes(),
		DeclaredFunctions: module.GetDeclaredFunctions(),
		DeclaredErrors:    module.GetDeclaredErrors(),
	})
	for _, d := range declared.GetDeclaredTypes() {
		d.MustSource = nil
		for _, f := range d.GetFields() {
			f.MustSource = nil
		}
	}

	return v1.CanonicalDigest(declared)
}
