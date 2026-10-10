package lsp

import (
	"slices"
	"sync"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Saving a module can change what the files that use it mean, and the editor is
// showing diagnostics for those files that were true of the module as it was. This
// file is how a save reaches them, and how it does not reach them when it need not.
//
// Every diagnostic is computed by the one compiler ([flowfile.ModuleCache], over
// [flowfile.ValidateSourceAt]), which reads a module from disk and so sees a save
// and nothing before it. What this file adds is the decision of whom to tell. A
// module's *interface* ([flowfile.ModuleCache.Interface]) is the digest of what it
// declares, so a save that only adds a comment or reformats leaves it as it was and
// republishes nothing beyond the module itself, and a save that changes what it
// declares republishes every open file that reaches it through modules whose own
// interface changed with it. The exception is a `use:` entry that pins the saved
// module with `digest:`: a pin is about bytes, so any save of that module
// republishes the file, and a module that pins it, which then no longer compiles
// and so has no interface, reaches its own users through that. A file is only ever republished, never skipped, when
// there is doubt: a module whose last interface is not known, that does not
// compile, or that the walk cannot finish within its budget counts as changed.

// moduleCache is shared by every server in the process. It is keyed by the bytes
// of each module and by the interfaces it was compiled against, so what one
// document leaves in it cannot make another's answer wrong, and it is bounded.
var moduleCache = flowfile.NewModuleCache(0, 0)

// maxInterfaceMemo bounds the interfaces a server remembers. Past it the memo is
// forgotten whole, and every module is then unknown, which is changed.
const maxInterfaceMemo = 4 * v1.MaxModules

// An interfaceMemo remembers the interface each module had when this server last
// told the editor about the files that use it. Safe for concurrent use; the zero
// value is empty.
type interfaceMemo struct {
	mu      sync.Mutex
	digests map[string]string
}

// swap records digest for the module at path and returns what was there.
func (m *interfaceMemo) swap(path, digest string) (previous string, known bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	previous, known = m.digests[path]
	if m.digests == nil || (!known && len(m.digests) >= maxInterfaceMemo) {
		m.digests = map[string]string{}
	}
	m.digests[path] = digest

	return previous, known
}

// rememberInterface records the interface of an opened module, so that its first
// save has something to differ from.
func (s *FlowfileServer) rememberInterface(doc *document) {
	path, ok := doc.filesystemPath()
	if !ok || !isModule(doc) {
		return
	}
	canon := canonicalPath(path)
	s.interfaces.swap(canon, moduleCache.Interface(canon))
}

// dependentsToRecheck is the open files that a save of saved changes the meaning
// of, at most [v1.MaxModules] of them: a module's own save republishes only that
// module, and this is who else.
func (s *FlowfileServer) dependentsToRecheck(saved *document) []*document {
	path, ok := saved.filesystemPath()
	if !ok || !isModule(saved) {
		return nil
	}
	walk := &interfaceWalk{memo: &s.interfaces, saved: canonicalPath(path), loads: v1.MaxModules, verdicts: map[string]bool{}}
	walk.changed(walk.saved, 0)

	var out []*document
	for _, doc := range s.docs.snapshot() {
		if doc == saved || doc.kind != docWorkflow || !walk.usesChanged(doc) {
			continue
		}
		if len(out) == v1.MaxModules {
			s.logger().Warn("module save changes more open files than are rechecked", "limit", v1.MaxModules)

			break
		}
		out = append(out, doc)
	}

	return out
}

// An interfaceWalk decides, for one save, which modules have a different
// interface than the editor was last told of.
type interfaceWalk struct {
	memo  *interfaceMemo
	saved string

	// loads is the module files left to read in this walk, shared by every file
	// asked about, so a tree of modules costs a bounded walk. Spent, everything is
	// changed.
	loads int

	// verdicts holds the answer for each module already decided, and doubles as the
	// guard against a cycle: a module in progress is true.
	verdicts map[string]bool
}

// changed reports whether the module at path has a different interface now than
// the last time this was asked, or whether it may have: a module is changed when
// one of the modules it uses is and its own interface is not what was remembered,
// and the saved module is changed when its interface is not what was remembered.
func (w *interfaceWalk) changed(path string, depth int) bool {
	if verdict, seen := w.verdicts[path]; seen {
		return verdict
	}
	w.verdicts[path] = true
	if depth > v1.MaxUseDepth {
		return true
	}

	if path != w.saved {
		w.loads--
		if w.loads < 0 {
			return true
		}
		module, ok := loadModule(path)
		if !ok {
			return true
		}
		if !slices.ContainsFunc(usedModules(module), func(m usedModule) bool { return w.reaches(m, depth+1) }) {
			w.verdicts[path] = false

			return false
		}
	}

	now := moduleCache.Interface(path)
	previous, known := w.memo.swap(path, now)
	verdict := now == "" || !known || previous != now
	w.verdicts[path] = verdict

	return verdict
}

// usesChanged reports whether doc uses a module that changed.
func (w *interfaceWalk) usesChanged(doc *document) bool {
	return slices.ContainsFunc(usedModules(doc), func(m usedModule) bool { return w.reaches(m, 1) })
}

// reaches reports whether the use m is affected by the save: the module changed,
// or the entry pins the saved module, whose bytes a save may have changed without
// changing what it declares.
func (w *interfaceWalk) reaches(m usedModule, depth int) bool {
	return w.changed(m.path, depth) || (m.pinned && m.path == w.saved)
}
