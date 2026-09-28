// Package protodocimpl is the run-time half of protoc-gen-flowstate-doc: the
// table its generated .doc.pb.go files register a schema's comments into.
//
// It is for generated code only, the way google.golang.org/protobuf's protoimpl
// is: hand-written code reads comments through
// [github.com/picatz/flowstate/pkg/flowstate/v1/protodoc], and nothing here is a
// stable API for any other use. It is public only because generated code in
// other modules (a plugin author's) must be able to import it.
//
// The generator writes one Go file per .proto file, and each registers that
// file's leading comments from an init function, the way protoc-gen-go
// registers descriptors with protoregistry.GlobalFiles. Comments are recorded
// raw, exactly as protoc reports them, and presentation is left to the reader.
// They exist as Go source because protoc-gen-go strips SourceCodeInfo from the
// descriptors a .pb.go embeds, and generating them in the same `buf generate`
// as the types keeps them under the same drift check and reviewable as text.
//
// This package is a leaf: it imports nothing from this module, so generated
// code in any package can register without linking the engine's own schema
// documentation.
package protodocimpl

import (
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Comment is one declaration's leading comment.
type Comment struct {
	// Name is the declaration's protobuf full name: a message, field, oneof,
	// enum, enum value, extension, service, or method.
	Name protoreflect.FullName

	// Leading is the comment directly above the declaration, exactly as protoc
	// reports it in SourceCodeInfo: the comment markers removed, the rest of
	// each line kept, and every line newline-terminated.
	Leading string
}

// RegisterFile records the leading comments of the .proto file at path, the
// path protoc knows it by (for example "flowstate/v1/run.proto").
//
// It is meant to be called from generated init functions. Registering the same
// comment twice is harmless. A name registered twice with different text or
// from a different file is ambiguous, so [Lookup] reports no comment for it
// rather than choosing one; a misattributed sentence is worse than none.
func RegisterFile(path string, comments []Comment) {
	global.registerFile(path, comments)
}

// Lookup returns the raw leading comment registered for name and the path of
// the file that declared it.
//
// It reports false when no comment is registered for the name, or when the
// name is ambiguous (see [RegisterFile]).
func Lookup(name protoreflect.FullName) (comment, path string, ok bool) {
	return global.lookup(name)
}

// global is the process-wide table. Generated code writes it during package
// initialization; readers take the read lock afterwards.
var global = newTable()

// entry is what the table knows about one name.
type entry struct {
	path      string
	leading   string
	ambiguous bool
}

// table is the registry's state, separate from the global so tests can build
// their own.
type table struct {
	mu      sync.RWMutex
	entries map[protoreflect.FullName]entry
}

func newTable() *table {
	return &table{entries: make(map[protoreflect.FullName]entry)}
}

func (t *table) registerFile(path string, comments []Comment) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, c := range comments {
		if c.Name == "" || c.Leading == "" {
			continue
		}
		prev, seen := t.entries[c.Name]
		switch {
		case !seen:
			t.entries[c.Name] = entry{path: path, leading: c.Leading}
		case prev.path != path || prev.leading != c.Leading:
			prev.ambiguous = true
			t.entries[c.Name] = prev
		}
	}
}

func (t *table) lookup(name protoreflect.FullName) (comment, path string, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	e, found := t.entries[name]
	if !found || e.ambiguous {
		return "", "", false
	}
	return e.leading, e.path, true
}
