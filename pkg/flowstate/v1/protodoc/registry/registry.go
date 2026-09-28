// Package registry holds the schema comments that protoc-gen-flowstate-doc
// generates, so a process can read them at run time.
//
// The plugin writes one Go file per .proto file. Each registers that file's
// leading comments here from an init function, the same way protoc-gen-go
// registers descriptors with protoregistry.GlobalFiles. Comments are recorded
// raw, exactly as protoc reports them, so every reader gets the original text
// and applies its own presentation;
// [github.com/picatz/flowstate/pkg/flowstate/v1/protodoc] is the reader for
// prose shown to people.
//
// The registry exists because protoc-gen-go strips SourceCodeInfo from the
// descriptors it embeds in a .pb.go: the linked descriptors have a schema's
// shape but none of its prose. Generating the comments as Go source keeps them
// in the same `buf generate` run, and the same drift check, as the types they
// describe, and lets them show up as text in a diff.
//
// This package is a leaf: it imports nothing from this module, so generated
// code in any package, including a plugin author's, can register without
// linking the engine's own schema documentation.
package registry

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
