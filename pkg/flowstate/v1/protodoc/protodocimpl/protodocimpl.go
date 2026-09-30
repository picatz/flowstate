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
	"fmt"
	"maps"
	"slices"
	"strings"
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
// [Conflicts] lists the names that ended up that way.
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

// Conflict is one name that registrations disagree about, so [Lookup] answers
// no comment for it.
type Conflict struct {
	// Name is the declaration's protobuf full name.
	Name protoreflect.FullName

	// Paths are the .proto files that registered Name, sorted and without
	// repeats. One path means a single file was registered with two different
	// comments, as two generations of one generated package linked into one
	// binary would; several mean different files each claim the name.
	Paths []string
}

// String describes the conflict for a test failure or a log line.
func (c Conflict) String() string {
	if len(c.Paths) == 1 {
		return fmt.Sprintf("%s: %s registered it with different comments", c.Name, c.Paths[0])
	}
	return fmt.Sprintf("%s: registered by %s", c.Name, strings.Join(c.Paths, ", "))
}

// Conflicts lists, in name order, every name that was registered with
// disagreeing comments or from disagreeing files, and so is no longer answered
// by [Lookup]. It is empty when every registration agrees.
//
// Like the rest of this package it is for tests and tooling that check a linked
// schema, not an API to build behavior on: Lookup stays silent about a conflict
// on purpose, and this is how someone who can fix one finds out. The engine's
// own schema is held to it by protodoc's tests, and a plugin author can do the
// same for the schema their binary links.
func Conflicts() []Conflict {
	return global.conflicts()
}

// global is the process-wide table. Generated code writes it during package
// initialization; readers take the read lock afterwards.
var global = newTable()

// entry is what the table knows about one name: the first registration, and any
// later one that disagreed with it. A name with a disagreement is ambiguous.
// Nothing is allocated for the common, agreeing case.
type entry struct {
	path    string
	leading string
	others  []variant
}

// variant is one registration that disagreed with an entry's first.
type variant struct {
	path    string
	leading string
}

func (e entry) ambiguous() bool { return len(e.others) > 0 }

// has reports whether a registration of leading from path repeats one already
// recorded for the name.
func (e entry) has(path, leading string) bool {
	if e.path == path && e.leading == leading {
		return true
	}
	return slices.Contains(e.others, variant{path, leading})
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
		case !prev.has(path, c.Leading):
			prev.others = append(prev.others, variant{path, c.Leading})
			t.entries[c.Name] = prev
		}
	}
}

func (t *table) lookup(name protoreflect.FullName) (comment, path string, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	e, found := t.entries[name]
	if !found || e.ambiguous() {
		return "", "", false
	}
	return e.leading, e.path, true
}

func (t *table) conflicts() []Conflict {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var out []Conflict
	for _, name := range slices.Sorted(maps.Keys(t.entries)) {
		e := t.entries[name]
		if !e.ambiguous() {
			continue
		}
		paths := []string{e.path}
		for _, v := range e.others {
			paths = append(paths, v.path)
		}
		slices.Sort(paths)
		out = append(out, Conflict{Name: name, Paths: slices.Compact(paths)})
	}
	return out
}
