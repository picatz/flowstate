package flowstatev1

import (
	"maps"
	"slices"
)

// A [Value] used to be one of three flat things — an expression, a literal, or a
// reference — and a mapping written in a Flowfile was compiled into one of the
// first two. That is why `bearer:` had to exist before a secret could be sent at
// all: an `Authorization` entry inside `headers:` was part of a single expression
// the *workflow* evaluated, and evaluating a reference is precisely what writes a
// secret into durable history.
//
// [Value_Structure] is the shape that lifts that limitation without moving the
// resolution anywhere it must not go. A structure's entries are Values, so a
// reference nested in one stays a reference through compilation, through the
// specification, across the activity payload, and is resolved only by the worker
// applying the header or encoding the body.
//
// Two rules keep it honest, and both are enforced where the file is compiled:
//
//   - A structure's entries are literals and references. An expression nested in
//     one would be invisible to the walkers that decide which step outputs a
//     resumed run has to carry, and an output they miss is pruned from a run that
//     then fails after Continue-As-New.
//   - Only a task input the task *applies itself*, entry by entry, accepts one.
//     See [TaskDef.NestedSecretInputs].

// maxStructureDepth bounds how deeply a structure may nest.
//
// The walks below are recursive and a specification does not have to have come
// from a Flowfile — the compiler's own nesting bound says nothing about a message
// submitted over the wire, which is a value an outside party chooses. Depth is the
// resource that walk spends, so depth is what is bounded here; breadth costs one
// pass per entry and is bounded by the size of the specification itself.
const maxStructureDepth = 32

// ValueHoldsSecretRef reports whether v is a secret reference or contains one at
// any depth.
//
// It answers the question "does executing this need the authority to read a
// secret", which is why it looks inside structures where [Value.GetSecretRef]
// cannot: a reference in a header map is as much a secret read as one written as
// the whole value of `bearer:`.
func ValueHoldsSecretRef(v *Value) bool {
	found := false
	walkSecretRefs(v, 0, func(*SecretRef) bool {
		found = true
		return false
	})
	return found
}

// SecretRefsIn returns every reference a task's inputs name, rendered as
// `scheme:name` and sorted.
//
// References and never values, which is what makes it safe to log or attach to a
// span: a [SecretRef] is a scheme and a name and holds no material by
// construction. Sorted because inputs are a map, and a set of attributes that
// reorders between two runs of one step is a diff nobody can read.
//
// It exists in this package rather than in a caller because there is more than one
// caller and the walk has to agree: the durable driver names them on a span, and
// anything else that wants to say which secrets a step will read gets the same
// answer, including the ones nested inside a structure that a top-level look
// cannot see.
func SecretRefsIn(task *Task) []string {
	var refs []string
	for _, value := range task.GetInputs() {
		walkSecretRefs(value, 0, func(ref *SecretRef) bool {
			refs = append(refs, secretRefText(ref))
			return true
		})
	}

	slices.Sort(refs)
	return slices.Compact(refs)
}

// walkSecretRefs visits every reference in v, stopping early when visit says so.
func walkSecretRefs(v *Value, depth int, visit func(*SecretRef) bool) bool {
	if v == nil || depth > maxStructureDepth {
		return true
	}

	switch kind := v.GetKind().(type) {
	case *Value_SecretRef:
		return visit(kind.SecretRef)
	case *Value_Structure_:
		for _, entry := range StructureValues(kind.Structure) {
			if !walkSecretRefs(entry, depth+1, visit) {
				return false
			}
		}
	}

	return true
}

// StructureValues returns the values a structure holds, in a fixed order: a list's
// elements as written, and a mapping's entries by sorted key.
//
// Sorted rather than in map order because more than one thing renders a structure
// — a form body, a query string, a diagnostic — and a rendering that reorders
// between two runs of the same specification is a difference nobody made.
func StructureValues(s *Value_Structure) []*Value {
	switch kind := s.GetKind().(type) {
	case *Value_Structure_List_:
		return kind.List.GetValues()
	case *Value_Structure_Map_:
		entries := kind.Map.GetEntries()
		values := make([]*Value, 0, len(entries))
		for _, name := range slices.Sorted(maps.Keys(entries)) {
			values = append(values, entries[name])
		}
		return values
	default:
		return nil
	}
}

// StructureMap returns the entries of a structure written as a mapping, and false
// for one written as a list.
func StructureMap(v *Value) (map[string]*Value, bool) {
	structure := v.GetStructure()
	if structure == nil {
		return nil, false
	}
	kind, ok := structure.GetKind().(*Value_Structure_Map_)
	if !ok {
		return nil, false
	}
	return kind.Map.GetEntries(), true
}

// NewStructureMap builds a mapping-shaped structure value.
func NewStructureMap(entries map[string]*Value) *Value {
	return &Value{Kind: &Value_Structure_{Structure: &Value_Structure{
		Kind: &Value_Structure_Map_{Map: &Value_Structure_Map{Entries: entries}},
	}}}
}

// NewStructureList builds a list-shaped structure value.
func NewStructureList(values ...*Value) *Value {
	return &Value{Kind: &Value_Structure_{Structure: &Value_Structure{
		Kind: &Value_Structure_List_{List: &Value_Structure_List{Values: values}},
	}}}
}
