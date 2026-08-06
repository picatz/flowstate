package flowstatev1

import (
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// NodeContainerKinds returns the names of the [Node] `kind` oneof arms that hold a
// body of nested nodes in the *same* run scope — the constructs every walk over the
// node tree has to descend: `for_each`, `loop`, `parallel`.
//
// Derived from the schema rather than written down, which is the entire point. Six
// separate walkers missed the `loop` arm when it was added because nothing forced
// them to have one, and a hand-kept list here would be a seventh place to forget.
// The walker-exhaustiveness guard tests build a probe inside every kind this returns
// and assert each walker descends into it, so the day a fourth container kind is
// added this set grows, the guards' builder maps no longer match it, and every one
// of them goes red naming the walker that still has to learn the new arm.
//
// A `call:` is deliberately *not* here, and the exclusion is structural rather than a
// special case: its nested nodes live inside a whole [Workflow] message, which is an
// isolated scope ([CallScope]) walkers treat differently on purpose — some descend a
// callee's steps (the expansion bound), most must not (a callee's step outputs are a
// different namespace than the caller's). "Reaches a `repeated Node` without passing
// through a Workflow" is what tells a same-scope body from an isolated callee.
func NodeContainerKinds() []string {
	nodeDesc := (&Node{}).ProtoReflect().Descriptor()
	workflowName := (&Workflow{}).ProtoReflect().Descriptor().FullName()
	kind := nodeDesc.Oneofs().ByName("kind")

	var out []string
	for i := range kind.Fields().Len() {
		f := kind.Fields().Get(i)
		if f.Kind() != protoreflect.MessageKind {
			continue
		}
		if messageHoldsSameScopeNodes(f.Message(), nodeDesc.FullName(), workflowName, map[protoreflect.FullName]bool{}) {
			out = append(out, string(f.Name()))
		}
	}

	sort.Strings(out)
	return out
}

// messageHoldsSameScopeNodes reports whether md reaches a `repeated Node` field
// without passing through the [Workflow] message that marks an isolated callee.
func messageHoldsSameScopeNodes(md protoreflect.MessageDescriptor, nodeName, workflowName protoreflect.FullName, visited map[protoreflect.FullName]bool) bool {
	if md == nil || md.FullName() == workflowName || visited[md.FullName()] {
		return false
	}
	visited[md.FullName()] = true

	for i := range md.Fields().Len() {
		f := md.Fields().Get(i)
		if f.Kind() != protoreflect.MessageKind && f.Kind() != protoreflect.GroupKind {
			continue
		}
		fm := f.Message()
		if fm == nil {
			continue
		}
		if fm.FullName() == nodeName {
			// A direct list of nodes is a same-scope body; a single Node field is not
			// something the schema has, but a list is exactly what a body is.
			if f.IsList() {
				return true
			}
			continue
		}
		if messageHoldsSameScopeNodes(fm, nodeName, workflowName, visited) {
			return true
		}
	}
	return false
}
