package flowstatev1

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CanonicalWorkflow returns a copy of workflow reduced to what the program
// means, so two frontends that write the same program in different ways produce
// the same message.
//
// Two things distinguish a program's meaning from how it was written down:
//
//   - Where each expression sat in its source, and how a parser happened to
//     number it. A [v1alpha1.ParsedExpr] carries the code-point offset of every
//     node, the offsets of its line starts, and an id on every node that only
//     says in what order some parser met it, so re-indenting a source, or
//     emitting the same tree from a producer with different numbering or no
//     source text at all, changes the message without changing a single
//     evaluation. The offsets are cleared and the ids are renumbered 1..n in a
//     fixed walk of the tree, with the ids that key and fill the macro-call
//     table rewritten to match, so the tree is what remains.
//   - What the admitting control plane added. [Workflow.ResolvedPlugins] and
//     [Workflow.ResolvedTaskCapabilities] pin the deployment a run was admitted
//     on, not the program, and are cleared on every workflow in the call tree,
//     exactly as [WorkflowIRDigest] does.
//
// The walk is reflective rather than a list of the fields that hold
// expressions, so an expression a later schema change places somewhere new is
// canonicalized without anyone remembering to teach this function. A nil
// workflow yields nil.
//
// The result is not a runnable program for a debugger: expression failure reporting reads the
// offsets this function removes to point at the failing operator. Run the
// original, and use this copy to compare, hash and diff.
func CanonicalWorkflow(workflow *Workflow) *Workflow {
	if workflow == nil {
		return nil
	}
	clone := proto.CloneOf(workflow)
	canonicalize(clone.ProtoReflect())

	return clone
}

// canonicalize clears, in place, everything [CanonicalWorkflow] says is not
// meaning, on m and every message beneath it. Recursion depth is bounded by the
// nesting protobuf itself accepts when the message was decoded.
func canonicalize(m protoreflect.Message) {
	switch msg := m.Interface().(type) {
	case *v1alpha1.ParsedExpr:
		renumberExprIDs(msg)
	case *v1alpha1.SourceInfo:
		msg.Positions = nil
		msg.LineOffsets = nil
		msg.Location = ""
	case *Workflow:
		msg.ResolvedPlugins = nil
		msg.ResolvedTaskCapabilities = nil
	}

	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Message() == nil {
			return true
		}
		switch {
		case fd.IsList():
			for i := range v.List().Len() {
				canonicalize(v.List().Get(i).Message())
			}
		case fd.IsMap():
			if fd.MapValue().Message() == nil {
				return true
			}
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				canonicalize(mv.Message())

				return true
			})
		default:
			canonicalize(v.Message())
		}

		return true
	})
}

// CanonicalDigest is the content digest of [CanonicalWorkflow]'s deterministic
// encoding: one name for one program, however it was written.
//
// It differs from [WorkflowIRDigest] on purpose. That digest names a program
// *and where its expressions sat*, because a debugger's source map is bound to
// those positions; this one names the program alone, so two Flowfiles that
// differ only in whitespace, or a Flowfile and a transpiler's output for the
// same logic, compare equal. Neither is a cross-language digest: the encoding
// is this build's deterministic Go marshal.
//
// The empty string means the workflow could not be encoded.
func CanonicalDigest(workflow *Workflow) string {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(CanonicalWorkflow(workflow))
	if err != nil {
		return ""
	}

	return ContentDigest(data)
}

// MarshalCanonicalJSON writes workflow as ProtoJSON that diffs: canonicalized
// by [CanonicalWorkflow], map keys sorted, two-space indented, and with the
// whitespace randomization protojson applies on purpose removed, so the same
// program is the same bytes on every run and every build that shares a schema.
func MarshalCanonicalJSON(workflow *Workflow) ([]byte, error) {
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(CanonicalWorkflow(workflow))
	if err != nil {
		return nil, fmt.Errorf("encode canonical workflow: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, fmt.Errorf("encode canonical workflow: %w", err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, compact.Bytes(), "", "  "); err != nil {
		return nil, fmt.Errorf("encode canonical workflow: %w", err)
	}
	out.WriteByte('\n')

	return out.Bytes(), nil
}

// renumberExprIDs rewrites every node id in parsed to the order a fixed
// pre-order walk of its tree meets the node, and rewrites the macro-call table
// to the same ids.
//
// The ids of the macro-call table are the ids of the tree nodes the macro
// replaced, so they are mapped, not renumbered on their own: an id first met in
// the table (a macro argument the expansion no longer contains) is numbered after
// the tree, in the order of the table's keys once those are mapped. The table is
// rebuilt rather than edited because its keys are the ids.
func renumberExprIDs(parsed *v1alpha1.ParsedExpr) {
	ids := map[int64]int64{}
	next := int64(0)
	assign := func(old int64) int64 {
		if id, ok := ids[old]; ok {
			return id
		}
		next++
		ids[old] = next

		return next
	}

	var walk func(*v1alpha1.Expr)
	walk = func(e *v1alpha1.Expr) {
		if e == nil {
			return
		}
		e.Id = assign(e.Id)
		switch k := e.ExprKind.(type) {
		case *v1alpha1.Expr_SelectExpr:
			walk(k.SelectExpr.GetOperand())
		case *v1alpha1.Expr_CallExpr:
			walk(k.CallExpr.GetTarget())
			for _, a := range k.CallExpr.GetArgs() {
				walk(a)
			}
		case *v1alpha1.Expr_ListExpr:
			for _, el := range k.ListExpr.GetElements() {
				walk(el)
			}
		case *v1alpha1.Expr_StructExpr:
			for _, en := range k.StructExpr.GetEntries() {
				en.Id = assign(en.Id)
				if key, ok := en.KeyKind.(*v1alpha1.Expr_CreateStruct_Entry_MapKey); ok {
					walk(key.MapKey)
				}
				walk(en.GetValue())
			}
		case *v1alpha1.Expr_ComprehensionExpr:
			c := k.ComprehensionExpr
			walk(c.GetIterRange())
			walk(c.GetAccuInit())
			walk(c.GetLoopCondition())
			walk(c.GetLoopStep())
			walk(c.GetResult())
		}
	}
	walk(parsed.GetExpr())

	macros := parsed.GetSourceInfo().GetMacroCalls()
	if len(macros) == 0 {
		return
	}
	// Order the table by mapped key so ids first met here are numbered the same
	// way whatever the producer's numbering; a key the tree never named sorts last,
	// by its own value.
	keys := slices.SortedFunc(maps.Keys(macros), func(a, b int64) int {
		ia, oka := ids[a]
		ib, okb := ids[b]
		switch {
		case oka && okb:
			return cmp.Compare(ia, ib)
		case oka != okb:
			if oka {
				return -1
			}

			return 1
		}

		return cmp.Compare(a, b)
	})
	rebuilt := make(map[int64]*v1alpha1.Expr, len(macros))
	for _, key := range keys {
		call := macros[key]
		walk(call)
		rebuilt[assign(key)] = call
	}
	parsed.SourceInfo.MacroCalls = rebuilt
}
