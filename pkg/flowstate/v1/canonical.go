package flowstatev1

import (
	"bytes"
	"encoding/json"
	"fmt"

	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CanonicalWorkflow returns a copy of workflow reduced to what the program
// means, so two frontends that write the same program in different ways produce
// the same message.
//
// Three things distinguish a program's meaning from how it was written down:
//
//   - Where each expression sat in its source. A [v1alpha1.ParsedExpr] carries
//     the byte offset of every node and the offsets of its line starts, so
//     re-indenting a Flowfile, or emitting the same expression from a transpiler
//     that has no source text at all, changes the message without changing a
//     single evaluation. The offsets are cleared; the node ids and macro calls
//     stay, because they are the tree.
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
