package flowfile

import (
	"math"
	"strconv"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// unfoldedStructure writes an expression that builds a mapping or a sequence in
// the shape an author wrote it, rather than as the one CEL literal the compiler
// folded it into.
//
// [compiler.composite] collapses any mapping or sequence holding a `${...}`
// anywhere inside it into a single `Value_Expr` — a CEL map or list literal —
// because that is the only way a per-key expression can be evaluated. The keys
// an author wrote are then not in the compiled workflow at all, and [Marshal]
// wrote the value back the only way it knew: one fenced expression on one line.
// That is how `examples/` came to hold a 778-character `params:` and how
// `flow fmt` came to refuse `docs/DSL.md`'s worked example — a comment anchored
// to a key that the rendered document no longer has (#850).
//
// # A candidate, verified, exactly as a scalar style is
//
// This is [styledScalarFor]'s idiom extended from a scalar's quoting to a
// value's shape. [scalarStyles] offers renderings and [scalarSurvives] writes
// each one and reads it back, so no scalar is written in a style nothing
// checked. Here the rendering offered is the mapping or sequence spelling, and
// [unfoldSurvives] verifies it by *re-compiling* it and comparing the value it
// compiles to against this one with [proto.Equal].
//
// Comparing compiled values rather than asserting the result still validates is
// the rule CLAUDE.md's rewriter section draws from both `flow fix` corruptions:
// each of those wrote a file that validated perfectly and meant something else.
// A candidate that does not compile back to exactly this value keeps the fenced
// one-line form, and so does a mapping whose key is not a constant string —
// there is no YAML spelling of a computed key, and nothing here guesses one.
//
// The cost, stated: a compile-and-compare per structure-shaped input value, on
// a path `flow fmt` walks directories with. It is the same trade the three
// emitter round trips per scalar above already make.
func unfoldedStructure(value *v1.Value) (any, bool) {
	parsed := value.GetExpr()
	if parsed == nil {
		return nil, false
	}

	// A macro's expansion cannot be written back as source at all — [exprToText]
	// refuses a whole expression for that reason — because the parsed form
	// records the macro call beside the tree rather than in it. A sub-expression
	// lifted out of this tree would carry none of that record, so unparsing it
	// would silently write the expanded comprehension. Left fenced instead,
	// where the record still travels with the whole expression.
	if len(parsed.GetSourceInfo().GetMacroCalls()) > 0 {
		return nil, false
	}

	candidate, ok := unfoldedExpr(parsed.GetExpr(), true)
	if !ok {
		return nil, false
	}
	if !unfoldSurvives(value, candidate) {
		return nil, false
	}
	return candidate, true
}

// unfoldedExpr renders one expression as the YAML an author would have written
// for it.
//
// structureOnly is what makes this the *structure* rendering rather than a
// second, unverified spelling of every value: at the top it demands a map or a
// list literal, so a plain `${a > 1}` is never touched. Inside one, every leaf
// is written the way [compiler.celTextString] reads one — a constant string is
// text, and anything else is a fenced expression — which is what makes the round
// trip land back on the same tree.
func unfoldedExpr(e *expr.Expr, structureOnly bool) (any, bool) {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_StructExpr:
		if kind.StructExpr.GetMessageName() != "" {
			// A message construction, not a mapping. There is no YAML spelling.
			return nil, false
		}
		entries := kind.StructExpr.GetEntries()
		out := make(yaml.MapSlice, 0, len(entries))
		for _, entry := range entries {
			name, isString := entry.GetMapKey().GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
			if !isString {
				// A computed key — `${{"a" + "b": 1}}` — or a key of another
				// type. A YAML mapping key is a name written down, so there is
				// no spelling of one, and the whole structure keeps the fence.
				return nil, false
			}
			written, ok := unfoldedExpr(entry.GetValue(), false)
			if !ok {
				return nil, false
			}
			// The key is handed over as the string it is, the way
			// [structureToYAML] hands one over: the emitter requires a string
			// key, and a key is a name rather than a value — nothing reads a
			// fence in one — so the style choices [textToYAML] makes about a
			// value do not apply. A key the emitter cannot write back as itself
			// fails verification below and the whole structure stays fenced.
			out = append(out, yaml.MapItem{Key: name.StringValue, Value: written})
		}
		// Entry order is the author's, kept: a CEL map literal written in
		// another order is a different tree, so sorting here would fail
		// verification rather than tidy anything. `structureToYAML` sorts
		// because a protobuf map genuinely has no order; this one does.
		return out, true

	case *expr.Expr_ListExpr:
		elements := kind.ListExpr.GetElements()
		out := make([]any, 0, len(elements))
		for _, element := range elements {
			written, ok := unfoldedExpr(element, false)
			if !ok {
				return nil, false
			}
			out = append(out, written)
		}
		return out, true
	}

	if structureOnly {
		return nil, false
	}

	if constant := e.GetConstExpr(); constant != nil {
		return unfoldedConst(constant)
	}

	text, err := exprToText(&expr.ParsedExpr{Expr: e})
	if err != nil {
		return nil, false
	}
	return fencedToYAML(text), true
}

// unfoldedConst writes a constant leaf as the YAML scalar that reads back as it.
//
// The shapes are [compiler.celText]'s, inverted: a number is written in the text
// that function would have produced for it, and a string goes through
// [textToYAML] so that a `${` inside it comes back escaped rather than as a
// fence this function just invented.
func unfoldedConst(constant *expr.Constant) (any, bool) {
	switch kind := constant.GetConstantKind().(type) {
	case *expr.Constant_StringValue:
		return textToYAML(kind.StringValue), true
	case *expr.Constant_Int64Value:
		return kind.Int64Value, true
	case *expr.Constant_Uint64Value:
		if kind.Uint64Value <= math.MaxInt64 {
			// celText writes the `u` suffix only above this, so below it a
			// written unsigned value reads back signed and the trees differ.
			return nil, false
		}
		return kind.Uint64Value, true
	case *expr.Constant_DoubleValue:
		return plainScalar(unfoldedFloat(kind.DoubleValue)), true
	case *expr.Constant_BoolValue:
		return kind.BoolValue, true
	case *expr.Constant_NullValue:
		return nil, true
	default:
		// A bytes constant, which CEL source spells as a call rather than as a
		// literal a YAML scalar could hold.
		return nil, false
	}
}

// unfoldedFloat writes a float in [compiler.celText]'s own spelling —
// `strconv.FormatFloat(…, 'g', -1, 64)` — rather than handing the Go value to
// the emitter, so that what is written is the text the compiler would produce
// for it and the round trip has the best chance of landing on the same tree.
//
// A round double is the case that does not survive, and deliberately nothing
// here tries to rescue it: `celText` renders 1.0 as `1`, so a structure holding
// one compiles to an *integer* whichever way this writes it, verification fails,
// and the whole structure keeps the fenced form. Writing `1.0` to force a float
// would produce a document that means something the value does not.
func unfoldedFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// unfoldSurvives reports whether candidate compiles back to exactly value.
//
// Written into both shapes [Marshal] ever places a structure in — a mapping
// value and a sequence entry — for [scalarSurvives]'s reason: a rendering can be
// read one way under a key and another way under a `-`, and a formatter that
// checked only one would change what a file says in the other.
//
// The comparison is [proto.Equal] over the compiled value, never "the document
// still validates": both `flow fix` corruptions on record produced files that
// validated and computed something else.
func unfoldSurvives(value *v1.Value, candidate any) bool {
	return unfoldCompilesBack(value, candidate, false) &&
		unfoldCompilesBack(value, candidate, true)
}

// unfoldCompilesBack writes one document holding candidate — under a key, or as
// the single entry of a sequence under one — reads it back through the
// compiler, and reports whether the value it found is value.
func unfoldCompilesBack(value *v1.Value, candidate any, inSequence bool) bool {
	document := yaml.MapSlice{{Key: "v", Value: candidate}}
	if inSequence {
		document = yaml.MapSlice{{Key: "v", Value: []any{candidate}}}
	}

	encoded, err := yaml.Marshal(document)
	if err != nil {
		return false
	}
	file, err := parser.ParseBytes(encoded, 0)
	if err != nil {
		return false
	}

	node := unfoldValueNode(file, inSequence)
	if node == nil {
		return false
	}

	// A compiler of its own, with no anchors and no file path: this document is
	// two lines that this function just wrote, so nothing in it can resolve an
	// alias or reach another file.
	c := &compiler{pos: newPositions(), anchors: map[string]ast.Node{}}
	back := c.inputValue(node, "v", ref{})
	if back == nil || len(c.diags) > 0 {
		return false
	}
	return proto.Equal(value, back)
}

// unfoldValueNode finds the candidate in the one-key document
// [unfoldCompilesBack] writes, unwrapping the enclosing sequence when the
// candidate was placed as an entry.
func unfoldValueNode(file *ast.File, inSequence bool) ast.Node {
	if len(file.Docs) != 1 {
		return nil
	}

	var value ast.Node
	switch body := file.Docs[0].Body.(type) {
	case *ast.MappingNode:
		if len(body.Values) != 1 {
			return nil
		}
		value = body.Values[0].Value
	case *ast.MappingValueNode:
		value = body.Value
	default:
		return nil
	}

	if !inSequence {
		return value
	}

	sequence, ok := value.(*ast.SequenceNode)
	if !ok || len(sequence.Values) != 1 {
		// The entry read back as something other than one entry, which is the
		// failure this position is checked for at all.
		return nil
	}
	return sequence.Values[0]
}
