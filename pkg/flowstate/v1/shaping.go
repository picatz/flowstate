package flowstatev1

import (
	"slices"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Output shaping, in one place, because it is one concept the language used to
// spell two ways.
//
// A step that shapes its outputs replaces the set it would otherwise produce
// with names the author writes. Two constructs do it — a `wait_for_signal:`'s
// `outputs:` and a shaping task's `outputs:` input — and the wait's spelling is
// the one the language keeps: a mapping of name to value, one entry per line,
// each value an ordinary expression position.
//
// The mapping form is not only nicer to read. It is what makes shaping
// *checkable*: the names are in the file, so a later `${steps.x.typo}` can be
// reported against the set the step actually produces. A map built by an
// expression has no statically knowable keys, so nothing downstream can say
// anything true about what the step produces — which is why that spelling stays
// legal and stays unchecked.

// ShapingInput is the input a shaping task reads its replacement outputs from.
//
// One name, read by the compiler, the validator, the language server and the
// rewriter, so that "which key shapes" cannot be answered differently in four
// places. Which *tasks* it shapes for is [TaskDef.ShapesOutputs] and not this
// constant: the name alone was the rule once, and #324 is the record of what
// that cost.
const ShapingInput = "outputs"

// TaskShapesOutputs reports whether a task evaluates [ShapingInput] as a
// replacement for the outputs it declares.
//
// An unregistered task shapes nothing. That is the fail-closed answer and it is
// the right one for the surface that asks most often: `flow validate` and the
// language server build their registry from the built-ins alone, so a plugin
// task is unknown there, and guessing that an unknown task shapes would stand
// every check down on the strength of a spelling.
func TaskShapesOutputs(taskName string) bool {
	def, found := LookupTask(taskName)
	return found && def.ShapesOutputs
}

// ShapedOutputNames returns the names a shaping value defines, sorted, and
// reports whether they are knowable in full.
//
// Three spellings are knowable, which is the whole point of collecting them
// here — the same value reaches this from a mapping the compiler kept entry by
// entry, from a mapping written entirely in literals, and from the older
// string-fenced CEL map:
//
//   - a [Value_Structure] map, whose entries are the names directly;
//   - a literal map, whose keys are string literals;
//   - an expression whose top level is a CEL map with every key a string
//     literal.
//
// Anything else — a variable, a function call, a computed key — yields names
// only the run can know, and the honest answer is no names at all rather than a
// guess. A fabricated candidate an author accepts is a reference nothing may
// produce.
func ShapedOutputNames(v *Value) ([]string, bool) {
	switch kind := v.GetKind().(type) {
	case *Value_Structure_:
		mapped := kind.Structure.GetMap()
		if mapped == nil {
			return nil, false
		}
		names := make([]string, 0, len(mapped.GetEntries()))
		for name := range mapped.GetEntries() {
			names = append(names, name)
		}
		slices.Sort(names)
		return names, true

	case *Value_Literal:
		return literalMapKeys(kind.Literal)

	case *Value_Expr:
		return staticMapKeys(kind.Expr.GetExpr())
	}

	return nil, false
}

// ShapedNamesInSource is [ShapedOutputNames] for a value still written as text,
// which is what an editor holds: it has the author's keystrokes and no compiled
// document to read them out of.
//
// Sharing the extraction rather than the parse, because the extraction is the
// part with an opinion in it. A surface that decided for itself which CEL map
// literals count would be a second answer to "what does this step produce", and
// the editor contradicting the diagnostics is the failure #324 records.
func ShapedNamesInSource(src string) ([]string, bool) {
	value := NewExpr(src)
	if value.Error() != nil {
		return nil, false
	}
	return ShapedOutputNames(value)
}

// literalMapKeys reads the keys of a literal map, when it is one.
func literalMapKeys(literal *expr.Value) ([]string, bool) {
	mapped, ok := literal.GetKind().(*expr.Value_MapValue)
	if !ok {
		return nil, false
	}

	names := make([]string, 0, len(mapped.MapValue.GetEntries()))
	for _, entry := range mapped.MapValue.GetEntries() {
		key, ok := entry.GetKey().GetKind().(*expr.Value_StringValue)
		if !ok {
			return nil, false
		}
		names = append(names, key.StringValue)
	}
	slices.Sort(names)
	return names, true
}

// staticMapKeys reads the keys of a CEL map literal written at the top level of
// an expression, when every one of them is a string literal.
//
// The one subtle refusal is a computed key. `{name: 1}` where `name` is a
// variable is a map whose key set the run decides, and a walk that took the
// identifier's spelling would report a name no step ever produces.
func staticMapKeys(e *expr.Expr) ([]string, bool) {
	structure := e.GetStructExpr()
	if structure == nil || structure.GetMessageName() != "" {
		return nil, false
	}

	names := make([]string, 0, len(structure.GetEntries()))
	for _, entry := range structure.GetEntries() {
		key := entry.GetMapKey()
		if key == nil {
			return nil, false
		}
		literal, ok := key.GetExprKind().(*expr.Expr_ConstExpr)
		if !ok {
			return nil, false
		}
		text, ok := literal.ConstExpr.GetConstantKind().(*expr.Constant_StringValue)
		if !ok {
			return nil, false
		}
		names = append(names, text.StringValue)
	}
	slices.Sort(names)
	return names, true
}
