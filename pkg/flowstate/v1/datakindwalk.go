package flowstatev1

import (
	"fmt"
	"slices"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Data kinds below the top of a value.
//
// [NormalizeDataKind] reads one declared timestamp, duration or bytes. A value is
// more than its top: a record has fields of these kinds, a list holds them, a map's
// values are them. This is the walk that reaches each by the structural type the
// declaration states, so a `must:` on a record, an expression over
// `inputs.window.starts` and a step that reads one all see the CEL kind rather than
// the text it was submitted as, wherever the kind sits.

// dataKindOf is the legacy declared type of a structural scalar that travels as a
// string, and false for every other type.
func dataKindOf(t *Type) (InputDeclaration_Type, bool) {
	scalar, ok := t.GetKind().(*Type_Scalar_)
	if !ok {
		return InputDeclaration_TYPE_UNSPECIFIED, false
	}

	switch scalar.Scalar {
	case Type_SCALAR_TIMESTAMP:
		return InputDeclaration_TYPE_TIMESTAMP, true
	case Type_SCALAR_DURATION:
		return InputDeclaration_TYPE_DURATION, true
	case Type_SCALAR_BYTES:
		return InputDeclaration_TYPE_BYTES, true
	}

	return InputDeclaration_TYPE_UNSPECIFIED, false
}

// checkDataKindAt judges one literal in a position whose type is a data kind and
// says what is wrong with it as "a value that is not an RFC 3339 timestamp … at
// .path", never repeating the value.
func checkDataKindAt(kind InputDeclaration_Type, literal *expr.Value, path string) error {
	if _, err := NormalizeDataKind(kind, literal); err != nil {
		return fmt.Errorf("a value that %w%s", err, atPath(path))
	}

	return nil
}

// NormalizeWireValue returns lit with every timestamp, duration and bytes the type
// t holds turned from the text it was written as into the value CEL reads,
// descending through lists, maps and the records table declares.
//
// It is the identity where there is nothing to turn: a value already normalized, a
// position t says nothing about, a literal the shape check would refuse (which is
// left for it to say so). The result is lit itself when nothing changed, so it is
// safe to apply twice and costs nothing for a value with no such kind. Nothing is
// mutated, and the walk is bounded by [MaxStructureDepth] and by the value.
func NormalizeWireValue(table TypeTable, t *Type, lit *expr.Value) *expr.Value {
	budget := maxDefaultFills

	return normalizeWire(table, t, lit, 0, &budget)
}

func normalizeWire(table TypeTable, t *Type, lit *expr.Value, depth int, budget *int) *expr.Value {
	if lit == nil || depth > MaxStructureDepth {
		return lit
	}

	if kind, ok := dataKindOf(t); ok {
		if normalized, err := NormalizeDataKind(kind, lit); err == nil {
			return normalized
		}

		return lit
	}

	switch kind := t.GetKind().(type) {
	case *Type_List:
		list, ok := lit.GetKind().(*expr.Value_ListValue)
		if !ok {
			return lit
		}

		values := list.ListValue.GetValues()
		var out []*expr.Value
		for i, element := range values {
			n := normalizeWire(table, kind.List, element, depth+1, budget)
			if n != element && out == nil {
				out = append([]*expr.Value(nil), values...)
			}
			if out != nil {
				out[i] = n
			}
		}
		if out == nil {
			return lit
		}

		return &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: out}}}

	case *Type_Map_:
		return normalizeEntries(lit, func(string) *Type { return kind.Map.GetValue() }, table, depth, budget)

	case *Type_Message:
		declared := table[kind.Message]
		if declared == nil {
			return lit
		}

		fields := make(map[string]*Type, len(declared.GetFields()))
		for _, field := range declared.GetFields() {
			fields[field.GetName()] = field.DeclaredType()
		}

		return normalizeEntries(fillFieldDefaults(declared, lit, budget), func(key string) *Type { return fields[key] }, table, depth, budget)
	}

	return lit
}

// normalizeEntries normalizes the values of a map literal, each by the type typeOf
// gives for its key.
func normalizeEntries(lit *expr.Value, typeOf func(key string) *Type, table TypeTable, depth int, budget *int) *expr.Value {
	m, ok := lit.GetKind().(*expr.Value_MapValue)
	if !ok {
		return lit
	}

	entries := m.MapValue.GetEntries()
	var out []*expr.MapValue_Entry
	for i, entry := range entries {
		key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue)
		if !isString {
			continue
		}

		n := normalizeWire(table, typeOf(key.StringValue), entry.GetValue(), depth+1, budget)
		if n == entry.GetValue() {
			continue
		}
		if out == nil {
			out = append([]*expr.MapValue_Entry(nil), entries...)
		}
		out[i] = &expr.MapValue_Entry{Key: entry.GetKey(), Value: n}
	}
	if out == nil {
		return lit
	}

	return &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: out}}}
}

// NormalizeInputValue is [NormalizeWireValue] for the value of an input or a record
// field's declaration, and the identity for a value that is not a literal.
//
// Called before a value's `must:` and record rules are judged wherever that
// happens (submit, a default, an example, a call's literal argument), so a rule
// reads a timestamp as a timestamp whether the value has been through a run or is
// still the text a file wrote.
func NormalizeInputValue(table TypeTable, declaration *InputDeclaration, value *Value) *Value {
	lit := value.GetLiteral()
	if lit == nil {
		return value
	}

	n := NormalizeWireValue(table, declaration.DeclaredType(), lit)
	if n == lit {
		return value
	}

	return &Value{Kind: &Value_Literal{Literal: n}}
}

// fillFieldDefaults returns lit with each field the record declares a `default:`
// for, and the value leaves out, set to that default, in the order the record
// declares them so the result is the same on every run and on both drivers. It is
// lit itself when nothing is missing, and anything that is not a map is left for the
// shape check to refuse.
//
// budget is shared by the whole walk and spent one per field filled.
//
// A default is a literal [CheckInputDefaultIn] already held to the field's type and
// rules when the record was declared, so nothing here judges it again; it is shared,
// never mutated, and as bounded as the declaration that carries it.
func fillFieldDefaults(declared *TypeDeclaration, lit *expr.Value, budget *int) *expr.Value {
	m, ok := lit.GetKind().(*expr.Value_MapValue)
	if !ok {
		return lit
	}

	entries := m.MapValue.GetEntries()
	var out []*expr.MapValue_Entry
	for _, field := range declared.GetFields() {
		fallback := field.GetDefault().GetLiteral()
		if fallback == nil {
			continue
		}

		if slices.ContainsFunc(entries, func(e *expr.MapValue_Entry) bool { return e.GetKey().GetStringValue() == field.GetName() }) {
			continue
		}

		// The work one normalization does is bounded where it is done, whoever the
		// caller is: past the budget a field stays as the value left it, and a
		// caller that needs a refusal has asked [CheckDefaultFillBound] first.
		if *budget <= 0 {
			break
		}
		*budget--

		if out == nil {
			out = append([]*expr.MapValue_Entry(nil), entries...)
		}
		out = append(out, &expr.MapValue_Entry{
			Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: field.GetName()}},
			Value: fallback,
		})
	}
	if out == nil {
		return lit
	}

	return &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: out}}}
}

// maxDefaultFills bounds how many field defaults filling one submitted value may
// write. A record that leaves every defaulted field out is a few bytes on the wire
// and one entry per field once filled, so the bound is on the work the fill does,
// stated apart from the size and element bounds that are judged after it.
const maxDefaultFills = 1 << 16

// CheckDefaultFillBound refuses a value whose fill would write more than
// [maxDefaultFills] field defaults, counting without allocating, so a submitted map
// or list of near-empty records cannot grow a thousandfold before any bound on the
// value is read. It counts what [NormalizeWireValue] would write, a default's own
// nested records included, and stops counting once the bound is passed.
func CheckDefaultFillBound(table TypeTable, t *Type, lit *expr.Value) error {
	budget := maxDefaultFills
	countFills(table, t, lit, 0, &budget)
	if budget < 0 {
		return fmt.Errorf("leaves out more than %d fields that take a default; send those fields, or fewer records", maxDefaultFills)
	}

	return nil
}

func countFills(table TypeTable, t *Type, lit *expr.Value, depth int, budget *int) {
	if lit == nil || depth > MaxStructureDepth || *budget < 0 {
		return
	}

	switch kind := t.GetKind().(type) {
	case *Type_List:
		for _, element := range lit.GetListValue().GetValues() {
			countFills(table, kind.List, element, depth+1, budget)
		}

	case *Type_Map_:
		for _, entry := range lit.GetMapValue().GetEntries() {
			countFills(table, kind.Map.GetValue(), entry.GetValue(), depth+1, budget)
		}

	case *Type_Message:
		declared := table[kind.Message]
		m := lit.GetMapValue()
		if declared == nil || m == nil {
			return
		}

		// Every entry, as [normalizeEntries] walks them, so a key written twice
		// cannot hide the nested work of the one that is not first.
		fields := make(map[string]*InputDeclaration, len(declared.GetFields()))
		for _, field := range declared.GetFields() {
			fields[field.GetName()] = field
		}
		for _, entry := range m.GetEntries() {
			if field := fields[entry.GetKey().GetStringValue()]; field != nil {
				countFills(table, field.DeclaredType(), entry.GetValue(), depth+1, budget)
			}
		}

		for _, field := range declared.GetFields() {
			if slices.ContainsFunc(m.GetEntries(), func(e *expr.MapValue_Entry) bool { return e.GetKey().GetStringValue() == field.GetName() }) {
				continue
			}

			if fallback := field.GetDefault().GetLiteral(); fallback != nil {
				*budget--
				countFills(table, field.DeclaredType(), fallback, depth+1, budget)
			}
		}
	}
}
