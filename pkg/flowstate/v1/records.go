package flowstatev1

import (
	"fmt"
	"slices"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Record types: the named, closed shapes a workflow declares with `types:`.
//
// A record is declared once ([TypeDeclaration]) and referred to by name from a
// [Type_Message]. Resolving that name needs the declarations, which travel in the
// [Workflow], so the checks that judge a value against a type take a [TypeTable]
// beside it. The `...In` variants of the input and output checks accept one; the
// plain names are the same checks with no table, where a record name is
// unresolvable and so accepts what it cannot judge, exactly as a message type did
// before there was a table to read.

// MaxRecordFields bounds the fields of one record type. It matches the bound the
// schema puts on [TypeDeclaration.fields], restated here because a hand-built
// declaration does not pass through the schema.
const MaxRecordFields = 64

// TypeTable resolves a record type's name to its declaration. The zero value and
// nil resolve nothing.
type TypeTable map[string]*TypeDeclaration

// TypesOf indexes the record types wf declares. A later declaration of a name
// already seen is ignored: the compiler refuses a duplicate, and a hand-built
// specification that carries one is judged by the first, which is the one a reader
// scanning the list meets first.
func TypesOf(wf *Workflow) TypeTable {
	declared := wf.GetDeclaredTypes()
	if len(declared) == 0 {
		return nil
	}

	table := make(TypeTable, len(declared))
	for _, d := range declared {
		if _, seen := table[d.GetName()]; !seen {
			table[d.GetName()] = d
		}
	}

	return table
}

// checkRecord refuses a literal that is not a value of the record name: a map
// keyed by field name that carries every required field, nothing the type does not
// declare, and in each field a value of the type the field declares.
//
// The first failure is reported and is named by its path, so a nested record says
// where: `a string at .address.zip`. A name the table cannot resolve is an error
// when the table exists, because a specification that points at a type it does not
// carry cannot be judged and fails closed; with no table at all it accepts, for
// the reason the package comment gives.
func (table TypeTable) checkRecord(name string, literal *expr.Value, path string, depth int) error {
	if table == nil {
		return nil
	}

	declaration, ok := table[name]
	if !ok {
		return fmt.Errorf("the record type %s, which this specification does not declare%s", name, atPath(path))
	}

	m, isMap := literal.GetKind().(*expr.Value_MapValue)
	if !isMap {
		return fmt.Errorf("%s%s, not a %s record", literalKindName(literal), atPath(path), name)
	}

	fields := declaration.GetFields()
	present := make(map[string]*expr.Value, len(m.MapValue.GetEntries()))
	for _, entry := range m.MapValue.GetEntries() {
		key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue)
		if !isString {
			return fmt.Errorf("a record with %s key%s", literalKindName(entry.GetKey()), atPath(path))
		}
		present[key.StringValue] = entry.GetValue()
	}

	for key := range present {
		if !slices.ContainsFunc(fields, func(f *InputDeclaration) bool { return f.GetName() == key }) {
			return fmt.Errorf("a field %q that %s does not declare%s; it declares %s",
				key, name, atPath(path), recordFieldNames(fields))
		}
	}

	for _, field := range fields {
		value, given := present[field.GetName()]
		fieldPath := path + "." + field.GetName()
		if !given {
			if field.GetRequired() {
				return fmt.Errorf("no required field %q of %s%s", field.GetName(), name, atPath(path))
			}
			continue
		}

		if err := table.checkField(field, value, fieldPath, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// checkField judges one field's value by the declaration of the field, by the
// rules an input is judged by: the kind of value the legacy type names, what a
// container holds, and an enum's members. It reports the leaf with the whole path
// from the record's root rather than wrapping one sentence in another per level,
// so a mistake three records down reads `a string at .lines[0].quantity`.
func (table TypeTable) checkField(field *InputDeclaration, value *expr.Value, path string, depth int) error {
	declared := field.GetType()
	if depth > MaxStructureDepth || declared == InputDeclaration_TYPE_UNSPECIFIED {
		return nil
	}

	got, ok := inputTypeOf(value)
	if !ok {
		return fmt.Errorf("%s%s, which is not a kind of value a field can hold; it is declared %s",
			literalKindName(value), atPath(path), DeclaredTypeName(declared))
	}

	// An enum travels as a string, the arm [checkDeclaredLiteralType] shares.
	switch {
	case StringShaped(declared) && got != InputDeclaration_TYPE_STRING,
		!StringShaped(declared) && got != declared:
		return fmt.Errorf("%s%s, but the field is declared %s", literalKindName(value), atPath(path), fieldTypeName(field))
	}

	if declared == InputDeclaration_TYPE_FLOAT {
		if spelling, nonFinite := nonFiniteSpelling(value.GetDoubleValue()); nonFinite {
			return fmt.Errorf("%s%s, which is not a finite number", spelling, atPath(path))
		}
	}

	if structural := field.GetValueType(); structural != nil {
		if err := checkLiteralShapeAt(table, structural, value, path, depth); err != nil {
			return err
		}
	}

	return checkEnumMembership("field", strings.TrimPrefix(path, "."), declared, field.GetValues(),
		valueRendering{bounded: true}, value)
}

func fieldTypeName(field *InputDeclaration) string {
	if vt := field.GetValueType(); vt != nil {
		return TypeString(vt)
	}

	return DeclaredTypeName(field.GetType())
}

func recordFieldNames(fields []*InputDeclaration) string {
	if len(fields) == 0 {
		return "no fields"
	}

	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.GetName())
	}

	return quotedStrings(names)
}

// CheckRecordDeclarations reports whether the record types wf declares are
// well-formed and whether every declaration that names one can find it.
//
// It is the schema's rules plus the ones a schema cannot state: a name declared
// twice, a field repeated, a type that refers to a record nobody declared, and a
// cycle. A cycle is refused because a value of a recursive record has no bound
// until the type has one, and the checks that walk a value would be the only
// thing standing between an author and an unbounded literal. A field that sets
// `default`, `example`, `sensitive`, `must` or a length or item bound is refused with the reason
// [TypeDeclaration.fields] gives, rather than carrying a promise nothing keeps.
//
// The compiler runs it against positions' worth of context through the same
// function, and [CheckDeclarationTypes] runs it again for a specification that
// never was a Flowfile.
func CheckRecordDeclarations(wf *Workflow) error {
	table := TypesOf(wf)

	seen := make(map[string]bool, len(wf.GetDeclaredTypes()))
	for _, declaration := range wf.GetDeclaredTypes() {
		name := declaration.GetName()
		if seen[name] {
			return fmt.Errorf("type %q is declared twice", name)
		}
		seen[name] = true

		if err := Validate(declaration); err != nil {
			return fmt.Errorf("type %q is invalid: %w", name, err)
		}
		if len(declaration.GetFields()) > MaxRecordFields {
			return fmt.Errorf("type %q declares %d fields; the most a record holds is %d",
				name, len(declaration.GetFields()), MaxRecordFields)
		}

		if len(declaration.GetFields()) == 0 {
			return fmt.Errorf("type %q declares no fields; a record holds at least one", name)
		}

		fields := make(map[string]bool, len(declaration.GetFields()))
		for _, field := range declaration.GetFields() {
			if fields[field.GetName()] {
				return fmt.Errorf("type %q declares field %q twice", name, field.GetName())
			}
			fields[field.GetName()] = true

			if err := checkRecordField(name, field, table); err != nil {
				return err
			}
		}
	}

	for _, declaration := range wf.GetDeclaredInputs() {
		if err := checkTypeResolves(declaration.GetValueType(), table); err != nil {
			return fmt.Errorf("input %q: %w", declaration.GetName(), err)
		}
	}
	for _, declaration := range wf.GetDeclaredOutputs() {
		if err := checkTypeResolves(declaration.GetValueType(), table); err != nil {
			return fmt.Errorf("output %q: %w", declaration.GetName(), err)
		}
	}

	return checkRecordCycles(wf.GetDeclaredTypes(), table)
}

func checkRecordField(record string, field *InputDeclaration, table TypeTable) error {
	name := field.GetName()
	if vt := field.GetValueType(); vt != nil {
		if err := checkTypeDepth(vt, MaxStructureDepth); err != nil {
			return fmt.Errorf("type %q field %q: %w", record, name, err)
		}
	}
	if err := Validate(field); err != nil {
		return fmt.Errorf("type %q field %q is invalid: %w", record, name, err)
	}

	for _, unsupported := range []struct {
		word string
		set  bool
	}{
		{"default", field.GetDefault() != nil},
		{"example", field.GetExample() != nil},
		{"sensitive", field.GetSensitive()},
		{"must", field.Must != nil},
		{"min_len", field.MinLen != nil},
		{"max_len", field.MaxLen != nil},
		{"min_items", field.MinItems != nil},
		{"max_items", field.MaxItems != nil},
	} {
		if unsupported.set {
			return fmt.Errorf("type %q field %q sets `%s`, which a record field does not carry yet; "+
				"put it on the input or output that uses the type", record, name, unsupported.word)
		}
	}

	if err := checkTypeResolves(field.GetValueType(), table); err != nil {
		return fmt.Errorf("type %q field %q: %w", record, name, err)
	}

	return nil
}

// messageNames appends to into every record name t mentions, at any depth.
func messageNames(t *Type, into []string, depth int) []string {
	if depth > MaxStructureDepth {
		return into
	}

	switch kind := t.GetKind().(type) {
	case *Type_Message:
		return append(into, kind.Message)
	case *Type_List:
		return messageNames(kind.List, into, depth+1)
	case *Type_Map_:
		return messageNames(kind.Map.GetValue(), into, depth+1)
	}

	return into
}

func checkTypeResolves(t *Type, table TypeTable) error {
	for _, name := range messageNames(t, nil, 0) {
		if _, ok := table[name]; !ok {
			return fmt.Errorf("the type %s is not declared; declare it under `types:`", name)
		}
	}

	return nil
}

// checkRecordCycles refuses a record that reaches itself. Depth-first with the
// path kept, so the report names the loop; each record is entered once, so the
// work is the number of references, which the field bound limits.
func checkRecordCycles(declared []*TypeDeclaration, table TypeTable) error {
	const (
		visiting = iota + 1
		done
	)

	state := make(map[string]int, len(declared))
	var path []string

	var visit func(name string) error
	visit = func(name string) error {
		switch state[name] {
		case visiting:
			start := slices.Index(path, name)
			return fmt.Errorf("type %s refers to itself: %s", name, strings.Join(append(slices.Clone(path[start:]), name), " -> "))
		case done:
			return nil
		}

		state[name] = visiting
		path = append(path, name)
		for _, field := range table[name].GetFields() {
			for _, next := range messageNames(field.GetValueType(), nil, 0) {
				if _, ok := table[next]; !ok {
					continue
				}
				if err := visit(next); err != nil {
					return err
				}
			}
		}
		path = path[:len(path)-1]
		state[name] = done

		return nil
	}

	for _, d := range declared {
		if err := visit(d.GetName()); err != nil {
			return err
		}
	}

	return nil
}
