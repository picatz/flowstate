package flowstatev1

import (
	"fmt"
	"slices"
	"strconv"
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

// MaxRecordTypes bounds the record types one workflow declares, matching the
// schema's bound on [Workflow].
const MaxRecordTypes = 64

// MaxRecordFields bounds the fields of one record type. It matches the bound the
// schema puts on [TypeDeclaration], restated here because a hand-built
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
func (table TypeTable) checkRecord(r valueRendering, name string, literal *expr.Value, path string, depth int) error {
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

	// In the order the value was written, so the key a refusal names is the same
	// one every time and on both drivers.
	for _, entry := range m.MapValue.GetEntries() {
		key := entry.GetKey().GetStringValue()
		if !slices.ContainsFunc(fields, func(f *InputDeclaration) bool { return f.GetName() == key }) {
			// The key is the sender's text, so it is bounded, and withheld for a
			// sensitive declaration like any other value the run computed.
			shown := redactedIfSensitive(r.sensitive, func() string { return strconv.Quote(r.show(key)) })
			return fmt.Errorf("a field %s that %s does not declare%s; it declares %s",
				shown, name, atPath(path), recordFieldNames(fields))
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

		if err := table.checkField(r, field, value, fieldPath, depth+1); err != nil {
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
func (table TypeTable) checkField(r valueRendering, field *InputDeclaration, value *expr.Value, path string, depth int) error {
	declared := field.GetType()
	if depth > MaxStructureDepth {
		// Declarations are bounded to this depth by [CheckRecordDeclarations], so
		// only a specification that skipped it arrives here; refuse rather than
		// stop judging, because a closed record that is judged only to a depth is
		// not closed.
		return fmt.Errorf("a record nested deeper than %d levels%s", MaxStructureDepth, atPath(path))
	}
	if declared == InputDeclaration_TYPE_UNSPECIFIED {
		return nil
	}

	// A timestamp, a duration and bytes are judged by whether the text reads as one,
	// not by the kind of literal; see [NormalizeDataKind].
	if IsDataKind(declared) {
		return checkDataKindAt(declared, value, path)
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

	if declared == InputDeclaration_TYPE_INT && aboveLargestInt(value) {
		return fmt.Errorf("a value above the largest int%s, but the field is declared %s", atPath(path), fieldTypeName(field))
	}

	if declared == InputDeclaration_TYPE_FLOAT {
		if spelling, nonFinite := nonFiniteSpelling(value.GetDoubleValue()); nonFinite {
			return fmt.Errorf("%s%s, which is not a finite number", spelling, atPath(path))
		}
	}

	if structural := field.GetValueType(); structural != nil {
		if err := checkLiteralShapeAt(table, r, structural, value, path, depth); err != nil {
			return err
		}
	}

	if err := checkEnumMembership("field", strings.TrimPrefix(path, "."), declared, field.GetValues(),
		r, value); err != nil {
		return err
	}

	// The bounds an input carries, by the same functions, so a length is counted
	// one way wherever it is declared.
	subject := "the field" + atPath(path)
	if err := checkStringConstraints(subject, field, value); err != nil {
		return err
	}

	return checkListConstraints(subject, field, value)
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
// thing standing between an author and an unbounded literal. An input or an
// output whose type holds a `sensitive` field must be sensitive itself (see
// [TypeTable.HoldsSensitive]).
//
// The compiler runs it with a position to point at through the same
// function, and [CheckDeclarationTypes] runs it again for a specification that
// never was a Flowfile.
func CheckRecordDeclarations(wf *Workflow) error {
	// The schema's bound, restated for a specification that skipped the schema:
	// everything below walks the declarations, so the count comes first.
	if n := len(wf.GetDeclaredTypes()); n > MaxRecordTypes {
		return fmt.Errorf("%d types are declared; the most a workflow declares is %d", n, MaxRecordTypes)
	}

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

		if declaration.Must != nil {
			if _, err := CompileMustExpression(wf.GetProfile(), declaration.GetMust(), InputDeclaration_TYPE_STRUCT); err != nil {
				return fmt.Errorf("type %q %w", name, err)
			}
		}

		fields := make(map[string]bool, len(declaration.GetFields()))
		for _, field := range declaration.GetFields() {
			if fields[field.GetName()] {
				return fmt.Errorf("type %q declares field %q twice", name, field.GetName())
			}
			fields[field.GetName()] = true

			if err := checkRecordField(name, field, table, wf.GetProfile()); err != nil {
				return err
			}
		}
	}

	for _, declaration := range wf.GetDeclaredInputs() {
		if err := checkTypeResolves(declaration.GetValueType(), table); err != nil {
			return fmt.Errorf("input %q: %w", declaration.GetName(), err)
		}
		if err := checkHoldsSensitive("input", declaration, table); err != nil {
			return err
		}
	}
	for _, declaration := range wf.GetDeclaredOutputs() {
		if err := checkTypeResolves(declaration.GetValueType(), table); err != nil {
			return fmt.Errorf("output %q: %w", declaration.GetName(), err)
		}
		if err := checkHoldsSensitive("output", declaration, table); err != nil {
			return err
		}
	}

	if err := checkRecordCycles(wf.GetDeclaredTypes(), table); err != nil {
		return err
	}

	if err := checkRecordDepth(wf.GetDeclaredTypes(), table); err != nil {
		return err
	}

	if err := checkRecordFillBound(wf.GetDeclaredTypes(), table); err != nil {
		return err
	}

	for _, declaration := range wf.GetDeclaredTypes() {
		for _, field := range declaration.GetFields() {
			if err := checkRecordFieldValues(declaration.GetName(), field, table, wf.GetProfile()); err != nil {
				return err
			}
		}
	}

	return nil
}

// maxTypeFill bounds the field defaults one record, left empty, expands to once its
// nested records take their own defaults. Each default is a small literal, but a
// default can be a record whose fields default to records, so the expansion
// multiplies down the type; it is refused where the type is declared, ahead of any
// value that reaches it.
const maxTypeFill = 4096

func checkRecordFillBound(declared []*TypeDeclaration, table TypeTable) error {
	empty := &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{}}}
	for _, declaration := range declared {
		budget := maxTypeFill
		countFills(table, recordTypeNamed(declaration.GetName()), empty, 0, &budget)
		if budget < 0 {
			return fmt.Errorf("type %q expands to more than %d field defaults when a value leaves its fields out; "+
				"flatten the defaults of the records it names", declaration.GetName(), maxTypeFill)
		}
	}

	return nil
}

func recordTypeNamed(name string) *Type { return &Type{Kind: &Type_Message{Message: name}} }

func checkRecordField(record string, field *InputDeclaration, table TypeTable, profile string) error {
	name := field.GetName()
	if vt := field.GetValueType(); vt != nil {
		if err := checkTypeDepth(vt, MaxStructureDepth); err != nil {
			return fmt.Errorf("type %q field %q: %w", record, name, err)
		}
	}
	if err := Validate(field); err != nil {
		return fmt.Errorf("type %q field %q is invalid: %w", record, name, err)
	}

	if err := checkBoundsShape(fmt.Sprintf("type %q field %q", record, name), "field", field); err != nil {
		return err
	}

	if field.Must != nil {
		if _, err := CompileMustExpression(profile, field.GetMust(), field.GetType()); err != nil {
			return fmt.Errorf("type %q field %q %w", record, name, err)
		}
	}

	if err := checkTypeResolves(field.GetValueType(), table); err != nil {
		return fmt.Errorf("type %q field %q: %w", record, name, err)
	}

	if field.GetRequired() && field.GetDefault() != nil {
		return fmt.Errorf("type %q field %q is `required: true` and also has a `default:`, which contradict: "+
			"a required field is never absent, so the default can never be used; remove one", record, name)
	}

	return nil
}

// checkRecordFieldValues holds a field's default and example to what an input's are:
// a literal of the field's type that satisfies the field's own rules, so a stale one
// is a defect in the file rather than a surprise at the first run that leaves the
// field out. It runs once the whole table has passed its structural checks (cycles,
// depth and the fill bound), because judging a default fills the records it names.
func checkRecordFieldValues(record string, field *InputDeclaration, table TypeTable, profile string) error {
	if err := CheckInputDefaultIn(table, profile, field); err != nil {
		return fmt.Errorf("type %q field %q default: %w", record, field.GetName(), err)
	}
	if err := CheckInputExampleIn(table, profile, field); err != nil {
		return fmt.Errorf("type %q field %q %w", record, field.GetName(), err)
	}

	return nil
}

// HoldsSensitive reports whether a value of t can carry a field declared
// `sensitive`, at any depth: t is a record with one, or a list or map of records
// that reach one.
//
// Sensitivity is whole-value everywhere a run is shown (results, timelines, the
// debugger, failure text), so a value that holds a sensitive field is withheld
// whole rather than field by field. Each record is entered once, so the work is
// bounded by the number of declared types.
func (table TypeTable) HoldsSensitive(t *Type) bool {
	seen := map[string]bool{}

	var walk func(*Type) bool
	walk = func(t *Type) bool {
		for _, name := range messageNames(t, nil, 0) {
			if seen[name] {
				continue
			}
			seen[name] = true

			for _, field := range table[name].GetFields() {
				if field.GetSensitive() || walk(field.GetValueType()) {
					return true
				}
			}
		}

		return false
	}

	return walk(t)
}

// DeriveSensitive marks every input and output whose type holds a sensitive field
// `sensitive` itself, so what is withheld is decided where the type is declared and
// not repeated at each use. The Flowfile compiler runs it; the specification check
// ([CheckRecordDeclarations]) refuses a declaration it has not been run for.
func DeriveSensitive(wf *Workflow) {
	table := TypesOf(wf)
	if table == nil {
		return
	}

	for _, declaration := range wf.GetDeclaredInputs() {
		if table.HoldsSensitive(declaration.GetValueType()) {
			declaration.Sensitive = true
		}
	}
	for _, declaration := range wf.GetDeclaredOutputs() {
		if table.HoldsSensitive(declaration.GetValueType()) {
			declaration.Sensitive = true
		}
	}
}

// typedDeclaration is what an input and an output share that the sensitivity
// check reads.
type typedDeclaration interface {
	GetName() string
	GetSensitive() bool
	GetValueType() *Type
	TypeText() string
}

func checkHoldsSensitive(kind string, declaration typedDeclaration, table TypeTable) error {
	if declaration.GetSensitive() || !table.HoldsSensitive(declaration.GetValueType()) {
		return nil
	}

	return fmt.Errorf("%s %q is typed %s, which holds a sensitive field, so it must be declared `sensitive: true`",
		kind, declaration.GetName(), declaration.TypeText())
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

// checkRecordDepth refuses a chain of records deeper than a value walk will
// follow. [checkRecordCycles] has already established the graph is acyclic, so
// the height of each record is finite; it is memoized, so the work is the number
// of references. One record level costs one level of the walk plus every list or
// map around the next record, which is what a value nested that deep would cost.
func checkRecordDepth(declared []*TypeDeclaration, table TypeTable) error {
	heights := make(map[string]int, len(declared))

	var height func(name string) int
	height = func(name string) int {
		if h, ok := heights[name]; ok {
			return h
		}

		h := 0
		for _, field := range table[name].GetFields() {
			h = max(h, 1+typeHeight(field.GetValueType(), height, 0))
		}
		heights[name] = h

		return h
	}

	for _, d := range declared {
		if h := height(d.GetName()); h > MaxStructureDepth {
			return fmt.Errorf("type %q nests records %d levels deep; the most a value may nest is %d",
				d.GetName(), h, MaxStructureDepth)
		}
	}

	return nil
}

func typeHeight(t *Type, record func(string) int, depth int) int {
	if depth > MaxStructureDepth {
		return depth
	}

	switch kind := t.GetKind().(type) {
	case *Type_Message:
		return record(kind.Message)
	case *Type_List:
		return 1 + typeHeight(kind.List, record, depth+1)
	case *Type_Map_:
		return 1 + typeHeight(kind.Map.GetValue(), record, depth+1)
	}

	return 0
}
