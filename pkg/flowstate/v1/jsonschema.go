package flowstatev1

import "slices"

// jsonSchemaDialect is the dialect every schema this file writes declares.
const jsonSchemaDialect = "https://json-schema.org/draft/2020-12/schema"

// InputsJSONSchema renders the inputs wf declares as a JSON Schema (2020-12): one
// object whose properties are the inputs, with the record types they reach under
// `$defs`.
//
// It is a projection of the declarations and nothing more. What the schema says
// is never stricter than what binding a run enforces, and says what JSON Schema
// can: the
// shape of each value, a field's required-ness, an enum's members, the length and
// item bounds, and that an input or a record field the workflow does not declare
// is refused (`additionalProperties: false`). What cannot be said is carried as an
// annotation for a reader to show, never as a keyword a validator would
// silently misjudge: a `must:` as `x-flowstate-must`, a `sensitive:` as
// `x-flowstate-sensitive`. A sensitive declaration's `default` and `examples` are
// left out, since a schema is handed to readers a specification is not.
//
// It is weaker than binding in places: a nested number is judged by its literal
// kind (a whole number for a `double` field is refused by binding and accepted
// here), an `int` is bounded to 64 bits, and a `must:` is not evaluated.
//
// A timestamp is a `date-time` string, bytes are base64 text, and a duration is
// a string described by its description (`format: duration` is ISO 8601, which
// is not what a Flowfile writes).
//
// A workflow that declares no inputs yields an empty closed object.
func InputsJSONSchema(wf *Workflow) map[string]any {
	b := newSchemaBuilder(wf)

	properties := make(map[string]any, len(wf.GetDeclaredInputs()))
	var required []string
	for _, declaration := range wf.GetDeclaredInputs() {
		properties[declaration.GetName()] = b.declaration(0, declarationFacts{
			description: declaration.GetDescription(),
			sensitive:   declaration.GetSensitive(),
			must:        declaration.GetMust(),
			values:      declaration.GetValues(),
			typed:       declaration.DeclaredType(),
			minLen:      declaration.GetMinLen(),
			maxLen:      declaration.GetMaxLen(),
			minItems:    declaration.GetMinItems(),
			maxItems:    declaration.GetMaxItems(),
			defaulted:   declaration.GetDefault(),
			example:     declaration.GetExample(),
		})
		if declaration.GetRequired() {
			required = append(required, declaration.GetName())
		}
	}

	return b.root(wf.GetName()+" inputs", properties, required)
}

// OutputsJSONSchema renders the outputs wf declares as a JSON Schema (2020-12), by
// the rules [InputsJSONSchema] states. A declared output is evaluated when the
// run ends, so every one is required.
func OutputsJSONSchema(wf *Workflow) map[string]any {
	b := newSchemaBuilder(wf)

	properties := make(map[string]any, len(wf.GetDeclaredOutputs()))
	required := make([]string, 0, len(wf.GetDeclaredOutputs()))
	for _, declaration := range wf.GetDeclaredOutputs() {
		properties[declaration.GetName()] = b.declaration(0, declarationFacts{
			description: declaration.GetDescription(),
			sensitive:   declaration.GetSensitive(),
			must:        declaration.GetMust(),
			values:      declaration.GetValues(),
			typed:       declaration.DeclaredType(),
		})
		required = append(required, declaration.GetName())
	}

	return b.root(wf.GetName()+" outputs", properties, required)
}

// declarationFacts is what an input, an output and a record field share that a
// schema carries, so the three are rendered by one function.
type declarationFacts struct {
	description string
	sensitive   bool
	must        string
	values      []string
	typed       *Type

	minLen, maxLen     uint64
	minItems, maxItems uint64
	defaulted, example *Value
}

type schemaBuilder struct {
	table TypeTable
	defs  map[string]any
}

func newSchemaBuilder(wf *Workflow) *schemaBuilder {
	return &schemaBuilder{table: TypesOf(wf), defs: map[string]any{}}
}

func (b *schemaBuilder) root(title string, properties map[string]any, required []string) map[string]any {
	schema := map[string]any{
		"$schema":              jsonSchemaDialect,
		"title":                title,
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	if len(b.defs) > 0 {
		schema["$defs"] = b.defs
	}

	return schema
}

// declaration is the schema of one declared value.
func (b *schemaBuilder) declaration(depth int, facts declarationFacts) map[string]any {
	schema := b.typed(facts.typed, depth)

	if facts.typed.GetEnum() && len(facts.values) > 0 {
		schema["enum"] = slices.Clone(facts.values)
	}

	switch facts.typed.GetKind().(type) {
	case *Type_List:
		putNonZero(schema, "minItems", facts.minItems)
		putNonZero(schema, "maxItems", facts.maxItems)
	case *Type_Map_:
		putNonZero(schema, "minProperties", facts.minItems)
		putNonZero(schema, "maxProperties", facts.maxItems)
	default:
		if schema["type"] == "string" {
			putNonZero(schema, "minLength", facts.minLen)
			putNonZero(schema, "maxLength", facts.maxLen)
		}
	}

	if facts.description != "" {
		// A duration carries how it is written; the author's words add to that.
		if built, ok := schema["description"].(string); ok {
			schema["description"] = facts.description + " (" + built + ")"
		} else {
			schema["description"] = facts.description
		}
	}
	if facts.sensitive {
		schema["x-flowstate-sensitive"] = true
	}
	if facts.must != "" {
		schema["x-flowstate-must"] = facts.must
	}
	if facts.defaulted != nil && !facts.sensitive {
		if value, err := LiteralToGo(facts.defaulted.GetLiteral()); err == nil {
			schema["default"] = value
		}
	}
	if facts.example != nil && !facts.sensitive {
		if value, err := LiteralToGo(facts.example.GetLiteral()); err == nil {
			schema["examples"] = []any{value}
		}
	}

	return schema
}

// typed is the schema of a type. A record is a `$ref` to its definition, which is
// written once however many places name it. A type deeper than
// [MaxStructureDepth], or one with no shape to state, is `{}`: any value.
func (b *schemaBuilder) typed(t *Type, depth int) map[string]any {
	if t == nil || depth > MaxStructureDepth {
		return map[string]any{}
	}

	switch kind := t.GetKind().(type) {
	case *Type_Scalar_:
		return scalarSchema(kind.Scalar)
	case *Type_List:
		return map[string]any{"type": "array", "items": b.typed(kind.List, depth+1)}
	case *Type_Map_:
		return map[string]any{"type": "object", "additionalProperties": b.typed(kind.Map.GetValue(), depth+1)}
	case *Type_Enum:
		return map[string]any{"type": "string"}
	case *Type_Message:
		return b.record(kind.Message, depth+1)
	}

	return map[string]any{}
}

func scalarSchema(scalar Type_Scalar) map[string]any {
	switch scalar {
	case Type_SCALAR_STRING:
		return map[string]any{"type": "string"}
	case Type_SCALAR_INT:
		return map[string]any{"type": "integer"}
	case Type_SCALAR_DOUBLE:
		return map[string]any{"type": "number"}
	case Type_SCALAR_BOOL:
		return map[string]any{"type": "boolean"}
	case Type_SCALAR_BYTES:
		return map[string]any{"type": "string", "contentEncoding": "base64"}
	case Type_SCALAR_TIMESTAMP:
		return map[string]any{"type": "string", "format": "date-time"}
	case Type_SCALAR_DURATION:
		return map[string]any{"type": "string", "description": "a duration such as 90m or 5400s"}
	case Type_SCALAR_NULL_TYPE:
		return map[string]any{"type": "null"}
	}

	return map[string]any{}
}

// record returns the reference to a record's definition, writing the definition
// the first time the record is named. The definition is entered before its fields
// are read, so a record that reaches itself (a hand-built specification; the
// compiler refuses a cycle) ends at the reference rather than looping.
func (b *schemaBuilder) record(name string, depth int) map[string]any {
	reference := map[string]any{"$ref": "#/$defs/" + name}

	declaration, ok := b.table[name]
	if !ok {
		// A descriptor-backed type: a closed object whose members this workflow
		// does not state.
		return map[string]any{"type": "object"}
	}
	if _, written := b.defs[name]; written {
		return reference
	}
	if depth > MaxStructureDepth {
		// Declarations are bounded to this depth by [CheckRecordDeclarations]; a
		// specification that skipped it ends here rather than recursing on.
		return map[string]any{"type": "object"}
	}
	b.defs[name] = nil

	properties := make(map[string]any, len(declaration.GetFields()))
	var required []string
	for _, field := range declaration.GetFields() {
		properties[field.GetName()] = b.declaration(depth, declarationFacts{
			description: field.GetDescription(),
			sensitive:   field.GetSensitive(),
			must:        field.GetMust(),
			values:      field.GetValues(),
			typed:       field.DeclaredType(),
			minLen:      field.GetMinLen(),
			maxLen:      field.GetMaxLen(),
			minItems:    field.GetMinItems(),
			maxItems:    field.GetMaxItems(),
			defaulted:   field.GetDefault(),
			example:     field.GetExample(),
		})
		if field.GetRequired() {
			required = append(required, field.GetName())
		}
	}

	definition := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		definition["required"] = required
	}
	if description := declaration.GetDescription(); description != "" {
		definition["description"] = description
	}
	if must := declaration.GetMust(); must != "" {
		definition["x-flowstate-must"] = must
	}
	b.defs[name] = definition

	return reference
}

func putNonZero(schema map[string]any, key string, n uint64) {
	if n > 0 {
		schema[key] = n
	}
}
