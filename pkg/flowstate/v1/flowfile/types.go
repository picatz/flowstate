package flowfile

import (
	"cmp"
	"fmt"

	yaml "github.com/goccy/go-yaml"

	"github.com/goccy/go-yaml/ast"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The `types:` block: the record types a file names.
//
//	types:
//	  Order:
//	    description: What a customer bought.
//	    fields:
//	      id:     {type: string, required: true}
//	      status: {type: enum, values: [open, paid]}
//	      lines:  {type: "list(Line)"}
//
// A type with `type:` instead of `fields:` is a constrained scalar: a built-in
// scalar and a rule over `this`, used as `type: Uuid` on an input, an output or
// a field.
//
//	types:
//	  Uuid:
//	    type: string
//	    must: isUuid(this)
//	    example: 123e4567-e89b-42d3-a456-426614174000
//
// The runtime has no such thing. [compiler.lowerScalarTypes] rewrites each use to the
// base type and the type's rule conjoined with the use's own, and keeps the name
// in `type_source` so a file is written back as authored; see [v1.TypeDeclaration].
//
// A field is written exactly like an input, and is compiled by the function that
// compiles one ([compiler.declaredInput]) so a field cannot come to differ from an
// input in what it accepts. A field's `sensitive:` makes what is typed by the record
// sensitive whole; see [v1.DeriveSensitive].

var typeKeys = []string{"description", "type", "fields", "must", "example"}

// scalarBaseWords are the spellings of the bases a constrained scalar may have, for
// the diagnostics that list them.
const scalarBaseWords = "string, int, double, bool, timestamp, duration, bytes"

// declaredTypes compiles the top-level `types:` block, one entry per record, in
// the order written.
//
// The names are collected first, before any declaration is read, so a type may
// refer to one written below it and an input may refer to either. Everything a
// set of types can be wrong about — a cycle, a name used twice, a reference to a
// type nobody declared — belongs to [v1.CheckRecordDeclarations], which sees the
// compiled workflow.
func (c *compiler) declaredTypes(n ast.Node, path string, r ref) []*v1.TypeDeclaration {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	// Bounded before anything is built from the names: the environment holds one
	// constant per name, and the file is the sender's.
	if len(entries) > v1.MaxRecordTypes {
		c.report(spanOfNode(c.resolveQuiet(n)), r,
			"declares %d types; the most a workflow declares is %d", len(entries), v1.MaxRecordTypes)

		return nil
	}

	c.typeNames = make(map[string]bool, len(entries))
	c.scalarNames = map[string]bool{}
	c.scalarTypes = map[string]*v1.TypeDeclaration{}
	for _, e := range entries {
		c.typeNames[e.name] = true
		if c.isScalarEntry(e.value) {
			c.scalarNames[e.name] = true
		}
	}
	env, err := newTypeEnv(c.typeNames)
	if err != nil {
		c.report(spanOfNode(c.resolveQuiet(n)), r, "type environment: %s", err)

		return nil
	}
	c.typeEnv = env

	declarations := make([]*v1.TypeDeclaration, 0, len(entries))
	for _, e := range entries {
		if declaration := c.declaredType(e, path); declaration != nil {
			declarations = append(declarations, declaration)
		}
	}

	if len(declarations) == 0 {
		return nil
	}

	return declarations
}

// isScalarEntry reports whether a type's declaration says `type:`, which is what
// makes it a constrained scalar. Silent: [compiler.declaredType] reads the same
// mapping and reports what is wrong with it.
func (c *compiler) isScalarEntry(n ast.Node) bool {
	var values []*ast.MappingValueNode
	switch node := c.resolveQuiet(n).(type) {
	case *ast.MappingNode:
		values = node.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{node}
	}
	for _, v := range values {
		if name, ok := keyNameOf(v.Key); ok && name == "type" {
			return true
		}
	}

	return false
}

func (c *compiler) declaredType(e entry, parent string) *v1.TypeDeclaration {
	path := fieldPath(parent, e.name)
	r := ref{path: path, label: "type " + e.name}

	c.pos.record(path, spanOfNode(c.resolveQuiet(e.value)))

	entries, ok := c.entries(e.value, path, r)
	if !ok {
		c.report(spanOfNode(e.value), r,
			"is declared as a mapping with `fields:`, a mapping of field names to declarations written like inputs")
		return nil
	}

	fields := c.check(entries, r, typeKeys)

	declaration := &v1.TypeDeclaration{Name: e.name}

	if f, found := fields.get("description"); found {
		descriptionPath := fieldPath(path, "description")
		if description, ok := c.text(f.value, descriptionPath,
			ref{path: descriptionPath, label: "type " + e.name + " description"}); ok {
			declaration.Description = proto.String(description)
		}
	}

	if f, found := fields.get("type"); found {
		c.scalarBase(declaration, f, path, e.name)
		c.scalarTypes[e.name] = declaration
	} else if f, found := fields.get("example"); found {
		c.report(spanOfNode(f.key), r,
			"is a record, which has no `example:`; a constrained scalar (`type:` and `must:`) does, and a field's own `example:` goes on the field")
	}

	if f, found := fields.get("must"); found {
		mustPath := fieldPath(path, "must")
		mustRef := ref{path: mustPath, label: "type " + e.name + " must"}
		if must, ok := c.text(f.value, mustPath, mustRef); ok {
			c.must(must, spanOfNode(c.resolveQuiet(f.value)), mustRef, func(must string, source *string) {
				declaration.Must, declaration.MustSource = proto.String(must), source
			})
		}
	}

	if f, found := fields.get("example"); found && declaration.Base != nil {
		examplePath := fieldPath(path, "example")
		declaration.Example = c.inputValue(f.value, examplePath,
			ref{path: examplePath, label: "type " + e.name + " example"})
	}

	if f, found := fields.get("fields"); found {
		if declaration.Base != nil {
			c.report(spanOfNode(f.key), r,
				"is both a constrained scalar (`type: %s`) and a record (`fields:`); a type is one or the other",
				declaredTypeText(declaration.GetBase(), nil))

			return declaration
		}
		fieldsPath := fieldPath(path, "fields")
		if n, ok := c.resolveQuiet(f.value).(*ast.MappingNode); ok && len(n.Values) > v1.MaxRecordFields {
			c.report(spanOfNode(n), ref{path: fieldsPath, label: "type " + e.name + " fields"},
				"declares %d fields; the most a record holds is %d", len(n.Values), v1.MaxRecordFields)

			return declaration
		}
		declaration.Fields = c.declaredInputs(f.value, fieldsPath,
			ref{path: fieldsPath, label: "type " + e.name + " fields"}, "field")
	}

	return declaration
}

// scalarBase reads a constrained scalar's `type:` into declaration.Base. The base is
// a built-in scalar and nothing else: not another declared type, which would make
// a scalar type a chain to follow (and a cycle to refuse) for no use a base and a
// rule do not already have, and not a list, a map, an enum or a record.
func (c *compiler) scalarBase(declaration *v1.TypeDeclaration, f field, path, name string) {
	typePath := fieldPath(path, "type")
	typeRef := ref{path: typePath, label: "type " + name + " type"}
	c.pos.record(typePath, spanOfNode(c.resolveQuiet(f.value)))

	text, ok := c.text(f.value, typePath, typeRef)
	if !ok {
		return
	}
	if c.typeNames[text] {
		c.report(spanOfNode(f.value), typeRef,
			"is %q, another declared type; a constrained scalar is based on a built-in scalar (%s), and declared types do not build on one another",
			text, scalarBaseWords)

		return
	}

	legacy, structural, err := declareType(c.typeEnv, text)
	switch {
	case err != nil:
		c.report(spanOfNode(f.value), typeRef,
			"is %q, which is not a scalar a type can constrain: %s; the bases are %s", text, err, scalarBaseWords)
	case !v1.ScalarBase(legacy) || (structural != nil && structural.GetScalar() == v1.Type_SCALAR_UNSPECIFIED):
		c.report(spanOfNode(f.value), typeRef,
			"is %q, which is not a scalar; the bases are %s. A record is declared with `fields:` instead", text, scalarBaseWords)
	default:
		declaration.Base = &legacy
	}
}

// scalarIn names the first constrained scalar a type expression holds, or "" when it
// holds none. A use is lowered only where the type is the whole of a `type:`, so one
// found inside `list(...)` or `map(...)` is a use the language does not have.
func (c *compiler) scalarIn(t *v1.Type) string {
	switch k := t.GetKind().(type) {
	case *v1.Type_Message:
		if c.scalarNames[k.Message] {
			return k.Message
		}
	case *v1.Type_List:
		return c.scalarIn(k.List)
	case *v1.Type_Map_:
		return c.scalarIn(k.Map.GetValue())
	}

	return ""
}

// scalarUse is a declaration whose `type:` named a constrained scalar, kept until
// the rule of the scalar is final. It holds pointers to the fields it lowers
// because an input, an output and a record field are all the same lowering over
// differently-typed messages.
type scalarUse struct {
	name string
	span Span
	r    ref

	typ        *v1.InputDeclaration_Type
	valueType  **v1.Type
	must       **string
	mustSource **string
	typeSource **string
}

// useScalar notes a declaration whose `type:` text read as structural. A
// constrained scalar that is the whole type is kept to lower later; one inside a
// list or map is refused where it is written.
func (c *compiler) useScalar(structural *v1.Type, text string, span Span, r ref, use scalarUse) {
	if name := structural.GetMessage(); name != "" && c.scalarNames[name] {
		use.name, use.span, use.r = name, span, r
		c.scalarUses = append(c.scalarUses, use)

		return
	}
	if name := c.scalarIn(structural); name != "" {
		c.report(span, r,
			"is %q, which holds the constrained scalar %s inside a container; a scalar type is used directly (`type: %s`), "+
				"and a rule over each element is a `must:` on the container, such as `this.all(x, ...)`",
			text, name, name)
	}
}

// lowerScalarTypes rewrites every use of a constrained scalar to what a run reads:
// the base type, and `must` as the type's rule conjoined with the use's own. It runs
// once the whole file is read, because a rule is final only after the `functions:`
// it calls have been expanded into it.
//
// The name stays in `type_source` and the use's own rule, as written, in
// `must_source`, so Marshal writes `type: Uuid` and the author's own `must:` and
// nothing the lowering made. Each use spends the type's rule against the same
// per-file budget a function expansion does, so a type with a large rule used
// many times is refused where the budget runs out and not at the first use.
func (c *compiler) lowerScalarTypes() {
	usable := map[string]bool{}
	for _, use := range c.scalarUses {
		t := c.scalarTypes[use.name]
		if t == nil || t.Base == nil {
			// Reported where the type is declared.
			continue
		}
		legacy, structural, err := declareType(nil, declaredTypeText(t.GetBase(), nil))
		if err != nil {
			continue
		}
		*use.typ, *use.valueType = legacy, structural

		if t.Must == nil {
			// A scalar with no rule is refused by the specification check, once,
			// at the type.
			continue
		}
		rule := t.GetMust()

		// A rule that does not compile is a defect of the type, reported once there
		// by the specification check; copying it to every use would report it at
		// each of them as well.
		ok, checked := usable[use.name]
		if !checked {
			_, err := v1.CompileMustExpression(v1.CurrentProfile, rule, t.GetBase())
			ok = err == nil
			usable[use.name] = ok
		}
		if !ok || !c.expanding(use.span, use.r, func() (int, error) { return v1.NodeCount(rule), nil }) {
			continue
		}

		if own := *use.must; own != nil {
			written := cmp.Or(deref(*use.mustSource), *own)
			*use.mustSource = &written
			conjoined := "(" + rule + ") && (" + *own + ")"
			*use.must = &conjoined
		} else {
			*use.must = &rule
		}
		typeSource := use.name
		*use.typeSource = &typeSource
	}
	c.scalarUses = nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

// writtenMust is the `must:` a declaration is written back with. A declaration
// that names a constrained scalar stores the type's rule in `must`, so only
// `must_source` is the author's: the author's own rule, or none. Otherwise it is the
// call as written when there is one, and the stored rule when there is not.
func writtenMust(must, source, typeSource *string) (string, bool) {
	if typeSource != nil {
		if source == nil {
			return "", false
		}

		return *source, true
	}
	if must == nil {
		return "", false
	}

	return cmp.Or(deref(source), *must), true
}

// declaredTypesToYAML is the inverse of [compiler.declaredTypes]: the `types:`
// block as written, in declaration order, with each field written by the one
// function an input is written by.
func declaredTypesToYAML(declared []*v1.TypeDeclaration) (yaml.MapSlice, error) {
	out := make(yaml.MapSlice, 0, len(declared))
	for _, d := range declared {
		var entry yaml.MapSlice
		if d.Description != nil {
			entry = append(entry, yaml.MapItem{Key: "description", Value: textToYAML(d.GetDescription())})
		}
		if d.Base != nil {
			entry = append(entry, yaml.MapItem{Key: "type", Value: declaredTypeText(d.GetBase(), nil)})
		}
		if d.Must != nil {
			entry = append(entry, yaml.MapItem{Key: "must", Value: textToYAML(cmp.Or(d.GetMustSource(), d.GetMust()))})
		}
		if d.GetExample() != nil {
			value, err := inputValueToYAML(d.GetExample())
			if err != nil {
				return nil, fmt.Errorf("type %s example: %w", d.GetName(), err)
			}
			entry = append(entry, yaml.MapItem{Key: "example", Value: value})
		}
		if len(d.GetFields()) > 0 {
			fields, err := declaredInputsToYAML(d.GetFields())
			if err != nil {
				return nil, fmt.Errorf("type %s: %w", d.GetName(), err)
			}
			entry = append(entry, yaml.MapItem{Key: "fields", Value: fields})
		}
		out = append(out, yaml.MapItem{Key: d.GetName(), Value: entry})
	}

	return out, nil
}

// validateDeclaredTypes reports what is wrong with the `types:` block as a
// whole, and with every declaration that names a type in it. The rules are
// [v1.CheckRecordDeclarations]'s, the same ones submit enforces for a
// specification that never was a Flowfile; this is where a line exists to point at.
func validateDeclaredTypes(wf *v1.Workflow) Diagnostics {
	if err := v1.CheckRecordDeclarations(wf); err != nil {
		return Diagnostics{{Field: "types", Message: err.Error()}}
	}

	return nil
}
