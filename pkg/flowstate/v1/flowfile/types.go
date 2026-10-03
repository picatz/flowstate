package flowfile

import (
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
// A field is written exactly like an input, and is compiled by the function that
// compiles one ([compiler.declaredInput]) so a field cannot come to differ from an
// input in what it accepts. What a record field does not carry yet is refused by
// [v1.CheckRecordDeclarations] with the reason, not parsed and ignored.

var typeKeys = []string{"description", "fields"}

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

	c.typeNames = make(map[string]bool, len(entries))
	for _, e := range entries {
		c.typeNames[e.name] = true
	}

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

	checkable := make([]entry, 0, len(entries))
	for _, en := range entries {
		if en.name == "must" {
			c.report(spanOfNode(en.key), r,
				"a rule over the whole record is not carried yet; put `must:` on a field, or on the input or output that uses the type")
			continue
		}
		checkable = append(checkable, en)
	}
	fields := c.check(checkable, r, typeKeys)

	declaration := &v1.TypeDeclaration{Name: e.name}

	if f, found := fields.get("description"); found {
		descriptionPath := fieldPath(path, "description")
		if description, ok := c.text(f.value, descriptionPath,
			ref{path: descriptionPath, label: "type " + e.name + " description"}); ok {
			declaration.Description = proto.String(description)
		}
	}

	if f, found := fields.get("fields"); found {
		fieldsPath := fieldPath(path, "fields")
		declaration.Fields = c.declaredInputs(f.value, fieldsPath,
			ref{path: fieldsPath, label: "type " + e.name + " fields"}, "field")
	}

	return declaration
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
