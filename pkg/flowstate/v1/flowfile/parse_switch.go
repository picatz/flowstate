package flowfile

import (
	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// These are the schema's repeated-field bounds. Enforce them while parsing as
// well as at the protobuf boundary: validation performs cross-case duplicate
// checks, so letting an oversized source reach that work would make the size
// limit too late to bound its cost.
const (
	maxSwitchCases      = 100
	maxSwitchCaseValues = 100
)

// A `switch:` is a mapping under the kind key — `value:`, `cases:`, optionally
// `default:` — matching the one-kind-key-per-step rule every block construct
// follows (`for_each: {items, as, steps}`, `loop: {as, init, update, until,
// steps}`). Each case is `case:` (a scalar literal, or a list of them sharing
// one body) and `steps:`; the default is `steps:` alone. What is compiled here
// is only the shape; everything a *set* of cases can be wrong about — a
// computed case, a duplicate, an impossible value, a claim about the domain —
// belongs to [validateSwitch], which sees the compiled workflow and reports
// with positions.

// switchNode compiles a `switch:` mapping.
func (c *compiler) switchNode(n ast.Node, path string, r ref) *v1.Switch {
	fields, ok := c.fields(n, path, r, switchKeys)
	if !ok {
		return nil
	}

	sw := &v1.Switch{}

	if f, found := fields.get("value"); found {
		valuePath := fieldPath(path, "value")
		// The same fence-optional reading a loop's `items:` gets: the schema
		// knows this is an expression, so a bare string here is expression
		// source rather than text.
		sw.Value = c.exprValue(f.value, valuePath,
			ref{step: r.step, path: valuePath, label: "switch value"})
	} else {
		c.report(spanOfNode(n), r, "switch requires value, the expression it dispatches on")
	}

	if f, found := fields.get("cases"); found {
		sw.Cases = c.switchCases(f.value, fieldPath(path, "cases"), r)
	} else {
		c.report(spanOfNode(n), r,
			"switch requires cases, the values it dispatches between; a case is `case:` (a literal, "+
				"or a list of literals sharing one body) and `steps:`")
	}

	if f, found := fields.get("default"); found {
		defaultPath := fieldPath(path, "default")
		c.pos.record(defaultPath, spanOfNode(c.resolveQuiet(f.value)))
		sw.Default = c.switchDefault(f.value, defaultPath, r)
	}

	return sw
}

// switchCases compiles the `cases:` list.
func (c *compiler) switchCases(n ast.Node, path string, r ref) []*v1.Switch_Case {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	sequence, ok := n.(*ast.SequenceNode)
	if !ok {
		c.report(spanOfNode(n), r,
			"switch cases must be a list, each entry a mapping with `case:` and `steps:`")
		return nil
	}
	if len(sequence.Values) == 0 {
		c.report(spanOfNode(n), r,
			"switch must have at least one case; a switch that is only a `default:` is an "+
				"unconditional block, so write the steps directly")
		return nil
	}
	if len(sequence.Values) > maxSwitchCases {
		c.report(spanOfNode(n), r, "switch has %d cases; at most %d are allowed",
			len(sequence.Values), maxSwitchCases)
	}

	caseCount := min(len(sequence.Values), maxSwitchCases)
	cases := make([]*v1.Switch_Case, 0, caseCount)
	for i, value := range sequence.Values[:caseCount] {
		if compiled := c.switchCase(value, indexPath(path, i), r); compiled != nil {
			cases = append(cases, compiled)
		}
	}
	return cases
}

// switchCase compiles one entry of the `cases:` list.
func (c *compiler) switchCase(n ast.Node, path string, r ref) *v1.Switch_Case {
	n = c.resolve(n, path, r)
	if n == nil || !c.enter(n, ref{step: r.step, path: path}) {
		return nil
	}
	defer c.exit()
	c.pos.record(path, spanOfNode(n))

	fields, ok := c.fields(n, path, ref{step: r.step, path: path}, switchCaseKeys)
	if !ok {
		return nil
	}

	compiled := &v1.Switch_Case{}

	if f, found := fields.get("case"); found {
		compiled.Values = c.switchCaseValues(f.value, fieldPath(path, "case"), r)
	} else {
		c.report(spanOfNode(n), r,
			"switch case requires `case:`, the literal (or list of literals) this body handles")
	}

	if f, found := fields.get(stepsKey); found {
		compiled.Steps = c.switchSteps(f.value, fieldPath(path, "steps"),
			ref{step: r.step, path: fieldPath(path, "steps"), label: "switch case steps"})
	} else {
		c.report(spanOfNode(n), r,
			"switch case requires steps, the body to run when it matches; an empty `steps: []` is "+
				"how ignoring a value is written down")
	}

	return compiled
}

// switchCaseValues compiles a `case:` value: one scalar, or a list flattened
// into the same membership check (Go's `case a, b:`).
//
// Each value goes through the ordinary input reading, so a `${...}` compiles to
// an expression — which [validateSwitch] then refuses with a position, since a
// computed case erases every check the construct exists for. Compiling rather
// than refusing here keeps the diagnostic with the rest of the case checks, one
// message per mistake.
func (c *compiler) switchCaseValues(n ast.Node, path string, r ref) []*v1.Value {
	resolved := c.resolve(n, path, r)
	if resolved == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(resolved))

	if sequence, isList := resolved.(*ast.SequenceNode); isList {
		if len(sequence.Values) == 0 {
			c.report(spanOfNode(resolved), r,
				"switch case has no values; write the literal this body handles, or a list of them")
			return nil
		}
		if len(sequence.Values) > maxSwitchCaseValues {
			c.report(spanOfNode(resolved), r, "switch case has %d values; at most %d are allowed",
				len(sequence.Values), maxSwitchCaseValues)
		}
		valueCount := min(len(sequence.Values), maxSwitchCaseValues)
		values := make([]*v1.Value, 0, valueCount)
		for i, element := range sequence.Values[:valueCount] {
			elementPath := indexPath(path, i)
			if value := c.inputValue(element, elementPath,
				ref{step: r.step, path: elementPath, label: "switch case"}); value != nil {
				values = append(values, value)
			}
		}
		return values
	}

	if value := c.inputValue(resolved, path,
		ref{step: r.step, path: path, label: "switch case"}); value != nil {
		return []*v1.Value{value}
	}
	return nil
}

// switchDefault compiles the `default:` mapping.
func (c *compiler) switchDefault(n ast.Node, path string, r ref) *v1.Switch_Default {
	fields, ok := c.fields(n, path, ref{step: r.step, path: path, label: "switch default"}, switchDefaultKeys)
	if !ok {
		return nil
	}

	compiled := &v1.Switch_Default{}
	if f, found := fields.get(stepsKey); found {
		compiled.Steps = c.switchSteps(f.value, fieldPath(path, "steps"),
			ref{step: r.step, path: fieldPath(path, "steps"), label: "switch default steps"})
	} else {
		c.report(spanOfNode(n), r,
			"switch default requires steps, the body to run when no case matches; an empty "+
				"`steps: []` is how deliberately handling nothing else is written down")
	}
	return compiled
}

// switchSteps compiles a case or default body: [compiler.steps] but with an
// empty list legal, because `steps: []` is load-bearing here — written-down
// ignoring, reviewable where silence would not be (`case: ignore` with an empty
// body, and the staged rollout's empty `default:`).
func (c *compiler) switchSteps(n ast.Node, path string, r ref) []*v1.Node {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	sequence, ok := n.(*ast.SequenceNode)
	if !ok {
		c.report(spanOfNode(n), r, "must be a list of steps, each with an id and one of %s; "+
			"an empty list (`steps: []`) is legal and means this branch deliberately runs nothing",
			stepKindList())
		return nil
	}

	nodes := make([]*v1.Node, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		nodes = append(nodes, c.step(value, indexPath(path, i)))
	}
	return nodes
}
