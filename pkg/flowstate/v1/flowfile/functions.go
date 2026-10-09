package flowfile

import (
	"fmt"
	"regexp"
	"strings"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The `functions:` block: the computations a file names once.
//
//	functions:
//	  slug:
//	    description: A title as it appears in a URL.
//	    params: {title: string}
//	    returns: string
//	    body: ${title.trim().lowerAscii().replace(' ', '-')}
//
// and used wherever an expression is, as `${slug(inputs.title)}`.
//
// # What a function is, and is not
//
// A definition, which the compiler inlines at every use, so a compiled
// specification holds the body (its arguments bound once through `cel.bind`) and
// no call to a declared name, and both drivers run plain CEL of the pinned
// profile. That is the whole reason this is safe next to Worker Versioning: a spec
// compiled with functions runs on a worker that has never heard of them, and no
// worker can give a name a different meaning. [v1.FunctionSet] owns the
// inlining, the checking and the bounds; this file reads the block, points at
// the right place when the set refuses something, and writes the block back.
//
// The definition is also the source form. The specification carries it
// ([v1.Workflow.DeclaredFunctions]) beside the expanded expressions so `flow fmt`
// and Marshal write the file back with the definition intact, and so a reader of
// the spec can see what was inlined. Nothing evaluates the carried copy.
//
// A body sees its parameters and the profile and nothing else, so `inputs`,
// `vars` and `steps` are refused there with the reason: a function that needs a
// value takes it as an argument, and the call shows every value the computation
// depends on.

// functionKeys are the keys under one function.
var functionKeys = []string{"description", "params", "returns", "body"}

// functionNamePattern is the schema's rule for a function name, checked here so
// the author reads a sentence about names rather than a pattern.
var functionNamePattern = regexp.MustCompile(`^[a-z][A-Za-z0-9]{0,63}$`)

// functionSpans are where one function's parts were written, so a refusal of the
// set lands on the part it is about.
type functionSpans struct {
	key, body Span
}

// declaredFunctions compiles the top-level `functions:` block, in the order
// written, and builds the set the rest of the file's expressions are expanded
// through.
//
// Read before anything that can call one, for the reason `types:` is: the
// expansion happens as each expression is compiled, so the names have to be known
// first.
func (c *compiler) declaredFunctions(n ast.Node, path string, r ref) []*v1.FunctionDeclaration {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	// Bounded before anything is checked: every name costs a declared overload and
	// a checked body, and the file is the sender's.
	if len(entries) > v1.MaxFunctions {
		c.report(spanOfNode(c.resolveQuiet(n)), r,
			"declares %d functions; the most a workflow declares is %d", len(entries), v1.MaxFunctions)

		return nil
	}

	var (
		declared []*v1.FunctionDeclaration
		spans    = make(map[string]functionSpans, len(entries))
	)
	for _, e := range entries {
		if declaration, where, ok := c.declaredFunction(e, path); ok {
			declared = append(declared, declaration)
			spans[e.name] = where
		}
	}

	set, errs := v1.NewFunctionSet(v1.CurrentProfile, declared)
	for _, fe := range errs {
		where := spans[fe.Function]
		span := where.key
		message := fe.Err.Error()
		if strings.HasPrefix(message, "body ") && where.body.Start.Line > 0 {
			span = where.body
		}
		c.report(span, ref{path: fieldPath(path, fe.Function), label: "function " + fe.Function}, "%s", forAFunctionAuthor(message))
	}
	if len(set.Names()) > 0 {
		c.functions = set
	}

	if len(declared) == 0 {
		return nil
	}

	return declared
}

// forAFunctionAuthor adds to the checker's sentence the one fact an author of a
// body needs and the checker cannot know: why `inputs` is not there.
func forAFunctionAuthor(message string) string {
	if strings.Contains(message, "undeclared reference to '") {
		return message + "; a function sees only its parameters, so pass the value in as an argument"
	}

	return message
}

// declaredFunction compiles one function's declaration. ok is false after a
// diagnostic, so the set is built from the functions that were written whole.
func (c *compiler) declaredFunction(e entry, parent string) (*v1.FunctionDeclaration, functionSpans, bool) {
	path := fieldPath(parent, e.name)
	r := ref{path: path, label: "function " + e.name}
	where := functionSpans{key: spanOfNode(e.key)}

	c.pos.record(path, spanOfNode(c.resolveQuiet(e.value)))

	if !functionNamePattern.MatchString(e.name) {
		c.report(spanOfNode(e.key), r,
			"is not a function name; a function is named in lowerCamel, such as `slug` or `isBusinessDay`, up to 64 letters and digits")

		return nil, where, false
	}

	entries, ok := c.entries(e.value, path, r)
	if !ok {
		c.report(spanOfNode(e.value), r,
			"is declared as a mapping with `params:`, `returns:` and `body:`")

		return nil, where, false
	}
	fields := c.check(entries, r, functionKeys)

	declaration := &v1.FunctionDeclaration{Name: e.name}
	complete := true

	if f, found := fields.get("description"); found {
		descriptionPath := fieldPath(path, "description")
		if description, ok := c.text(f.value, descriptionPath,
			ref{path: descriptionPath, label: "function " + e.name + " description"}); ok {
			declaration.Description = proto.String(description)
		}
	}

	if f, found := fields.get("params"); found {
		params, ok := c.functionParameters(f.value, fieldPath(path, "params"), e.name)
		declaration.Parameters = params
		complete = complete && ok
	}

	if f, found := fields.get("returns"); found {
		returnsPath := fieldPath(path, "returns")
		returnsRef := ref{path: returnsPath, label: "function " + e.name + " returns"}
		if text, ok := c.text(f.value, returnsPath, returnsRef); ok {
			c.pos.record(returnsPath, spanOfNode(c.resolveQuiet(f.value)))
			if t, err := parseType(c.typeEnv, text); err != nil {
				c.report(spanOfNode(f.value), returnsRef, "is %q, which is not a type: %s", text, err)
				complete = false
			} else {
				declaration.Result = t
			}
		} else {
			complete = false
		}
	} else {
		c.report(spanOfNode(e.key), r,
			"has no `returns:`; write the type the body produces, such as `returns: string`, or `dyn` for any")
		complete = false
	}

	if f, found := fields.get("body"); found {
		bodyPath := fieldPath(path, "body")
		body := c.exprValue(f.value, bodyPath, ref{path: bodyPath, label: "function " + e.name + " body"})
		where.body = spanOfNode(c.resolveQuiet(f.value))
		if body.GetExpr() == nil {
			// Reported already when the expression did not compile; a literal is the
			// remaining way to get here, such as `body: 1`, which is not a computation
			// over anything.
			if body != nil {
				c.report(spanOfNode(f.value), ref{path: bodyPath, label: "function " + e.name + " body"},
					"is a literal; write the computation as an expression, such as ${n + 1}")
			}
			complete = false
		} else {
			declaration.Body = body.GetExpr()
		}
	} else {
		c.report(spanOfNode(e.key), r, "has no `body:`; write the expression the function computes, such as `body: ${n + 1}`")
		complete = false
	}

	return declaration, where, complete
}

// functionParameters compiles `params:`, a mapping of parameter name to type, in
// the order written, which is the order a call passes arguments.
func (c *compiler) functionParameters(n ast.Node, path, function string) ([]*v1.FunctionParameter, bool) {
	r := ref{path: path, label: "function " + function + " params"}
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil, false
	}
	if len(entries) > v1.MaxFunctionParameters {
		c.report(spanOfNode(c.resolveQuiet(n)), r,
			"takes %d parameters; the most a function takes is %d", len(entries), v1.MaxFunctionParameters)

		return nil, false
	}

	complete := true
	params := make([]*v1.FunctionParameter, 0, len(entries))
	for _, e := range entries {
		paramPath := fieldPath(path, e.name)
		paramRef := ref{path: paramPath, label: "function " + function + " parameter " + e.name}
		c.pos.record(paramPath, spanOfNode(c.resolveQuiet(e.value)))

		text, ok := c.text(e.value, paramPath, paramRef)
		if !ok {
			complete = false
			continue
		}
		t, err := parseType(c.typeEnv, text)
		if err != nil {
			c.report(spanOfNode(e.value), paramRef, "is %q, which is not a type: %s", text, err)
			complete = false
			continue
		}
		params = append(params, &v1.FunctionParameter{Name: e.name, Type: t})
	}

	return params, complete
}

// expandFunctions replaces the calls to a declared function in val with their
// bodies, or reports why it cannot and returns nil. A file with no functions, and
// an expression that calls none, are returned untouched.
//
// Run on the expression as written and before it is normalized, so the stored
// form is the expansion and is still a fixed point of the round trip through
// Marshal.
func (c *compiler) expandFunctions(val *v1.Value, span Span, r ref) *v1.Value {
	if c.functions == nil || !c.functions.Calls(val.GetExpr()) {
		return val
	}

	if !c.expanding(span, r, func() (int, error) { return c.functions.Expand(val) }) {
		return nil
	}

	return val
}

// expanding runs one expansion under the file's budget and reports why it did not
// finish. It is the part of an expansion that does not depend on what is being
// expanded, so an expression and a `must:` spend the same budget.
func (c *compiler) expanding(span Span, r ref, expand func() (int, error)) bool {
	if c.expansionOverflowed {
		// The budget is spent and said so once; expanding the rest would spend the
		// work the budget exists to refuse.
		return false
	}

	nodes, err := expand()
	if err != nil {
		for _, message := range celCheckMessages(err.Error()) {
			c.report(span, r, "%s", message)
		}

		return false
	}

	// One budget for the file: each expansion is bounded, and a few hundred uses of
	// a large composed function are each within it.
	c.expandedNodes += nodes
	if c.expandedNodes > v1.MaxFunctionExpansionNodes {
		if !c.expansionOverflowed {
			c.expansionOverflowed = true
			c.report(span, r, "calls functions that expand past %d CEL nodes in this file altogether; "+
				"call fewer functions, or make the large ones smaller", v1.MaxFunctionExpansionNodes)
		}

		return false
	}

	return true
}

// deferredMust is a `must:` met before the functions it may call were known.
type deferredMust struct {
	text string
	span Span
	r    ref
	set  func(must string, source *string)
}

// must hands set the text a `must:` is stored as and, when a call to a declared
// function was expanded, the text as written.
//
// The runtime evaluates `must` and has no functions, so what is stored there is the
// expansion, plain CEL of the profile; `must_source` keeps the call form so Marshal
// writes the file back as authored, and nothing evaluates it. The expansion is the
// one [v1.FunctionSet] makes of an expression, under the same budget, with `this`
// an argument like any other name; a body still sees only its parameters.
//
// Before the `functions:` block is read the text is stored as written and the
// expansion is made once it has been, by [compiler.settleMusts].
func (c *compiler) must(text string, span Span, r ref, set func(must string, source *string)) {
	set(text, nil)
	if !c.functionsRead {
		c.deferredMusts = append(c.deferredMusts, deferredMust{text: text, span: span, r: r, set: set})

		return
	}

	c.expandMust(deferredMust{text: text, span: span, r: r, set: set})
}

// settleMusts expands the `must:` texts that waited for the functions.
func (c *compiler) settleMusts() {
	c.functionsRead = true
	for _, m := range c.deferredMusts {
		c.expandMust(m)
	}
	c.deferredMusts = nil
}

func (c *compiler) expandMust(m deferredMust) {
	if c.functions == nil {
		return
	}

	var expanded string
	if !c.expanding(m.span, m.r, func() (int, error) {
		var (
			nodes int
			err   error
		)
		expanded, nodes, err = c.functions.ExpandText(m.text)

		return nodes, err
	}) {
		return
	}
	if expanded != m.text {
		m.set(expanded, &m.text)
	}
}

// expandPredicate expands the calls in an `allow:` predicate, which the server
// stores and evaluates as source text and, like a `must:`, has no functions. It
// returns what is stored and, when a call was expanded, the predicate as written
// for `allow_source`, so Marshal writes the file back as authored. The scope rule
// ("a predicate over `inputs` must also read the caller") and the cost bound are
// asked of the expansion by [validatePolicyRules], because the expansion is what
// runs.
func (c *compiler) expandPredicate(text string, span Span, r ref) (string, *string, bool) {
	if c.functions == nil {
		return text, nil, true
	}

	var expanded string
	if !c.expanding(span, r, func() (int, error) {
		var (
			nodes int
			err   error
		)
		expanded, nodes, err = c.functions.ExpandText(text)

		return nodes, err
	}) {
		return "", nil, false
	}
	if expanded == text {
		return text, nil, true
	}

	return expanded, &text, true
}

// declaredFunctionsToYAML is the inverse of [compiler.declaredFunctions]: the
// `functions:` block as written, in declaration order.
func declaredFunctionsToYAML(declared []*v1.FunctionDeclaration) (yaml.MapSlice, error) {
	out := make(yaml.MapSlice, 0, len(declared))
	for _, f := range declared {
		var entry yaml.MapSlice
		if f.Description != nil {
			entry = append(entry, yaml.MapItem{Key: "description", Value: textToYAML(f.GetDescription())})
		}
		if len(f.GetParameters()) > 0 {
			params := make(yaml.MapSlice, 0, len(f.GetParameters()))
			for _, p := range f.GetParameters() {
				text, err := FormatType(p.GetType())
				if err != nil {
					return nil, fmt.Errorf("function %s parameter %s: %w", f.GetName(), p.GetName(), err)
				}
				params = append(params, yaml.MapItem{Key: p.GetName(), Value: textToYAML(text)})
			}
			entry = append(entry, yaml.MapItem{Key: "params", Value: params})
		}
		result, err := FormatType(f.GetResult())
		if err != nil {
			return nil, fmt.Errorf("function %s returns: %w", f.GetName(), err)
		}
		entry = append(entry, yaml.MapItem{Key: "returns", Value: textToYAML(result)})

		body, err := fencedExprToYAML(&v1.Value{Kind: &v1.Value_Expr{Expr: f.GetBody()}})
		if err != nil {
			return nil, fmt.Errorf("function %s body: %w", f.GetName(), err)
		}
		entry = append(entry, yaml.MapItem{Key: "body", Value: body})

		out = append(out, yaml.MapItem{Key: f.GetName(), Value: entry})
	}

	return out, nil
}

// checkFunctionBodies reports a record parameter's field that the record does not
// declare, in a function's body.
//
// The body is checked at the definition with a record parameter as a map, because
// that is what a record is to the checker, so `user.misspelled` passes there and
// fails as a missing key in a durable run. This is the check an input's fields get
// ([typeTable.fieldErrors]) with the parameter as the root instead of `inputs`.
func checkFunctionBodies(wf *v1.Workflow) Diagnostics {
	records := v1.TypesOf(wf)
	if len(records) == 0 {
		return nil
	}

	var ds Diagnostics
	for _, f := range wf.GetDeclaredFunctions() {
		table := &typeTable{records: records, inputTypes: map[string]*v1.Type{}}
		for _, p := range f.GetParameters() {
			if p.GetType().GetMessage() != "" {
				table.inputTypes[p.GetName()] = p.GetType()
			}
		}
		if len(table.inputTypes) == 0 {
			continue
		}

		field := fieldPath("functions", f.GetName()) + ".body"
		ds = append(ds, pathErrors(table.parameterPaths(f.GetBody().GetExpr()), "", field)...)
	}

	return ds
}
