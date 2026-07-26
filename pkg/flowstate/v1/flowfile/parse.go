package flowfile

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/goccy/go-yaml/token"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/proto"
)

// Parsing goes straight from the YAML document tree to the schema. There is no
// intermediate set of Go structs shaped like the DSL, because that would be a
// second definition of the workflow model to keep in step with the schema by hand,
// and because decoding into structs discards the one thing a compiler most needs:
// where in the source each value was written.

// implicitNull is the token type the parser gives a key written with no value at
// all, as distinct from one written as null. The difference is worth keeping: the
// first is nearly always an unfinished line, the second is a deliberate value.
const implicitNull = token.ImplicitNullType

// A Flowfile is written by a person, so how deeply it nests and how many values it
// holds have limits well above anything hand-written and well below what would
// exhaust the machine.
//
// Both are needed, and neither is theoretical. Every level of nesting is another
// path recorded, so depth alone costs quadratic memory; and an alias may be
// referenced many times, so a short document can expand into an enormous one — the
// YAML equivalent of a billion laughs. A compiler that a Flowfile can hang is a
// compiler an editor and a server cannot safely run on input from elsewhere.
// The third bound is on the input itself, because the two above are enforced while
// walking a document the YAML parser has already built. Building that tree is work
// too, and it happens before this package sees a single node.
//
// One mebibyte is the same limit the language server analyzes up to, so a file an
// editor will check is a file `flow validate` will compile.
const (
	maxDepth = 64
	maxNodes = 100_000
	maxBytes = 1 << 20
)

// The keys each part of a Flowfile accepts. Anything else is reported: a
// misspelled `timout:` that is silently ignored does nothing at run time and gives
// the author no reason to doubt it, which is the worst of both outcomes.
var (
	workflowKeys = []string{"name", "description", "steps"}
	stepKeys     = []string{"id", "if", "timeout", "retry", "continue_on_error", "task", "for_each", "parallel", "sleep", "wait_until", "wait_for_signal"}
	stepKindKeys = []string{"task", "for_each", "parallel", "sleep", "wait_until", "wait_for_signal"}
	taskKeys     = []string{"name", "description", "inputs"}
	retryKeys    = []string{"attempts", "interval", "backoff", "max_interval"}
	forEachKeys  = []string{"items", "iterator", "max_parallel", "steps"}
	branchKeys   = []string{"steps"}
)

// Parse compiles a Flowfile into a workflow and the source positions of
// everything in it.
//
// The positions are what lets a diagnostic name a line and column, and what lets
// an editor underline the token at fault rather than the whole step. Callers that
// do not need them can use [Unmarshal].
//
// A failure to compile is returned as [Diagnostics], one per problem found, so a
// caller can report all of them at once. A failure to parse the YAML itself is
// returned as the parser's own error, which already carries a position and an
// excerpt of the offending line.
func Parse(data []byte) (*v1.Workflow, *Positions, error) {
	if len(data) > maxBytes {
		return nil, nil, Diagnostics{{
			Line:   1,
			Column: 1,
			Message: fmt.Sprintf(
				"file is %d bytes, larger than the %d byte limit a Flowfile is compiled up to; nothing was read",
				len(data), maxBytes),
		}}
	}

	file, err := parser.ParseBytes(data, 0)
	if err != nil {
		return nil, nil, err
	}

	c := &compiler{pos: newPositions(), anchors: make(map[string]ast.Node)}
	workflow := c.compile(file)
	if len(c.diags) > 0 {
		return nil, nil, c.sorted()
	}
	return workflow, c.pos, nil
}

// A compiler walks one document tree, building the workflow and collecting every
// problem it finds rather than stopping at the first.
type compiler struct {
	diags   Diagnostics
	pos     *Positions
	anchors map[string]ast.Node

	// depth and nodes account for the work one document causes, bounded by
	// maxDepth and maxNodes. overflowed records that the limit was already
	// reported, since every enclosing value would otherwise report it again.
	depth      int
	nodes      int
	overflowed bool
}

// enter accounts for descending into one more value, and reports whether the
// compiler may continue. A caller given true must call [compiler.exit].
func (c *compiler) enter(n ast.Node, r ref) bool {
	c.nodes++
	c.depth++
	if c.depth <= maxDepth && c.nodes <= maxNodes {
		return true
	}

	c.depth--
	if !c.overflowed {
		c.overflowed = true
		if c.depth >= maxDepth {
			c.report(spanOfToken(nodeToken(n)), r,
				"nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
		} else {
			c.report(spanOfToken(nodeToken(n)), r,
				"holds more than %d values once aliases are expanded, which is more than a Flowfile is meant to hold", maxNodes)
		}
	}
	return false
}

// exit undoes one [compiler.enter].
func (c *compiler) exit() { c.depth-- }

// nodeToken returns a node's own token without walking its subtree, which matters
// on the path that reports a document too large to walk.
func nodeToken(n ast.Node) *token.Token {
	if n == nil {
		return nil
	}
	return n.GetToken()
}

// A ref names what a diagnostic is about, so that every message reads the way the
// ones in validate.go do: the step it is in, and the input or property at fault.
type ref struct {
	// step is the enclosing step's id, when it has one.
	step string

	// input is the task input at fault, when the value is one.
	input string

	// path addresses the value in the source, and names it in a message when
	// there is no step id to name instead.
	path string

	// label is how a message should refer to the property — "timeout",
	// "task name" — for a field that is not a task input.
	label string
}

// report records one problem.
func (c *compiler) report(span Span, r ref, format string, args ...any) {
	message := fmt.Sprintf(format, args...)

	d := Diagnostic{
		Line:    span.Start.Line,
		Column:  span.Start.Column,
		Message: message,
	}
	switch {
	case r.input != "":
		d.Step, d.Field = r.step, r.input
	case r.step != "":
		d.Step = r.step
		if r.label != "" {
			d.Message = r.label + " " + message
		}
	default:
		d.Field = r.path
	}

	c.diags = append(c.diags, d)
}

// sorted returns the diagnostics in source order, so the same file always reports
// the same way.
func (c *compiler) sorted() Diagnostics {
	slices.SortStableFunc(c.diags, func(a, b Diagnostic) int {
		if a.Line != b.Line {
			return a.Line - b.Line
		}
		if a.Column != b.Column {
			return a.Column - b.Column
		}
		return strings.Compare(a.Message, b.Message)
	})
	return c.diags
}

// compile builds the workflow from a parsed file.
func (c *compiler) compile(file *ast.File) *v1.Workflow {
	var bodies []ast.Node
	for _, doc := range file.Docs {
		if doc.Body != nil {
			bodies = append(bodies, doc.Body)
		}
	}

	start := Span{Start: Position{Line: 1, Column: 1}, End: Position{Line: 1, Column: 1}}
	switch {
	case len(bodies) == 0:
		c.report(start, ref{}, "the file is empty; a Flowfile needs a name and at least one step")
		return nil
	case len(bodies) > 1:
		c.report(spanOfNode(bodies[1]), ref{},
			"a Flowfile holds one workflow, but this file has %d documents separated by ---; put each workflow in its own file",
			len(bodies))
		return nil
	}

	for _, doc := range file.Docs {
		c.collectAnchors(doc.Body)
	}
	if !c.checkAnchorCycles() {
		// Every walk below follows aliases, so a cycle would not merely produce a
		// wrong answer: it would not terminate.
		return nil
	}

	root := bodies[0]
	fields, ok := c.fields(root, "", ref{path: "workflow"}, workflowKeys)
	if !ok {
		return nil
	}

	workflow := &v1.Workflow{}

	if f, found := fields.get("name"); found {
		name, _ := c.text(f.value, "name", ref{path: "name", label: "name"})
		workflow.Name = name
	}

	// Description is set only when the key is present, so that "no description"
	// and "an empty description" stay distinguishable — which is what the schema's
	// optional means, and what makes Marshal an exact inverse.
	if f, found := fields.get("description"); found {
		if description, ok := c.text(f.value, "description", ref{path: "description", label: "description"}); ok {
			workflow.Description = proto.String(description)
		}
	}

	if f, found := fields.get("steps"); found {
		workflow.Steps = c.steps(f.value, "steps", ref{path: "steps", label: "steps"})
	}

	return workflow
}

// collectAnchors records every anchor in the document so that an alias can be
// resolved wherever it appears.
func (c *compiler) collectAnchors(n ast.Node) {
	switch node := n.(type) {
	case nil:
		return
	case *ast.AnchorNode:
		if name := node.Name; name != nil {
			c.anchors[name.String()] = node.Value
		}
		c.collectAnchors(node.Value)
	case *ast.MappingNode:
		for _, v := range node.Values {
			c.collectAnchors(v)
		}
	case *ast.MappingValueNode:
		c.collectAnchors(node.Key)
		c.collectAnchors(node.Value)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			c.collectAnchors(v)
		}
	case *ast.TagNode:
		c.collectAnchors(node.Value)
	}
}

// checkAnchorCycles reports an anchor that is part of its own value, and returns
// whether the document is free of them.
//
// A Flowfile is a tree, so an alias pointing back into the value it names is
// meaningless — and following one is unbounded. Detecting it once here is what lets
// every other walk resolve an alias without carrying a visited set.
func (c *compiler) checkAnchorCycles() bool {
	// The graph is anchor name to the anchors its value refers to.
	edges := make(map[string][]string, len(c.anchors))
	for name, value := range c.anchors {
		referenced := make(map[string]bool)
		collectAliases(value, referenced)
		edges[name] = slices.Sorted(maps.Keys(referenced))
	}

	state := make(map[string]int, len(edges)) // 0 unvisited, 1 in progress, 2 done
	var cyclic func(name string) bool
	cyclic = func(name string) bool {
		switch state[name] {
		case 1:
			return true
		case 2:
			return false
		}
		state[name] = 1
		for _, next := range edges[name] {
			if cyclic(next) {
				return true
			}
		}
		state[name] = 2
		return false
	}

	ok := true
	for _, name := range slices.Sorted(maps.Keys(c.anchors)) {
		if cyclic(name) {
			c.report(spanOfNode(c.anchors[name]), ref{path: "anchor &" + name},
				"anchor &%s is part of its own value; an alias cannot refer to the value it appears in", name)
			ok = false
		}
	}
	return ok
}

// collectAliases records the anchor names a node's subtree refers to.
func collectAliases(n ast.Node, into map[string]bool) {
	switch node := n.(type) {
	case nil:
		return
	case *ast.AliasNode:
		into[node.Value.String()] = true
	case *ast.AnchorNode:
		collectAliases(node.Value, into)
	case *ast.MappingNode:
		for _, v := range node.Values {
			collectAliases(v, into)
		}
	case *ast.MappingValueNode:
		collectAliases(node.Key, into)
		collectAliases(node.Value, into)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			collectAliases(v, into)
		}
	case *ast.TagNode:
		collectAliases(node.Value, into)
	}
}

// steps compiles a list of steps.
func (c *compiler) steps(n ast.Node, path string, r ref) []*v1.Node {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	sequence, ok := n.(*ast.SequenceNode)
	if !ok {
		c.report(spanOfNode(n), r, "must be a list of steps, each with an id and one of %s", stepKindList())
		return nil
	}
	if len(sequence.Values) == 0 {
		c.report(spanOfNode(n), r, "must have at least one step")
		return nil
	}

	nodes := make([]*v1.Node, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		nodes = append(nodes, c.step(value, indexPath(path, i)))
	}
	return nodes
}

// step compiles one step: its id, the one kind of work it does, and the policy
// controlling how it runs.
func (c *compiler) step(n ast.Node, path string) *v1.Node {
	n = c.resolve(n, path, ref{path: path})
	if n == nil || !c.enter(n, ref{path: path}) {
		return nil
	}
	defer c.exit()

	span := spanOfNode(n)
	c.pos.record(path, span)

	entries, ok := c.entries(n, path, ref{path: path})
	if !ok {
		return nil
	}

	// The id is read before anything else so that every diagnostic below can name
	// the step, which is how an author finds it in a file of thirty — including a
	// diagnostic about a key written above the id.
	step := &v1.Node{}
	for _, e := range entries {
		if e.name == "id" {
			id, _ := c.text(e.value, fieldPath(path, "id"), ref{path: fieldPath(path, "id"), label: "id"})
			step.Id = id
			c.pos.recordStep(id, path)
			break
		}
	}

	r := ref{step: step.GetId(), path: path}
	fields := c.check(entries, r, stepKeys)

	var kinds []field
	for _, name := range stepKindKeys {
		if f, found := fields.get(name); found {
			kinds = append(kinds, f)
		}
	}

	switch len(kinds) {
	case 0:
		c.report(span, r, "must have one of %s; a step has to do something", stepKindList())
	case 1:
		kind := kinds[0]
		kindPath := fieldPath(path, kind.name)
		c.pos.record(kindPath, spanOfNode(kind.value))

		switch kind.name {
		case "task":
			if task := c.task(kind.value, kindPath, r); task != nil {
				step.Kind = &v1.Node_Task{Task: task}
			}
		case "for_each":
			if loop := c.forEach(kind.value, kindPath, r); loop != nil {
				step.Kind = &v1.Node_ForEach{ForEach: loop}
			}
		case "parallel":
			if parallel := c.parallel(kind.value, kindPath, r); parallel != nil {
				step.Kind = &v1.Node_Parallel{Parallel: parallel}
			}
		case "sleep":
			if wait := c.sleep(kind.value, kindPath, r); wait != nil {
				step.Kind = &v1.Node_Wait{Wait: wait}
			}
		case "wait_until":
			if wait := c.waitUntil(kind.value, kindPath, r); wait != nil {
				step.Kind = &v1.Node_Wait{Wait: wait}
			}
		case "wait_for_signal":
			if wait := c.waitForSignal(kind.value, kindPath, r); wait != nil {
				step.Kind = &v1.Node_Wait{Wait: wait}
			}
		}
	default:
		c.report(spanOfNode(kinds[1].key), r,
			"has both %s and %s; a step does exactly one kind of work, so split it into two steps",
			kinds[0].name, kinds[1].name)
	}

	if f, found := fields.get("if"); found {
		condition := ref{step: step.GetId(), path: fieldPath(path, "if"), label: "if"}
		step.Condition = c.exprValue(f.value, fieldPath(path, "if"), condition)
	}

	step.Policy = c.policy(fields, path, r)
	c.checkWaitPolicy(step, fields, path, r)

	return step
}

// task compiles a task step's name, description, and inputs.
func (c *compiler) task(n ast.Node, path string, r ref) *v1.Task {
	fields, ok := c.fields(n, path, r, taskKeys)
	if !ok {
		return nil
	}

	task := &v1.Task{Inputs: map[string]*v1.Value{}}

	if f, found := fields.get("name"); found {
		name, _ := c.text(f.value, fieldPath(path, "name"),
			ref{step: r.step, path: fieldPath(path, "name"), label: "task name"})
		task.Name = name
	}
	if f, found := fields.get("description"); found {
		if description, ok := c.text(f.value, fieldPath(path, "description"),
			ref{step: r.step, path: fieldPath(path, "description"), label: "task description"}); ok {
			task.Description = proto.String(description)
		}
	}
	if f, found := fields.get("inputs"); found {
		c.inputs(f.value, fieldPath(path, "inputs"), r, task.Inputs)
	}

	return task
}

// inputs compiles a task's inputs into the given map.
//
// Input names are whatever the task declares, so unlike everywhere else there is
// no set of known keys to check against; the registry's descriptors are what
// [Validate] and the language server check names against.
func (c *compiler) inputs(n ast.Node, path string, r ref, into map[string]*v1.Value) {
	n = c.resolve(n, path, r)
	if n == nil {
		return
	}
	c.pos.record(path, spanOfNode(n))

	if _, empty := n.(*ast.NullNode); empty {
		return
	}

	entries, ok := c.entries(n, path, ref{step: r.step, path: path, label: "inputs"})
	if !ok {
		return
	}

	for _, e := range entries {
		// `vars` is flattened into the surrounding inputs, because the cel task
		// binds every input it does not recognize as a variable and needs each one
		// resolved individually: a nested map containing an expression would
		// arrive as one expression the task cannot resolve. This is a
		// compatibility shim for that task's input shape, not a general rule.
		if e.name == "vars" {
			c.hoistVars(e.value, fieldPath(path, "vars"), r, into)
			continue
		}

		valuePath := fieldPath(path, e.name)
		if value := c.inputValue(e.value, valuePath, ref{step: r.step, input: e.name, path: valuePath}); value != nil {
			into[e.name] = value
		}
	}
}

// hoistVars flattens a `vars` mapping into the inputs around it.
func (c *compiler) hoistVars(n ast.Node, path string, r ref, into map[string]*v1.Value) {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, ref{step: r.step, input: "vars", path: path})
	if !ok {
		return
	}
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		if value := c.inputValue(e.value, valuePath, ref{step: r.step, input: e.name, path: valuePath}); value != nil {
			into[e.name] = value
		}
	}
}

// forEach compiles a loop and its body.
func (c *compiler) forEach(n ast.Node, path string, r ref) *v1.ForEach {
	fields, ok := c.fields(n, path, r, forEachKeys)
	if !ok {
		return nil
	}

	loop := &v1.ForEach{}

	if f, found := fields.get("items"); found {
		itemsPath := fieldPath(path, "items")
		loop.Items = c.exprValue(f.value, itemsPath,
			ref{step: r.step, path: itemsPath, label: "for_each items"})
	} else {
		c.report(spanOfNode(n), r, "for_each requires items, an expression producing the list to iterate over")
	}

	if f, found := fields.get("iterator"); found {
		iterator, _ := c.text(f.value, fieldPath(path, "iterator"),
			ref{step: r.step, path: fieldPath(path, "iterator"), label: "for_each iterator"})
		loop.Iterator = iterator
	}

	if f, found := fields.get("max_parallel"); found {
		maxParallel, _ := c.integer(f.value, fieldPath(path, "max_parallel"),
			ref{step: r.step, path: fieldPath(path, "max_parallel"), label: "for_each max_parallel"}, 0, 1000)
		loop.MaxParallel = maxParallel
	}

	if f, found := fields.get("steps"); found {
		loop.Body = c.steps(f.value, fieldPath(path, "steps"),
			ref{step: r.step, path: fieldPath(path, "steps"), label: "for_each steps"})
	} else {
		c.report(spanOfNode(n), r, "for_each requires steps, the body to run for each item")
	}

	return loop
}

// parallel compiles the branches of a parallel step.
func (c *compiler) parallel(n ast.Node, path string, r ref) *v1.Parallel {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}

	sequence, ok := n.(*ast.SequenceNode)
	if !ok {
		c.report(spanOfNode(n), r, "parallel must be a list of branches, each a mapping with its own steps")
		return nil
	}
	if len(sequence.Values) == 0 {
		c.report(spanOfNode(n), r, "parallel must have at least one branch")
		return nil
	}

	parallel := &v1.Parallel{Branches: make([]*v1.Parallel_Branch, 0, len(sequence.Values))}
	for i, value := range sequence.Values {
		branchPath := indexPath(path, i)
		c.pos.record(branchPath, spanOfNode(c.resolveQuiet(value)))

		branch := &v1.Parallel_Branch{}
		fields, ok := c.fields(value, branchPath, ref{step: r.step, path: branchPath}, branchKeys)
		if ok {
			if f, found := fields.get("steps"); found {
				branch.Steps = c.steps(f.value, fieldPath(branchPath, "steps"),
					ref{step: r.step, path: fieldPath(branchPath, "steps"), label: fmt.Sprintf("parallel branch %d steps", i+1)})
			} else {
				c.report(spanOfNode(value), ref{step: r.step, path: branchPath},
					"parallel branch %d requires steps", i+1)
			}
		}
		parallel.Branches = append(parallel.Branches, branch)
	}
	return parallel
}

// policy compiles a step's execution settings.
//
// It returns nil when the step declares none, so that the engine's defaults apply
// rather than a policy of zeroes that happens to mean the same thing today.
func (c *compiler) policy(fields *fieldSet, path string, r ref) *v1.StepPolicy {
	policy := &v1.StepPolicy{}
	declared := false

	if f, found := fields.get("timeout"); found {
		timeout, ok := c.duration(f.value, fieldPath(path, "timeout"),
			ref{step: r.step, path: fieldPath(path, "timeout"), label: "timeout"})
		if ok {
			policy.Timeout = timeout
			declared = true
		}
	}

	if f, found := fields.get("retry"); found {
		if retry := c.retry(f.value, fieldPath(path, "retry"), r); retry != nil {
			policy.Retry = retry
			declared = true
		}
	}

	if f, found := fields.get("continue_on_error"); found {
		continueOnError, ok := c.boolean(f.value, fieldPath(path, "continue_on_error"),
			ref{step: r.step, path: fieldPath(path, "continue_on_error"), label: "continue_on_error"})
		if ok {
			policy.ContinueOnError = continueOnError
			// A step that only says continue_on_error: false has said nothing: it
			// asked for the default. Recording a policy for it would make two
			// identical workflows unequal depending on whether one spelled the
			// default out.
			declared = declared || continueOnError
		}
	}

	if !declared {
		return nil
	}
	return policy
}

// retry compiles a retry policy.
func (c *compiler) retry(n ast.Node, path string, r ref) *v1.RetryPolicy {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	// `retry:` with nothing under it asks for the engine's retry defaults, which is
	// a legitimate thing to write.
	if _, empty := n.(*ast.NullNode); empty {
		return &v1.RetryPolicy{}
	}

	fields, ok := c.fields(n, path, ref{step: r.step, path: path, label: "retry"}, retryKeys)
	if !ok {
		return nil
	}

	retry := &v1.RetryPolicy{}

	if f, found := fields.get("attempts"); found {
		attempts, _ := c.integer(f.value, fieldPath(path, "attempts"),
			ref{step: r.step, path: fieldPath(path, "attempts"), label: "retry attempts"}, 0, 1<<31-1)
		retry.MaxAttempts = attempts
	}
	if f, found := fields.get("interval"); found {
		interval, ok := c.duration(f.value, fieldPath(path, "interval"),
			ref{step: r.step, path: fieldPath(path, "interval"), label: "retry interval"})
		if ok {
			retry.InitialInterval = interval
		}
	}
	if f, found := fields.get("backoff"); found {
		backoff, _ := c.number(f.value, fieldPath(path, "backoff"),
			ref{step: r.step, path: fieldPath(path, "backoff"), label: "retry backoff"})
		retry.BackoffCoefficient = backoff
	}
	if f, found := fields.get("max_interval"); found {
		maxInterval, ok := c.duration(f.value, fieldPath(path, "max_interval"),
			ref{step: r.step, path: fieldPath(path, "max_interval"), label: "retry max_interval"})
		if ok {
			retry.MaxInterval = maxInterval
		}
	}

	return retry
}

// recordTree records the span of a value and of everything nested inside it, so
// that a diagnostic about one entry of a map of headers can point at that entry.
func (c *compiler) recordTree(n ast.Node, path string) {
	n = c.resolveQuiet(n)
	if n == nil || !c.enter(n, ref{path: path}) {
		return
	}
	defer c.exit()
	c.pos.record(path, spanOfNode(n))

	switch node := n.(type) {
	case *ast.SequenceNode:
		for i, v := range node.Values {
			c.recordTree(v, indexPath(path, i))
		}
	case *ast.MappingNode:
		for _, v := range node.Values {
			if name, ok := v.Key.(*ast.StringNode); ok {
				c.recordTree(v.Value, fieldPath(path, name.Value))
			}
		}
	case *ast.MappingValueNode:
		if name, ok := node.Key.(*ast.StringNode); ok {
			c.recordTree(node.Value, fieldPath(path, name.Value))
		}
	}
}

// recordExpr records the span of an expression's own source text.
func (c *compiler) recordExpr(path string, span Span) {
	c.pos.recordExpr(path, span)
}
