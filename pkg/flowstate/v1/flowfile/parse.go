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
	workflowKeys = []string{"edition", "name", "description", "steps"}

	// stepPropertyKeys say which step this is, how it runs, and what it is for —
	// everything except what work it does.
	stepPropertyKeys = []string{"id", "description", "if", "timeout", "retry", "continue_on_error"}

	// nodeKindKeys are the kinds of work that are not a task, and so name a node
	// kind in the schema rather than anything in the registry.
	nodeKindKeys = []string{"for_each", "parallel", "sleep", "wait_until", "wait_for_signal"}

	retryKeys   = []string{"attempts", "interval", "backoff", "max_interval"}
	forEachKeys = []string{"items", "iterator", "max_parallel", "steps"}
	branchKeys  = []string{"steps"}
)

// A step names the work it does directly — `http:` with the request under it —
// so the keys a step accepts are not a constant. They are the properties, plus
// the non-task kinds, plus every task the registry has.
//
// Deriving it means a task added to the registry becomes writable with no change
// here, and an unknown-key diagnostic offers task names alongside grammar
// keywords, because from an author's position those are the same kind of thing.
// [v1.ReservedStepKeys] keeps the two halves disjoint, so a key is never
// ambiguous.
func stepKeys() []string {
	keys := make([]string, 0, len(stepPropertyKeys)+len(nodeKindKeys)+8)
	keys = append(keys, stepPropertyKeys...)
	keys = append(keys, nodeKindKeys...)
	return append(keys, v1.TaskNames()...)
}

// couldBeATaskName reports whether a key is spelled the way a task name must be.
//
// The same pattern the schema puts on Task.name, so this package and the
// validator agree about what a task could be called.
//
// The leading character is checked separately because a name may not start with
// a digit — `TaskManifest.name` is `^[a-z][a-z0-9_]*$` — and without that, `123:`
// reads as a plausible task. YAML gives that key to the parser as a number, which
// the compiler refuses outright with "keys must be strings"; a promotion rule
// looser than the schema it claims to mirror makes the language server model a
// task where the compiler sees an error.
func couldBeATaskName(key string) bool {
	if key == "" {
		return false
	}
	if first := key[0]; !(first >= 'a' && first <= 'z') && !(first >= 'A' && first <= 'Z') {
		return false
	}
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

// stepKindKeys are the keys saying what a step *does*. Exactly one is required.
func stepKindKeys() []string {
	return append(slices.Clone(nodeKindKeys), v1.TaskNames()...)
}

// retiredStepKeys are spellings a step used to have, and what to write instead.
//
// A key the language no longer has is otherwise reported as an unknown task,
// which is true and useless: it tells an author what their file is not and leaves
// them to guess what it should be. `task:` is the one that matters, because it is
// the shape every Flowfile written before the flattening has, and the shape a
// model trained on them will keep producing.
//
// This is where a future retirement goes. A key listed here is never taken for a
// task, so the specific message is the only one reported — see [StepTaskKeys].
var retiredStepKeys = map[string]string{
	"task": "a step names its task directly now: replace `task:` with the task's own name and put its inputs beneath, so `task:` / `name: echo` / `inputs:` / `message: hi` becomes `echo:` / `message: hi`",
}

// StepTaskKeys reports which of a step's keys name the task it runs, in the order
// they were written.
//
// A step names its task directly, so deciding which key is the task is a question
// with no fixed answer: any key that is not grammar might be one. Two callers ask
// it — the compiler, deciding what to build, and the language server, deciding
// what to underline — and they must reach the same answer, because a diagnostic
// the editor puts on a different token than `flow validate` does is a diagnostic
// the author has to reconcile by hand. Exported for that reason rather than for
// general use.
//
// The rule:
//
//   - A registered task name is a task. Nothing else needs deciding.
//   - A step property (`id`, `timeout`, ...) or a non-task kind (`for_each`, ...)
//     is not.
//   - Neither is a near-miss of one: `timout:` is a misspelled property and wants
//     "did you mean timeout?", which is a better message than "unknown task".
//   - Nor is anything spelled the way a task name cannot be — `${chosen.task}:` is
//     somebody reaching for a task chosen at run time, which the grammar
//     deliberately cannot express.
//   - What is left is an unregistered name, and it counts only when the step has
//     no other kind. A step that already loops and also has a stray key has a key
//     problem; calling that key a task would report "this does two kinds of work",
//     which is true of the reading and not of the file.
//
// The last clause is why this takes the whole step rather than one key: the answer
// for `shell` depends on what else the step says.
func StepTaskKeys(keys []string) []string {
	// The words the step grammar speaks for, which is [v1.ReservedStepKeys] and
	// not the subset this build happens to implement.
	//
	// Those two are deliberately different: `call`, `vars`, `undo` and `needs` are
	// reserved for grammar not written yet, precisely so that adding them later is
	// a change to one package rather than a break for anyone who registered a task
	// under the name. Promoting them here would give that away — a `needs:` written
	// today would compile as a task nobody registered, and the day `needs:` becomes
	// grammar, a file that compiles would silently mean something else.
	grammar := v1.ReservedStepKeys()
	kinds := stepKindKeys()

	var out []string
	for _, key := range keys {
		if _, known := v1.LookupTask(key); known {
			out = append(out, key)
		}
	}

	// A registered name, or a `for_each:`, settles it: the step says what it does,
	// so no other key needs promoting to say it.
	if len(out) > 0 || slices.ContainsFunc(keys, func(k string) bool { return slices.Contains(kinds, k) }) {
		return out
	}

	// At most one, and the first. A second unrecognised key is a *key* problem —
	// a stray line, a misspelling — and promoting it too would report "has both
	// run and environment; split it into two steps", which is true of the reading
	// and not of the file, and whose advice yields two broken steps.
	//
	// Which one is arbitrary only in appearance: the first is the one the author
	// most likely meant as the work, and everything after it then gets the
	// "unknown key; the keys here are ..." message it would have got beside a
	// registered task name. One authoring mistake should not draw two different
	// diagnostics depending on whether the neighbouring key is in the registry.
	for _, key := range keys {
		if slices.Contains(grammar, key) {
			continue
		}
		if _, retired := retiredStepKeys[key]; retired {
			// Spelled like a task and reads like one, which is exactly why it must
			// not be taken for one: `unknown task "task"` would bury the message
			// that says what to write.
			continue
		}
		if _, near := nearest(key, grammar); near {
			continue
		}
		if !couldBeATaskName(key) {
			continue
		}
		return append(out, key)
	}
	return out
}

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
	entries, ok := c.entries(root, "", ref{path: "workflow"})
	if !ok {
		return nil
	}

	// The edition is settled before any other key is judged, which is why this
	// reads the entries itself rather than the checked field set.
	//
	// A file written in a grammar this build does not have will have other
	// problems, and every one of them describes the wrong language: `nonsense:` is
	// an unknown key *here*, and might be a perfectly good key in the edition the
	// file claims. Reporting those alongside would bury the one diagnostic that
	// explains all the rest.
	//
	// Absent means the current edition. Requiring it would put a line of ceremony
	// at the top of every file to say the only thing it can currently say, and a
	// file that does not care which grammar it is in is the common case.
	if !c.checkDeclaredEdition(entries) {
		return nil
	}

	fields := c.check(entries, ref{path: "workflow"}, workflowKeys)

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

// checkDeclaredEdition reads an `edition:` key if one was written and reports
// whether the document may be compiled.
//
// Takes the raw entries rather than a checked field set so that it can run before
// unknown-key reporting — see the call site.
func (c *compiler) checkDeclaredEdition(entries []entry) bool {
	for _, e := range entries {
		if e.name != "edition" {
			continue
		}
		r := ref{path: "edition", label: "edition"}
		declared, ok := editionText(c.resolve(e.value, "edition", r))
		if !ok {
			c.report(spanOfNode(e.value), r,
				"edition must be written as %s, but %s was written here",
				CurrentEdition, describeNode(e.value))
			return false
		}
		c.pos.record("edition", spanOfNode(e.value))
		if err := checkEdition(declared); err != nil {
			c.report(spanOfNode(e.value), r, "%s", err.Error())
			return false
		}
		return true
	}
	return true
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

	// Which key names the task is [StepTaskKeys]'s question, asked here and by the
	// language server so that both place a diagnostic on the same token.
	//
	// An unregistered name it returns — `shell:` — is accepted as a key here and
	// compiled as a task, so the *validator* reports it: "unknown task; available
	// tasks are ..." is a better message than "unknown key", and it belongs where
	// task names are known.
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.name
	}
	promoted := StepTaskKeys(names)

	// stepKeys already holds every *registered* name, so only an unregistered one
	// is news here. Appending the whole promoted set would list `echo` twice in
	// "the keys here are ...", which reads like a bug in the tool.
	known := stepKeys()
	for _, name := range promoted {
		if !slices.Contains(known, name) {
			known = append(known, name)
		}
	}
	kindKeys := append(slices.Clone(nodeKindKeys), promoted...)

	// A retired spelling is reported here and then held back from the key check,
	// so that the message naming its replacement is the only thing said about it.
	//
	// Held back rather than accepted, because the count of checked keys is what
	// tells the kind check below that a key was already rejected — and a step
	// written the old way has no kind, so without this it would also be told it
	// does nothing, which is the same mistake reported a second time in worse
	// words.
	checkable := make([]entry, 0, len(entries))
	for _, e := range entries {
		if instead, retired := retiredStepKeys[e.name]; retired {
			c.report(spanOfNode(e.key), r, "`%s:` is no longer a step key; %s", e.name, instead)
			continue
		}
		checkable = append(checkable, e)
	}

	fields := c.check(checkable, r, known)

	// Collected in the order they were *written*, not in the order of a canonical
	// list. It only matters when there are two, and then it is what makes the
	// diagnostic point at the second one — the key the author added to a step that
	// already did something.
	var kinds []field
	for _, f := range fields.list {
		if slices.Contains(kindKeys, f.name) {
			kinds = append(kinds, f)
		}
	}

	switch len(kinds) {
	case 0:
		// Silent when a key was already rejected, because that key is almost
		// certainly what should have been the kind — a misspelled `htpp:`, an
		// expression written where a key goes. Reporting "this has no kind of
		// work" as well says the same mistake twice and buries the one that names
		// the token at fault.
		if len(fields.list) == len(entries) {
			c.report(span, r, "must have one of %s; a step has to do something", stepKindList())
		}
	case 1:
		kind := kinds[0]
		kindPath := fieldPath(path, kind.name)

		// What `steps[N].<kind>` addresses depends on the kind.
		//
		// For a task it is the *key*: under the flattening the key is the task's
		// name, so a problem with the task is a problem with that word, and
		// `unknown task "shell"` wants to underline `shell` rather than the six
		// inputs an author wrote correctly beneath it. For every other kind the key
		// is a fixed grammar word nobody can get wrong and the interesting extent is
		// what was written under it.
		//
		// Recorded once, here, rather than by each arm: two writes to one path leave
		// the answer decided by call order, which is how this briefly addressed a
		// task's inputs while appearing to address its name.
		if slices.Contains(nodeKindKeys, kind.name) {
			c.pos.record(kindPath, spanOfNode(kind.value))
		} else {
			c.pos.record(kindPath, spanOfNode(kind.key))
		}

		switch kind.name {
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
		default:
			// A task: the key is its name, the value is its inputs. That is the
			// whole of the flattening — what used to be three levels of scaffolding
			// is the one fact a reader wanted.
			step.Kind = &v1.Node_Task{Task: c.task(kind.name, kind.value, kindPath, r)}
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

	// Set only when written, so that "no description" and "an empty description"
	// stay distinguishable — the same rule the workflow's own description follows,
	// and what keeps Marshal an exact inverse.
	if f, found := fields.get("description"); found {
		descriptionPath := fieldPath(path, "description")
		if description, ok := c.text(f.value, descriptionPath, ref{step: step.GetId(), path: descriptionPath, label: "description"}); ok {
			step.Description = proto.String(description)
		}
	}

	step.Policy = c.policy(fields, path, r)
	c.checkWaitPolicy(step, fields, path, r)

	return step
}

// task compiles a task step: the key is the task's name, the value is its inputs.
//
// A task written with no value at all — `echo:` on a line by itself — is a task
// with no inputs rather than a mistake. Whether that is *legal* is the task's
// question, not the grammar's: the registry declares which inputs are required
// and [Validate] answers from the schema, where a reader can see it written down.
func (c *compiler) task(name string, n ast.Node, path string, r ref) *v1.Task {
	task := &v1.Task{Name: name, Inputs: map[string]*v1.Value{}}
	c.inputs(n, path, r, name, task.Inputs)
	return task
}

// inputs compiles a task's inputs into the given map.
//
// Input names are whatever the task declares, so unlike everywhere else there is
// no set of known keys to check against; the registry's descriptors are what
// [Validate] and the language server check names against.
//
// The task's own path is recorded by its caller, which knows the key, and not
// here from the value: see the task case of [compiler.stepNode].
func (c *compiler) inputs(n ast.Node, path string, r ref, taskName string, into map[string]*v1.Value) {
	n = c.resolve(n, path, r)
	if n == nil {
		return
	}

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
		//
		// Which is why it is gated on the task. It used to fire on the key name
		// alone, for every task, so writing `vars:` under `echo` silently emptied
		// it into the surrounding inputs and produced a diagnostic naming a key at
		// a level the author never wrote it at — `task "echo" has no such input`
		// pointing at `greeting`, when what they wrote was `vars`. A diagnostic
		// that names something the file does not contain is worse than none, and
		// this file's rule is that a misspelling is reported where it was made.
		if e.name == "vars" && c.taskAcceptsUndeclaredInputs(taskName) {
			c.hoistVars(e.value, fieldPath(path, "vars"), r, into)
			continue
		}

		valuePath := fieldPath(path, e.name)
		if value := c.inputValue(e.value, valuePath, ref{step: r.step, input: e.name, path: valuePath}); value != nil {
			into[e.name] = value
		}
	}
}

// taskAcceptsUndeclaredInputs reports whether a task binds input names its schema
// does not declare, which is the only shape the `vars` hoist above is for.
//
// An unregistered task answers false: an unknown task name is already reported by
// [Validate], and flattening on behalf of a task nobody can run would only add a
// second, more confusing diagnostic to the first.
func (c *compiler) taskAcceptsUndeclaredInputs(name string) bool {
	def, found := v1.LookupTask(name)
	if !found {
		return false
	}

	return acceptsUndeclaredInputs(def)
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
