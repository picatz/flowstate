package flowfile

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// What a loop's `results` entry holds, as an expression reads it.
//
// `steps.<loop>.results` is a list whose every entry is a map of the loop body's
// step ids to those steps' outputs. The step outputs table types the list
// (`list(dyn)`), and cel-go cannot say more: it has no anonymous record type, and
// the variable of `results.filter(r, ...)` is the macro's own, so no declaration of
// the file reaches it. The file states the entry's keys, though: they are the step
// ids of the body, and for a task, value or call step the names it outputs. So an
// expression that reads `r.okk.value` through that variable is checked against
// them here, the way [typeTable.fieldPaths] checks a record: a key the entry
// cannot have is a missing key at run time, hours into a durable run, and is
// refused where it is written with the keys it can have.
//
// The entry does not hold the `as:` item. It rides inside the output of a step that
// failed and was tolerated (`r.lookup.item`, see v1.AttachIterationBinding), so it
// is one of the names a step may have, not a step id.
//
// Silent where it cannot be sure: a loop whose id is not unique, a read before the
// loop, a body step whose outputs are not fully known, and any variable that is
// not provably an element of the list stay unjudged.

// A resultShape is the entry of one loop's `results`.
type resultShape struct {
	// loop is the loop step's id and index its place among the top-level steps.
	loop  string
	index int

	// steps are the ids of the nodes directly in the body.
	steps map[string]*shapeStep
}

// A shapeStep is one body step of an entry.
type shapeStep struct {
	// outputs are what the step's definition says it outputs, nil when it does not
	// name them all (a task, value or call step is the only kind that does).
	outputs []v1.NamedOutput

	// names are the outputs' names and the ones a tolerated failure adds; nil with
	// outputs, meaning any name may be read.
	names []string
}

// resultShapeOf is the entry shape of a top-level `loop:` or `for_each` step, nil
// for any other.
func resultShapeOf(node *v1.Node, index int) *resultShape {
	var body []*v1.Node
	switch kind := node.GetKind().(type) {
	case *v1.Node_Loop:
		body = kind.Loop.GetBody()
	case *v1.Node_ForEach:
		body = kind.ForEach.GetBody()
	default:
		return nil
	}

	shape := &resultShape{loop: node.GetId(), index: index, steps: map[string]*shapeStep{}}
	// Only the nodes directly in the body: both drivers narrow an iteration to them
	// (onlyBodyOutputs, bodyOutputs), so a step nested in a block is not a key. A
	// `parallel:` has no output under its own id (its branches merge into the
	// enclosing scope), so it is not one either, unless it tolerates failure: a failed
	// one records its failure under its own id like any step, and stays open.
	for _, n := range body {
		if _, parallel := n.GetKind().(*v1.Node_Parallel); parallel && !n.GetPolicy().GetContinueOnError() {
			continue
		}
		shape.steps[n.GetId()] = closedOutputs(n)
	}

	return shape
}

// closedOutputs are the outputs of a task, value or call step when its definition
// names every one of them, and an open step otherwise. A tolerated failure adds the
// names the policy records, which are allowed beside them.
func closedOutputs(node *v1.Node) *shapeStep {
	switch node.GetKind().(type) {
	case *v1.Node_Task, *v1.Node_Value, *v1.Node_Call:
	default:
		return &shapeStep{}
	}

	outputs, ok := v1.OutputNames(node, nil)
	if !ok || len(outputs) == 0 {
		return &shapeStep{}
	}

	names := make([]string, 0, len(outputs)+3)
	for _, named := range outputs {
		if named.Name == "" {
			return &shapeStep{}
		}
		names = append(names, named.Name)
	}

	return &shapeStep{outputs: outputs, names: append(names, v1.StepErrorOutput, v1.StepFailureOutput, v1.StepErrorItemOutput)}
}

// A resultPath is one chain an expression reads from a `results` entry.
type resultPath struct {
	shape *resultShape

	// root is how the chain is written before its first field: the macro variable,
	// or the indexed list.
	root   string
	fields []string
}

// listShape is the entry shape of the list e evaluates to, nil where e is not
// known to be a loop's `results` or a `filter` of it.
func (t *typeTable) listShape(e *expr.Expr, before int) *resultShape {
	if t == nil || e == nil {
		return nil
	}

	if root, fields, ok := fieldChain(e); ok && root == v1.StepsRoot && len(fields) == 2 && fields[1] == v1.LoopResultsField {
		if shape := t.shapes[fields[0]]; shape != nil && shape.index < before {
			return shape
		}

		return nil
	}

	c := e.GetComprehensionExpr()
	if c == nil || c.GetIterVar2() != "" || !isFilter(c) {
		return nil
	}

	return t.listShape(c.GetIterRange(), before)
}

// isFilter reports whether c is the expansion of `filter`: it collects the iteration
// variable itself, so what it yields are entries of the list it ranges over.
func isFilter(c *expr.Expr_Comprehension) bool {
	if c.GetResult().GetIdentExpr().GetName() != c.GetAccuVar() ||
		c.GetAccuInit().GetListExpr() == nil || len(c.GetAccuInit().GetListExpr().GetElements()) != 0 {
		return false
	}

	step := c.GetLoopStep().GetCallExpr()
	if step.GetFunction() != "_?_:_" || len(step.GetArgs()) != 3 {
		return false
	}
	add := step.GetArgs()[1].GetCallExpr()
	if add.GetFunction() != "_+_" || len(add.GetArgs()) != 2 {
		return false
	}
	items := add.GetArgs()[1].GetListExpr().GetElements()

	return len(items) == 1 && items[0].GetIdentExpr().GetName() == c.GetIterVar()
}

// entryChain reads e as a chain of field reads from an entry: rooted at a macro
// variable bound to one, or at an index into a list of them
// (`steps.l.results[0].check.value`).
func (t *typeTable) entryChain(e *expr.Expr, bound map[string]*resultShape, before int) (resultPath, bool) {
	var fields []string
	for {
		switch kind := e.GetExprKind().(type) {
		case *expr.Expr_SelectExpr:
			fields = append(fields, kind.SelectExpr.GetField())
			e = kind.SelectExpr.GetOperand()

		case *expr.Expr_IdentExpr:
			shape := bound[kind.IdentExpr.GetName()]
			if shape == nil || len(fields) == 0 {
				return resultPath{}, false
			}
			slices.Reverse(fields)

			return resultPath{shape: shape, root: kind.IdentExpr.GetName(), fields: fields}, true

		case *expr.Expr_CallExpr:
			call := kind.CallExpr
			if call.GetTarget() != nil || len(call.GetArgs()) != 2 {
				return resultPath{}, false
			}
			key, isString := call.GetArgs()[1].GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
			switch {
			case call.GetFunction() == "_[_]" && !isString:
				shape := t.listShape(call.GetArgs()[0], before)
				if shape == nil || len(fields) == 0 {
					return resultPath{}, false
				}
				slices.Reverse(fields)

				return resultPath{shape: shape, root: v1.StepsRoot + "." + shape.loop + ".results[…]", fields: fields}, true

			case isString && (call.GetFunction() == "_?._" || call.GetFunction() == "_[_]" || call.GetFunction() == "_[?_]"):
				fields = append(fields, key.StringValue)
				e = call.GetArgs()[0]

			default:
				return resultPath{}, false
			}

		default:
			return resultPath{}, false
		}
	}
}

// resultPaths collects every entry chain the expression reads, tracking which
// macro variables are bound to an entry.
func (t *typeTable) resultPaths(e *expr.Expr, bound map[string]*resultShape, before int, seen map[string]bool, out *[]resultPath) {
	if e == nil {
		return
	}

	if path, ok := t.entryChain(e, bound, before); ok {
		if key := path.root + "." + strings.Join(path.fields, "."); !seen[key] {
			seen[key] = true
			*out = append(*out, path)
		}

		return
	}

	again := func(child *expr.Expr) { t.resultPaths(child, bound, before, seen, out) }

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		again(kind.SelectExpr.GetOperand())
	case *expr.Expr_CallExpr:
		again(kind.CallExpr.GetTarget())
		for _, arg := range kind.CallExpr.GetArgs() {
			again(arg)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			again(el)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			again(entry.GetMapKey())
			again(entry.GetValue())
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		again(c.GetIterRange())
		again(c.GetAccuInit())

		inner := maps.Clone(bound)
		if inner == nil {
			inner = map[string]*resultShape{}
		}
		// The variable that holds the element is the second of a two-variable
		// comprehension; the first is its index.
		element := c.GetIterVar()
		if c.GetIterVar2() != "" {
			delete(inner, c.GetIterVar())
			element = c.GetIterVar2()
		}
		if shape := t.listShape(c.GetIterRange(), before); shape != nil {
			inner[element] = shape
		} else {
			delete(inner, element)
		}
		delete(inner, c.GetAccuVar())

		for _, child := range []*expr.Expr{c.GetLoopCondition(), c.GetLoopStep(), c.GetResult()} {
			t.resultPaths(child, inner, before, seen, out)
		}
	}
}

// resultErrors reports each read of a loop's `results` entry that names a step the
// loop's body does not hold, or an output the step is known not to have, up to
// [maxFieldErrors].
func (t *typeTable) resultErrors(site v1.ValueSite) Diagnostics {
	if t == nil || len(t.shapes) == 0 || site.Value.GetExpr() == nil {
		return nil
	}

	var paths []resultPath
	t.resultPaths(site.Value.GetExpr().GetExpr(), nil, t.before(site), map[string]bool{}, &paths)

	var ds Diagnostics
	for _, path := range paths {
		if len(ds) == maxFieldErrors {
			break
		}

		missing := path.fields[0]
		known := slices.Sorted(maps.Keys(path.shape.steps))
		subject := fmt.Sprintf("the body of %q has no step %q", path.shape.loop, echo(missing))
		shown := 1

		if body, found := path.shape.steps[missing]; found {
			if len(path.fields) < 2 || body.names == nil || slices.Contains(body.names, path.fields[1]) {
				continue
			}
			missing, known, shown = path.fields[1], body.names, 2
			subject = fmt.Sprintf("step %q has no output %q", path.fields[0], echo(missing))
		}

		message := fmt.Sprintf("%s.%s: %s; it has %s", path.root,
			strings.Join(echoAll(path.fields[:shown]), "."), subject, quoteAll(known[:min(len(known), maxEchoedFieldList)]))
		if len(known) > maxEchoedFieldList {
			message += fmt.Sprintf(", and %d more", len(known)-maxEchoedFieldList)
		}
		if len(missing) <= maxEchoedName {
			if suggestion, ok := nearest.Name(missing, known); ok {
				message += fmt.Sprintf(". Did you mean %q?", suggestion)
			}
		}

		ds = append(ds, Diagnostic{
			Step: site.Step, Field: site.Field(), Message: message,
			Code: v1.DiagnosticCodeUnresolvedReference,
		})
	}

	return ds
}

func echoAll(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = echo(name)
	}

	return out
}

// A ResultEntry is what one entry of a loop's `results` holds, for a surface that
// describes it: the loop, and the body steps an entry is keyed by.
type ResultEntry struct {
	// Loop is the id of the `loop:` or `for_each` step.
	Loop string

	// Steps are the body's step ids, sorted.
	Steps []ResultEntryStep
}

// A ResultEntryStep is one key of an entry.
type ResultEntryStep struct {
	ID string

	// Outputs are the step's outputs when its definition names every one of them,
	// and nil when any name may be read.
	Outputs []ResultEntryOutput
}

// A ResultEntryOutput is one output of a body step: its name, the type it holds
// ("" when the definition does not state one) and what it is.
type ResultEntryOutput struct {
	Name, Type, Description string
}

// ResultEntryOf is the entry of the list that receiver, an expression written in
// step, evaluates to: `steps.<loop>.results`, or a `filter` of it. False where it
// is neither, or the loop is not one the file can name.
//
// It reads the table [Validate] judges with, so a hover or a completion cannot
// offer a key the diagnostic refuses.
func ResultEntryOf(wf *v1.Workflow, step, receiver string) (ResultEntry, bool) {
	table := newTypeTable(wf)
	parsed := v1.NewExpr(receiver).GetExpr().GetExpr()

	shape := table.listShape(parsed, table.before(v1.ValueSite{Step: step}))
	if shape == nil {
		return ResultEntry{}, false
	}

	entry := ResultEntry{Loop: shape.loop}
	for _, id := range slices.Sorted(maps.Keys(shape.steps)) {
		listed := ResultEntryStep{ID: id}
		for _, named := range shape.steps[id].outputs {
			output := ResultEntryOutput{Name: named.Name, Description: named.Description}
			if !v1.IsDyn(named.Type) {
				output.Type = v1.TypeString(named.Type)
			}
			listed.Outputs = append(listed.Outputs, output)
		}
		entry.Steps = append(entry.Steps, listed)
	}

	return entry, true
}
