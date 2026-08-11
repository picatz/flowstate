package flowfile

import (
	"fmt"
	"strings"

	"github.com/goccy/go-yaml/ast"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// A secret reference is recognized when the workflow is compiled, not called when
// it runs.
//
// It looks like a function — ${secret('vault:prod/api#token')} — because that is
// what an author expects to write, but there is no such function and there must not
// be one. A function returns a value to whatever called it, and what called it here
// is workflow code, whose every result is written to durable, widely readable
// history. Compiling the call into a [flowstatev1.SecretRef] instead keeps the
// reference a reference for the whole path: the specification carries it, the
// control plane transports it, workflow-side evaluation refuses to read it, and only
// the worker executing the step resolves it.
//
// Recognizing it at compile time is also the only way to report a mistake. CEL's
// parser accepts a call to a function it has never heard of, so before this existed
// `flow validate` said "ok" to a misspelled reference and the run failed later with
// "no such overload" — a validator that passes and then breaks is worse than none.

// SecretMarker is what a Flowfile writes to reference a secret. It is compiled into
// the reference itself, so nothing evaluates a call by this name.
//
// It is exported so that editor completion and hover offer the same spelling the
// compiler recognizes.
const SecretMarker = "secret"

// secretPlacement says whether a value is somewhere a secret reference may appear,
// and when it is not, which of the two reasons applies.
type secretPlacement int

const (
	// secretAllowed is the whole value of a task input, or the whole value of one
	// entry of a structure written in an input that carries its entries to the
	// worker: the places a reference reaches the task untouched.
	secretAllowed secretPlacement = iota

	// secretNotWholeValue is inside a larger expression, where the reference would
	// have to be combined with something.
	secretNotWholeValue

	// secretInStructure is nested in a list or a mapping that cannot carry one.
	secretInStructure

	// secretNotEvaluable is a field the workflow evaluates itself: a step's
	// condition, or a loop's items.
	secretNotEvaluable

	// A call's `with:` has no placement of its own here, unlike every other
	// position a reference is refused from: [compiler.callArgumentValue] checks
	// for a marker — bare or nested — before ever reaching [compiler.value],
	// so this switch never sees one from that position to refuse. See
	// [notAcrossCallHelp] for the message, and callArgumentValue for why the
	// check has to happen before the whole-value/nested distinction this type
	// exists to draw: an argument is never "the whole value of a task input"
	// in the first place, so none of the other reasons describe it honestly.
)

// Why a reference can be out of place. Each says what is actually true of the
// position it is in, because the three reasons are genuinely different and an
// author sent hunting for the wrong mistake is worse off than one told nothing.
const (
	notWholeValueHelp = "a secret reference has to be the whole value of a task input, " +
		"or the whole value of one entry inside one; " +
		"it names a value that does not exist until the worker running the step resolves it, " +
		"so nothing workflow-side can combine it with anything else"

	// A reference nested in a structure is legal now — `headers: {Authorization:
	// ${secret('env:TOKEN')}}` compiles, and the entries reach the worker as
	// values, the reference among them still a reference. What this reports is
	// the position where that is *not* true.
	//
	// It is a statement about the input rather than about the shape, which is why
	// it names the inputs that would work. A task can carry a reference through an
	// input only if it applies that input's entries itself, inside the activity:
	// everything else about an input — including that it is a map of the right
	// type — is beside the point, because an input the workflow resolves is one
	// whose resolved value rides the activity payload into history.
	inStructureHelp = "a secret reference cannot be nested inside this input's list or mapping: " +
		"only an input the task applies entry by entry, inside the activity that makes the request, " +
		"can carry one: anything else is resolved by the workflow, and a secret the workflow " +
		"resolved is a secret in durable history"

	// The other half of the same rule, for the mixture the schema deliberately
	// cannot represent. See [flowstatev1.Value_Structure].
	mixedStructureHelp = "a secret reference and an expression cannot share a list or a mapping: " +
		"the entries of a structure holding a reference are carried to the worker one at a time, " +
		"and an expression among them would have to be evaluated by the workflow, which is what " +
		"nesting the reference exists to avoid. Keep the computed entries in a structure of their " +
		"own; for an Authorization header, `bearer:` takes the credential and leaves the rest of " +
		"`headers:` free to hold expressions"

	notEvaluableHelp = "a secret reference cannot go where the workflow evaluates the value itself; " +
		"resolving it here would put the secret in workflow history, which is durable and " +
		"broadly readable, so pass it to a task input instead"

	malformedCallHelp = "secret() takes one reference, written out, like ${secret('env:API_KEY')}; " +
		"a computed reference cannot be checked when the workflow is compiled"

	// notAcrossCallHelp is deliberately specific rather than a reuse of
	// notEvaluableHelp: `with:` is not a field the workflow evaluates for its
	// own purposes the way a condition is, so the generic reason would be true
	// of the wrong thing. What is actually true here is a boundary — an
	// argument is resolved in the caller's scope and crosses as an ordinary
	// value, and a reference is not one: it names something that does not
	// exist until a worker resolves it. Refused rather than modeled, because
	// the alternative needs a declared input to mean "a string, or a reference
	// that resolves to one" — a type nobody has designed — while refusing costs
	// nothing a workflow could not already do: the callee's own task can write
	// ${secret(...)} directly, in the file that actually uses it.
	notAcrossCallHelp = "a secret reference cannot cross a call boundary; pass it to the task that " +
		"needs it inside the callee, or declare the input there"

	// notInVarHelp is the same shape of refusal as [notAcrossCallHelp], for the
	// same reason read one level down: a `vars:` entry is not a place a
	// resolution could be contained.
	//
	// Every position that *does* carry a reference carries it to a worker — a task
	// input, or an entry of a structure a task applies inside its own activity. A
	// var has no such destination. It is evaluated by the workflow, at the top of
	// the run or just before a step, and its value is bound into the scope every
	// later expression reads and written into durable state — `RunState.vars` on
	// the durable driver, carried across Continue-As-New — before anything has
	// asked what it is for. So there is no activity to resolve it in and no moment
	// at which the resolved value is not already in history.
	//
	// Refused rather than deferred, and refused at both levels, because the
	// alternative is a var that holds a reference until something reads it — which
	// is a task input with extra steps, spelled somewhere the author cannot see
	// which reads are safe. Referencing the secret where it is consumed costs a
	// workflow nothing it could otherwise do: the same `${secret(...)}` written on
	// the input that needs it reaches the worker unresolved, which is the whole of
	// what a var here was being asked to arrange.
	notInVarHelp = "a secret reference cannot be stored in `vars:`; a var is evaluated by the " +
		"workflow and its value is written to durable history, and there is no activity here to " +
		"resolve it in. Write ${secret('...')} directly on the task input that consumes the " +
		"secret instead"
)

// secretMarkerSpan returns the span of the first ${secret(...)} reference inside n,
// so that a refusal about a value holding one underlines the reference itself.
//
// It resolves down to the marker the same way [compiler.markerNode] does, and then
// one step further, through [markerSpan]: a reference buried in a longer expression
// — `${'Bearer ' + secret('env:T')}` — reports at the `secret` rather than at the
// quote the value starts with. Falling back to the node's own span wherever that
// cannot be established, which is honest about what was found rather than guessing
// at a column.
func (c *compiler) secretMarkerSpan(n ast.Node) Span {
	at := spanOfNode(n)

	c.walkMarkers(n, func(scalar ast.Node, src string) bool {
		span := spanWithin(scalar, src)
		at = span

		parsed := v1.NewExpr(src)
		if parsed.Error() != nil {
			return false
		}
		call, found := findSecretCall(parsed.GetExpr().GetExpr())
		if !found {
			return false
		}
		at = markerSpan(parsed.GetExpr(), call.GetId(), src, span)
		return false
	})

	return at
}

// secret compiles a ${secret(...)} marker into a reference, or reports why it
// cannot be one here.
//
// The third return reports whether the expression held a marker at all, so that an
// ordinary expression carries on being compiled as one.
func (c *compiler) secret(parsed *expr.ParsedExpr, src string, span Span, r ref, placement secretPlacement) (*v1.Value, bool) {
	call, found := findSecretCall(parsed.GetExpr())
	if !found {
		return nil, false
	}

	at := markerSpan(parsed, call.GetId(), src, span)

	if help := misplacedHelp(placement, call == parsed.GetExpr()); help != "" {
		c.report(at, r, "%s", help)
		return nil, true
	}

	text, ok := secretArgument(call)
	if !ok {
		c.report(at, r, "%s", malformedCallHelp)
		return nil, true
	}

	// The reference is validated by the package that resolves them, so that a
	// Flowfile cannot compile a reference a worker would later refuse. That
	// includes what the schema's pattern cannot say: a name holding a control
	// character is rejected, because a reference reaching a log must not be able to
	// forge lines in it.
	reference, err := secrets.ParseRef(text)
	if err != nil {
		c.report(at, r, "%s", err)
		return nil, true
	}

	return &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
		Scheme: reference.GetScheme(),
		Name:   reference.GetName(),
	}}}, true
}

// structure compiles a list or a mapping that holds a secret reference somewhere
// inside it, or reports why this input cannot carry one.
//
// It is the whole of what [compiler.composite] does differently when a marker is
// present. An ordinary structure containing expressions still compiles to the one
// CEL expression that builds it, which is what makes every entry evaluable; a
// structure holding a reference cannot, because evaluating the reference is the one
// thing none of this may do. So its entries stay Values and travel as they were
// written.
func (c *compiler) structure(n ast.Node, path string, r ref) *v1.Value {
	if !v1.AcceptsNestedSecret(r.task, r.input) {
		c.report(spanOfNode(c.markerNode(n)), r, "%s%s", inStructureHelp, acceptedElsewhere(r.task))
		return nil
	}

	return c.structureValue(n, path, r)
}

// acceptedElsewhere names the inputs of a task that do accept a nested reference,
// so a refusal points somewhere rather than only saying no.
//
// Read from the task's own definition rather than written out, for the reason the
// rest of this package reads descriptors: an input built to take one is offered
// here on the day it is built, and an input this build has never heard of is not
// offered at all.
func acceptedElsewhere(taskName string) string {
	accepted := v1.NestedSecretInputs(taskName)
	if len(accepted) == 0 {
		return ""
	}

	return fmt.Sprintf("; %q accepts one in %s", taskName, strings.Join(accepted, ", "))
}

// markerNode returns the node holding the first secret marker inside n, so that a
// refusal underlines the reference rather than the mapping it is in.
//
// It falls back to n itself, which is honest about what it could find: a structure
// this cannot walk is one the report should cover whole.
func (c *compiler) markerNode(n ast.Node) ast.Node {
	found := n
	c.walkMarkers(n, func(scalar ast.Node, _ string) bool {
		found = scalar
		return false
	})
	return found
}

// structureValue compiles one node of a structure that holds a reference.
func (c *compiler) structureValue(n ast.Node, path string, r ref) *v1.Value {
	n = c.resolve(n, path, r)
	if n == nil || !c.enter(n, r) {
		return nil
	}
	defer c.exit()
	c.pos.record(path, spanOfNode(n))

	switch node := n.(type) {
	case *ast.SequenceNode:
		values := make([]*v1.Value, 0, len(node.Values))
		for i, element := range node.Values {
			value := c.structureValue(element, indexPath(path, i), r)
			if value == nil {
				return nil
			}
			values = append(values, value)
		}
		return v1.NewStructureList(values...)

	case *ast.MappingNode, *ast.MappingValueNode:
		entries, ok := c.entries(n, path, r)
		if !ok {
			return nil
		}
		mapped := make(map[string]*v1.Value, len(entries))
		for _, e := range entries {
			value := c.structureValue(e.value, fieldPath(path, e.name), r)
			if value == nil {
				return nil
			}
			mapped[e.name] = value
		}
		return v1.NewStructureMap(mapped)

	case *ast.StringNode:
		return c.structureScalar(n, node.Value, path, r)

	case *ast.LiteralNode:
		return c.structureScalar(n, blockText(node), path, r)

	default:
		lit := c.literal(n, path, r)
		if lit == nil {
			return nil
		}
		return &v1.Value{Kind: &v1.Value_Literal{Literal: lit}}
	}
}

// structureScalar compiles one entry of such a structure: a reference, or literal
// text.
func (c *compiler) structureScalar(n ast.Node, text, path string, r ref) *v1.Value {
	inner, fenced := SplitFence(text)
	if !fenced {
		if err := fenceError(text); err != nil {
			c.report(spanOfNode(n), r, "%s", err)
			return nil
		}
		return &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{
			Kind: &expr.Value_StringValue{StringValue: text},
		}}}
	}

	span := spanWithin(n, inner)
	c.recordExpr(path, span)

	val := v1.NewExpr(inner)
	if err := val.Error(); err != nil {
		at, msg := celFailure(err, span, inner)
		c.report(at, r, "is not a valid expression: %s", msg)
		return nil
	}

	// A whole entry, so the reference may be the whole of it and nothing else:
	// `${'Bearer ' + secret('env:T')}` is refused here by the same rule that
	// refuses it as a whole input, and with the same sentence.
	if reference, isSecret := c.secret(val.GetExpr(), inner, span, r, secretAllowed); isSecret {
		return reference
	}

	c.report(span, r, "%s", mixedStructureHelp)
	return nil
}

// holdsSecretMarker reports whether a structure has a ${secret(...)} anywhere
// inside it, which is what decides how the structure is compiled at all.
func (c *compiler) holdsSecretMarker(n ast.Node) bool {
	found := false
	c.walkMarkers(n, func(ast.Node, string) bool {
		found = true
		return false
	})
	return found
}

// walkMarkers visits every scalar inside n whose expression calls the marker,
// stopping early when visit says so.
//
// Parsing each fenced scalar rather than looking for the text `secret(`, because
// the question is whether CEL sees a call to it: `${steps.secret.result}` and
// `${'secret('}` both contain those characters and neither is a reference. The
// same walk answers where to point a diagnostic, so the two cannot disagree about
// what counts as a marker.
func (c *compiler) walkMarkers(n ast.Node, visit func(ast.Node, string) bool) bool {
	n = c.resolveQuiet(n)
	if n == nil || !c.enter(n, ref{}) {
		return true
	}
	defer c.exit()

	scalar := func(text string) bool {
		inner, fenced := SplitFence(text)
		if !fenced {
			return true
		}
		parsed := v1.NewExpr(inner)
		if parsed.Error() != nil {
			return true
		}
		if _, found := findSecretCall(parsed.GetExpr().GetExpr()); !found {
			return true
		}
		return visit(n, inner)
	}

	switch node := n.(type) {
	case *ast.StringNode:
		return scalar(node.Value)
	case *ast.LiteralNode:
		return scalar(blockText(node))
	case *ast.SequenceNode:
		for _, v := range node.Values {
			if !c.walkMarkers(v, visit) {
				return false
			}
		}
	case *ast.MappingNode:
		for _, v := range node.Values {
			if !c.walkMarkers(v.Value, visit) {
				return false
			}
		}
	case *ast.MappingValueNode:
		return c.walkMarkers(node.Value, visit)
	}

	return true
}

// misplacedHelp explains why a reference cannot be where it is, or returns empty
// when it can.
//
// isWholeExpression distinguishes a reference written on its own from one buried in
// a larger expression, which is the difference between a task input that is a
// reference and one that computes with a value it must never see.
func misplacedHelp(placement secretPlacement, isWholeExpression bool) string {
	switch {
	case placement == secretNotEvaluable:
		return notEvaluableHelp
	case placement == secretInStructure:
		// Reached only if [compiler.holdsSecretMarker] and this disagree about what
		// a marker is — they call the same [findSecretCall] over the same scalars,
		// so they should not. Kept because the direction of the disagreement
		// matters: without this, a marker the first walk missed would be compiled
		// into the one expression that builds the structure, and the workflow would
		// evaluate a secret. Refusing is the safe half of a contradiction.
		return inStructureHelp
	case placement == secretNotWholeValue, !isWholeExpression:
		return notWholeValueHelp
	default:
		return ""
	}
}

// findSecretCall returns the outermost call to the marker anywhere in an
// expression.
//
// The search is not limited to the root because a reference nested in a larger
// expression has to be reported rather than missed: ${'Bearer ' + secret('env:T')}
// is a mistake with a specific fix, and the alternative is compiling it into an
// expression that fails at run time.
//
// Only a global call counts. `secret` as a step id (${steps.secret.result}), a bare name
// (${secret}), or a method on something (${x.secret('a')}) is not the marker, since
// none of those is the thing a Flowfile writes to name a secret.
func findSecretCall(e *expr.Expr) (*expr.Expr, bool) {
	if e == nil {
		return nil, false
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_CallExpr:
		if kind.CallExpr.GetTarget() == nil && kind.CallExpr.GetFunction() == SecretMarker {
			return e, true
		}
		if found, ok := findSecretCall(kind.CallExpr.GetTarget()); ok {
			return found, true
		}
		for _, arg := range kind.CallExpr.GetArgs() {
			if found, ok := findSecretCall(arg); ok {
				return found, true
			}
		}
	case *expr.Expr_SelectExpr:
		return findSecretCall(kind.SelectExpr.GetOperand())
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			if found, ok := findSecretCall(element); ok {
				return found, true
			}
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			if found, ok := findSecretCall(entry.GetMapKey()); ok {
				return found, true
			}
			if found, ok := findSecretCall(entry.GetValue()); ok {
				return found, true
			}
		}
	case *expr.Expr_ComprehensionExpr:
		comprehension := kind.ComprehensionExpr
		for _, part := range []*expr.Expr{
			comprehension.GetIterRange(),
			comprehension.GetAccuInit(),
			comprehension.GetLoopCondition(),
			comprehension.GetLoopStep(),
			comprehension.GetResult(),
		} {
			if found, ok := findSecretCall(part); ok {
				return found, true
			}
		}
	}
	return nil, false
}

// secretArgument returns the reference a marker names, when it names exactly one
// and names it literally.
func secretArgument(call *expr.Expr) (string, bool) {
	args := call.GetCallExpr().GetArgs()
	if len(args) != 1 {
		return "", false
	}

	constant, ok := args[0].GetExprKind().(*expr.Expr_ConstExpr)
	if !ok {
		return "", false
	}
	text, ok := constant.ConstExpr.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return "", false
	}
	return text.StringValue, true
}

// markerSpan returns the span of the marker call within the expression source, so
// that a diagnostic about a reference buried in a longer expression lands on the
// reference.
//
// Two adjustments make it land where a reader looks. The parser records a call at
// its opening parenthesis, so the span moves back onto the name when the source
// confirms the name is there — and simply stays on the parenthesis when it does not,
// rather than trusting a convention it cannot see. And positions are offsets into
// the expression source, which is one line in any Flowfile not using a block scalar;
// a multi-line expression falls back to the whole span rather than guessing.
func markerSpan(parsed *expr.ParsedExpr, id int64, src string, span Span) Span {
	offset, ok := parsed.GetSourceInfo().GetPositions()[id]
	if !ok || !span.IsValid() || offset < 0 || strings.ContainsRune(src, '\n') {
		return span
	}

	runes := []rune(src)
	at := int(offset)
	if at > len(runes) {
		return span
	}
	if name := len([]rune(SecretMarker)); at >= name && string(runes[at-name:at]) == SecretMarker {
		at -= name
	}

	start := span.Start
	start.Column += at
	return Span{Start: start, End: span.End}
}

// secretRefToDSL renders a reference back into the form a Flowfile writes.
func secretRefToDSL(reference *v1.SecretRef) (string, error) {
	if err := secrets.ValidateRef(reference); err != nil {
		return "", fmt.Errorf("cannot be written: %w", err)
	}
	return fmt.Sprintf("%s%s(%s)%s",
		fenceOpen, SecretMarker, quoteCELString(secrets.RefString(reference)), fenceClose), nil
}
