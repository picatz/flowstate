package flowfile

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/google/cel-go/cel"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Values are the one place the DSL is genuinely ambiguous, so the rule it follows
// is stated in one place — the package documentation in doc.go — and implemented
// here. In short: the schema says whether a field holds an expression, and the
// parser follows it. Where the schema knows the field is an expression, a string
// is expression source and the ${...} fence is optional; where the field can be
// either, the fence is what makes it an expression.

// The fence is the punctuation that marks a value as an expression.
const (
	fenceOpen  = "${"
	fenceClose = "}"
)

// SplitFence returns the expression source inside a whole-value ${...} fence, and
// reports whether the value is written with that shape.
//
// It answers only the question of shape. Whether the source inside is a usable
// expression is [ExprError]'s question, and [ExprSource] is the two together — which
// is the safer default for a caller that would otherwise treat a malformed fence as
// literal text. Use this one when you compile the contents yourself, as the language
// server does to place a CEL syntax error at the character it is on.
//
// Where the fence ends is not decided by counting braces — CEL decides, when the
// contents are compiled — because an expression may legitimately contain them:
// ${ {'k': 1} } is one expression, not a fence around {'k'.
//
// It is exported because what counts as an expression has to mean the same thing in
// the compiler, in validation, and in the editor. A second implementation of this
// rule is how they would come to disagree.
func SplitFence(s string) (inner string, fenced bool) {
	if len(s) < len(fenceOpen)+len(fenceClose) {
		return "", false
	}
	if !strings.HasPrefix(s, fenceOpen) || !strings.HasSuffix(s, fenceClose) {
		return "", false
	}
	return s[len(fenceOpen) : len(s)-len(fenceClose)], true
}

// containsFence reports whether a value has a ${ anywhere in it, and so cannot be
// taken at face value as literal text.
func containsFence(s string) bool { return strings.Contains(s, fenceOpen) }

// interpolationHelp is the fix for a value that tried to interpolate somewhere
// that cannot: say it as one expression, since CEL can concatenate.
//
// "here" is doing real work in that sentence since #413. A Flowfile value
// interpolates, so a message saying an expression *must* be the whole value
// would now be a plain falsehood about the language. What is left are the
// surfaces that are not Flowfile values and keep the older, narrower rule — a
// stub's `returns:` in a test document, a `--input` on the command line — and
// there the rule is true of the position rather than of the syntax. Naming the
// position is what keeps the sentence honest, and keeps an author from
// concluding that the spelling they just used in a workflow does not exist.
const interpolationHelp = "a ${...} expression has to be the whole value here; " +
	"write it as one expression instead, like ${'total: ' + count.result}"

// ExprSource returns the expression source inside a whole-value ${...} fence, and
// reports whether the value is a usable expression.
//
// It reports false for a fence whose contents do not compile, so a caller that
// ignores [ExprError] cannot act on a corrupt expression — which is the failure this
// replaced, where "${a} and ${b}" matched a regex and compiled the mangled middle.
// [SplitFence] is the shape alone, for a caller that compiles the contents itself.
func ExprSource(s string) (string, bool) {
	inner, fenced := SplitFence(s)
	if !fenced || ExprError(s) != nil {
		return "", false
	}
	return inner, true
}

// ExprError reports why a value written as an expression is not a usable one, and
// nil when it is either a valid expression or plainly not meant to be one.
//
// Three mistakes are worth telling apart, because their fixes differ: text left
// outside the fence, a fence that was never closed, and a fence whose contents are
// not an expression.
func ExprError(s string) error {
	inner, fenced := SplitFence(s)
	if !fenced {
		return fenceError(s)
	}
	if value := v1.NewExpr(inner); value.Error() != nil {
		_, message := celFailure(value.Error(), Span{}, inner)
		if containsFence(inner) {
			return fmt.Errorf("invalid expression %q: %s; %s", inner, message, interpolationHelp)
		}
		return fmt.Errorf("invalid expression %q: %s", inner, message)
	}
	return nil
}

// fenceError reports a ${...} that does not span the whole value, which is the
// mistake behind writing "hello ${steps.name.result}" and expecting interpolation.
func fenceError(s string) error {
	if !containsFence(s) {
		return nil
	}
	if opened := s[strings.Index(s, fenceOpen)+len(fenceOpen):]; !strings.Contains(opened, fenceClose) {
		return fmt.Errorf("unterminated expression in %q: a ${ has to be closed with a }", s)
	}
	return fmt.Errorf("%q mixes literal text with an expression: %s", s, interpolationHelp)
}

// inputValue compiles a task input, where what the value holds is whatever the task
// declares and so only a fenced string is an expression.
func (c *compiler) inputValue(n ast.Node, path string, r ref) *v1.Value {
	return c.value(n, path, r, false)
}

// exprValue compiles a field the schema types as an expression — a step's
// condition, a loop's items — where a string is expression source and the fence is
// optional.
func (c *compiler) exprValue(n ast.Node, path string, r ref) *v1.Value {
	return c.value(n, path, r, true)
}

// callArgumentValue compiles one entry of a call's `with:` — a value bound to a
// callee's declared input, resolved in the caller's scope.
//
// Checked for a secret reference first, and refused wherever one is found —
// whether the entry is a bare ${secret(...)} or one buried inside a structure —
// rather than delegated to [compiler.value], which would allow the bare case:
// an argument is an ordinary value the whole way from here to [CallScope],
// never a reference a worker resolves later, so nothing about "the whole value
// of a task input" applies to it. See [notAcrossCallHelp] for why this is
// refused rather than modeled.
func (c *compiler) callArgumentValue(n ast.Node, path string, r ref) *v1.Value {
	if resolved := c.resolveQuiet(n); resolved != nil && c.holdsSecretMarker(resolved) {
		c.report(spanOfNode(resolved), r, "%s", notAcrossCallHelp)
		return nil
	}
	return c.value(n, path, r, false)
}

// varValue compiles one entry of a `vars:` block, at the workflow or on a step.
//
// Checked for a secret reference first, and refused wherever one is found — bare,
// buried in a larger expression, or nested in a list or a mapping — rather than
// delegated to [compiler.inputValue], which would allow the bare case: a var is not
// a task input, so "the whole value of a task input" never describes one. The check
// is [compiler.holdsSecretMarker] rather than a placement, for the reason
// [compiler.callArgumentValue] uses the same one — none of the placement reasons is
// true of this position, so the switch that draws them would have to be told a
// falsehood to reach a refusal here. See [notInVarHelp].
func (c *compiler) varValue(n ast.Node, path string, r ref) *v1.Value {
	if resolved := c.resolveQuiet(n); resolved != nil && c.holdsSecretMarker(resolved) {
		c.report(c.secretMarkerSpan(resolved), r, "%s", notInVarHelp)
		return nil
	}
	return c.inputValue(n, path, r)
}

// value compiles one node into a schema Value.
//
// It returns nil after reporting a diagnostic, so a caller building a message must
// tolerate a missing value; the workflow is discarded when any diagnostic is
// reported.
func (c *compiler) value(n ast.Node, path string, r ref, exprCtx bool) *v1.Value {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.recordTree(n, path)

	switch node := n.(type) {
	case *ast.StringNode:
		return c.scalarString(n, node.Value, path, r, exprCtx)
	case *ast.LiteralNode:
		// A block scalar: | or >. Its text is a string like any other.
		return c.scalarString(n, blockText(node), path, r, exprCtx)
	case *ast.MappingNode, *ast.MappingValueNode, *ast.SequenceNode:
		return c.composite(n, path, r)
	default:
		lit := c.literal(n, path, r)
		if lit == nil {
			return nil
		}
		return &v1.Value{Kind: &v1.Value_Literal{Literal: lit}}
	}
}

// blockText returns the text of a block scalar, written with | or >.
func blockText(n *ast.LiteralNode) string {
	if n == nil || n.Value == nil {
		return ""
	}
	return n.Value.Value
}

// scalarString applies the expression rule to a string scalar.
//
// The order of the three questions is the whole of the compatibility argument,
// and exprCtx changes only the last of them.
//
// A scalar that is exactly one fence is asked about first, and answered exactly
// as it always was, so `init: ${0}` is still the integer zero and a
// `${secret(...)}` is still a reference. A scalar holding a fence *and* anything
// else is interpolation — text built from its expressions — which is a set the
// old rule refused entirely, so nothing that used to mean something means
// something else now. Only a scalar with no fence at all reaches the last
// question, and that is the one the two contexts answer differently: where the
// schema types a field as an expression, a bare string is expression source
// (`if: a > 1`), and everywhere else it is literal text.
//
// Interpolation reaches both contexts rather than task inputs alone. It is
// tempting to stop at inputs, on the ground that a condition wants a boolean and
// a loop's `items:` wants a list, so text is no use to either — but a declared
// output's `value:` is expression-typed too, and a run's answer document is
// exactly where an author writes a sentence. Refusing there would leave the
// language with two rules about what a `${` means, decided by which field it is
// in, and an author with no way to predict which they were in. A condition that
// interpolates is a type error, which the type checker reports as a type error;
// that is a better thing to be told than that the syntax is wrong.
func (c *compiler) scalarString(n ast.Node, text, path string, r ref, exprCtx bool) *v1.Value {
	// A whole scalar is the one place a secret reference can go, and only when
	// the scalar is a task input: a field the workflow evaluates itself cannot
	// hold one.
	placement := secretAllowed
	if exprCtx {
		placement = secretNotEvaluable
	}

	segs, err := scanInterpolation(text)
	if err != nil {
		c.report(spanOfNode(n), r, "%s", err)
		return nil
	}

	if inner, ok := wholeValueFence(segs, text); ok {
		return c.expression(n, inner, path, r, placement)
	}

	if hasFence(segs) {
		// A reference among text is refused by whichever rule already covers the
		// position: in a field the workflow evaluates, because nothing there may
		// hold one at all; elsewhere, because a reference has to be the whole
		// value rather than part of a sentence built from it.
		misplaced := secretNotWholeValue
		if exprCtx {
			misplaced = secretNotEvaluable
		}
		return c.interpolation(n, text, segs, path, r, misplaced)
	}

	if exprCtx {
		// The raw text rather than the scan's, because `$${` is the escape a
		// *value* spells a literal fence with, and this is not a value — it is
		// CEL source, where those characters are already CEL's to interpret.
		return c.expression(n, text, path, r, placement)
	}

	return &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{
		Kind: &expr.Value_StringValue{StringValue: literalText(segs)},
	}}}
}

// interpolation compiles a scalar mixing literal text with one or more fences
// into the single expression that builds the text.
//
// Each fence is compiled on its own first, before the desugaring, for two
// reasons that both come down to reporting the truth about what the author
// wrote. A CEL error belongs at the character it is on *inside its own fence*,
// which is only knowable while the fence is still a fence — the desugared source
// has different offsets and an added `string(` in front of every one of them.
// And a `${secret(...)}` in mixed position has to be refused rather than
// desugared: the desugaring would evaluate it, which is the one thing a secret
// reference must never be.
func (c *compiler) interpolation(n ast.Node, text string, segs []segment, path string, r ref, misplaced secretPlacement) *v1.Value {
	for _, sg := range segs {
		if !sg.fence {
			continue
		}

		span := spanOfFence(n, text, sg)
		val := v1.NewExpr(sg.text)
		if err := val.Error(); err != nil {
			at, msg := celFailure(err, span, sg.text)
			c.report(at, r, "is not a valid expression: %s", msg)
			return nil
		}

		// Refused rather than compiled: a reference is resolved by the worker
		// that needs the value, and text built from one is a value the workflow
		// holds. The caller chooses which of the two sentences fits the position
		// the value is in.
		if _, isSecret := c.secret(val.GetExpr(), sg.text, span, r, misplaced); isSecret {
			return nil
		}
	}

	src := interpolationSource(segs)
	c.recordExpr(path, spanOfNode(n))

	val := v1.NewExpr(src)
	if err := val.Error(); err != nil {
		// Unreachable while every fence parses and every literal is quoted, and
		// so reported against the whole value rather than guessed at: a position
		// invented for an impossible case is the wrong-position failure one
		// surface over from `flow fix` corruption.
		_, msg := celFailure(err, spanOfNode(n), src)
		c.report(spanOfNode(n), r, "is not a valid expression: %s", msg)
		return nil
	}
	return normalizeExpr(val)
}

// expression compiles expression source written at n into a Value, recording the
// span of the source itself so a later diagnostic about the expression can
// underline it rather than the whole line.
//
// placement says whether this is somewhere a ${secret(...)} reference may appear;
// see [compiler.secret], which compiles the marker rather than leaving a call for
// something to evaluate later.
func (c *compiler) expression(n ast.Node, src, path string, r ref, placement secretPlacement) *v1.Value {
	span := spanWithin(n, src)
	c.recordExpr(path, span)

	val := v1.NewExpr(src)
	if err := val.Error(); err != nil {
		at, msg := celFailure(err, span, src)
		if containsFence(src) {
			// A second fence inside the first: "${a} ${b}" opens at the start and
			// closes at the end, so it parses as one expression and fails inside.
			// The syntax error is true but unhelpful on its own.
			msg += "; " + interpolationHelp
		}
		c.report(at, r, "is not a valid expression: %s", msg)
		return nil
	}

	if reference, isSecret := c.secret(val.GetExpr(), src, span, r, placement); isSecret {
		return reference
	}
	return normalizeExpr(val)
}

// normalizeExpr returns the expression as it would be written back out.
//
// Two spellings of one expression — ${a.b} and ${ a.b } — parse to ASTs that
// differ in the source positions they carry, so without this a workflow compiled
// from the second spelling is not equal to the same workflow after a round trip
// through Marshal. Normalizing at compile time makes the stored expression a
// fixed point of that round trip.
func normalizeExpr(val *v1.Value) *v1.Value {
	text, err := cel.AstToString(cel.ParsedExprToAst(val.GetExpr()))
	if err != nil {
		// cel-go cannot write back an expression whose source used a macro, such
		// as a comprehension, because the parsed form no longer records the macro
		// call. Keeping it exactly as parsed is right: it executes correctly, and
		// Marshal is the only thing that cannot represent it.
		return val
	}
	normalized := v1.NewExpr(text)
	if normalized.Error() != nil {
		return val
	}
	return normalized
}

// composite compiles a sequence or mapping.
//
// A structure whose values are all literal stays a literal, so that a map of
// headers is carried as data. One containing a ${...} anywhere inside becomes a
// single CEL expression building the whole structure, which is the only way a
// per-key expression can be evaluated at all — unless one of those ${...} is a
// secret reference, which is the one thing that must not be evaluated. That case
// keeps every entry a value of its own; see [compiler.structure].
func (c *compiler) composite(n ast.Node, path string, r ref) *v1.Value {
	if c.containsExpr(n) {
		// A structure holding a secret reference is compiled entry by entry
		// instead, so the reference stays one — see [compiler.structure], which
		// also reports the inputs where that is not allowed.
		if c.holdsSecretMarker(n) {
			return c.structure(n, path, r)
		}

		text, ok := c.celText(n, path, r)
		if !ok {
			return nil
		}
		return c.expression(n, text, path, r, secretInStructure)
	}
	lit := c.literal(n, path, r)
	if lit == nil {
		return nil
	}
	return &v1.Value{Kind: &v1.Value_Literal{Literal: lit}}
}

// scalarHoldsFence reports whether one scalar holds an expression anywhere in
// it, whether as the whole value or interpolated among text.
//
// It is the question [compiler.containsExpr] asks of each leaf, and it asks it
// through the scanner rather than through [SplitFence] deliberately: a walk that
// still asked the whole-value question would answer "no expression here" for
// `a ${b}` nested in a mapping, and the structure would then be compiled as
// literal data with the fence shipped as characters. That is the walk-misses-a-
// new-branch defect this repository has now had four of, and the fix is that
// every walk asks one function.
func scalarHoldsFence(text string) bool {
	segs, err := scanInterpolation(text)
	if err != nil {
		// A malformed fence is not literal text either. Saying so here routes the
		// value to the path that reports the error with a position, rather than
		// letting it be quietly carried as data.
		return true
	}
	return hasFence(segs)
}

// containsExpr reports whether a node holds a fenced expression anywhere inside
// it.
func (c *compiler) containsExpr(n ast.Node) bool {
	n = c.resolveQuiet(n)
	if n == nil || !c.enter(n, ref{}) {
		return false
	}
	defer c.exit()

	switch node := n.(type) {
	case *ast.StringNode:
		return scalarHoldsFence(node.Value)
	case *ast.LiteralNode:
		return scalarHoldsFence(blockText(node))
	case *ast.SequenceNode:
		for _, v := range node.Values {
			if c.containsExpr(v) {
				return true
			}
		}
	case *ast.MappingNode:
		for _, v := range node.Values {
			if c.containsExpr(v.Value) {
				return true
			}
		}
	case *ast.MappingValueNode:
		return c.containsExpr(node.Value)
	}
	return false
}

// literal builds the schema representation of a value written entirely as data.
func (c *compiler) literal(n ast.Node, path string, r ref) *expr.Value {
	n = c.resolve(n, path, r)
	if n == nil || !c.enter(n, r) {
		return nil
	}
	defer c.exit()

	switch node := n.(type) {
	case *ast.StringNode:
		return c.literalString(n, node.Value, r)
	case *ast.LiteralNode:
		// A block scalar is a string like any other, so it carries the same rule:
		// text that opens a fence is not literal text, whatever it is nested in.
		// [compiler.scalarString] applies this where a block scalar is a value on
		// its own; without it here, the same document with the block scalar one
		// level down — a `note: |` inside a `json:` mapping — ships the `${...}`
		// as characters and says nothing, which leaves the author no reason to
		// doubt the file.
		return c.literalString(n, blockText(node), r)
	case *ast.IntegerNode:
		switch v := node.Value.(type) {
		case int64:
			return &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: v}}
		case uint64:
			// YAML has one integer type; whether the parser hands back a signed or
			// an unsigned value is an artifact of how the digits were written. Only
			// a number too large to be signed stays unsigned, because there it is
			// the difference between the value and an overflow.
			if v > math.MaxInt64 {
				return &expr.Value{Kind: &expr.Value_Uint64Value{Uint64Value: v}}
			}
			return &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: int64(v)}}
		default:
			c.report(spanOfNode(n), r, "%v is out of range for an integer", node.Value)
			return nil
		}
	case *ast.FloatNode:
		return &expr.Value{Kind: &expr.Value_DoubleValue{DoubleValue: node.Value}}
	case *ast.BoolNode:
		return &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: node.Value}}
	case *ast.NullNode:
		if node.GetToken() != nil && node.GetToken().Type == implicitNull {
			c.report(spanOfNode(n), r, "has no value; give it one or remove the key")
			return nil
		}
		return &expr.Value{Kind: &expr.Value_NullValue{}}
	case *ast.SequenceNode:
		values := make([]*expr.Value, 0, len(node.Values))
		for i, elem := range node.Values {
			v := c.literal(elem, indexPath(path, i), r)
			if v == nil {
				return nil
			}
			values = append(values, v)
		}
		return &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: values}}}
	case *ast.MappingNode, *ast.MappingValueNode:
		entries, ok := c.entries(n, path, r)
		if !ok {
			return nil
		}
		mapped := make([]*expr.MapValue_Entry, 0, len(entries))
		for _, e := range entries {
			v := c.literal(e.value, fieldPath(path, e.name), r)
			if v == nil {
				return nil
			}
			mapped = append(mapped, &expr.MapValue_Entry{
				Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: e.name}},
				Value: v,
			})
		}
		return &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: mapped}}}
	default:
		c.report(spanOfNode(n), r, "%s cannot be used as a value", describeNode(n))
		return nil
	}
}

// literalString builds the literal for a scalar that holds no expression,
// resolving the `$${` escape into the `${` it stands for.
//
// The escape is resolved here and not only where a value is interpolated,
// because a value written entirely as text is exactly where an author needs to
// say `${` and mean it. Reached only after [compiler.containsExpr] has answered
// that the scalar holds no fence, so the remaining errors are the malformed
// ones — an unterminated `${`, which is still no more literal text than it ever
// was.
func (c *compiler) literalString(n ast.Node, text string, r ref) *expr.Value {
	segs, err := scanInterpolation(text)
	if err != nil {
		c.report(spanOfNode(n), r, "%s", err)
		return nil
	}
	if hasFence(segs) {
		// Unreachable: every caller reaches a string through containsExpr, which
		// answers through the same scan. Kept as a refusal rather than a silent
		// pass-through, because the direction of a disagreement between two walks
		// matters — this is the half that does not ship a fence as characters.
		c.report(spanOfNode(n), r, "%s", fenceError(text))
		return nil
	}
	return &expr.Value{Kind: &expr.Value_StringValue{StringValue: literalText(segs)}}
}

// celText builds the CEL source for a structure containing expressions.
//
// Keys and elements keep the order they were written, so the expression reads like
// the YAML it came from and compiles to the same thing every time.
func (c *compiler) celText(n ast.Node, path string, r ref) (string, bool) {
	n = c.resolve(n, path, r)
	if n == nil || !c.enter(n, r) {
		return "", false
	}
	defer c.exit()

	switch node := n.(type) {
	case *ast.StringNode:
		return c.celTextString(n, node.Value, r)
	case *ast.LiteralNode:
		return c.celTextString(n, blockText(node), r)
	case *ast.IntegerNode:
		switch v := node.Value.(type) {
		case int64:
			return strconv.FormatInt(v, 10), true
		case uint64:
			// See literal: unsigned only when the number does not fit signed, so
			// that a list of small numbers is a list of CEL ints either way.
			if v > math.MaxInt64 {
				return strconv.FormatUint(v, 10) + "u", true
			}
			return strconv.FormatUint(v, 10), true
		default:
			c.report(spanOfNode(n), r, "%v is out of range for an integer", node.Value)
			return "", false
		}
	case *ast.FloatNode:
		return strconv.FormatFloat(node.Value, 'g', -1, 64), true
	case *ast.BoolNode:
		return strconv.FormatBool(node.Value), true
	case *ast.NullNode:
		if node.GetToken() != nil && node.GetToken().Type == implicitNull {
			c.report(spanOfNode(n), r, "has no value; give it one or remove the key")
			return "", false
		}
		return "null", true
	case *ast.SequenceNode:
		parts := make([]string, 0, len(node.Values))
		for i, elem := range node.Values {
			text, ok := c.celText(elem, indexPath(path, i), r)
			if !ok {
				return "", false
			}
			parts = append(parts, text)
		}
		return "[" + strings.Join(parts, ", ") + "]", true
	case *ast.MappingNode, *ast.MappingValueNode:
		entries, ok := c.entries(n, path, r)
		if !ok {
			return "", false
		}
		parts := make([]string, 0, len(entries))
		for _, e := range entries {
			text, ok := c.celText(e.value, fieldPath(path, e.name), r)
			if !ok {
				return "", false
			}
			parts = append(parts, quoteCELString(e.name)+": "+text)
		}
		return "{" + strings.Join(parts, ", ") + "}", true
	default:
		c.report(spanOfNode(n), r, "%s cannot be used as a value", describeNode(n))
		return "", false
	}
}

// celTextString renders one string inside a structure: a fenced value is the
// expression it fences, an interpolated one is the expression that builds its
// text, and anything else is a CEL string literal.
//
// It answers with the same three-way split [compiler.scalarString] uses in a
// task input, and for the reason that split exists at all: a scalar one level
// down inside a `fields:` mapping or a list is the same kind of position as one
// written directly, so `fields: {msg: "a ${b}"}` has to mean what `msg: a ${b}`
// means. Two rules here would be two spellings of one concept, which is the
// shape every driver disagreement in this repository has had.
func (c *compiler) celTextString(n ast.Node, text string, r ref) (string, bool) {
	segs, err := scanInterpolation(text)
	if err != nil {
		c.report(spanOfNode(n), r, "%s", err)
		return "", false
	}

	if inner, ok := wholeValueFence(segs, text); ok {
		return inner, true
	}

	if hasFence(segs) {
		for _, sg := range segs {
			if !sg.fence {
				continue
			}
			span := spanOfFence(n, text, sg)
			if val := v1.NewExpr(sg.text); val.Error() != nil {
				at, msg := celFailure(val.Error(), span, sg.text)
				c.report(at, r, "is not a valid expression: %s", msg)
				return "", false
			}
		}
		return "(" + interpolationSource(segs) + ")", true
	}

	return quoteCELString(literalText(segs)), true
}

// quoteCELString renders a Go string as a CEL string literal.
//
// The line breaks are escaped, not only the backslash and the quote, because a
// single-quoted CEL string cannot span a line: a text holding one would close
// nothing and the expression built around it would not parse. That is not
// hypothetical since #413 — a block scalar is the natural place to write a long
// message, `|` and `>` both keep a trailing newline, and interpolating one puts
// that newline inside a literal here.
func quoteCELString(s string) string {
	esc := strings.NewReplacer(
		`\`, `\\`,
		`'`, `\'`,
		"\n", `\n`,
		"\r", `\r`,
		"\t", `\t`,
	).Replace(s)
	return "'" + esc + "'"
}

// celErrorPattern matches the location cel-go puts in front of a parse error.
// The location is dropped: the diagnostic carries a position in the Flowfile,
// which is where the author is looking, and "<input>:1:7" alongside it reads like
// a second, contradictory answer.
var celErrorPattern = regexp.MustCompile(`ERROR: <input>:(\d+):(\d+): (.*)`)

// celFailure narrows an expression's compile failure to the character at fault
// and strips the wrapping that says only that a CEL expression failed to parse.
//
// src is the expression's own source, carried so that [TranslateCELMessage] can
// name the last token an author wrote when the parser ran out of input. Nothing
// else here reads it, and a caller that does not have it may pass "": the
// translation loses one clause and stays correct.
func celFailure(err error, span Span, src string) (Span, string) {
	text := err.Error()
	match := celErrorPattern.FindStringSubmatch(text)
	if match == nil {
		return span, lastCause(text)
	}

	msg := strings.TrimSpace(match[3])
	line, _ := strconv.Atoi(match[1])
	column, _ := strconv.Atoi(match[2])

	// Translated before the position is applied, because the translation reads
	// the line and column as cel-go reported them: a position within src, whose
	// column is relative to its own line. The narrowing below is a separate
	// question and only answers it for a single-line expression.
	msg = TranslateCELMessage(msg, src, line, column)

	if !span.IsValid() || line != 1 || column < 1 {
		return span, msg
	}

	// The reported column is within the expression source, which begins at the
	// span's start.
	at := span.Start
	at.Column += column - 1
	return Span{Start: at, End: span.End}, msg
}

// lastCause returns the innermost message of a wrapped error, so that a reader
// sees what went wrong rather than the chain of functions that noticed.
func lastCause(text string) string {
	if i := strings.LastIndex(text, ": "); i >= 0 && i+2 < len(text) {
		return text[i+2:]
	}
	return text
}

// describeNode names a YAML construct the way an author would, for a message about
// one that cannot appear where it does.
func describeNode(n ast.Node) string {
	switch n.(type) {
	case nil:
		return "nothing"
	case *ast.StringNode:
		return "a string"
	case *ast.LiteralNode:
		return "a block of text"
	case *ast.IntegerNode, *ast.FloatNode:
		return "a number"
	case *ast.BoolNode:
		return "a true or false"
	case *ast.NullNode:
		return "nothing"
	case *ast.SequenceNode:
		return "a list"
	case *ast.MappingNode, *ast.MappingValueNode:
		return "a mapping"
	case *ast.TagNode:
		return "a tagged value"
	case *ast.MergeKeyNode:
		return "a merge key"
	case *ast.AliasNode:
		return "an alias"
	case *ast.AnchorNode:
		return "an anchor"
	case *ast.InfinityNode, *ast.NanNode:
		return "a number"
	default:
		return fmt.Sprintf("a %s", strings.ToLower(strings.TrimSuffix(n.Type().String(), "Node")))
	}
}
