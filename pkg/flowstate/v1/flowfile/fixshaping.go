package flowfile

import (
	"slices"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml/ast"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Rewriting a CEL map literal smuggled through a quoted string into the mapping
// form the language keeps.
//
//	outputs: '${ {"reference": response.json.reference} }'
//
//	outputs:
//	  reference: ${response.json.reference}
//
// Both spellings compile and both run, which is what makes this a style sweep
// rather than a migration — see [Fix]'s own note on what an edition boundary is
// for. What the second one has that the first does not is keys a reader and a
// validator can both see: shaped names survive compilation, so `flow validate`
// can report `${steps.web.referance}` against the set the step produces.
//
// # What it will not touch
//
// A map whose keys are not written down — `{name: 1}`, where `name` is a
// variable — is left exactly as it is, silently. It is correct code, its shaped
// set is genuinely a run-time question, and the string-fenced form is the
// documented spelling for it. Silence rather than a note, because a note about
// every dynamic shaping in a repository is a line an author learns to skip.
//
// Anything the rewriter cannot render back without guessing is also left alone:
// a key that is not a plain YAML name, a duplicate key, an expression cel-go
// will not write back as source, a line carrying a trailing comment that
// replacing it would delete. Each of those is a file that keeps working, which
// is the only outcome this command may produce for input it does not fully
// understand.

// mapLiteralKeys are the keys whose fenced map literal is rewritten into the
// mapping form, for the block they are written in.
//
// A loop's carried state and a shaping task's outputs, and the two are here for
// slightly different reasons. `init:`/`update:` gain readability: the mapping
// form is the same value written with one entry per line. `outputs:` gains
// checking as well, because the compiler keeps a shaping mapping's entries
// (see [compiler.shapedOutputs]).
var (
	loopStateKeys  = []string{loopInitKey, loopUpdateKey}
	shapingOutputs = []string{v1.ShapingInput}
)

// mappingForm rewrites the named keys of a block, where each holds a fenced map
// literal with keys written down.
func (f *fixer) mappingForm(block ast.Node, keys []string) {
	mapping, ok := unwrapAnchor(block).(*ast.MappingNode)
	if !ok {
		if single, isSingle := unwrapAnchor(block).(*ast.MappingValueNode); isSingle {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			return
		}
	}

	for _, value := range mapping.Values {
		name, named := keyNameOf(value.Key)
		if !named || !slices.Contains(keys, name) {
			continue
		}
		f.mapLiteralToMapping(value)
	}
}

// mapLiteralToMapping rewrites one entry, or leaves it alone.
func (f *fixer) mapLiteralToMapping(node *ast.MappingValueNode) {
	name, named := keyNameOf(node.Key)
	if !named {
		return
	}

	text, isScalar := scalarText(unwrapAnchor(node.Value))
	if !isScalar {
		// Already a mapping, or a shape this does not read. Either way there is
		// nothing here to promote.
		return
	}

	source, fenced := SplitFence(text)
	if !fenced {
		// The fence is what says this is one expression. A bare CEL map in an
		// expression-typed position (`init: {a: 1}` unfenced) is already a YAML
		// mapping to the parser and is not this rewrite's business.
		return
	}

	entries, ok := mapLiteralEntries(source)
	if !ok {
		return
	}

	keySpan, valueSpan := spanOfNode(node.Key), spanOfNode(unwrapAnchor(node.Value))
	if !keySpan.IsValid() || !valueSpan.IsValid() || valueSpan.End.Line < keySpan.Start.Line {
		return
	}

	// A comment anywhere in the run about to be replaced would be deleted by
	// replacing it. Left alone instead: prose an author wrote beside a value is
	// worth more than the spelling of the value.
	for line := keySpan.Start.Line; line <= valueSpan.End.Line; line++ {
		if strings.Contains(f.line(line), "#") {
			return
		}
	}

	prefix, ok := f.throughKeyColon(keySpan)
	if !ok {
		return
	}

	indent := strings.Repeat(" ", keySpan.Start.Column-1+2)
	replacement := []string{prefix}
	for _, entry := range entries {
		replacement = append(replacement, indent+entry.name+": "+entry.value)
	}

	f.record(keySpan.Start.Line, valueSpan.End.Line, replacement,
		"wrote `"+name+":` as a mapping, one name per line",
		"`"+name+":` would be written as a mapping, one name per line")
}

// throughKeyColon returns the key's own line, cut after the colon that ends it,
// so the replacement keeps whatever the author wrote in front of the key — a
// sequence dash, their indentation — rather than this rebuilding it from a span.
func (f *fixer) throughKeyColon(keySpan Span) (string, bool) {
	line := f.line(keySpan.Start.Line)
	from, located := byteOffsetOfColumn(line, keySpan.Start.Column)
	if !located {
		return "", false
	}
	colon := strings.Index(line[from:], ":")
	if colon < 0 {
		return "", false
	}
	return line[:from+colon+1], true
}

// A mapEntry is one name of a rewritten mapping, with its value already rendered
// as the YAML scalar it will be written as.
type mapEntry struct {
	name  string
	value string
}

// mapLiteralEntries reads a CEL map literal's entries in the order they were
// written, when every key is a name and every value can be written back.
//
// Whether the map's keys are knowable at all is [v1.ShapedOutputNames]'s
// question, asked here rather than answered again: the compiler decides what a
// shaped set is, the validator reports against that decision, and a rewriter
// with its own opinion would rewrite files those two disagree about.
func mapLiteralEntries(source string) ([]mapEntry, bool) {
	value := v1.NewExpr(source)
	if value.Error() != nil {
		return nil, false
	}

	names, knowable := v1.ShapedOutputNames(value)
	if !knowable || len(names) == 0 {
		return nil, false
	}

	parsed := value.GetExpr()
	structure := parsed.GetExpr().GetStructExpr()
	if structure == nil {
		// A literal map with no expression in it parses to a map expression all
		// the same, so this is unreachable for a knowable set; declining rather
		// than asserting, because a rewriter that assumes is a rewriter that
		// panics on somebody's file.
		return nil, false
	}

	written := make([]mapEntry, 0, len(structure.GetEntries()))
	seen := make(map[string]bool, len(structure.GetEntries()))
	for _, entry := range structure.GetEntries() {
		key, ok := entry.GetMapKey().GetExprKind().(*expr.Expr_ConstExpr)
		if !ok {
			return nil, false
		}
		name := key.ConstExpr.GetStringValue()
		if !plainYAMLName(name) || seen[name] {
			// A key needing quotes, or written twice. Both are legal CEL and
			// neither survives the trip to a mapping unremarked: one changes how
			// the key is written, the other loses an entry.
			return nil, false
		}
		seen[name] = true

		if literal, isLiteral := literalScalar(entry.GetValue()); isLiteral {
			// A constant needs no fence once it is a mapping entry: both of these
			// positions hold *values*, and the schema is what says so, which is
			// the carve-out the fence rule reserved for exactly this. `${0}`
			// becomes `0` and reads as the number it always was.
			written = append(written, mapEntry{name: name, value: literal})
			continue
		}

		text, err := exprToText(&expr.ParsedExpr{
			Expr: entry.GetValue(),
			// The source info travels with the sub-expression, because it is
			// what records the macro a comprehension came from. Without it, an
			// entry written `${records.map(r, r.id)}` is written back in its
			// expanded form, which is valid CEL nobody would recognise as
			// theirs.
			SourceInfo: parsed.GetSourceInfo(),
		})
		if err != nil {
			return nil, false
		}

		scalar, ok := fencedScalar(text)
		if !ok {
			return nil, false
		}
		written = append(written, mapEntry{name: name, value: scalar})
	}

	if len(written) != len(names) {
		return nil, false
	}

	return written, true
}

// fencedScalar renders one expression as the YAML scalar holding it.
//
// Plain where a plain scalar reads back as the same characters, quoted where it
// would not — which is the collision the string-fenced spelling existed to dodge
// in the first place: a `: ` inside a ternary is YAML mapping syntax. Quoting one
// entry is a smaller apology than quoting the whole shaping, and it is per entry
// rather than per step.
func fencedScalar(source string) (string, bool) {
	if strings.ContainsAny(source, "\n\r") || strings.Contains(source, fenceOpen) {
		// A fence inside a fence has no spelling here, and a multi-line
		// expression cannot be written as one scalar without deciding how to
		// fold it.
		return "", false
	}

	fencedText := fenceOpen + source + fenceClose
	if !strings.Contains(fencedText, ": ") && !strings.Contains(fencedText, " #") &&
		!strings.HasSuffix(fencedText, ":") {
		return fencedText, true
	}

	// Single quotes, with any of its own doubled, because the text is CEL source
	// and a double-quoted YAML scalar would read the backslashes in it as escapes.
	return "'" + strings.ReplaceAll(fencedText, "'", "''") + "'", true
}

// literalScalar renders a constant as the YAML scalar holding it, when it is one
// of the constants a Flowfile can write as data.
//
// Numbers, booleans and strings, and nothing else. A double is left fenced
// because writing one back means choosing a rendering (`1e-7`, `0.0000001`) and
// a rewriter that picks is a rewriter that changed the file's text for its own
// reasons; a null or a byte string has no YAML spelling this position reads back
// the same way. Each of those keeps the fence, which was never wrong — only
// noisy.
//
// [quoteScalar] decides the quoting, so a string that spells `yes` comes back
// quoted rather than as a boolean.
func literalScalar(e *expr.Expr) (string, bool) {
	constant := e.GetConstExpr()
	if constant == nil {
		return "", false
	}

	switch kind := constant.GetConstantKind().(type) {
	case *expr.Constant_Int64Value:
		return strconv.FormatInt(kind.Int64Value, 10), true
	case *expr.Constant_Uint64Value:
		return strconv.FormatUint(kind.Uint64Value, 10), true
	case *expr.Constant_BoolValue:
		return strconv.FormatBool(kind.BoolValue), true
	case *expr.Constant_StringValue:
		return quoteScalar(kind.StringValue), true
	}

	return "", false
}

// plainYAMLName reports whether a name can be written as a mapping key with no
// quoting and read back as itself.
//
// Deliberately narrower than YAML allows. These are output names, which the rest
// of the language already spells as identifiers, and the cost of being wrong here
// is a rewritten file that means something else.
func plainYAMLName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && (r >= '0' && r <= '9' || r == '-'):
		default:
			return false
		}
	}
	return true
}
