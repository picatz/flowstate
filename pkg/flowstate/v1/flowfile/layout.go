package flowfile

import (
	"reflect"
	"regexp"
	"strings"

	yaml "github.com/goccy/go-yaml"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// This file holds the three places [Marshal] writes what an author wrote rather
// than what the compiler reduced it to. None of them reads the source: the
// proto records neither a sequence's flow style nor an interpolated string's
// segments, and a formatter that kept "what was written" would give two files
// that mean one thing two spellings, which docs/STYLE.md R7 rules out. Each is
// instead a *canonical* form chosen from the compiled value alone, and each is
// verified the way [scalarSurvives] and [unfoldSurvives] verify theirs: the
// candidate is read back, and a candidate that does not read back as the same
// thing is dropped for the form that did.

// maxFlowSequence is the widest `[a, b]` literal written on one line, brackets
// included. A fixed number and not an option, for the reason R7 gives; it is
// the width at which `argv: [make, build, test]` is still one glance and a
// list of eight flags is a list.
const maxFlowSequence = 60

// flowSafe is the scalar a flow sequence may hold unquoted and unspaced: no
// flow indicator, no `#`, no `: `, and no leading character YAML reads as
// something else. Narrower than YAML's own rule on purpose, because anything
// outside it is simply written as a block sequence, which is always correct.
var flowSafe = regexp.MustCompile(`^(?:[A-Za-z0-9_./=+@%]|-[^\s-])(?:[A-Za-z0-9_./=+@% -]|:[A-Za-z0-9_/])*$`)

// flowSequence writes a short sequence of plain scalars on one line.
//
// `argv: [make, build]` is two words and is not worth five lines; gofmt keeps
// `[]string{"a", "b"}` on one line for the same reason. Only a sequence whose
// every element is a plain string (or a number or boolean) and whose one-line
// form fits [maxFlowSequence] qualifies, so a sequence of mappings, of quoted
// text, or of fenced expressions keeps the block form the corpus uses.
func flowSequence(elements []any) any {
	if len(elements) == 0 {
		return elements
	}

	parts := make([]string, 0, len(elements))
	for _, element := range elements {
		switch element := element.(type) {
		case styledScalar:
			text := string(element)
			if !flowSafe.MatchString(text) || strings.HasSuffix(text, " ") {
				return elements
			}
			parts = append(parts, text)
		case int64, uint64, bool:
			encoded, err := yaml.Marshal(element)
			if err != nil {
				return elements
			}
			parts = append(parts, strings.TrimSpace(string(encoded)))
		default:
			return elements
		}
	}

	candidate := styledScalar("[" + strings.Join(parts, ", ") + "]")
	if len(candidate) > maxFlowSequence {
		return elements
	}
	if !flowSurvives(elements, candidate) {
		return elements
	}
	return candidate
}

// flowSurvives reports whether candidate reads back as the same data as the
// block sequence it replaces, under a key and as an entry of a sequence.
func flowSurvives(elements []any, candidate styledScalar) bool {
	for _, inSequence := range []bool{false, true} {
		read := func(v any) (any, bool) {
			var document any = yaml.MapSlice{{Key: "v", Value: v}}
			if inSequence {
				document = yaml.MapSlice{{Key: "v", Value: []any{v}}}
			}
			encoded, err := yaml.Marshal(document)
			if err != nil {
				return nil, false
			}
			var back any
			if err := yaml.Unmarshal(encoded, &back); err != nil {
				return nil, false
			}
			return back, true
		}
		want, ok := read(elements)
		if !ok {
			return false
		}
		got, ok := read(candidate)
		if !ok || !reflect.DeepEqual(want, got) {
			return false
		}
	}
	return true
}

// interpolatedString writes an expression back as the interpolated string it
// came from: `"SERVICE=" + string(svc)` as `SERVICE=${svc}`.
//
// The compiler desugars `"a ${b} c"` into exactly that concatenation (see
// [interpolationSource]) and records nothing else, so the author's spelling and
// the hand-written `${"a " + string(b) + " c"}` are one value. The shorter,
// documented spelling is the canonical one. The candidate is accepted only when
// [unfoldSurvives] compiles it back to this same value, which also refuses
// every shape that is not exactly what interpolation produces.
func interpolatedString(value *v1.Value) (any, bool) {
	parsed := value.GetExpr()
	if parsed == nil || len(parsed.GetSourceInfo().GetMacroCalls()) > 0 {
		return nil, false
	}
	candidate, ok := interpolatedExpr(parsed.GetExpr())
	if !ok || !unfoldSurvives(value, candidate) {
		return nil, false
	}
	return candidate, true
}

// interpolatedExpr is [interpolatedString] without the verification, for the
// leaves of an unfolded structure, whose enclosing value is verified whole.
func interpolatedExpr(e *expr.Expr) (any, bool) {
	var operands []*expr.Expr
	for {
		call := e.GetCallExpr()
		if call == nil || call.GetFunction() != "_+_" || len(call.GetArgs()) != 2 {
			operands = append(operands, e)
			break
		}
		operands = append(operands, call.GetArgs()[1])
		e = call.GetArgs()[0]
	}

	var text strings.Builder
	fences, literals := 0, 0
	for i := len(operands) - 1; i >= 0; i-- {
		operand := operands[i]
		if s, isString := operand.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue); isString {
			literals++
			text.WriteString(escapeFences(s.StringValue))
			continue
		}
		call := operand.GetCallExpr()
		if call == nil || call.GetFunction() != "string" || call.GetTarget() != nil || len(call.GetArgs()) != 1 {
			return nil, false
		}
		inner, err := exprToText(&expr.ParsedExpr{Expr: call.GetArgs()[0]})
		if err != nil {
			return nil, false
		}
		fences++
		text.WriteString(fenceOpen + inner + fenceClose)
	}

	// `string(a) + string(b)` with nothing between is as likely a hand-written
	// conversion as an interpolation, and `${a}` alone compiles to `a`, not to
	// `string(a)`. Interpolation needs text beside a fence, or two fences.
	if fences == 0 || (literals == 0 && fences < 2) {
		return nil, false
	}
	return styledScalarFor(text.String()), true
}
