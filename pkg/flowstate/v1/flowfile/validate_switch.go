package flowfile

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A switch groups what used to be sibling `if:` steps against one value, and the
// grouping is what makes these diagnostics possible at all: nothing ties three
// equalities together, but one `switch:` says "this is a total dispatch over one
// value", which is a claim a validator can check. Every diagnostic here is fatal,
// like every other diagnostic in this package — there is no warning tier, and the
// opt-outs are spelled in the language instead: `default: {steps: []}` is how an
// author with a checkable domain says "I mean to handle nothing else".
//
// # What "knowable" means today
//
// The domain checks (impossible value, exhaustiveness, unreachable default, type
// mismatch) fire only where the discriminant's domain is a property of the file:
// a `${steps.<id>.<name>}` naming a wait's shaped output whose expression is
// built from conditionals over string literals — the approval gate's ternary
// yields exactly `deployed | rejected | expired`, which is inferable. An open
// domain (a webhook field, an input, anything shaped by an expression the
// validator cannot bound) is deliberately silent, per the report-what-the-file-
// owns rule: the runtime half — the unmatched record in the step's outputs —
// covers that gap. The duplicate and computed-case checks need no domain and
// fire everywhere.

// validateSwitch checks a switch node: its discriminant, its case literals, its
// claim about the domain, and its bodies.
//
// placement passes through to the bodies unchanged: exactly one body runs, once,
// in order, in the run's own scope, so an `undo:` there means exactly what it
// would mean on the same step written under an `if:` at this position — both
// drivers run the body the same way.
func validateSwitch(id string, sw *v1.Switch, enclosing refScope, index int, wf *v1.Workflow, depth int, placement v1.UndoScope) Diagnostics {
	var ds Diagnostics

	if sw.GetValue() == nil {
		ds = append(ds, Diagnostic{
			Step: id, Field: "switch",
			Message: "value is required: the expression the switch dispatches on",
		})
	} else {
		ds = append(ds, validateInputRefs(id, "value", sw.GetValue(), enclosing, index, wf)...)
	}

	if len(sw.GetCases()) == 0 {
		ds = append(ds, Diagnostic{
			Step: id, Field: "switch",
			Message: "at least one case is required; a switch that is only a `default:` is an " +
				"unconditional block wearing a switch's clothes — write the steps directly",
		})
	}

	domain, domainKnown := switchDomain(sw.GetValue(), wf)

	// The case literals: computed values refused, duplicates found after
	// flattening `case: [a, b]` lists, and — where the domain is knowable —
	// impossible values and type mismatches. `seen` carries every literal already
	// accepted, with the field it was written under, so the duplicate diagnostic
	// can name the first occurrence.
	type acceptedCase struct {
		literal *expr.Value
		field   string
	}
	var seen []acceptedCase
	handled := map[string]bool{}

	for i, c := range sw.GetCases() {
		values := c.GetValues()
		for j, value := range values {
			field := switchCaseField(i, len(values), j)

			literal, ok := value.GetKind().(*v1.Value_Literal)
			if !ok {
				// Diagnostic 6: a computed case. Every other check here needs
				// case values the validator can see, so a single computed case
				// would erase all of them for this switch — refused, with the
				// settled sentence saying what to write instead.
				ds = append(ds, Diagnostic{
					Step: id, Field: field,
					Message: "must be a static value: cases are literals; a computed comparison is what `if:` is for",
				})
				continue
			}

			lit := literal.Literal
			text := switchLiteralText(lit)

			if _, isNull := lit.GetKind().(*expr.Value_NullValue); isNull || lit.GetKind() == nil {
				ds = append(ds, Diagnostic{
					Step: id, Field: field,
					Message: "is null, which is not a value to dispatch on; a discriminant nobody set is " +
						"what `default:` (or failing the step) is for, and a null case would make the " +
						"step's own `case` record ambiguous",
				})
				continue
			}
			if !switchScalarLiteral(lit) {
				ds = append(ds, Diagnostic{
					Step: id, Field: field,
					Message: "must be a scalar literal; a case matches one value by equality, and a list " +
						"here is already how several values share one body (`case: [a, b]`)",
					Code: v1.DiagnosticCodeTypeMismatch,
				})
				continue
			}

			if str, isStr := lit.GetKind().(*expr.Value_StringValue); isStr && looksLikeRange(str.StringValue) {
				// The same refusal a `${...}` gets, because it is the same reach
				// for a predicate: `case: 2xx` and `case: 400-499` are the first
				// thing someone routing HTTP statuses will try.
				ds = append(ds, Diagnostic{
					Step: id, Field: field, Value: str.StringValue,
					Message: fmt.Sprintf(
						"%s looks like a range, and a case matches one literal value exactly: "+
							"cases are literals; a computed comparison is what `if:` is for",
						text),
				})
				continue
			}

			// Diagnostic 2: a duplicate after flattening. The second occurrence
			// can never match — cases are tried in written order and the first
			// match wins — so it is a mistake by construction.
			duplicated := false
			for _, previous := range seen {
				if v1.SwitchLiteralsEqual(previous.literal, lit) {
					ds = append(ds, Diagnostic{
						Step: id, Field: field, Value: text,
						Message: fmt.Sprintf(
							"case %s is already handled by `%s`; cases are tried in written order and "+
								"the first match wins, so this one can never match",
							text, previous.field),
						Code: v1.DiagnosticCodeConstraintViolation,
					})
					duplicated = true
					break
				}
			}
			if duplicated {
				continue
			}
			seen = append(seen, acceptedCase{literal: lit, field: field})

			if !domainKnown {
				continue
			}

			str, isStr := lit.GetKind().(*expr.Value_StringValue)
			if !isStr {
				// Diagnostic 5: a type the discriminant can never produce. The
				// knowable tier is string-shaped (literal-leaved conditionals),
				// so a non-string case against it can never match.
				ds = append(ds, Diagnostic{
					Step: id, Field: field, Value: text,
					Message: fmt.Sprintf(
						"case %s is not a string, and %s is always one of %s; a case of a type the "+
							"value can never produce can never match",
						text, switchValueText(sw.GetValue()), quotedList(domain)),
					Code: v1.DiagnosticCodeTypeMismatch,
				})
				continue
			}

			if !slices.Contains(domain, str.StringValue) {
				// Diagnostic 1: an impossible value, with the nearest legal
				// spelling — nearestChoice, the closed-set matcher, because
				// against a small enumerated domain a suggestion is nearly
				// always the answer.
				message := fmt.Sprintf(
					"case %s is not a value %s can produce; the values are %s",
					text, switchValueText(sw.GetValue()), quotedList(domain))
				if suggestion, ok := nearestChoice(str.StringValue, domain); ok {
					message += fmt.Sprintf("; did you mean %q?", suggestion)
				}
				ds = append(ds, Diagnostic{
					Step: id, Field: field, Value: str.StringValue,
					Message: message,
					Code:    v1.DiagnosticCodeConstraintViolation,
				})
				continue
			}
			handled[str.StringValue] = true
		}
	}

	if domainKnown {
		var missing []string
		for _, value := range domain {
			if !handled[value] {
				missing = append(missing, value)
			}
		}

		switch {
		case sw.GetDefault() == nil && len(missing) > 0:
			// Diagnostic 3: writing no `default:` claims exhaustiveness, and the
			// claim is checkable here, so it is checked — Rust's E0004 at this
			// tier. The remedy names both ways out, because deliberately
			// handling a subset is legal and has a spelling.
			ds = append(ds, Diagnostic{
				Step: id, Field: "switch",
				Value: strings.Join(missing, ", "),
				Message: fmt.Sprintf(
					"cases do not handle %s, which %s can produce; a switch with no `default:` claims "+
						"to handle every value, so add the missing cases, or add a `default:` — an empty "+
						"`default: {steps: []}` is how deliberately handling nothing else is written down",
					quotedList(missing), switchValueText(sw.GetValue())),
				Code: v1.DiagnosticCodeConstraintViolation,
			})

		case sw.GetDefault() != nil && len(missing) == 0:
			// Diagnostic 4: the mirror claim. A `default:` beside cases that
			// already exhaust the domain is dead code, the same mistake class as
			// a duplicate case — Rust's unreachable_patterns on the `_` arm.
			ds = append(ds, Diagnostic{
				Step: id, Field: "default",
				Message: fmt.Sprintf(
					"`default:` can never run: the cases already handle every value %s can produce (%s); "+
						"remove it, or remove the case whose value it was meant to catch",
					switchValueText(sw.GetValue()), quotedList(domain)),
				Code: v1.DiagnosticCodeConstraintViolation,
			})
		}
	}

	// The bodies. Ids must be unique across the whole switch and against the
	// enclosing scope, the parallel-branch rule for the parallel-branch reason:
	// every body's outputs merge into one namespace after the block (exactly one
	// body ran, so nothing collides at run time — but a reference has to mean one
	// step at validate time). Each body is validated against the enclosing scope
	// only: a body cannot see a sibling body's steps, since at most one of them
	// exists in any run.
	seenIDs := make(map[string]bool, len(enclosing.steps))
	maps.Copy(seenIDs, enclosing.steps)

	checkBody := func(body []*v1.Node, where string) {
		for _, node := range body {
			if seenIDs[node.GetId()] {
				ds = append(ds, Diagnostic{
					Step: node.GetId(),
					Message: fmt.Sprintf(
						"id is already used outside %s; switch bodies share one output namespace — "+
							"exactly one of them runs — so ids must be unique across them for a "+
							"reference to mean one step",
						where),
				})
			}
		}
		ds = append(ds, validateNested(body, enclosing, index, wf, depth, placement)...)
		for _, node := range body {
			seenIDs[node.GetId()] = true
		}
	}

	for i, c := range sw.GetCases() {
		checkBody(c.GetSteps(), "case "+strconv.Itoa(i+1))
	}
	if def := sw.GetDefault(); def != nil {
		checkBody(def.GetSteps(), "the default")
	}

	return ds
}

// switchStepIDs returns the ids of every step across a switch's bodies,
// including those nested control flow merges out, mirroring [branchStepIDs].
func switchStepIDs(sw *v1.Switch) []string {
	var ids []string
	for _, body := range v1.SwitchBodies(sw) {
		ids = append(ids, mergedStepIDs(body)...)
	}
	return ids
}

// switchCaseField addresses one case literal the way its position was recorded:
// the whole `case:` key for a scalar, the element for a list entry.
func switchCaseField(caseIndex, valueCount, valueIndex int) string {
	field := "cases[" + strconv.Itoa(caseIndex) + "].case"
	if valueCount > 1 {
		field += "[" + strconv.Itoa(valueIndex) + "]"
	}
	return field
}

// switchDomain reports the set of values a switch's discriminant can produce,
// where that is a property of the file.
//
// The one inferable tier today: the discriminant is `${steps.<id>.<name>}`, the
// step is a `wait_for_signal:` shaping `<name>` in its `outputs:`, and the
// shaping expression is conditionals over string literals all the way down —
// the approval gate's ternary. Enum-typed workflow inputs extend this tier when
// they land. A `must:` constraint is deliberately *not* mined for a domain: a
// constraint expression is not a type.
func switchDomain(value *v1.Value, wf *v1.Workflow) ([]string, bool) {
	parsed := value.GetExpr()
	if parsed == nil {
		return nil, false
	}

	// `steps.<id>.<name>` and nothing else: a select of a select of the root.
	outer := parsed.GetExpr().GetSelectExpr()
	inner := outer.GetOperand().GetSelectExpr()
	if outer == nil || inner == nil || outer.GetTestOnly() || inner.GetTestOnly() {
		return nil, false
	}
	if inner.GetOperand().GetIdentExpr().GetName() != v1.StepsRoot {
		return nil, false
	}
	stepID, outputName := inner.GetField(), outer.GetField()

	signal := nodeWithID(stepID, wf).GetWait().GetSignal()
	if signal == nil {
		return nil, false
	}
	shaped := signal.GetOutputs()[outputName]
	if shaped.GetExpr() == nil {
		return nil, false
	}

	var leaves []string
	if !literalStringLeaves(shaped.GetExpr().GetExpr(), &leaves) {
		return nil, false
	}

	// In first-appearance order, deduplicated, so diagnostics enumerate the
	// domain the way the shaping expression reads.
	var domain []string
	for _, leaf := range leaves {
		if !slices.Contains(domain, leaf) {
			domain = append(domain, leaf)
		}
	}
	if len(domain) == 0 {
		return nil, false
	}
	return domain, true
}

// literalStringLeaves walks a shaping expression and collects the string
// literals it can produce, reporting false the moment any leaf is something
// else — at which point the domain is open and every domain check stays silent.
func literalStringLeaves(e *expr.Expr, into *[]string) bool {
	switch {
	case e.GetCallExpr() != nil:
		call := e.GetCallExpr()
		// The conditional operator, spelled the way cel-go names it. Only its
		// two result branches contribute values; the condition itself decides
		// which, and can be anything.
		if call.GetFunction() != "_?_:_" || len(call.GetArgs()) != 3 {
			return false
		}
		return literalStringLeaves(call.GetArgs()[1], into) && literalStringLeaves(call.GetArgs()[2], into)

	case e.GetConstExpr() != nil:
		s, ok := e.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
		if !ok {
			return false
		}
		*into = append(*into, s.StringValue)
		return true

	default:
		return false
	}
}

// switchScalarLiteral reports whether a literal is a scalar a case may hold.
func switchScalarLiteral(lit *expr.Value) bool {
	switch lit.GetKind().(type) {
	case *expr.Value_StringValue, *expr.Value_Int64Value, *expr.Value_Uint64Value,
		*expr.Value_DoubleValue, *expr.Value_BoolValue, *expr.Value_BytesValue:
		return true
	default:
		return false
	}
}

// looksLikeRange reports a string case spelled the way an HTTP status range is:
// `2xx`, or `400-499`. Both are the first thing someone dispatching on a status
// code tries, and both silently never match anything, which is the exact
// failure mode this construct exists to prevent.
func looksLikeRange(s string) bool {
	if len(s) < 2 {
		return false
	}

	digits := 0
	for digits < len(s) && s[digits] >= '0' && s[digits] <= '9' {
		digits++
	}
	if digits == 0 {
		return false
	}
	rest := s[digits:]

	// `2xx`, `4XX`: digits then only x's.
	if rest != "" && strings.Trim(rest, "xX") == "" {
		return true
	}

	// `400-499`: digits, a dash, digits.
	if after, dashed := strings.CutPrefix(rest, "-"); dashed && after != "" {
		for i := 0; i < len(after); i++ {
			if after[i] < '0' || after[i] > '9' {
				return false
			}
		}
		return true
	}

	return false
}

// switchLiteralText renders a case literal for a message the way an author
// wrote it: strings quoted, everything else as its value.
func switchLiteralText(lit *expr.Value) string {
	switch kind := lit.GetKind().(type) {
	case *expr.Value_StringValue:
		return strconv.Quote(kind.StringValue)
	case *expr.Value_Int64Value:
		return strconv.FormatInt(kind.Int64Value, 10)
	case *expr.Value_Uint64Value:
		return strconv.FormatUint(kind.Uint64Value, 10)
	case *expr.Value_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64)
	case *expr.Value_BoolValue:
		return strconv.FormatBool(kind.BoolValue)
	case nil, *expr.Value_NullValue:
		return "null"
	default:
		return "this value"
	}
}

// switchValueText names the discriminant in a message: its own source text when
// it can be rendered, and the key it was written under otherwise.
func switchValueText(value *v1.Value) string {
	if parsed := value.GetExpr(); parsed != nil {
		if text, err := exprToText(parsed); err == nil {
			return "`" + text + "`"
		}
	}
	return "the switch's `value:`"
}

// quotedList renders a small closed set for a message: `"a", "b", "c"`.
func quotedList(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = strconv.Quote(v)
	}
	return strings.Join(quoted, ", ")
}
