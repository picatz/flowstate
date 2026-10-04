package flowstatev1

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// The rules of one value also share one CEL cost budget, [DefaultCostLimit]: each
// evaluation is bounded alone, and 4096 of them would otherwise each spend a whole
// expression's allowance.

// MaxRuleEvaluations bounds how many `must:` rules one value is held to: a list of
// records each with rules is one evaluation per rule per element, and the element
// bound of a value ([maxListElements]) says nothing about how many rules a type
// carries. A value that would take more is refused rather than judged in part,
// because a rule that is silently skipped past a count is a rule that does not hold.
const MaxRuleEvaluations = 4096

// CheckRecordRules holds a value to the `must:` rules of the record types it is of:
// each field's own rule, with `this` bound to the field's value, and the type's
// rule, with `this` bound to the record, which is where a rule across fields
// (`this.start < this.end`) is written.
//
// kind is `input` or `output`, for the sentence a refusal opens with, and sensitive
// says the declaration is marked so, which keeps a value out of the refusal.
//
// It is the rule half of [CheckInputValueIn], called after it and beside
// [CheckInputConstraints] by every place those two are: submit, a literal default
// or example, a call boundary's literal `with:` argument, and a computed or literal
// output. The value is therefore already the shape the type states, so this walks
// it by the same structure and evaluates, in written order, at the first rule that
// does not hold. A rule is evaluated by the one function an input's `must:` is
// (compiled and run under the same profile, cost bound and determinism rule), so
// there is no second evaluator to disagree with.
//
// Nil for a nil table, a type that names no record, and a value with no literal.
// Work is bounded by the value ([maxListElements] elements, [MaxStructureDepth]
// levels) and by [MaxRuleEvaluations].
func CheckRecordRules(table TypeTable, profile, kind, name string, sensitive bool, t *Type, value *Value) error {
	lit := value.GetLiteral()
	if lit == nil || len(table) == 0 || len(messageNames(t, nil, 0)) == 0 {
		return nil
	}

	// The element bound a rule's comprehension is costed against, applied here
	// because an output without a `must:` of its own never reaches the check that
	// applies it.
	if err := checkConstraintValueBound(kind, name, lit); err != nil {
		return err
	}

	w := &ruleWalk{table: table, profile: profile, sensitive: sensitive, asts: map[ruleKey]*cel.Ast{}}
	if err := w.value(t, lit, "", 0); err != nil {
		return fmt.Errorf("%s %q: %w", kind, name, err)
	}

	return nil
}

type ruleKey struct {
	must string
	t    InputDeclaration_Type
}

type ruleWalk struct {
	table     TypeTable
	profile   string
	sensitive bool
	asts      map[ruleKey]*cel.Ast
	spent     int
	cost      uint64
}

// value walks lit by the structural type t, evaluating the rules of every record it
// meets.
func (w *ruleWalk) value(t *Type, lit *expr.Value, path string, depth int) error {
	if depth > MaxStructureDepth {
		return nil
	}

	switch kind := t.GetKind().(type) {
	case *Type_List:
		list, ok := lit.GetKind().(*expr.Value_ListValue)
		if !ok {
			return nil
		}
		for i, element := range list.ListValue.GetValues() {
			if err := w.value(kind.List, element, fmt.Sprintf("%s[%d]", path, i), depth+1); err != nil {
				return err
			}
		}
	case *Type_Map_:
		m, ok := lit.GetKind().(*expr.Value_MapValue)
		if !ok {
			return nil
		}
		for _, entry := range m.MapValue.GetEntries() {
			key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue)
			if !isString {
				continue
			}
			if err := w.value(kind.Map.GetValue(), entry.GetValue(), path+w.key(key.StringValue), depth+1); err != nil {
				return err
			}
		}
	case *Type_Message:
		return w.record(kind.Message, lit, path, depth)
	}

	return nil
}

func (w *ruleWalk) record(name string, lit *expr.Value, path string, depth int) error {
	declared := w.table[name]
	m, ok := lit.GetKind().(*expr.Value_MapValue)
	if declared == nil || !ok {
		return nil
	}

	present := make(map[string]*expr.Value, len(m.MapValue.GetEntries()))
	for _, entry := range m.MapValue.GetEntries() {
		if key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue); isString {
			present[key.StringValue] = entry.GetValue()
		}
	}

	for _, field := range declared.GetFields() {
		value, given := present[field.GetName()]
		if !given {
			continue
		}
		fieldPath := path + "." + field.GetName()

		if field.Must != nil {
			if err := w.rule(field.GetMust(), field.GetType(), value, "the field", fieldPath); err != nil {
				return err
			}
		}
		if err := w.value(field.DeclaredType(), value, fieldPath, depth+1); err != nil {
			return err
		}
	}

	if declared.Must != nil {
		return w.rule(declared.GetMust(), InputDeclaration_TYPE_STRUCT, lit, "the record "+name, path)
	}

	return nil
}

// rule evaluates one `must:` over value, whose declared kind is t.
func (w *ruleWalk) rule(must string, t InputDeclaration_Type, value *expr.Value, subject, path string) error {
	if w.spent++; w.spent > MaxRuleEvaluations {
		return fmt.Errorf("more than %d `must:` rules apply to this value; a rule that is not evaluated does not hold, so it is refused", MaxRuleEvaluations)
	}

	key := ruleKey{must, t}
	ast, compiled := w.asts[key]
	if !compiled {
		var err error
		if ast, err = CompileMustExpression(w.profile, must, t); err != nil {
			return fmt.Errorf("%s%s %w", subject, atPath(path), err)
		}
		w.asts[key] = ast
	}

	satisfied, cost, err := evalMustWithCost(context.Background(), w.profile, t, ast, value)
	if w.cost += cost; w.cost > DefaultCostLimit {
		return fmt.Errorf("the `must:` rules of this value spend more than %d cost units together, which is the budget one expression is held to; a rule that is not evaluated does not hold, so it is refused", DefaultCostLimit)
	}
	if err != nil {
		if w.sensitive {
			return fmt.Errorf("%s%s: evaluating `must: %s` failed", subject, atPath(path), echoName(must))
		}
		return fmt.Errorf("%s%s: evaluating `must: %s`: %w", subject, atPath(path), echoName(must), err)
	}
	if !satisfied {
		return fmt.Errorf("%s%s must satisfy `%s`%s", subject, atPath(path), echoName(must), w.got(value))
	}

	return nil
}

// key renders a map key as a path segment, which for a declaration marked sensitive
// is a placeholder: a key is data the file supplies, and the word says none is repeated.
func (w *ruleWalk) key(k string) string {
	if w.sensitive {
		return "[*]"
	}

	return "." + echoName(k)
}

// got renders "; got v" for a value short enough to read and not a container,
// because a refusal that echoes a whole record echoes what the file controls. It
// renders nothing for a declaration marked sensitive: the value is exactly what that
// word says is not to be repeated.
func (w *ruleWalk) got(value *expr.Value) string {
	if w.sensitive {
		return ""
	}

	switch value.GetKind().(type) {
	case *expr.Value_ListValue, *expr.Value_MapValue, *expr.Value_ObjectValue:
		return ""
	}

	native, err := literalToNative(value)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("; got %s", echoName(fmt.Sprint(native)))
}

// maxEchoedRuleText bounds how much of a name or an expression a refusal repeats.
const maxEchoedRuleText = 128

// echoName cuts s to [maxEchoedRuleText] bytes on a rune boundary, with an ellipsis.
func echoName(s string) string {
	if len(s) <= maxEchoedRuleText {
		return s
	}

	cut := maxEchoedRuleText
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}

	return s[:cut] + "…"
}

// CheckLiteralOutputRules holds an output written as a literal or a structure to the
// `must:` rules of the records its type holds, before anything runs: the
// admission-time half of what [EvalRunOutputs] does at completion, so a constant
// answer that breaks its type's rule is refused with the specification rather than
// after the steps have had their effects. Nil for an expression, which is only
// knowable once evaluated.
func CheckLiteralOutputRules(table TypeTable, profile string, declaration *OutputDeclaration, value *Value) error {
	if _, isStructure := value.GetKind().(*Value_Structure_); isStructure {
		literal, err := structureLiteral(value)
		if err != nil {
			return nil // CheckOutputValueIn names the shape error
		}
		value = &Value{Kind: &Value_Literal{Literal: literal}}
	}

	return CheckRecordRules(table, profile, "output", declaration.GetName(), declaration.GetSensitive(), declaration.DeclaredType(), value)
}
