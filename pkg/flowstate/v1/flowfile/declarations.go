package flowfile

import (
	"errors"
	"fmt"
	"slices"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a set of declarations can be wrong about.
//
// The schema checks one declaration at a time — a name of bounded length matching a
// CEL identifier, a type that is defined, a description that fits — because
// protovalidate rules are per-field. Everything that is a fact about the *set*, or
// about a name CEL's lexer refuses, or about which arm of a oneof a value is, is
// listed on `InputDeclaration` in the schema as the compiler's, and this is the
// compiler keeping that list.
//
// Each check reports where the mistake is written, what is wrong, and what to write
// instead. They are gathered here rather than spread through [Validate] because the
// list in the schema is one list, and a reader comparing the two should not have to
// find seven places.

// declarationRoots are the rooted namespaces a name may not shadow.
//
// A step id, a loop's binding and a step's own `vars:` key are all names that win
// over a root when an expression resolves — the first because a specification
// compiled before a root existed may hold a step of that name, the other two
// because a bare binding wins over everything. So a file that takes one of these
// names does not collide with the root: it *hides* it, silently, for every
// expression after the point it is bound.
//
// Written as the category rather than as the one root that needed it first. `steps`
// was refused as a step id when rooting landed and the rule stopped there, which is
// how `vars` — a root since — could be taken by a loop's `as:` and hide the
// workflow's whole var namespace inside the body with nothing said.
var declarationRoots = []string{v1.StepsRoot, v1.VarsRoot, v1.InputsRoot, v1.RunRoot, v1.TriggerRoot}

// isDeclarationRoot reports whether a name is one of the rooted namespaces.
func isDeclarationRoot(name string) bool { return slices.Contains(declarationRoots, name) }

// shadowsRoot renders the refusal for a name that would hide a root.
func shadowsRoot(what, name string) string {
	return fmt.Sprintf(
		"%q is the root %s are named under, so a %s of that name would hide all of them; choose another %s name",
		name, rootHolds(name), what, what)
}

// rootHolds says what a root answers with, for the sentence above.
func rootHolds(root string) string {
	switch root {
	case v1.StepsRoot:
		return "every step's outputs"
	case v1.VarsRoot:
		return "the workflow's vars"
	case v1.InputsRoot:
		return "the run's inputs"
	case v1.RunRoot:
		return "the run's own address and starter identity"
	case v1.TriggerRoot:
		return "how the run started"
	default:
		return "those values"
	}
}

// validateDeclaredInputs reports what is wrong with the `inputs:` block as a whole.
func validateDeclaredInputs(wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	seen := make(map[string]int, len(wf.GetDeclaredInputs()))
	for i, declaration := range wf.GetDeclaredInputs() {
		name := declaration.GetName()
		field := v1.InputsRoot + "." + name

		switch {
		case name == "":
			ds = append(ds, Diagnostic{
				Field:   fmt.Sprintf("%s[%d]", v1.InputsRoot, i),
				Message: "input has no name; an input is declared by the name a run supplies it under",
			})
		case slices.Contains(celUnusableStepIDs, name):
			// The four CEL lexer tokens. Rooting makes the other seventeen reserved
			// words legal here — `inputs.namespace` is a field selection — but these
			// four are literals and an operator, so `inputs.in` is a syntax error in
			// the grammar itself rather than an unknown field. The same four refused
			// as step ids, for the same reason.
			ds = append(ds, Diagnostic{
				Field: field, Value: name,
				Message: fmt.Sprintf(
					"name %q is punctuation in CEL rather than a name, so ${%s.%s} cannot be parsed at all; choose another name",
					name, v1.InputsRoot, name),
			})
		case !isCELIdentifier(name):
			ds = append(ds, Diagnostic{
				Field: field, Value: name,
				Message: fmt.Sprintf(
					"name %q is not a valid identifier, so ${%s.%s} cannot be parsed; use letters, digits, and underscores, starting with a letter or underscore",
					name, v1.InputsRoot, name),
			})
		}

		if first, duplicate := seen[name]; duplicate && name != "" {
			// A repeated field cannot say this, which is why the schema hands it here.
			// Left alone, the later declaration would win at submit and the earlier one
			// would be a line that reads as if it did something.
			ds = append(ds, Diagnostic{
				Field: field, Value: name,
				Message: fmt.Sprintf(
					"duplicate input %q, already declared as input %d; a run supplies each input once, so one declaration would silently replace the other",
					name, first+1),
			})
		} else if name != "" {
			seen[name] = i
		}

		ds = append(ds, validateInputDefault(declaration, field)...)
		ds = append(ds, validateInputConstraintShape(declaration, field)...)
		ds = append(ds, validateInputExample(declaration, field)...)
	}

	return ds
}

// validateInputConstraintShape reports what is wrong with a declaration's
// standard-rule constraints and its `must:` as *written* — a key that does
// not apply to the declared type, an unusable pattern, a `must:` that will
// not compile — before any value is checked against them.
//
// This is [v1.CheckInputConstraintShape], the identical check [BindRunInputs]
// runs at submit, run here where there is a position to report it against:
// "rules compile and type-check when configuration loads" means, for a file,
// when it is validated.
func validateInputConstraintShape(declaration *v1.InputDeclaration, field string) Diagnostics {
	err := v1.CheckInputConstraintShape(declaration)
	if err == nil {
		return nil
	}

	// A per-member or list-size violation of `values:` carries its own field
	// path — "values" for a whole-list rule, "values[i]" when member i is the
	// one at fault — which [enumValues] recorded a position for while parsing
	// the list, so this points there directly rather than falling back to
	// [inputConstraintShapeField]'s coarser guess.
	var shapeErr *v1.EnumValuesShapeError
	if errors.As(err, &shapeErr) {
		return Diagnostics{{Field: field + "." + shapeErr.Field, Message: err.Error()}}
	}

	return Diagnostics{{Field: inputConstraintShapeField(declaration, field), Message: err.Error()}}
}

// inputConstraintShapeField decides which part of a declaration a
// [v1.CheckInputConstraintShape] error is actually about, so a diagnostic
// lands on the line an author wrote the mistake on rather than always on the
// declaration as a whole.
//
// Only reached for an error that is not a [v1.EnumValuesShapeError] —
// [validateInputConstraintShape] handles that case itself, since the error
// already names its own field path. What is left is: [v1.CheckInputConstraintShape]
// walks a fixed order and reports the first defect it finds — the string
// constraints, then the list constraints, then `values:`, then `must:` — so
// this mirrors that same order rather than pattern-matching the message
// text: a declaration that also has a min_len/max_len or
// min_items/max_items mismatch is reported against the declaration as a
// whole, because that earlier check is the one that actually fired, and
// pointing at `values:` for it would send a reader to a line that is not
// what failed. `values:` beside a type that is not enum is the one case with
// a more specific home than the declaration: the line the author actually
// wrote `values:` on. Everything else — including `type: enum` with no
// `values:` at all, which has no line of its own to point at instead — is
// reported against the declaration, unchanged from before `values:` existed.
func inputConstraintShapeField(declaration *v1.InputDeclaration, field string) string {
	t := declaration.GetType()

	stringMismatch := (declaration.MinLen != nil || declaration.MaxLen != nil) && t != v1.InputDeclaration_TYPE_STRING
	lenInverted := declaration.MinLen != nil && declaration.MaxLen != nil && declaration.GetMinLen() > declaration.GetMaxLen()
	listMismatch := (declaration.MinItems != nil || declaration.MaxItems != nil) && t != v1.InputDeclaration_TYPE_LIST
	itemsInverted := declaration.MinItems != nil && declaration.MaxItems != nil && declaration.GetMinItems() > declaration.GetMaxItems()

	if stringMismatch || lenInverted || listMismatch || itemsInverted {
		return field
	}

	if len(declaration.GetValues()) > 0 && t != v1.InputDeclaration_TYPE_ENUM {
		return field + ".values"
	}

	return field
}

// validateInputExample reports what is wrong with a declaration's `example:`
// — a literal that does not match the declared type, or one that violates
// the declaration's own constraints. The same check [v1.CheckInputExample]
// runs, so an example that rots after a `must:` is tightened is caught here
// rather than discovered by a reader who trusted it.
func validateInputExample(declaration *v1.InputDeclaration, field string) Diagnostics {
	if declaration.GetExample() == nil {
		return nil
	}
	if err := v1.CheckInputExample(declaration); err != nil {
		return Diagnostics{{Field: field + ".example", Message: err.Error()}}
	}
	return nil
}

// validateInputDefault reports what is wrong with one declaration's default.
func validateInputDefault(declaration *v1.InputDeclaration, field string) Diagnostics {
	value := declaration.GetDefault()
	if value == nil {
		return nil
	}

	defaultField := field + ".default"

	switch value.GetKind().(type) {
	case *v1.Value_Expr:
		// An expression here would have to be evaluated somewhere, and the only place
		// with a scope to evaluate it in is the run — which would make a default a
		// different kind of thing from the value it stands in for, evaluated at a
		// different moment against names the caller's value never sees.
		return Diagnostics{{
			Field: defaultField,
			Message: "a default must be a value rather than an expression: it stands in for what a caller " +
				"would have sent, and a caller sends values; write the value out, or compute it in a step",
		}}
	case *v1.Value_SecretRef:
		return Diagnostics{{
			Field: defaultField,
			Message: "a default may not be a secret reference: it stands in for what a caller would have " +
				"sent, and a secret is resolved inside the task that needs it rather than carried as an argument",
		}}
	}

	var ds Diagnostics

	if declaration.GetRequired() {
		// Both are legal on their own and together they contradict: the default can
		// never be used, because a run that omits the input is refused before it
		// starts. One of the two is a mistake, and which one only the author knows —
		// so the diagnostic names both rather than resolving it by precedence.
		ds = append(ds, Diagnostic{
			Field: defaultField,
			Message: fmt.Sprintf(
				"input %q is `required: true` and also has a `default:`, which contradict: a required input "+
					"is never absent, so the default can never be used. Remove the default, or remove `required: true`",
				declaration.GetName()),
		})
	}

	// A default is part of the specification, so a mistyped one is a property of the
	// file — reported here, where there is a line to point at, rather than at submit
	// where it would name a field path in a protobuf message.
	if err := v1.CheckInputDefault(declaration); err != nil {
		ds = append(ds, Diagnostic{Field: defaultField, Message: err.Error()})
	}

	return ds
}

// validateDeclaredOutputs reports what is wrong with the `outputs:` block.
//
// The scope is the one a finished run has: every step's outputs, the workflow's
// vars, and the run's inputs. Nothing bare is in it — a loop's binding exists only
// inside its body, and `now` only inside a wait's own expressions — so a reference to one is
// reported like any other unknown name.
func validateDeclaredOutputs(wf *v1.Workflow, scope refScope, index int) Diagnostics {
	var ds Diagnostics

	seen := make(map[string]int, len(wf.GetDeclaredOutputs()))
	for i, declaration := range wf.GetDeclaredOutputs() {
		name := declaration.GetName()
		field := "outputs." + name

		switch {
		case name == "":
			ds = append(ds, Diagnostic{
				Field:   fmt.Sprintf("outputs[%d]", i),
				Message: "output has no name; an output is the name a caller reads the value back under",
			})
		case slices.Contains(celUnusableStepIDs, name):
			// Refused for the reason an input's name is, though nothing selects an
			// output through CEL today: the two halves of one contract should not
			// disagree about what a name is, and a later `${outputs.<name>}` must not
			// have to break a name that was legal when it was written.
			ds = append(ds, Diagnostic{
				Field: field, Value: name,
				Message: fmt.Sprintf(
					"name %q is punctuation in CEL rather than a name; an output is named the way an input is, so choose another name",
					name),
			})
		case !isCELIdentifier(name):
			ds = append(ds, Diagnostic{
				Field: field, Value: name,
				Message: fmt.Sprintf(
					"name %q is not a valid identifier; use letters, digits, and underscores, starting with a letter or underscore",
					name),
			})
		}

		if first, duplicate := seen[name]; duplicate && name != "" {
			ds = append(ds, Diagnostic{
				Field: field, Value: name,
				Message: fmt.Sprintf(
					"duplicate output %q, already declared as output %d; a run reports each output once, so one declaration would silently replace the other",
					name, first+1),
			})
		} else if name != "" {
			seen[name] = i
		}

		if declaration.GetValue() == nil {
			ds = append(ds, Diagnostic{
				Field:   field,
				Message: "has no value; an output is the expression that produces it",
			})

			continue
		}

		// Checked against the whole file's scope, because an output is evaluated after
		// every step: a reference to the last step is correct here and would be a
		// forward reference anywhere else.
		ds = append(ds, validateInputRefs("", field, declaration.GetValue(), scope, index, wf)...)

		// An output's own `must:` is checked against the value the run computes,
		// which does not exist yet — so what is checkable now is only that the
		// expression itself compiles and type-checks as a bool predicate, the
		// same load-time half [validateInputConstraintShape] runs for an input.
		if err := v1.CheckOutputConstraintShape(declaration); err != nil {
			ds = append(ds, Diagnostic{Field: outputConstraintShapeField(declaration, field, err), Message: err.Error()})
		}

		if d := checkOutputValueType(wf, declaration, field); d != nil {
			ds = append(ds, *d)
		}
	}

	return ds
}

// outputConstraintShapeField decides which part of a declaration a
// [v1.CheckOutputConstraintShape] error is about, the way
// [inputConstraintShapeField] does for its own half and for the same reason: a
// `values:` list beside a type that is not enum has a line of its own, and
// sending the reader to the declaration as a whole would point past it.
//
// Only that one case, because it is the only one with a more specific home.
// An enum with no `values:` at all has no line to point at, and a `must:` that
// will not compile is reported against the declaration for the reason the
// input side reports it there.
func outputConstraintShapeField(declaration *v1.OutputDeclaration, field string, err error) string {
	var shapeErr *v1.EnumValuesShapeError
	if errors.As(err, &shapeErr) {
		return field + "." + shapeErr.Field
	}
	if len(declaration.GetValues()) > 0 && declaration.GetType() != v1.InputDeclaration_TYPE_ENUM {
		return field + ".values"
	}

	return field
}

// checkOutputValueType reports a declared output type that contradicts what is
// statically knowable about the expression under `value:`, and nothing at all
// where nothing is knowable.
//
// The knowable set is deliberately small and each member of it is exact:
//
//   - A literal, or an all-constant mapping or list, whose type is the value
//     itself. Judged by [v1.CheckOutputValue] — the same function the run
//     reaches through [v1.EvalRunOutputs], so a file `flow validate` passed
//     cannot fail this check at completion instead.
//   - A bare `${inputs.<name>}` naming an input this workflow declares, whose
//     type is that declaration's. This is the shape most typed outputs have —
//     an argument handed back to a caller who no longer holds it — and it is
//     the one reference the file answers for on its own.
//   - A closed expression the profile's own checker can pin down without
//     knowing anything the file does not hold: `${1 + 2}`, `${"a" + "b"}`.
//     The identical machinery [checkCallArgumentType] uses on a `with:`
//     argument, reached the same way.
//
// Everything else — an expression over a step's outputs, a var, a loop's
// results — types as `dyn`, which is read as "not knowable" rather than as a
// mismatch. That is not a shortfall this slice could close by trying harder:
// `checkExpressionTypes` declares every referenced name `dyn` on purpose (see
// celcheck.go), and a checker guessing at a step's output type would report
// mismatches against workflows that are correct.
func checkOutputValueType(wf *v1.Workflow, declaration *v1.OutputDeclaration, field string) *Diagnostic {
	declared := declaration.GetType()
	if declared == v1.InputDeclaration_TYPE_UNSPECIFIED {
		return nil
	}

	value := declaration.GetValue()
	switch value.GetKind().(type) {
	case *v1.Value_Expr:
		known, inferred, ok := staticExpressionType(wf, value.GetExpr())
		if !ok {
			return nil
		}
		if known == declared {
			if keyType, decided := containerKeyMismatch(declared, inferred); decided {
				// The one mismatch matching kinds cannot see: a map keyed by
				// anything is a map, and a list holding one is still a list,
				// while both declared types promise the plain value a caller
				// reads (see [v1.CheckOutputValue] on the projection this keeps
				// honest). Reported here only where the checker decided the key
				// type — `${{}}` types as `map(dyn, dyn)` and a mixed-key
				// literal as `map(dyn, …)`, and which keys either holds is a
				// fact about the value, left to completion (#1404).
				//
				// The sentence says "is typed as" rather than "always produces"
				// because this arm is a type check and the two are not the same
				// claim. `${false ? {1: "a"} : {}}` types as `map(int, string)`
				// — cel-go joins the branches — and evaluates to `{}`, which
				// [v1.LiteralToGo] converts happily. So the type-level rule here
				// and the value-level rule at completion agree on every
				// non-empty value and part company on an empty map an int-typed
				// expression produced, which this refuses by its type. That is
				// the same judgement every other arm of this function makes, and
				// the author's fix is the same one: write a struct-typed
				// expression. Deciding it by walking the AST for non-empty map
				// literals would buy a contrived case with a second walk.
				return &Diagnostic{
					Field: field, Value: declaration.GetName(),
					Code: v1.DiagnosticCodeTypeMismatch,
					Message: fmt.Sprintf(
						"output %q is declared %s, but this expression is typed as %s with %s keys; %s",
						declaration.GetName(), v1.DeclaredTypeName(declared),
						containerHolds(declared), v1.DeclaredTypeName(keyType),
						containerKeyRule(declared)),
				}
			}

			return nil
		}
		if v1.StringShaped(known) && v1.StringShaped(declared) {
			// One of the two is an enum, and an enum value travels as a string
			// (see [v1.StringShaped]) — so the shapes agree and only membership
			// could still be wrong. That is a value-level question nothing here
			// can answer, and [v1.CheckOutputValue] answers it at completion,
			// against the value the run actually produced.
			return nil
		}

		return &Diagnostic{
			Field: field, Value: declaration.GetName(),
			Code: v1.DiagnosticCodeTypeMismatch,
			Message: fmt.Sprintf(
				"output %q is declared %s, but this expression always produces %s",
				declaration.GetName(), v1.DeclaredTypeName(declared), v1.DeclaredTypeName(known)),
		}

	default:
		// A literal or a structure, exact either way.
		if err := v1.CheckOutputValue(declaration, value); err != nil {
			return &Diagnostic{
				Field: field, Value: declaration.GetName(),
				Code: v1.DiagnosticCodeTypeMismatch, Message: err.Error(),
			}
		}

		return nil
	}
}

// staticExpressionType reports the declared type an output expression is known
// to produce, and false where it is not knowable.
//
// The CEL type it was derived from travels beside it, nil where there is none:
// a declared type is coarser than the type the checker inferred — every map is
// `struct` — so a caller asking a question the coarse name cannot answer, such
// as whether a struct's keys are strings, needs the type the answer came from.
//
// The input-reference arm comes first because the checker cannot reach it: an
// environment that declares every referenced name `dyn` types `inputs.release`
// as `dyn` however precisely the file declared `release`. Widening that
// environment is #177's road rather than this one's, so the one reference whose
// type the file already states is answered here directly — and it is the arm
// with no CEL type to carry, because the answer came from the declaration
// rather than from the checker.
func staticExpressionType(wf *v1.Workflow, parsed *expr.ParsedExpr) (v1.InputDeclaration_Type, *cel.Type, bool) {
	if parsed == nil {
		return v1.InputDeclaration_TYPE_UNSPECIFIED, nil, false
	}

	if t, ok := declaredInputRefType(wf, parsed.GetExpr()); ok {
		return t, nil, true
	}

	env, err := envDeclaring(referencedNames(parsed.GetExpr()))
	if err != nil {
		// A defect in this build rather than in the file; the same answer
		// [checkCallArgumentType] gives for the identical call.
		return v1.InputDeclaration_TYPE_UNSPECIFIED, nil, false
	}

	checked, issues := env.Check(cel.ParsedExprToAst(parsed))
	if issues != nil && issues.Err() != nil {
		// Does not type-check on its own terms, which [checkExpressionTypes]
		// already reports; a second sentence here would say the same thing in
		// a different voice.
		return v1.InputDeclaration_TYPE_UNSPECIFIED, nil, false
	}

	inferred := checked.OutputType()
	t, ok := declaredTypeOfCEL(inferred)

	return t, inferred, ok
}

// containerKeyMismatch reports the key type that makes inferred a value a
// declared container may not hold, and false for every other declared type.
//
// The declared-type guard lives here rather than at the call site so the two
// halves of one question — "is this a container?" and "are its map keys
// strings?" — read as one. `struct` and `list` both ask it, because the
// projection converts a whole output and gives up on all of it: a map with an
// int key defeats the array a `list` promised from inside an element exactly as
// it defeats a `struct` from the top. The scalar types do not ask, and an
// untyped output promises nothing about its projection at all.
func containerKeyMismatch(declared v1.InputDeclaration_Type, inferred *cel.Type) (v1.InputDeclaration_Type, bool) {
	if declared != v1.InputDeclaration_TYPE_STRUCT && declared != v1.InputDeclaration_TYPE_LIST {
		return v1.InputDeclaration_TYPE_UNSPECIFIED, false
	}

	return nonStringMapKeyType(inferred)
}

// containerHolds names what the refused expression produced, and
// containerKeyRule the promise it broke.
//
// Two clauses rather than two whole sentences, so the shared middle — the key
// type, named the same way at both checks — stays one string a reader can match
// across `flow validate` and a run's failure.
func containerHolds(declared v1.InputDeclaration_Type) string {
	if declared == v1.InputDeclaration_TYPE_LIST {
		return "a list holding a map"
	}

	return "a map"
}

func containerKeyRule(declared v1.InputDeclaration_Type) string {
	if declared == v1.InputDeclaration_TYPE_LIST {
		return "a list reads back as a plain array, whose maps have string keys"
	}

	return "a struct is a map with string keys"
}

// nonStringMapKeyType reports the key type of the first map inside t that a
// plain object cannot hold, and false when every map key t describes is a
// string or was not decided.
//
// Recursive through a map's value type and a list's element type, mirroring
// [v1.LiteralToGo]'s own recursion over the value — the conversion a declared
// `struct` or `list` promises will succeed — so the static half judges the same
// shape the completion half does rather than the outer container alone.
//
// Keys only. The completion check also refuses a container holding a kind with
// no plain value at all (a type, an enum, a packed message), and that is not one
// more arm of this switch: those have no key to name and the sentence about them
// is a different sentence. They stay a completion-time judgement, which costs an
// author nothing here — no Flowfile can write one.
//
// `dyn` is silence rather than a refusal, and that is the whole reason this
// returns a decision instead of a type: `${{}}` types as `map(dyn, dyn)` because
// there is no entry to infer a key from, and a mixed-key literal as
// `map(dyn, …)`. Which keys either actually holds is a fact about the value, and
// [v1.CheckOutputValue] decides it at completion against the map the run
// produced.
func nonStringMapKeyType(t *cel.Type) (v1.InputDeclaration_Type, bool) {
	if t == nil {
		return v1.InputDeclaration_TYPE_UNSPECIFIED, false
	}

	switch t.Kind() {
	case types.MapKind:
		parameters := t.Parameters()
		if len(parameters) != 2 {
			return v1.InputDeclaration_TYPE_UNSPECIFIED, false
		}
		if key := parameters[0]; key.Kind() != types.StringKind && key.Kind() != types.DynKind {
			return declaredTypeOfCEL(key)
		}

		return nonStringMapKeyType(parameters[1])

	case types.ListKind:
		parameters := t.Parameters()
		if len(parameters) != 1 {
			return v1.InputDeclaration_TYPE_UNSPECIFIED, false
		}

		return nonStringMapKeyType(parameters[0])

	default:
		return v1.InputDeclaration_TYPE_UNSPECIFIED, false
	}
}

// declaredInputRefType reports the type wf declares for a bare
// `${inputs.<name>}`, and false for anything else — including an input the
// workflow does not declare (reported as an unresolved reference, not as a
// type mismatch) and one whose own `type:` is missing.
//
// Bare specifically: a selection *through* the reference (`inputs.config.host`)
// reaches inside a value whose shape this schema does not describe, so its type
// is not knowable and saying so is the honest answer.
func declaredInputRefType(wf *v1.Workflow, e *expr.Expr) (v1.InputDeclaration_Type, bool) {
	sel, ok := e.GetExprKind().(*expr.Expr_SelectExpr)
	if !ok || sel.SelectExpr.GetTestOnly() {
		return v1.InputDeclaration_TYPE_UNSPECIFIED, false
	}
	ident, ok := sel.SelectExpr.GetOperand().GetExprKind().(*expr.Expr_IdentExpr)
	if !ok || ident.IdentExpr.GetName() != v1.InputsRoot {
		return v1.InputDeclaration_TYPE_UNSPECIFIED, false
	}

	for _, input := range wf.GetDeclaredInputs() {
		if input.GetName() != sel.SelectExpr.GetField() {
			continue
		}
		if input.GetType() == v1.InputDeclaration_TYPE_UNSPECIFIED {
			return v1.InputDeclaration_TYPE_UNSPECIFIED, false
		}

		return input.GetType(), true
	}

	return v1.InputDeclaration_TYPE_UNSPECIFIED, false
}
