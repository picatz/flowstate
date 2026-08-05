package flowfile

import (
	"fmt"
	"slices"

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
var declarationRoots = []string{v1.StepsRoot, v1.VarsRoot, v1.InputsRoot}

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
	if err := v1.CheckInputConstraintShape(declaration); err != nil {
		return Diagnostics{{Field: field, Message: err.Error()}}
	}
	return nil
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
// inside its body, and `now` only inside a `wait_until:` — so a reference to one is
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
			ds = append(ds, Diagnostic{Field: field, Message: err.Error()})
		}
	}

	return ds
}
