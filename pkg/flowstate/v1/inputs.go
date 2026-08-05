package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// What a caller may start a run with, checked in one place.
//
// A run's arguments arrive from whoever can call Run, which is the definition of an
// untrusted party, so every rule about them is a refusal: an undeclared name, a
// value of the wrong type, an expression where a value belongs, a required argument
// left out. Each denies, and each denies while the caller is still there to be told
// (invariant 6) rather than three steps into a run with two requests already sent.
//
// # One function, two callers
//
// The durable path checks here because the server calls [BindRunInputs] at submit;
// the local path checks here because `flow run local` reaches the same function
// before the first step. That is deliberate and it is the shape this repository
// keeps rediscovering the need for: the retry-attempt default was `1` in one driver
// and `5` in the other because nothing imported both. A caller that starts a run
// binds its inputs through this and gets whatever the other driver got.
//
// Applied *once*, at submit, and then carried: [RunState.inputs] holds the checked
// and defaulted map, so a later segment of a long run sees the values the first
// segment saw. Re-deriving them per segment would let a declaration edited between
// deploys change an argument underneath a run in flight.

// BindRunInputs validates a workflow's declarations — both its declared
// inputs' shapes and its declared outputs' `must:` shapes — and checks a
// run's submitted arguments against what the workflow declares, returning
// the values the run will actually see.
//
// The returned map holds one entry per declaration the run has a value for:
// whatever the caller supplied, or the declaration's default where they supplied
// nothing. A declaration that is optional, has no default and was not supplied is
// absent rather than null — "not given" and "given as null" are different things,
// and an expression asking for the first gets a missing key.
//
// Every failure names the input it is about and what to do instead, because these
// are read by a person running a command as often as by a program.
//
// The output-declaration check runs first, before a single input is bound and
// well before any step executes, because this is the one function every
// submit path already calls: the server binds through it at both `Run` and
// `CreateSchedule`, and `flow run local` reaches it before its first step —
// the same "one function, two callers" shape this file's package doc
// describes for inputs. Without this, a hand-built [Workflow] that never
// passed through `flow validate` — the parser calls
// [CheckOutputConstraintShape] itself, but only there — has its output
// `must:` compiled for the first time inside [EvalRunOutputs], after every
// step has already run and produced whatever side effects it has. Checking
// the shape here, before execution starts, is what turns a malformed output
// `must:` into a submission refused rather than a result discovered too late
// to matter.
func BindRunInputs(wf *Workflow, submitted map[string]*Value) (map[string]*Value, error) {
	for _, declaration := range wf.GetDeclaredOutputs() {
		if err := CheckOutputConstraintShape(declaration); err != nil {
			return nil, err
		}
	}

	declared := make(map[string]*InputDeclaration, len(wf.GetDeclaredInputs()))
	for _, declaration := range wf.GetDeclaredInputs() {
		declared[declaration.GetName()] = declaration
	}

	// Undeclared first, and sorted, so a caller who misspelled one name is told
	// about that name rather than about the first required input they now appear to
	// be missing.
	for _, name := range slices.Sorted(maps.Keys(submitted)) {
		if _, ok := declared[name]; ok {
			continue
		}

		return nil, fmt.Errorf("input %q is not declared by workflow %q%s",
			name, wf.GetName(), declaresWhat(wf))
	}

	bound := make(map[string]*Value, len(declared))
	for _, declaration := range wf.GetDeclaredInputs() {
		name := declaration.GetName()

		// Checked before anything below reads a value: "rules compile when
		// configuration loads, not when a request arrives" applies here even
		// though there is no separate load moment for a hand-built specification
		// — this is the earliest point every caller reaches, so it is where the
		// fail-closed rule is enforced for one. `flow validate` runs the
		// identical check earlier still, against a position, for a file that
		// went through the compiler.
		if err := CheckInputConstraintShape(declaration); err != nil {
			return nil, err
		}

		value, supplied := submitted[name]
		if !supplied {
			switch {
			case declaration.GetDefault() != nil:
				value = declaration.GetDefault()
			case declaration.GetRequired():
				return nil, fmt.Errorf("input %q is required and was not given%s",
					name, describedAs(declaration))
			default:
				continue
			}
		}

		// Checked even when it came from the declaration's own default, because a
		// default is part of the specification and a specification can be built by
		// hand: `flow validate` refuses a mistyped default in a Flowfile, and this is
		// what refuses one in a message that never was a Flowfile.
		if err := CheckInputValue(name, declaration, value); err != nil {
			return nil, err
		}

		// #204 found the element bound was gated on a declaration carrying
		// `must:`/`unique:`, so a list-typed (or struct-typed, carrying a
		// nested list) input with *no* constraint declared reached `if:`,
		// `for_each`, and every other CEL expression over it exactly as
		// unbounded. [CheckInputConstraints] now applies
		// [checkInputListElementBound] unconditionally, before its `must:`
		// check, to every literal it is handed — which closes the gap here
		// and, by the same call, at a literal `default:`/`example:` and a
		// call boundary's literal `with:` argument, since all of those reach
		// the identical function. See [checkInputListElementBound] and
		// [maxListElements] for the resource, the reused walker, and why the
		// limit is what it is.
		if err := CheckInputConstraints(name, declaration, value); err != nil {
			return nil, err
		}

		bound[name] = value
	}

	return bound, nil
}

// CheckInputDefault reports whether a declaration's default is a value of the type
// the declaration says.
//
// Exported for `flow validate`, which asks it where a line number exists. The same
// check runs again at submit through [BindRunInputs], because a specification can
// be built by something that never was a Flowfile — an author gets the diagnostic,
// and a caller gets the refusal.
func CheckInputDefault(declaration *InputDeclaration) error {
	if declaration.GetDefault() == nil {
		return nil
	}

	if err := CheckInputValue(declaration.GetName(), declaration, declaration.GetDefault()); err != nil {
		return err
	}

	return CheckInputConstraints(declaration.GetName(), declaration, declaration.GetDefault())
}

// CheckInputExample reports whether a declaration's example is a literal of
// the declared type that satisfies the declaration's own constraints.
//
// Exported for the same reason [CheckInputDefault] is, and checked the same
// way: an example is part of the specification too, so a stale one — a
// `must:` tightened after the example was written, a type changed underneath
// it — is a defect in the file rather than something a reader discovers by
// noticing it lied. Never bound to a run: [BindRunInputs] never reads this
// field, which is the whole difference between an example and a default.
func CheckInputExample(declaration *InputDeclaration) error {
	if declaration.GetExample() == nil {
		return nil
	}

	if err := CheckInputValue(declaration.GetName(), declaration, declaration.GetExample()); err != nil {
		return fmt.Errorf("example: %w", err)
	}

	if err := CheckInputConstraints(declaration.GetName(), declaration, declaration.GetExample()); err != nil {
		return fmt.Errorf("example: %w", err)
	}

	return nil
}

// CheckInputValue refuses a value that is not a literal of the declared type.
//
// Exported for the same reason [CheckInputDefault] is: a `with:` argument a
// call step binds is checked against the callee's declaration by this exact
// function, at compile time, in `flowfile/validate_call.go` — the same rule
// [BindRunInputs] enforces at submit, reached once rather than written twice.
// A literal argument that is the wrong type is refused here, before a run
// starts and before any of its own earlier steps have had an effect; an
// expression is left to [BindRunInputs] to refuse at the moment it is
// resolved to one, since its type is not known until then.
func CheckInputValue(name string, declaration *InputDeclaration, value *Value) error {
	switch kind := value.GetKind().(type) {
	case *Value_Literal:
		// Below.
	case *Value_Expr:
		// The security posture of the whole surface, not a convention. An expression
		// accepted from a caller is code the server would evaluate on its own behalf,
		// in a scope holding the run's own values — so it is refused here rather than
		// resolved anywhere.
		return fmt.Errorf(
			"input %q is an expression, and an input is a value: expressions come from the "+
				"file, which is reviewed and compiled, so compute this before submitting it "+
				"or write the expression into the workflow", name)
	case *Value_SecretRef:
		// Neighbouring reason: a caller naming a secret would be choosing which
		// credential the run resolves, and that is the specification's decision under
		// the deployment's policy.
		return fmt.Errorf(
			"input %q is a secret reference, which a caller may not choose: name the secret "+
				"in the workflow, where the deployment's policy decides whether it may be read",
			name)
	case nil:
		return fmt.Errorf("input %q has no value; give it one or leave it out", name)
	default:
		return fmt.Errorf("input %q cannot be used as a value: %v", name, kind)
	}

	got, ok := inputTypeOf(value.GetLiteral())
	if !ok {
		return fmt.Errorf("input %q is %s, which is not a kind of value an input can hold; "+
			"it is declared %s", name, literalKindName(value.GetLiteral()), DeclaredTypeName(declaration.GetType()))
	}
	if got != declaration.GetType() {
		return fmt.Errorf("input %q is declared %s but was given %s",
			name, DeclaredTypeName(declaration.GetType()), DeclaredTypeName(got))
	}

	return nil
}

// declaresWhat lists the inputs a workflow declares, for a caller who named one it
// does not.
//
// The declared names are offered because they are few and known in full — the same
// reasoning `flow validate` applies to an unknown var, and the shortest path from a
// refusal to a corrected command.
func declaresWhat(wf *Workflow) string {
	names := make([]string, 0, len(wf.GetDeclaredInputs()))
	for _, declaration := range wf.GetDeclaredInputs() {
		names = append(names, declaration.GetName())
	}

	if len(names) == 0 {
		return "; it declares no `inputs:` at all"
	}

	return "; it declares " + strings.Join(names, ", ")
}

// describedAs renders a declaration's description as a clause, when it has one.
//
// It is the only part of a declaration written for whoever supplies the value, so
// this is exactly the moment it is worth carrying.
func describedAs(declaration *InputDeclaration) string {
	if declaration.GetDescription() == "" {
		return ""
	}

	return " (" + declaration.GetDescription() + ")"
}

// inputTypeOf reports which declared type a literal has, and false for a literal
// that no declaration can describe.
//
// A switch rather than a cast, deliberately: [InputDeclaration_Type] and
// [Value.Type] share six names and six numbers and are different sets, so a
// conversion between them is something somebody wrote and somebody reviewed.
//
// Uint64 answers as an int for the same reason the parser stores small unsigned
// numbers as signed: YAML has one integer type, and whether a scalar arrives signed
// is an artifact of how its digits were written rather than a decision an author
// made.
func inputTypeOf(literal *expr.Value) (InputDeclaration_Type, bool) {
	switch literal.GetKind().(type) {
	case *expr.Value_StringValue:
		return InputDeclaration_TYPE_STRING, true
	case *expr.Value_Int64Value, *expr.Value_Uint64Value:
		return InputDeclaration_TYPE_INT, true
	case *expr.Value_DoubleValue:
		return InputDeclaration_TYPE_FLOAT, true
	case *expr.Value_BoolValue:
		return InputDeclaration_TYPE_BOOL, true
	case *expr.Value_MapValue:
		return InputDeclaration_TYPE_STRUCT, true
	case *expr.Value_ListValue:
		return InputDeclaration_TYPE_LIST, true
	default:
		return InputDeclaration_TYPE_UNSPECIFIED, false
	}
}

// DeclaredTypeName is how a declared type is written in a Flowfile, which is how a
// message about one should name it.
//
// Derived from the enum by stripping the prefix and lowering, so a type added to
// the schema is spelled here without anyone editing a list — and refusing to invent
// a name for the unspecified value, which is not a type an author can write.
func DeclaredTypeName(t InputDeclaration_Type) string {
	if t == InputDeclaration_TYPE_UNSPECIFIED {
		return "no type"
	}

	return strings.ToLower(strings.TrimPrefix(t.String(), "TYPE_"))
}

// DeclaredTypeNames returns every type an input may be declared as, in the order the
// schema declares them, for a diagnostic offering the alternatives.
func DeclaredTypeNames() []string {
	values := InputDeclaration_Type(0).Descriptor().Values()

	names := make([]string, 0, values.Len())
	for i := range values.Len() {
		t := InputDeclaration_Type(values.Get(i).Number())
		if t == InputDeclaration_TYPE_UNSPECIFIED {
			continue
		}
		names = append(names, DeclaredTypeName(t))
	}

	return names
}

// ParseDeclaredType reads a type as a Flowfile spells it.
func ParseDeclaredType(name string) (InputDeclaration_Type, bool) {
	for _, t := range DeclaredTypeNames() {
		if t == name {
			value, ok := InputDeclaration_Type_value["TYPE_"+strings.ToUpper(name)]
			return InputDeclaration_Type(value), ok
		}
	}

	return InputDeclaration_TYPE_UNSPECIFIED, false
}

// CheckSubmissionSize reports whether a specification and the arguments a run is
// starting with fit together.
//
// Separate from [CheckSpecSize] because a caller supplies the second half. The spec
// is what an author sized and the arguments are what a caller chose, and a caller
// able to push a run past the blob limit with arguments alone would have found the
// hang invariant 9 exists to convert into an answer — the specification would pass
// its own check, the run would start, and the first Continue-As-New would wedge it.
//
// Weighed against the same [MaxSpecBytes] the specification is, because the two
// travel together from here on: both are carried in `RunState` across every
// suspension, and the budget the number reserves is for what *executing* adds.
func CheckSubmissionSize(wf *Workflow, inputs map[string]*Value) error {
	// Measured through a message rather than by summing entries by hand, so that a
	// field added to the map's value type is counted on the day it is added.
	carried := &RunState{Workflow: wf, Inputs: inputs}

	size := proto.Size(carried)
	if size <= MaxSpecBytes {
		return nil
	}

	return fmt.Errorf(
		"the workflow and the inputs it is being run with are %d bytes together, over the %d byte "+
			"limit; the workflow alone is %d bytes. A run carries both across every suspension, "+
			"so a large value belongs somewhere a step can fetch it rather than in the arguments",
		size, MaxSpecBytes, proto.Size(wf))
}
