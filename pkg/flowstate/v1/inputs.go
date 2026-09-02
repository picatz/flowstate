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
//
// The same argument reaches one step further, to the *value* of an output
// that already has one: a declaration whose `value:` is a literal (or an
// all-literal structure) contradicting its own `type:` or `values:` is wrong
// the moment it is submitted, and nothing about running the workflow can make
// it right. [CheckOutputValue] answers that statically and returns nil for an
// expression, so the run-time half stays exactly where it was — a computed
// output is still judged at completion, against the value it actually
// produced, because that is the first moment there is one.
func BindRunInputs(wf *Workflow, submitted map[string]*Value) (map[string]*Value, error) {
	return bindRunInputs(wf, wf.GetProfile(), submitted)
}

// bindRunInputs is BindRunInputs with the effective profile supplied by the
// execution context. A top-level workflow records its own profile; an old
// unprofiled callee inherits its caller's through [CalleeProfile].
func bindRunInputs(wf *Workflow, profile string, submitted map[string]*Value) (map[string]*Value, error) {
	if err := CheckDeclarationTypes(wf); err != nil {
		return nil, err
	}

	for _, declaration := range wf.GetDeclaredOutputs() {
		if err := CheckOutputConstraintShape(profile, declaration); err != nil {
			return nil, err
		}
		// Statically knowable only: nil for an expression, so this refuses the
		// half a caller could have been told about before anything ran and
		// leaves the other half to [EvalRunOutputs]. Same function, so the
		// sentence, the `sensitive:` withholding and the length bound are the
		// ones the completion-time refusal uses rather than a second rendering
		// that could drift from it.
		if err := CheckOutputValue(declaration, declaration.GetValue()); err != nil {
			return nil, err
		}
	}

	// Before a single input is bound, for the same reason the output-shape check
	// above runs here: this is the one function every submit path already calls,
	// and a secret reference in a `vars:` block is a specification that would put
	// a resolved secret in durable history the moment the first block is
	// evaluated. `flow validate` refuses it earlier and against a position; this
	// is what refuses it in a specification that never was a Flowfile. See
	// [CheckVarsHoldNoSecretRef].
	if err := CheckVarsHoldNoSecretRef(wf); err != nil {
		return nil, err
	}

	// Beside it, at the same boundary and for the same reason. A gate's
	// `prompt:` is rendered to whoever is being asked to approve - somebody who
	// was handed a run id rather than this file - so a prompt reaching a
	// `sensitive:` input or holding a secret reference is refused here, in the
	// specification that never was a Flowfile, exactly as the compiler refuses
	// it against a line and a column. See [CheckWaitPromptsAreAskable].
	if err := CheckWaitPromptsAreAskable(wf); err != nil {
		return nil, err
	}

	// And beside those two, for the third time the same reason applies. A
	// `verify:` key written as a literal rather than as a secret reference
	// satisfies the schema's map shape perfectly — protovalidate has nothing to
	// say about which `Value.kind` a signing key is — so before this line a
	// hand-built `RunRequest` carrying `verify: {hmac-sha256: "whsec_live_…"}`
	// was accepted and the signing key was then written into Temporal history
	// with the specification, which invariant 8 says is durable and broadly
	// readable. `flow validate` refused it against a line and a column and a
	// specification that never was a Flowfile went through untouched: the
	// Flowfile path and the RPC path were enforcing different rules about a
	// secret.
	//
	// The whole set-level checker rather than only the `verify:` half, because
	// the rest of what it refuses is wrong on a hand-built specification for the
	// identical reasons it is wrong in a file — a scheme nothing implements can
	// never accept a delivery, a missing `idempotency_key` turns every
	// redelivery into a second run, and two webhooks under one name are a
	// mapping nothing can address unambiguously.
	if err := CheckWebhookTriggers(wf.GetTriggers()); err != nil {
		return nil, err
	}

	// And the bridge's own rules, which are the same argument one boundary
	// further in. `signal:` is answerable from a public route by whoever holds
	// one signing key, and the rule that keeps that from reaching an unpoliced
	// gate — an explicit `signals:` entry that can admit the trigger — is a
	// property of the specification, not of the file it was written in. Left to
	// `flow validate` alone, a hand-built `RunRequest` would register a bridge
	// the compiler refuses.
	if err := CheckWebhookSignalBridges(wf); err != nil {
		return nil, err
	}

	// And the fourth, for the fourth time the same reason applies. A `manual:`
	// block that both refuses manual starts and narrows them satisfies the
	// schema perfectly — protovalidate has nothing to say about two booleans
	// that contradict — and it is a specification an author cannot write,
	// because the compiler refuses it against a line and a column. Refused here
	// so that a hand-built `RunRequest` cannot carry a contradiction into
	// durable history, where [CheckManualStart] would then have to decide which
	// half of it to believe. Fail closed at the boundary, once, rather than
	// twice with a precedence rule.
	if err := CheckManualTrigger(wf.GetTriggers().GetManual()); err != nil {
		return nil, err
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
		if err := CheckInputConstraintShape(profile, declaration); err != nil {
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
		if err := CheckInputConstraints(profile, name, declaration, value); err != nil {
			return nil, err
		}

		bound[name] = value
	}

	return bound, nil
}

// CheckDeclarationTypes reports whether every declaration in a workflow and its
// embedded callees can be enforced by this runtime.
//
// It is currently the temporary seam between the additive structural schema and
// the edition that teaches runtime checks to consume it.
// Programmatic submissions can carry a schema-valid structural-only declaration
// before the compiler writes one, so refuse it rather than interpreting the
// legacy zero value as unspecified and silently skipping enforcement.
//
// Every embedded callee is checked before the root workflow starts. Waiting to
// bind a callee's arguments inside CallScope would let earlier parent steps make
// requests before discovering that the callee's contract cannot be enforced.
func CheckDeclarationTypes(wf *Workflow) error {
	return walkEmbeddedWorkflows(wf, 0, func(current *Workflow) error {
		for _, declaration := range current.GetDeclaredInputs() {
			if declaration.GetValueType() != nil && declaration.GetType() == InputDeclaration_TYPE_UNSPECIFIED {
				return fmt.Errorf("input %q uses value_type without a legacy type; structural declaration types are not executable yet", declaration.GetName())
			}
		}
		for _, declaration := range current.GetDeclaredOutputs() {
			if declaration.GetValueType() != nil && declaration.GetType() == InputDeclaration_TYPE_UNSPECIFIED {
				return fmt.Errorf("output %q uses value_type without a legacy type; structural declaration types are not executable yet", declaration.GetName())
			}
		}
		return nil
	})
}

// CheckInputDefault reports whether a declaration's default is a value of the type
// the declaration says.
//
// Exported for `flow validate`, which asks it where a line number exists. The same
// check runs again at submit through [BindRunInputs], because a specification can
// be built by something that never was a Flowfile — an author gets the diagnostic,
// and a caller gets the refusal.
func CheckInputDefault(profile string, declaration *InputDeclaration) error {
	if declaration.GetDefault() == nil {
		return nil
	}

	if err := CheckInputValue(declaration.GetName(), declaration, declaration.GetDefault()); err != nil {
		return err
	}

	return CheckInputConstraints(profile, declaration.GetName(), declaration, declaration.GetDefault())
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
func CheckInputExample(profile string, declaration *InputDeclaration) error {
	if declaration.GetExample() == nil {
		return nil
	}

	if err := CheckInputValue(declaration.GetName(), declaration, declaration.GetExample()); err != nil {
		return fmt.Errorf("example: %w", err)
	}

	if err := CheckInputConstraints(profile, declaration.GetName(), declaration, declaration.GetExample()); err != nil {
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

	return checkDeclaredLiteralType("input", "was given", name, declaration.GetType(), value.GetLiteral())
}

// checkDeclaredLiteralType is the "does this literal have the declared type"
// rule, over a declared type rather than over a message that holds one.
//
// Split out of [CheckInputValue] because an output declares the same type in
// the same vocabulary (see [OutputDeclaration.type]) and the rule about a
// literal is the same rule. kind is the noun ("input", "output" — both
// vowel-initial, which is what lets one format string say "an %s") and verb is
// how the sentence says the value arrived — a caller *gave* an input, a run
// *computed* an output — since those are the two halves that differ and the
// judgement is what does not.
func checkDeclaredLiteralType(kind, verb, name string, declared InputDeclaration_Type, literal *expr.Value) error {
	got, ok := inputTypeOf(literal)
	if !ok {
		return fmt.Errorf("%s %q is %s, which is not a kind of value an %s can hold; "+
			"it is declared %s", kind, name, literalKindName(literal), kind, DeclaredTypeName(declared))
	}

	// TYPE_ENUM has no counterpart in [inputTypeOf]'s switch, deliberately:
	// the wire shape an enum value travels in is a string, the same shape
	// TYPE_STRING travels in, so the only rule this function checks is that
	// shape — which is why the two share this arm through [StringShaped]
	// rather than TYPE_STRING falling through to the comparison below.
	// *Which* string is checked against the declaration's own `values:` is a
	// set-fact about that declaration, and [CheckInputConstraints] and
	// [CheckOutputValue] are where set-facts are enforced.
	if StringShaped(declared) {
		if got != InputDeclaration_TYPE_STRING {
			return fmt.Errorf("%s %q is declared %s but %s %s",
				kind, name, DeclaredTypeName(declared), verb, DeclaredTypeName(got))
		}
		return nil
	}

	if got != declared {
		return fmt.Errorf("%s %q is declared %s but %s %s",
			kind, name, DeclaredTypeName(declared), verb, DeclaredTypeName(got))
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

// StringShaped reports whether a declared type's wire shape is a string:
// true for TYPE_STRING itself, and for TYPE_ENUM, whose values travel as
// strings and are judged by membership once resolved rather than by shape
// (see [CheckInputValue] and [constraintCELType]).
//
// Written as one predicate rather than repeated at each call site, because
// TYPE_ENUM's wire shape being string-shaped is a fact about the schema, and
// a fact restated three times is a fact one of the three restatements will
// eventually get wrong — which is exactly how TYPE_ENUM went unhandled in
// `flow breaking`, a `call:` argument's static type check, and
// `representativeValue`'s task-field validation, all at once: each compared
// a declared type to TYPE_STRING directly instead of asking whether the two
// were compatible. A caller that needs to know whether a statically known
// string type is compatible with a declared type should ask this rather than
// add a fourth `== TYPE_ENUM` beside the other three.
func StringShaped(t InputDeclaration_Type) bool {
	return t == InputDeclaration_TYPE_STRING || t == InputDeclaration_TYPE_ENUM
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
