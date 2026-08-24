package flowtest

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `expect.check:` (#1072): CEL claims over the finished run, for everything
// the named expectation fields cannot say — a shape claim over an output, an
// error that must name its step, a relation between two steps' values.
//
// The evaluation is the engine's own: [v1.DefaultEvaluator], the workflow's
// profile libraries, and the run's scope through [v1.Scope.ActivationWith] —
// the same activation the debugger's `inspect` answers from, so a claim an
// author rehearsed at a breakpoint asserts identically in the file. A check
// is therefore bounded exactly as any expression in the run is
// ([v1.DefaultCostLimit]): a test file is untrusted input, and it is bounded
// by reusing the bound rather than by inventing a second one.
//
// The named fields stay the idiomatic spelling for structure claims — `ran:`
// and `skipped:` feed coverage and read the transcript's record — and
// `check:` is for value, shape, and error claims. Stated in docs/CLI.md.

// MaxCheckWitnesses bounds how many referenced values a failing check prints.
// The resource is lines in a report an author did not size: a check over a
// comprehension can reference arbitrarily many paths, and eight is enough to
// see why a claim failed without the failure becoming the document.
const MaxCheckWitnesses = 8

// CheckClaim is one entry of `expect.check:`: a CEL predicate over the
// finished run, with an optional author-written sentence for the failure.
//
// Two spellings, one meaning: a bare string is the claim alone, and the
// `{that:, because:}` form adds the sentence — the same pairing
// `coverage.allow_unreached` gives a decision and its reason.
type CheckClaim struct {
	// That is the claim, in CEL, over `steps.*`, `inputs.*`, and a `run`
	// root carrying `failed`, `error`, and `local`. Bare CEL is the house
	// spelling for a predicate position (`where:`, `must:`); a whole-value
	// `${...}` fence is tolerated and stripped at load, because an author
	// who writes the Flowfile's fence out of habit has said the same thing.
	That string

	// Because is the sentence a failure prints, written for whoever reads
	// the red line. Optional; the claim and its witnessed values print
	// either way.
	Because string

	// fromDefaults marks a claim [mergeDefaults] prepended, and exists for
	// the same reason [Stub.fromDefaults] does: the fold runs at [Load] and
	// again on the Go door ([File.withDefaultsApplied]), and every other
	// merged field is idempotent by its own shape — a prepend is not, so the
	// second fold skips a list that already carries a marked claim rather
	// than doubling the file's claims.
	fromDefaults bool
}

// UnmarshalYAML accepts both spellings. Key checking is done by hand rather
// than through a struct decode, so the strictness the loader promises
// ([yaml.Strict]) holds inside this entry too: a misspelled `becuase:` is
// refused with the keys named, never silently dropped.
func (c *CheckClaim) UnmarshalYAML(unmarshal func(any) error) error {
	var claim string
	if err := unmarshal(&claim); err == nil {
		c.That = claim
		return nil
	}

	var entry map[string]string
	if err := unmarshal(&entry); err != nil {
		return fmt.Errorf("a check is a CEL string, or a mapping with `that:` and optionally `because:`")
	}
	for key := range entry {
		if key != "that" && key != "because" {
			return fmt.Errorf("a check takes `that:` and `because:`, and %q is neither", key)
		}
	}
	c.That, c.Because = entry["that"], entry["because"]
	if c.That == "" {
		return fmt.Errorf("a check names its claim under `that:`")
	}
	return nil
}

// checkCheckClaims validates one list of claims at load: the fence rule, then
// a parse — syntax is a property of the file, so a malformed claim is refused
// while the author is still there to be told, with its position named
// (`where` is `test "x" expect.check[2]` or `defaults.check[0]`). What parsing
// cannot see — an unknown function from a profile library, a name the run
// does not bind — stays a run-time failure of the case, because it depends on
// the workflow the case targets.
//
// Mutates the slice in place: a whole-value fence is stripped here, once, so
// everything downstream evaluates bare CEL.
func checkCheckClaims(where string, claims []CheckClaim) error {
	if len(claims) == 0 {
		return nil
	}

	env, err := v1.DefaultEvaluator().Env()
	if err != nil {
		return fmt.Errorf("%s: building the expression environment: %w", where, err)
	}

	for i := range claims {
		if inner, fenced := flowfile.SplitFence(claims[i].That); fenced {
			claims[i].That = inner
		}
		if strings.TrimSpace(claims[i].That) == "" {
			return fmt.Errorf("%s.check[%d] holds an empty claim; write the CEL predicate, or drop the entry", where, i)
		}
		if _, issues := env.Parse(claims[i].That); issues != nil && issues.Err() != nil {
			return fmt.Errorf("%s.check[%d]: %w", where, i, issues.Err())
		}
	}

	return nil
}

// assertChecks evaluates a case's claims against the finished run, returning
// one diagnostic per claim that did not hold — with the values the claim read,
// so a red check arrives with its evidence rather than only its text.
//
// Fail closed, twice: a claim that errors is a failure (a claim the run
// cannot answer did not hold), and one that produces a non-boolean is refused
// the way `if:` refuses one, never coerced.
//
// Checks run whether or not the run failed — an error claim (`run.error`)
// exists precisely for failed runs — against whatever the partial transcript
// holds; a claim reaching a step the failure preceded errors, honestly.
func assertChecks(ctx context.Context, claims []CheckClaim, spec *v1.Workflow, bound map[string]*v1.Value, outputs *v1.Workflow_StepOutputs, runErr error, sensitive sensitiveInputs) []*v1.Diagnostic {
	if len(claims) == 0 {
		return nil
	}

	scope := &v1.Scope{Profile: spec.GetProfile(), Outputs: outputs, Inputs: bound, Local: true}

	// The `run` root, bound as a bare local. [v1.Scope.ActivationWith]'s
	// extras shadow the activation's own rooted namespaces, so this map
	// carries `local` as well as the two fields checks exist for — dropping
	// it would make `run.local` unreadable inside a check while every other
	// expression in the run reads it true.
	errText := ""
	if runErr != nil {
		errText = runErr.Error()
	}
	extra := map[string]ref.Val{"run": v1.TypeAdapter.NativeToValue(map[string]any{
		"failed": runErr != nil,
		"error":  errText,
		"local":  true,
	})}
	activation := scope.ActivationWith(ctx, extra)

	libs, err := v1.ProfileLibraries(spec.GetProfile())
	if err != nil {
		return []*v1.Diagnostic{{Field: "expect.check", Message: fmt.Sprintf("resolving the workflow's profile: %v", err)}}
	}
	ev := v1.DefaultEvaluator()

	var failures []*v1.Diagnostic
	for i, claim := range claims {
		field := fmt.Sprintf("expect.check[%d]", i)

		out, err := ev.EvalString(ctx, claim.That, libs, activation)
		if err != nil {
			failures = append(failures, &v1.Diagnostic{Field: field,
				Message: fmt.Sprintf("check errored: %s\n           %v", claim.That, err)})
			continue
		}
		held, ok := out.Value().(bool)
		if !ok {
			failures = append(failures, &v1.Diagnostic{Field: field,
				Message: fmt.Sprintf("check must evaluate to a boolean, got %s: %s", out.Type(), claim.That)})
			continue
		}
		if held {
			continue
		}

		message := "check failed: " + claim.That
		if claim.Because != "" {
			message += "\n           because: " + claim.Because
		}
		for _, witness := range checkWitnesses(ctx, ev, libs, activation, claim.That, sensitive) {
			message += "\n           " + witness
		}
		failures = append(failures, &v1.Diagnostic{Field: field, Message: message})
	}

	return failures
}

// checkWitnesses renders the values a failed claim read: each maximal
// reference path in the claim's own AST (`steps.join.value.regions[0]`,
// `vars.order.region`, `run.error`), re-evaluated once against the same
// activation and printed as `path = value` — the introspection that turns
// "this was false" into "this was false, and here is what it saw".
//
// Every rendered value passes through [redactedScalarText]: the one redaction
// spelling the transcript and the stub diagnostics already use, so a
// sensitive input or a case secret that a claim happens to reference never
// reaches the report by this new road (#1052's rule).
//
// A path that fails to re-evaluate is skipped rather than reported — a
// comprehension's iteration variable resolves nowhere outside its loop, and a
// step a failed run never reached has no value to witness. Bounded by
// [MaxCheckWitnesses]; only ever paid on a failing check.
func checkWitnesses(ctx context.Context, ev *v1.Evaluator, libs []string, activation cel.Activation, claim string, sensitive sensitiveInputs) []string {
	if sensitive.withholdAll {
		return []string{"(values withheld: the redaction set could not be built)"}
	}

	env, err := ev.Env()
	if err != nil {
		return nil
	}
	parsed, issues := env.Parse(claim)
	if issues != nil && issues.Err() != nil {
		return nil
	}

	seen := map[string]bool{}
	var paths []string
	collectReferencePaths(parsed.NativeRep().Expr(), func(path string) {
		if !seen[path] {
			seen[path] = true
			paths = append(paths, path)
		}
	})

	var witnesses []string
	for _, path := range paths {
		if len(witnesses) >= MaxCheckWitnesses {
			witnesses = append(witnesses, fmt.Sprintf("(and %d more)", len(paths)-MaxCheckWitnesses))
			break
		}
		out, err := ev.EvalString(ctx, path, libs, activation)
		if err != nil {
			continue
		}
		lit, err := cel.RefValueToValue(out)
		if err != nil {
			continue
		}
		native, err := literalToGo(lit)
		if err != nil {
			continue
		}
		witnesses = append(witnesses, path+" = "+redactedScalarText(native, sensitive))
	}

	return witnesses
}

// collectReferencePaths walks a parsed claim for its maximal reference
// chains: an identifier extended by field selection and constant indexing,
// stopped where anything else intervenes. Maximal, so `steps.a.b` yields one
// path rather than three prefixes; and a chain hanging off a call's *result*
// (`fn().field`) yields nothing, because re-evaluating it would run the call
// a second time.
func collectReferencePaths(e celast.Expr, add func(string)) {
	if path, ok := referencePath(e); ok {
		// A bare identifier alone (`x`) is almost always a binding the
		// activation roots anyway; a path earns a witness once it selects
		// or indexes into something.
		if strings.ContainsAny(path, ".[") {
			add(path)
		}
		return
	}

	switch e.Kind() {
	case celast.SelectKind:
		collectReferencePaths(e.AsSelect().Operand(), add)
	case celast.CallKind:
		call := e.AsCall()
		if call.IsMemberFunction() {
			collectReferencePaths(call.Target(), add)
		}
		for _, arg := range call.Args() {
			collectReferencePaths(arg, add)
		}
	case celast.ListKind:
		for _, element := range e.AsList().Elements() {
			collectReferencePaths(element, add)
		}
	case celast.MapKind:
		for _, entry := range e.AsMap().Entries() {
			pair := entry.AsMapEntry()
			collectReferencePaths(pair.Key(), add)
			collectReferencePaths(pair.Value(), add)
		}
	case celast.StructKind:
		for _, field := range e.AsStruct().Fields() {
			collectReferencePaths(field.AsStructField().Value(), add)
		}
	case celast.ComprehensionKind:
		comp := e.AsComprehension()
		collectReferencePaths(comp.IterRange(), add)
		collectReferencePaths(comp.AccuInit(), add)
		collectReferencePaths(comp.LoopCondition(), add)
		collectReferencePaths(comp.LoopStep(), add)
		collectReferencePaths(comp.Result(), add)
	}
}

// referencePath renders one expression as a re-evaluable CEL path, reporting
// false for anything that is not an ident/select/constant-index chain.
func referencePath(e celast.Expr) (string, bool) {
	switch e.Kind() {
	case celast.IdentKind:
		return e.AsIdent(), true
	case celast.SelectKind:
		sel := e.AsSelect()
		if sel.IsTestOnly() {
			// has(a.b): a presence test, not a value — witnessing it would
			// evaluate the very thing the author asked whether exists.
			return "", false
		}
		base, ok := referencePath(sel.Operand())
		if !ok {
			return "", false
		}
		return base + "." + sel.FieldName(), true
	case celast.CallKind:
		call := e.AsCall()
		if call.FunctionName() != "_[_]" || len(call.Args()) != 2 {
			return "", false
		}
		base, ok := referencePath(call.Args()[0])
		if !ok {
			return "", false
		}
		index := call.Args()[1]
		if index.Kind() != celast.LiteralKind {
			return "", false
		}
		switch value := index.AsLiteral().Value().(type) {
		case int64:
			return fmt.Sprintf("%s[%d]", base, value), true
		case uint64:
			return fmt.Sprintf("%s[%du]", base, value), true
		case string:
			return fmt.Sprintf("%s['%s']", base, strings.ReplaceAll(value, "'", `\'`)), true
		default:
			return "", false
		}
	default:
		return "", false
	}
}
