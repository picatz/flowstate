package flowtest

import (
	"context"
	"errors"
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
// while the author is still there to be told, named by the case and the entry
// it is (`where` is `test "x" expect.check[2]` or `defaults.check[0]`) and
// positioned at the line that entry was written on. What parsing cannot see —
// an unknown function from a profile library, a name the run does not bind —
// stays a run-time failure of the case, because it depends on the workflow the
// case targets.
//
// own is how many of the claims the document wrote at `at` itself. A merged
// list is every level's claims accumulated — `defaults:` first, then a table
// entry's, then the case's own last — so only the final `own` entries are
// addressed by this path at all. The prose keeps counting from the front of the
// merged list, because that list is what the run evaluates and what a reader is
// being told about.
//
// inheritedFrom names the document the *earlier* entries came from, when the
// caller knows it: a `defaults:` block's inherited claims were prepended from
// the directory's file. It is named here rather than addressed by a path,
// because the prepend puts the two documents' claims in one index namespace —
// `defaults.check[0]` is the sibling's first claim and the suite's first claim
// at once, and a path keyed on that string sent a suite-written claim to the
// wrong file and lost its real position (Codex, #1185). A case's inherited
// claims came from a block or an entry this path does not reach, so the caller
// passes nothing and they are refused with the same words and no line.
//
// Mutates the slice in place: a whole-value fence is stripped here, once, so
// everything downstream evaluates bare CEL.
func checkCheckClaims(p *problems, r site, where string, claims []CheckClaim, own int, inheritedFrom string) {
	if len(claims) == 0 {
		return
	}

	env, err := v1.DefaultEvaluator().Env()
	if err != nil {
		// Nothing can be parsed without an environment, so this is the whole
		// report for this list rather than one entry's worth of it.
		p.report(r, "%s: building the expression environment: %s", where, err)

		return
	}

	inherited := len(claims) - own
	for i := range claims {
		// A claim [mergeDefaults] prepended was judged at the `defaults:` block
		// a moment ago, where it is addressable and where the file that wrote
		// it is known. Judging the copy again would report one mistake once per
		// case — five hundred diagnostics for one line, spending a bound meant
		// for five hundred *different* mistakes — while saying less about it.
		// Only the copies are marked, so nothing goes unjudged: the block's own
		// pass sees the originals, and a claim inherited from a table entry
		// carries no mark and is judged here.
		if claims[i].fromDefaults {
			continue
		}
		// Nothing rather than the list's own node for an inherited claim a
		// caller cannot address: a position on the case's `check:` block would
		// underline claims that are fine because one it inherited is not.
		spot := r.in(nil)
		switch {
		case i >= inherited:
			// The document holding this list wrote this one, at the index it
			// has once the inherited ones ahead of it are subtracted.
			spot = r.in(r.at.item(i - inherited))
		case inheritedFrom != "":
			spot = r.writtenIn(inheritedFrom)
		}
		if inner, fenced := flowfile.SplitFence(claims[i].That); fenced {
			claims[i].That = inner
		}
		if strings.TrimSpace(claims[i].That) == "" {
			p.report(spot, "%s.check[%d] holds an empty claim; write the CEL predicate, or drop the entry", where, i)

			continue
		}
		if _, issues := env.Parse(claims[i].That); issues != nil && issues.Err() != nil {
			p.report(spot, "%s.check[%d]: %s", where, i, issues.Err())
		}
	}
}

// postRunScope is the scope a finished case is questioned against: the
// run's outputs, the case's bound inputs, the workflow's profile, and
// `Local: true` — a rehearsal must never look attested. One construction,
// shared by `expect.check:` evaluation and the debugger's autopsy, so the
// two surfaces cannot answer the same question against two different
// scopes; it is the site the #737 guard blesses (see
// engine/scope_guard_test.go), and it is never an activity argument.
func postRunScope(spec *v1.Workflow, bound map[string]*v1.Value, outputs *v1.Workflow_StepOutputs) *v1.Scope {
	return &v1.Scope{Profile: spec.GetProfile(), Outputs: outputs, Inputs: bound, Local: true}
}

// postRunExtras is the other half of [postRunScope]: the bare bindings a
// finished case is questioned under. The `run` root is the engine's own,
// read through the unshadowed activation and extended with failed/error —
// derived, never restated, so no hand-kept key list exists to stop being
// complete — and the file's `vars` bind beside it.
//
// Nothing to shadow, still: [postRunScope] carries no workflow ambient vars,
// so a file var and a workflow var of the same name are two names in two
// scopes rather than a collision. #1072's per-case collision refusal is
// narrowed to exactly that — it gates the day workflow ambient vars join this
// scope and no earlier (repair 2), because refusing the pair today would
// refuse TestAStubsVarsAreTheWorkflowsNotTheFiles, a file where both meanings
// are deliberate: a stub's `where:` reads the workflow's `greeting` and a
// check reads the file's, in the same case, on purpose.
//
// One construction, shared by
// `expect.check:` evaluation and the debugger's autopsy (Codex, #1107) —
// the autopsy through [autopsyExtras], which redacts what these bindings
// would print, so the two surfaces share the names and shapes while only
// the printing one withholds values.
func postRunExtras(ctx context.Context, scope *v1.Scope, vars map[string]any, runErr error) map[string]ref.Val {
	errText := ""
	if runErr != nil {
		errText = runErr.Error()
	}
	root := map[string]any{"local": true}
	if out, err := v1.DefaultEvaluator().EvalString(ctx, "run", nil, scope.Activation(ctx)); err == nil {
		if lit, err := cel.RefValueToValue(out); err == nil {
			if native, err := literalToGo(lit); err == nil {
				if m, ok := native.(map[string]any); ok {
					root = m
				}
			}
		}
	}
	root["failed"] = runErr != nil
	root["error"] = errText

	extra := map[string]ref.Val{"run": v1.TypeAdapter.NativeToValue(root)}
	if len(vars) > 0 {
		extra["vars"] = v1.TypeAdapter.NativeToValue(vars)
	}

	return extra
}

// autopsyExtras is [postRunExtras] with the case's redaction posture applied,
// because the autopsy *prints* (Codex, #1109). What a check prints — its
// witnesses — already withholds the case's secrets and declared-sensitive
// values through the one shared set; an `inspect vars.token` that rendered
// the same value in the clear would be a second output channel around that
// set. The file's vars can genuinely hold a secret's plaintext, since
// resolveVars substitutes them into a case's `secrets:`, and a run's failure
// text can echo one. Evaluation stays raw in [assertChecks]: a check
// comparing a secret value must see the value; only what renders withholds
// it — the same split the transcript already lives by.
func autopsyExtras(ctx context.Context, scope *v1.Scope, vars fileVars, runErr error, sensitive sensitiveInputs) map[string]ref.Val {
	if runErr != nil {
		text := sensitive.RedactSubstrings(runErr.Error())
		if sensitive.WithholdAll() {
			text = "[withheld]"
		}
		runErr = errors.New(text)
	}

	return postRunExtras(ctx, scope, redactedVars(vars, sensitive), runErr)
}

// redactedVars is the vars map as the autopsy may show it: each value through
// [redactSensitiveTree], then the substring backstop over every string the
// structure holds — the identical pair every witness rendering applies — and
// everything withheld when the case's posture withholds everything.
//
// The backstop recurses rather than checking the top-level type, because the
// witness path gets recursion for free — it redacts the *rendered* text, one
// string however deep the value was — while this one hands back a structured
// value for CEL to walk: a map var holding "Bearer " + secret in a nested
// field would otherwise reach `inspect vars.request` intact (Codex, #1109).
//
// A var the file withholds is replaced whole rather than walked, which is the
// name half of [withheldVars]: its *material* is in the redaction set too and
// would clear the strings, but a value derived from a secret can carry the
// shape of one — a size, a count, a key that survives its value — and the
// question "may this var print" was already answered at load (#1072, repair 4).
func redactedVars(vars fileVars, sensitive sensitiveInputs) map[string]any {
	if len(vars.values) == 0 {
		return vars.values
	}

	out := make(map[string]any, len(vars.values))
	for name, value := range vars.values {
		if sensitive.WithholdAll() {
			// The word [redactedScalarText] uses for the same posture.
			out[name] = "[withheld]"
			continue
		}
		if vars.withheld.holds(name) {
			out[name] = sensitiveMarker
			continue
		}
		out[name] = redactSubstringsTree(sensitive.RedactTree(value), sensitive)
	}

	return out
}

// redactSubstringsTree applies [v1.SensitiveValues.RedactSubstrings] to every string a
// value holds — leaves and map keys alike, since a key is as capable of
// embedding a secret as a value is. Depth and breadth are the file's own,
// already bounded before a var exists ([checkExpansionBounds],
// [MaxTestFileBytes]): vars are load-time literals, never workload output.
func redactSubstringsTree(v any, sensitive sensitiveInputs) any {
	switch t := v.(type) {
	case string:
		return sensitive.RedactSubstrings(t)
	case map[string]any:
		out := make(map[string]any, len(t))
		for key, entry := range t {
			out[sensitive.RedactSubstrings(key)] = redactSubstringsTree(entry, sensitive)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, entry := range t {
			out[i] = redactSubstringsTree(entry, sensitive)
		}
		return out
	default:
		return v
	}
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
func assertChecks(ctx context.Context, claims []CheckClaim, spec *v1.Workflow, bound map[string]*v1.Value, vars fileVars, outputs *v1.Workflow_StepOutputs, runErr error, sensitive sensitiveInputs) []*v1.Diagnostic {
	if len(claims) == 0 {
		return nil
	}

	scope := postRunScope(spec, bound, outputs)

	// The `run` root, bound as a bare local. [v1.Scope.ActivationWith]'s
	// extras shadow the activation's own rooted namespaces, so this map
	// carries `local` as well as the two fields checks exist for — dropping
	// it would make `run.local` unreadable inside a check while every other
	// expression in the run reads it true.
	extra := postRunExtras(ctx, scope, vars.values, runErr)
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
				Message: fmt.Sprintf("check errored: %s\n           %s", claim.That,
					checkErrorText(ev, err, claim.That, vars.withheld, sensitive))})
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
		for _, witness := range checkWitnesses(ctx, ev, libs, activation, claim.That, vars.withheld, sensitive) {
			message += "\n           " + witness
		}
		failures = append(failures, &v1.Diagnostic{Field: field, Message: message})
	}

	return failures
}

// checkErrorText is what a claim's own evaluator failure may say (Codex,
// #1197).
//
// A check that *errors* rather than answering false is the third rendering in
// this file, and it was the one that went through neither redaction: it
// formatted cel-go's error straight into the diagnostic while the witnesses
// beside it went through [redactedScalarText] and the withheld-var rule. An
// error carries its operands — `no such key: <value>` — so a claim reading a
// withheld var printed that var's value in the clear, past every guard the
// witnesses apply.
//
// Two rules, and they are the two every other rendering here already uses:
//
//   - The set's own pair. A posture that withholds everything withholds this
//     too, and otherwise the substring backstop clears what it recognises —
//     the shape [formatUnmatchedStubEvalError] states one file over.
//   - The withheld-var rule, because the set is not enough on its own. An
//     evaluator error can report on a value *derived inside the claim*, which
//     no set ever saw: `[0][size(vars.header)]` fails with the length of a
//     withheld string, and there is no "13" to match. So a claim that reads a
//     withheld var says which one and stops, rather than quoting a failure
//     computed from it — the same answer [checkWitnesses] gives a path rooted
//     at one.
//
// Naming the var rather than withholding silently is the useful half: an author
// reading it knows which claim to rewrite, and a name is not a value.
func checkErrorText(ev *v1.Evaluator, err error, claim string, withheld withheldVars, sensitive sensitiveInputs) string {
	if sensitive.WithholdAll() {
		return "[withheld]"
	}
	if name, reads := claimReadsWithheld(ev, claim, withheld); reads {
		return fmt.Sprintf("[withheld: this claim reads vars.%s, which this file withholds]", name)
	}

	return sensitive.RedactSubstrings(err.Error())
}

// claimReadsWithheld reports whether a claim references a withheld var, and
// names the first one it finds in the claim's own reading order.
//
// Over the same AST walk [checkWitnesses] uses, so the two surfaces cannot
// come to disagree about what "reads a withheld var" means. A claim this
// package cannot parse reads nothing: it never evaluated either, so its error
// is about syntax rather than about a value.
func claimReadsWithheld(ev *v1.Evaluator, claim string, withheld withheldVars) (string, bool) {
	if len(withheld.names) == 0 {
		return "", false
	}
	env, err := ev.Env()
	if err != nil {
		return "", false
	}
	parsed, issues := env.Parse(claim)
	if issues != nil && issues.Err() != nil {
		return "", false
	}

	found := ""
	collectReferencePaths(parsed.NativeRep().Expr(), func(path string) {
		if found != "" {
			return
		}
		if name, covered := withheld.coveredName(path); covered {
			found = name
		}
	})

	return found, found != ""
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
// A path reading a var the file withholds renders as the marker without being
// evaluated at all (#1072, repair 4). The set above catches a withheld var's
// *strings* wherever they travel, and a value computed from a secret can carry
// what a string comparison cannot see — a size, a boolean answer about it —
// so the question "may this var print" is answered by the name, once, at load.
//
// A path that fails to re-evaluate is skipped rather than reported — a
// comprehension's iteration variable resolves nowhere outside its loop, and a
// step a failed run never reached has no value to witness. Bounded by
// [MaxCheckWitnesses]; only ever paid on a failing check.
func checkWitnesses(ctx context.Context, ev *v1.Evaluator, libs []string, activation cel.Activation, claim string, withheld withheldVars, sensitive sensitiveInputs) []string {
	if sensitive.WithholdAll() {
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
		if withheld.covers(path) {
			// Named rather than dropped: a claim that read a withheld var and
			// failed is one whose evidence exists and is being kept back, which
			// is a different thing for a reader than a value nothing could
			// produce.
			witnesses = append(witnesses, path+" = "+sensitiveMarker)

			continue
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
