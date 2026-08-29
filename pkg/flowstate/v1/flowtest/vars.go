package flowtest

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// File-level `vars:` (#1072, slices 2 and 4): the values a suite states once
// and references everywhere — a URL, an id, a payload fragment — so the
// fixture is DRY without becoming a program.
//
// # Literals, and expressions over them
//
// A var value that is a whole-value `${...}` fence is an expression; every
// other value is the literal it has always been. That is the workflow's own
// `vars:` rule (docs/DSL.md), and the fence is required there for the reason
// it is required here: a var legitimately holds the literal string
// `"steps.greet.result"`, so the syntax rather than the content has to say
// which one an author meant.
//
// A computed var reads its siblings and nothing else — `rush_order:
// ${vars.base_order}` composed from a base stated once — which is the branch
// the DSL doc left open on purpose ("a dependency sort with a cycle
// diagnostic… allowing it later is additive"). Evaluation is at load, once,
// in dependency order: see [File.evaluateVars] for the ordering, the cycle
// diagnostic, the bounds and the redaction rule that come with it.
//
// # One spelling, two mechanisms, and the asymmetry that keeps it honest
//
// A *fixture* position — a case's `inputs:`, a trigger's fields, a scripted
// sender, `expect.outputs:` — references a var as a whole-value `${vars.x}`
// fence, and the reference is resolved AT LOAD, by substitution: what reaches
// the run is the literal, so the #416 fixture rule ("a default holds no
// expression") is not weakened, it is satisfied by the time it is checked. A
// *claim* position — `expect.check:` — reads `vars.x` at evaluation, bound as
// the check activation's `vars` root.
//
// A *stub* position (`where:`, `returns:`) is deliberately NEITHER: a stub's
// expressions evaluate against the run's own scope, where `vars.` has always
// meant the workflow's `vars:` block, and a load-time substitution there
// would silently hijack that meaning. A stub speaks the run's language;
// everywhere else in the test file, `vars.` is the file's. The
// disambiguation is pinned by TestAStubsVarsAreTheWorkflowsNotTheFiles.
//
// # The containment contract, whole
//
// Five review rounds on #1197 found seven ways a value derived from a secret
// reaches a report, and each was answered on its own. The answers are one rule,
// and it is written here rather than left spread across the functions that
// implement it, so that the next review has something to check against instead
// of rediscovering the shape. This table is the contract:
//
//	A value derived from secret material is WITHHELD where it is a non-empty
//	string, and REFUSED INTO EXISTENCE where it is anything else — because
//	anything else carries the secret in a form redaction cannot reach: its
//	digits, its truth, its shape, its very emptiness.
//
// Every row is a decision an owner made on a filed finding rather than an
// implementation detail:
//
//	what                  | answer   | why, and where
//	----------------------|----------|-------------------------------------------
//	a tainted NON-EMPTY   | withheld | the set matches content: whole in a witness
//	  string              |          | and the autopsy, cleared from any line that
//	                      |          | embeds it ([withheldMaterial], run.go)
//	a tainted non-string  | refused  | nothing in a number or a boolean can be
//	  scalar              |          | matched once a fixture carries it into a
//	                      |          | run, and a length is a fact about a secret
//	                      |          | ([refuseUnprotectableVar])
//	a tainted container   | refused  | its SHAPE survives leaf redaction entirely:
//	                      |          | `${t == 'guess' ? {} : {'x':'y'}}` answers a
//	                      |          | question about the secret either way
//	a tainted EMPTY       | refused  | emptiness is shape, and the set cannot hold
//	  string              |          | "" — it occurs at every position of every
//	                      |          | string — so `${t == 'guess' ? '' : 'x'}`
//	                      |          | renders `""` beside `[redacted]`
//	which vars are        | both     | the closure walks readers AND dependencies
//	  tainted             | ways, to | to a fixed point — the source material of a
//	                      | a fixed  | secret is secret, and a var reached backward
//	                      | point    | is itself a source for its own readers
//	                      |          | ([taintedVars])
//	what a reference IS   | one      | CEL binds `vars.token` and `vars['token']`
//	                      | recog-   | to one value, so ONE function answers "which
//	                      | nizer,   | var does this reference" and every site routes
//	                      | both     | through it ([readsVar] on an AST node,
//	                      | spell-   | [varNameInPath] on a rendered path). The list
//	                      | ings     | of sites is pinned by
//	                      |          | TestEverySiteRecognisesBothSpellings, which
//	                      |          | drives each in both and fails when they differ
//	what graph it walks   | what the | a declaration refused for an unrelated
//	                      | file     | mistake still SAYS what it reads, and a
//	                      | SAYS     | refusal removes a value from existence, never
//	                      |          | an edge from the graph. [File.declareVars]
//	                      |          | records deps before any check can skip the
//	                      |          | entry, and reads them out of the text when
//	                      |          | there is no AST ([textualVarDeps])
//	when a refusal fires  | eagerly, | a value judged after the block evaluates has
//	                      | per var  | already been quotable by a later expression's
//	                      |          | error ([File.evaluateVars]'s loop)
//	what a refused var's  | nothing  | the root refusal stands for the chain, the
//	  dependents report   |          | rule #1185 set for a refused stub's shape
//	how a rendering hides | one pair | WithholdAll withholds, else RedactSubstrings,
//	  what it must        | only     | plus the withheld-var rule. SIX surfaces:
//	                      |          | witnesses, the autopsy, stub diagnostics, a
//	                      |          | check's evaluator error ([checkErrorText]),
//	                      |          | a var's own ([scrubbedVarError]), and the
//	                      |          | case's ([redactedErrorText], run.go's
//	                      |          | caseError). A seventh is a leak until it
//	                      |          | meets this row — the sixth was, exactly as
//	                      |          | this row predicted when it named five
//	when the posture       | before   | it is LOAD-time information: the taint
//	  exists               | setup    | closure and the case's `secrets:` are both
//	                      | can fail | known before anything runs, so a setup
//	                      |          | failure quoting a withheld fixture
//	                      |          | (`loading workflow %q`) has no excuse. It
//	                      |          | widens to the run's own set once bound
//	                      |          | inputs are known; every exit renders through
//	                      |          | whatever is established when it is taken
//	what a rendering does | withhold | an error can quote something computed INSIDE
//	  with a derived fact |  whole   | the failing expression — `[0][size(vars.t)]`
//	                      |          | says `index out of bounds: 11` — which no set
//	                      |          | ever held, so clearing known strings cannot
//	                      |          | reach it and the whole message is withheld
//
// Two costs, accepted rather than discovered: a benign var that merely
// contributed to a secret is withheld, and refused if it is not a non-empty
// string; and the refusal is per file rather than per case, since the block is
// shared.
//
// Two residuals, named rather than claimed away:
//
//   - A fence inside a *structured* var value does not evaluate ([checkVars]
//     refuses it), so the respelling the container refusal points at — the
//     structure at the position that uses it, with `${vars.x}` at its leaves —
//     is the only spelling that composes. An expressiveness gap, tracked on
//     #1072 rather than widened in a review.
//   - `vars[<expression>]` is refused in the block, because a dependency
//     decided at evaluation cannot order it or carry a taint. In a *claim* it
//     is legal and fails closed instead: the error is withheld without naming a
//     var, since naming one would be a guess. The cost is a claim that reads
//     nothing withheld losing its evaluator detail for using that spelling.
//   - The empty-string refusal's own message reveals which branch produced ""
//     for a file whose author wrote the conditional. It is deterministic per
//     file, second-order, and strictly less than what every witness of that var
//     would otherwise have printed — but it is not nothing, and a contract that
//     claimed completeness here would be claiming more than it has.

// MaxVarsPerFile bounds how many vars one file may declare. A test file is
// untrusted input (CLAUDE.md); each var is substituted into every position
// that references it, so the resource the author controls is the walk this
// package does per reference, and 200 is far past what a fixture needs.
const MaxVarsPerFile = 200

// maxVarCost is the CEL cost budget one computed var's expression may spend.
//
// Deliberately far below [v1.DefaultCostLimit], and the reason is
// multiplication rather than caution — the same reason `maxFilterCost` is
// (v1/runfilter.go). A file may declare [MaxVarsPerFile] vars and every one of
// them may be an expression, so the work a document can ask a loader for is
// the product of the two, and the document's author chooses both factors.
// Derived from the two bounds rather than typed out, so that the sentence
// stays true if either moves: the whole `vars:` block of a maximally-declared
// file spends [v1.DefaultCostLimit] — one ordinary evaluation's budget, for
// the entire file.
//
// It is also generous for what a var legitimately is. The estimator prices a
// traversal at cel-go's StringTraversalCostFactor of 0.1 (v1/celcost.go), so
// this buys roughly fifty thousand characters produced or accumulated per var;
// composing an id, a URL or a payload fragment costs single-digit units, and
// anything approaching this bound is a program hiding in a fixture.
const maxVarCost uint64 = v1.DefaultCostLimit / MaxVarsPerFile

// maxVarCycles bounds how many cycles one block's sort reports.
//
// The count is the document's, and it multiplies against the length of what
// each one renders: a block of [MaxVarsPerFile] vars each reading every other
// has one back edge per edge — forty thousand — and every one would be
// formatted into a path up to [MaxVarsPerFile] hops long before
// [MaxLoadProblems] dropped all but twenty of them. So the *search* stops
// rather than the report, which is the difference between a refusal that costs
// what the document earned and one that costs what it asked for; the worst
// legal document measured 761ms before this bound and 398ms after, the
// remainder being the parse of half a megabyte of CEL. Nothing is lost by
// stopping: the file is refused by the first cycle, and an author acts on a
// path rather than on a census.
//
// Equal to [MaxLoadProblems], because that is how many could ever have been
// shown.
const maxVarCycles = MaxLoadProblems

// maxWithheldVarStrings bounds how many strings one withheld var contributes
// to a case's redaction set.
//
// The set is scanned against every rendered line, so its size is a per-line
// cost that the file's author would otherwise choose: a withheld var holding
// thousands of strings prices every transcript line by the size of a fixture.
// Over the bound the file is refused rather than partly protected, which is
// this repository's direction at every redaction seam.
const maxWithheldVarStrings = 64

// varReference matches a whole-value reference: `${vars.<name>}` and nothing
// around it. The name grammar is CEL's identifier grammar, because a var must
// also be reachable as `vars.<name>` inside a check.
var varReference = regexp.MustCompile(`^\$\{\s*vars\.([A-Za-z_][A-Za-z0-9_]*)\s*\}$`)

// varName is the same grammar, for declaration-side validation.
var varName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// varTextualRead matches a read of a file var anywhere in an expression's
// text, in either spelling the grammar binds — see [readsVar] for why both
// have to be here and not just the one an author usually writes.
var varTextualRead = regexp.MustCompile(
	`\bvars(?:\.([A-Za-z_][A-Za-z0-9_]*)|\[\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\])`)

// A varRead is what [readsVar] answers with: which file var an expression node
// reads, or that it reads one whose name cannot be known at load.
type varRead struct {
	// name is the var read, empty when dynamic.
	name string

	// dynamic marks `vars[<expression>]` — a read of the block whose subject
	// is decided at evaluation. Every caller fails closed on it.
	dynamic bool

	// bracket marks the `vars['x']` spelling, which the `vars:` block refuses
	// and a check accepts. Carried because the *recognizer* answers what is
	// read and each site decides what to do about it.
	bracket bool
}

// readsVar is THE recognizer: given one expression node, which file var does it
// reference?
//
// It exists because the answer was being computed in four places and two of
// them knew only the dotted spelling (Codex, #1197, ninth and tenth). CEL binds
// `vars.token` and `vars['token']` to the same value, so a walk that recognises
// only the first records no dependency edge for the second — which defeated the
// round-eight rule that a refusal must not remove an edge — and a check reading
// `vars['header']` rendered its evaluator error unredacted. That is CLAUDE.md's
// rewriter lesson verbatim: the recognizer has to know what the *grammar*
// binds, taken from where the evaluator binds it rather than from the spelling
// an author usually reaches for.
//
// So there is one function, both spellings, and every site that asks the
// question routes through it — the list is pinned by
// TestEverySiteRecognisesBothSpellings, which drives each one in both and fails
// when their answers differ.
//
// bound is the comprehension's own bindings: a macro that binds `vars` shadows
// the root, and inside it no reference is a file var at all.
func readsVar(e celast.Expr, bound map[string]bool) (varRead, bool) {
	if bound[v1.VarsRoot] {
		return varRead{}, false
	}
	switch e.Kind() {
	case celast.SelectKind:
		// `vars.x`, and `has(vars.x)` — the same selection marked test-only.
		if sel := e.AsSelect(); rootsAtVars(sel.Operand()) {
			return varRead{name: sel.FieldName()}, true
		}
	case celast.CallKind:
		call := e.AsCall()
		if call.FunctionName() != operatorIndex || len(call.Args()) != 2 || !rootsAtVars(call.Args()[0]) {
			return varRead{}, false
		}
		if index := call.Args()[1]; index.Kind() == celast.LiteralKind {
			if name, isText := index.AsLiteral().Value().(string); isText {
				return varRead{name: name, bracket: true}, true
			}
		}

		return varRead{dynamic: true, bracket: true}, true
	}

	return varRead{}, false
}

// operatorIndex is CEL's index operator, which is how `vars['x']` parses.
const operatorIndex = "_[_]"

// walkVarReads visits every file-var read in an expression, through [readsVar]
// and nothing else.
//
// The traversal a caller needs when it wants the reads and not the rest —
// [checkVarExpression] has its own walk because it also judges roots and
// functions along the way, and this is that walk's reading half for everyone
// else. Comprehension bindings are tracked here too, for the reason they are
// tracked there: a macro may bind `vars`, and inside it nothing is a file var.
func walkVarReads(e celast.Expr, bound map[string]bool, visit func(varRead)) {
	if read, reads := readsVar(e, bound); reads {
		visit(read)

		return
	}
	switch e.Kind() {
	case celast.SelectKind:
		walkVarReads(e.AsSelect().Operand(), bound, visit)
	case celast.CallKind:
		call := e.AsCall()
		if call.IsMemberFunction() {
			walkVarReads(call.Target(), bound, visit)
		}
		for _, arg := range call.Args() {
			walkVarReads(arg, bound, visit)
		}
	case celast.ListKind:
		for _, element := range e.AsList().Elements() {
			walkVarReads(element, bound, visit)
		}
	case celast.MapKind:
		for _, entry := range e.AsMap().Entries() {
			pair := entry.AsMapEntry()
			walkVarReads(pair.Key(), bound, visit)
			walkVarReads(pair.Value(), bound, visit)
		}
	case celast.StructKind:
		for _, field := range e.AsStruct().Fields() {
			walkVarReads(field.AsStructField().Value(), bound, visit)
		}
	case celast.ComprehensionKind:
		comp := e.AsComprehension()
		walkVarReads(comp.IterRange(), bound, visit)
		walkVarReads(comp.AccuInit(), bound, visit)

		inner := maps.Clone(bound)
		for _, bind := range []string{comp.IterVar(), comp.IterVar2(), comp.AccuVar()} {
			if bind != "" {
				inner[bind] = true
			}
		}
		walkVarReads(comp.LoopCondition(), inner, visit)
		walkVarReads(comp.LoopStep(), inner, visit)
		walkVarReads(comp.Result(), inner, visit)
	}
}

// rootsAtVars reports whether one node is the bare `vars` root.
func rootsAtVars(e celast.Expr) bool {
	return e.Kind() == celast.IdentKind && e.AsIdent() == v1.VarsRoot
}

// varNameInPath is [readsVar]'s answer for a *rendered* path rather than an
// AST node — the form [referencePath] produces for a witness, which keeps
// whichever spelling the author wrote.
//
// The same recognizer's job at the other end of the pipe, and it has to agree
// with it: `vars.token`, `vars['token']` and anything selected or indexed from
// either all name `token`.
func varNameInPath(path string) (string, bool) {
	rest, rooted := strings.CutPrefix(path, v1.VarsRoot)
	if !rooted {
		return "", false
	}
	name := ""
	switch {
	case strings.HasPrefix(rest, "."):
		name = rest[1:]
		if cut := strings.IndexAny(name, ".["); cut >= 0 {
			name = name[:cut]
		}
	case strings.HasPrefix(rest, "['"):
		inner := strings.TrimPrefix(rest, "['")
		end := strings.Index(inner, "']")
		if end < 0 {
			return "", false
		}
		name = inner[:end]
	default:
		return "", false
	}
	// A name that is not a CEL identifier is not a var of this file's: the
	// block refuses one at declaration ([checkVars]), so a path carrying one
	// roots at something else. Answering with the text *and* false would let a
	// caller that reads only the string act on a name nothing declares.
	if !varName.MatchString(name) {
		return "", false
	}

	return name, true
}

// textualVarDeps reads one expression's sibling reads out of its *text*, for
// the one case where there is no AST to read them from: the expression did not
// parse.
//
// A deliberate over-approximation, and the direction is the whole justification
// (Codex, #1197, eighth). This package refuses textual analysis where a wrong
// answer would change what a working file *means* — that is why the taint
// follows references rather than value renderings, and why [flow fix] parses
// rather than searches. Here neither risk applies: the file carrying this
// expression is already refused, so nothing will run whatever this returns, and
// an edge that is not real can only *widen* the taint. A missing edge costs a
// secret; a spurious one costs a diagnostic on a document nobody can execute.
//
// It cannot see through string literals — `${'vars.token'}` yields a dep that
// is only text — which is the same trade, taken the same way.
func textualVarDeps(text string) []string {
	found := map[string]bool{}
	for _, match := range varTextualRead.FindAllStringSubmatch(text, -1) {
		// Group 1 is the dotted spelling, group 2 the bracket one; exactly one
		// of them matched. Both are here for [readsVar]'s reason — the grammar
		// binds the same value either way, and a recognizer that knows one
		// spelling drops the edges written in the other.
		for _, name := range match[1:] {
			if name != "" {
				found[name] = true
			}
		}
	}

	return slices.Sorted(maps.Keys(found))
}

// varEvaluator is the bounded evaluator every computed var goes through: the
// engine's own machinery — the byte-aware cost estimator, the cancellation
// checks, the cached environments — under [maxVarCost] rather than the default
// budget.
//
// One for the process, as [v1.DefaultEvaluator] is: an [v1.Evaluator]'s value
// is the environment cache it holds, and a fresh one per load would rebuild
// the base environment for every file this package reads.
var varEvaluator = sync.OnceValue(func() *v1.Evaluator {
	return v1.NewEvaluator(v1.WithLimits(v1.Limits{
		Cost:                    maxVarCost,
		InterruptCheckFrequency: v1.DefaultInterruptCheckFrequency,
	}))
})

// varProfileFunctions are the functions a workflow profile provides and the
// environment a var evaluates in does not — the set that turns a profile-gated
// call in a var into a load-time diagnostic naming the function, rather than
// cel-go's runtime `no such overload`, which for a member call names nothing
// at all.
//
// Derived from the two environments' own declarations rather than written
// down, which is how [flowfile] answers the same question one package over
// (`functionNamespaces` in validate.go, `qualifiedFunctions` in celcheck.go):
// a set built from library names is wrong in both directions, since `encoders`
// declares `base64.encode` and `protos` declares `proto.getExt`. An
// environment this build cannot construct answers with an empty set, which
// costs one diagnostic its extra sentence where panicking would cost every
// command — the trade those two already make.
var varProfileFunctions = sync.OnceValue(func() map[string]bool {
	gated := map[string]bool{}

	base, err := varEvaluator().Env()
	if err != nil {
		return gated
	}
	profile, err := varEvaluator().ProfileEnv(v1.CurrentProfile)
	if err != nil {
		return gated
	}
	for name := range profile.Functions() {
		if !base.HasFunction(name) {
			gated[name] = true
		}
	}

	return gated
})

// checkVars validates the block itself: bounded, CEL-addressable names, and —
// for every value that is not a whole-value fence — literals all the way down.
//
// Every name is judged rather than the first bad one, and they are judged in
// sorted order, because a map's iteration order is not something a report may
// depend on — the rule [checkScriptedIdentity] already states for claims.
//
// A value that *is* a whole-value fence is an expression, and is judged by
// [File.evaluateVars] instead. A fence nested inside a structure is still
// refused here, and says so in its own words: an expression is the whole value
// or it is nothing, which is the rule every reference position in this file
// already follows.
//
// Reports false when the count bound stopped it, which the loader takes as a
// refusal of the whole document: the walk below is per var, and a legal file
// can declare tens of thousands of them.
func checkVars(p *problems, vars map[string]any) bool {
	block := at(v1.VarsRoot)
	if len(vars) > MaxVarsPerFile {
		p.report(site{at: block}, "this file declares %d vars, more than the limit of %d", len(vars), MaxVarsPerFile)

		return false
	}
	for _, name := range slices.Sorted(maps.Keys(vars)) {
		if !varName.MatchString(name) {
			p.reportKey(site{at: block.field(name)},
				"vars.%s: a var's name must be a CEL identifier (letters, digits, underscores, "+
					"not starting with a digit), or `vars.%s` could never be read back", name, name)
		}
		if _, fenced := fencedVarValue(vars[name]); fenced {
			continue
		}
		checkNoExpressions(p, site{at: block.field(name)}, v1.VarsRoot+"."+name,
			varsFenceWholeValues, vars[name], 0)
	}

	return true
}

// fencedVarValue reports whether one declared value is an expression, and
// answers with the CEL inside the fence.
//
// A function rather than the two lines inlined, because the answer decides two
// separate things a page apart — whether [checkVars] refuses the value and
// whether [File.evaluateVars] computes it — and a classification made twice is
// one that eventually disagrees with itself.
func fencedVarValue(value any) (string, bool) {
	text, isText := value.(string)
	if !isText {
		return "", false
	}

	return flowfile.SplitFence(text)
}

// A varDeclaration is one computed var: the fence as the file wrote it, the
// expression parsed once, and the siblings it reads.
type varDeclaration struct {
	// fence is the value as written, `${...}` and all. Every diagnostic about
	// this var quotes *this* and never the value the expression produced or
	// read: a computed var can hold a secret's material (see [withheldFrom]),
	// and a refusal that echoed it would be a second output channel around the
	// redaction set.
	fence string

	// ast is the expression inside the fence, parsed in the library-less
	// environment it will evaluate in. Nil when the parse or a root check
	// refused it, which is how [File.evaluateVars] knows not to evaluate.
	ast *cel.Ast

	// deps are the sibling vars the expression reads, sorted and deduplicated.
	deps []string
}

// withheldVars is what a file's `vars:` withhold: the names a value surface
// renders as [sensitiveMarker], and the strings that join every case's
// redaction set.
//
// Both halves, because they answer different questions. The names withhold a
// var *as a var* — `inspect vars.derived` in the autopsy — and the strings
// withhold its material wherever it travelled to: into a case's `inputs:`, out
// of a stub, into a transcript line. That is the same pair
// [v1.SensitiveValues] already keeps for a declared sensitive input, applied
// one layer up.
type withheldVars struct {
	// names are the withheld vars, sorted.
	names []string

	// text are the strings those vars hold, sorted and deduplicated.
	text []string
}

// holds reports whether one var is withheld. Linear over a slice bounded by
// [MaxVarsPerFile], which is cheaper than the map it would otherwise be for
// the handful of names a real file withholds.
func (w withheldVars) holds(name string) bool { return slices.Contains(w.names, name) }

// covers reports whether one witness path reads a withheld var: the whole
// value (`vars.token`) or anything selected or indexed from it
// (`vars.request.headers`, `vars.pair[0]`).
//
// A string test, and sound only because this package built both strings. A
// witness path is rendered from the claim's own AST by [referencePath], which
// joins CEL identifiers with `.` and constant indices with brackets, and a
// var's name is a CEL identifier by [checkVars] — so `vars.<name>` followed by
// a separator or the end of the path cannot be produced by any other var.
func (w withheldVars) covers(path string) bool {
	_, covered := w.coveredName(path)

	return covered
}

// coveredName is [withheldVars.covers] with the var it matched, for a caller
// that has to say which one — a check's evaluator error names the var it may
// not quote, since a name is not a value and an author needs to know which
// claim to rewrite.
func (w withheldVars) coveredName(path string) (string, bool) {
	name, rooted := varNameInPath(path)
	if !rooted || !slices.Contains(w.names, name) {
		return "", false
	}

	return name, true
}

// fileVars is what one case is given of the file's `vars:`: the values a check
// reads, and what a value surface may never print of them.
//
// One value rather than two parameters, because the two are only ever correct
// together — a case handed the values without the withholding would print what
// the file says it must not.
type fileVars struct {
	values   map[string]any
	withheld withheldVars
}

// evaluateVars turns every computed var into its value, once, in dependency
// order — where [File.resolveVars] runs, before tables expand and before
// `defaults:` is validated, so a substituted reference carries a literal and
// the fixture rule still inspects what the run will see (#1072, repair 1).
//
// # What it costs, in the shape an author chooses
//
// Over V declared vars (bounded by [MaxVarsPerFile]) and E dependency edges
// (E ≤ V², since a var's dependencies are a subset of the block's own names),
// the sort is one depth-first walk at O(V+E) and evaluation is exactly V — one
// per *var*, never one per reference. That distinction is the bound: a diamond
// (`d` reads `b` and `c`, both reading `a`) evaluates `a` once, and a chain of
// length V costs V rather than 2^V, so no billion-laughs shape exists here
// even though a reference is a reference to a reference. The CEL spend is
// therefore V × [maxVarCost] for the whole block, which is one ordinary
// evaluation's budget by construction.
//
// # Every problem, once
//
// A var whose dependency was refused is skipped in silence: its dependency's
// own diagnostic is the one an author acts on, and a cascade would report one
// mistake once per reader of it — the rule [problems] already states for a
// value whose kind is already wrong.
func (f *File) evaluateVars(p *problems) {
	block := at(v1.VarsRoot)

	declared := f.declareVars(p)
	if len(declared) == 0 {
		return
	}

	order, cycles := varOrder(declared)
	for _, cycle := range cycles {
		p.report(site{at: block.field(cycle[0])}, "vars.%s is computed from itself: %s",
			cycle[0], p.renderVarCycle(cycle))
	}

	// Decided before anything is evaluated, which is what "checkable at load"
	// means in #1072's record: the seed is a syntactic fact about the document
	// and the closure is a fact about the dependency graph, so a refusal below
	// knows what it may not print before there is a value to print.
	taint := taintedVars(declared, secretHoldingVars(f.Tests))

	base, err := varEvaluator().Env()
	if err != nil {
		// Nothing can be evaluated without an environment, so this is the whole
		// report for the block rather than one entry's worth of it — the answer
		// [checkCheckClaims] gives to the same failure.
		p.report(site{at: block}, "vars: building the expression environment: %s", err)

		return
	}

	// One activation over the file's own map, which the loop below writes into:
	// dependency order guarantees every name an expression reads already holds
	// its value, and a var can read nothing else.
	activation := map[string]any{v1.VarsRoot: f.Vars}

	resolved := make(map[string]bool, len(f.Vars))
	for _, name := range slices.Sorted(maps.Keys(f.Vars)) {
		if _, computed := declared[name]; computed {
			continue
		}
		// A literal is in the activation before anything evaluates, so it is
		// judged before anything evaluates. There is no moment at which it
		// exists unprotected and reachable.
		if refuseUnprotectableVar(p, block, taint, name, f.Vars[name]) {
			continue
		}
		resolved[name] = true
	}

	for _, name := range order {
		d := declared[name]
		if d.ast == nil {
			// Refused at its parse or by a root check, where it was reported.
			continue
		}
		if slices.ContainsFunc(d.deps, func(dep string) bool { return !resolved[dep] }) {
			// A dependency was refused, or sits on a cycle. Silence here, for
			// the reason this function's doc gives: the dependency's own
			// diagnostic is the one to act on.
			continue
		}

		value, err := evaluateVar(base, d, activation)
		if err != nil {
			p.report(site{at: block.field(name)}, "vars.%s: evaluating %s: %s",
				name, d.fence, scrubbedVarError(err, taint, d.deps))

			continue
		}
		// Judged the moment it exists, and *before* it is stored — so it never
		// enters the activation, and no later evaluation can quote it (Codex,
		// #1197). Its dependents then skip on the guard above, which is the
		// cascade rule this loop already follows: the root refusal stands for
		// the chain.
		if refuseUnprotectableVar(p, block, taint, name, value) {
			continue
		}
		f.Vars[name] = value
		resolved[name] = true
	}

	f.varsWithheld = withheldMaterial(p, block, declared, taint, resolved, f.Vars)
}

// declareVars classifies the block and prepares every computed var: the fence
// parsed in the environment it will evaluate in, the roots it may not read
// refused, and the siblings it reads collected.
//
// Sorted, so a file with two bad vars reports them in the same order every
// time — the rule every map walk in this package follows.
func (f *File) declareVars(p *problems) map[string]*varDeclaration {
	block := at(v1.VarsRoot)

	base, err := varEvaluator().Env()
	if err != nil {
		// Reported by [File.evaluateVars], which meets the same failure a moment
		// later and is the one place it becomes a diagnostic: saying it twice
		// would be the same fact in two sentences.
		return nil
	}

	declared := map[string]*varDeclaration{}
	for _, name := range slices.Sorted(maps.Keys(f.Vars)) {
		text, fenced := fencedVarValue(f.Vars[name])
		if !fenced {
			continue
		}
		spot := site{at: block.field(name)}
		d := &varDeclaration{fence: f.Vars[name].(string)}
		declared[name] = d

		if strings.TrimSpace(text) == "" {
			p.report(spot, "vars.%s holds an empty expression; write the CEL, or state the value literally", name)

			continue
		}
		parsed, issues := base.Parse(text)
		if issues != nil && issues.Err() != nil {
			p.report(spot, "vars.%s: %s", name, issues.Err())
			// No AST to read edges from, so they are read from the text —
			// see [textualVarDeps] for why an over-approximation is the safe
			// direction and a missing edge is not.
			d.deps = textualVarDeps(text)

			continue
		}
		deps, ok := checkVarExpression(p, spot, name, parsed.NativeRep().Expr(), base)
		// Recorded whatever the checks below say, and *before* any of them can
		// skip this entry (Codex, #1197, eighth). A refusal removes a value
		// from existence; it must not remove edges from the graph. A var
		// refused for an unrelated mistake — `${vars.token + steps.nope}` — is
		// still a var that reads `vars.token`, and if a `secrets:` entry names
		// it then `token` is secret material whether or not this declaration
		// survived validation. Dropping the edge left the backward closure
		// walking an incomplete graph, and an independent var's evaluator
		// error printed the plaintext.
		d.deps = deps
		if !ok {
			continue
		}
		for _, dep := range deps {
			if _, declaredDep := f.Vars[dep]; !declaredDep {
				p.report(spot, "vars.%s reads vars.%s, and this file's `vars:` names no %q", name, dep, dep)
				ok = false
			}
		}
		if !ok {
			continue
		}
		d.ast = parsed
	}

	return declared
}

// checkVarExpression walks one parsed var expression, reporting every root a
// var may not read and every function the environment it evaluates in does not
// have, and answering with the sibling vars it reads.
//
// Each refusal is its own sentence, because they are different mistakes:
// `steps` is a question of *when* (nothing has run), `inputs` is a question of
// *whose* (a file's vars are every case's, a case's inputs are its own), and
// the bare block is a self-reference. The workflow's own `vars:` validator
// words its three the same way and for the same reason (`validateWorkflowVars`
// in flowfile/validate.go).
//
// Any *other* unresolved name is deliberately left to evaluation, which is a
// moment away and names it exactly (`no such attribute(s): whatever`). A walk
// that also judged bare identifiers would have to know that `string` and
// `null_type` resolve — CEL's type names are identifiers, and
// `type(vars.a) == string` evaluates perfectly well — and refusing one of
// those would be a false diagnostic about a working expression, which is worse
// than a missing one (CLAUDE.md, "diagnostics are a feature").
//
// The names a comprehension binds are tracked, for the reason CLAUDE.md gives
// under "a rewriter has to know what the grammar binds": `[1,2].map(x, x)`
// binds `x`, and a walk that did not know it would report the macro's own
// iteration variable as something the file got wrong.
func checkVarExpression(p *problems, spot site, name string, root celast.Expr, base *cel.Env) ([]string, bool) {
	deps := map[string]bool{}
	roots := map[string]bool{}
	calls := map[string]bool{}
	block := false

	named := map[string]bool{
		v1.StepsRoot: true, v1.InputsRoot: true, v1.RunRoot: true, v1.TriggerRoot: true,
	}

	bracketed := map[string]bool{}
	dynamic := false

	var walk func(e celast.Expr, bound map[string]bool)
	walk = func(e celast.Expr, bound map[string]bool) {
		// The one recognizer, first, so both spellings record an edge before
		// any policy below decides what to do about the one that was written.
		// An edge recorded here survives a refusal, which is what the taint
		// component is built from ([File.declareVars]).
		if read, reads := readsVar(e, bound); reads {
			switch {
			case read.dynamic:
				dynamic = true
			default:
				deps[read.name] = true
				if read.bracket {
					bracketed[read.name] = true
				}
			}

			return
		}
		switch e.Kind() {
		case celast.IdentKind:
			ident := e.AsIdent()
			switch {
			case bound[ident]:
			case ident == v1.VarsRoot:
				block = true
			case named[ident]:
				roots[ident] = true
			}
		case celast.SelectKind:
			walk(e.AsSelect().Operand(), bound)
		case celast.CallKind:
			call := e.AsCall()
			function := call.FunctionName()
			target := call.Target()
			switch {
			case !call.IsMemberFunction():
				calls[function] = true
			case target.Kind() == celast.IdentKind && !bound[target.AsIdent()] &&
				target.AsIdent() != v1.VarsRoot && !named[target.AsIdent()]:
				// A qualified call. `base64.encode(x)` parses as `encode` on the
				// bare name `base64`, and nothing in a var's scope binds a bare
				// name, so the pair is one function reference rather than a
				// value with a method. Reading it as a value would report the
				// qualifier and the method separately: two diagnostics for one
				// mistake, neither naming the spelling the author wrote.
				calls[target.AsIdent()+"."+function] = true
			default:
				calls[function] = true
				walk(target, bound)
			}
			for _, arg := range call.Args() {
				walk(arg, bound)
			}
		case celast.ListKind:
			for _, element := range e.AsList().Elements() {
				walk(element, bound)
			}
		case celast.MapKind:
			for _, entry := range e.AsMap().Entries() {
				pair := entry.AsMapEntry()
				walk(pair.Key(), bound)
				walk(pair.Value(), bound)
			}
		case celast.StructKind:
			for _, field := range e.AsStruct().Fields() {
				walk(field.AsStructField().Value(), bound)
			}
		case celast.ComprehensionKind:
			comp := e.AsComprehension()
			// The range and the accumulator's start are evaluated outside the
			// comprehension's own scope, so they see the outer bindings only —
			// flowfile's `collectReferences` splits them the same way.
			walk(comp.IterRange(), bound)
			walk(comp.AccuInit(), bound)

			inner := maps.Clone(bound)
			for _, bind := range []string{comp.IterVar(), comp.IterVar2(), comp.AccuVar()} {
				if bind != "" {
					inner[bind] = true
				}
			}
			walk(comp.LoopCondition(), inner)
			walk(comp.LoopStep(), inner)
			walk(comp.Result(), inner)
		}
	}
	walk(root, map[string]bool{})

	ok := true
	refuse := func(format string, args ...any) {
		p.report(spot, format, args...)
		ok = false
	}
	if block {
		refuse("vars.%s reads the whole `vars` block: a var reads a sibling by name (`vars.other`), "+
			"because reading the block would make every var depend on every other one — itself included", name)
	}
	if dynamic {
		refuse("vars.%s indexes `vars` with an expression: which sibling that reads is decided when "+
			"the expression runs, and a var's dependencies have to be known before anything is "+
			"evaluated — they are what orders the block and what carries a secret's taint. "+
			"Name the sibling (`vars.other`)", name)
	}
	// The bracket spelling reads exactly one sibling, and the recognizer above
	// has already recorded that edge — so this refusal is about the spelling
	// and nothing else. It used to arrive as "reads the whole `vars` block",
	// which was false about what the expression does and, worse, was reported
	// by a path that recorded no dependency at all (Codex, #1197, ninth).
	for _, read := range slices.Sorted(maps.Keys(bracketed)) {
		refuse("vars.%s reads vars['%s']: a var names its sibling with a dot (`vars.%s`), which is "+
			"the one spelling this block takes — the same value, said the way every reference "+
			"position in a test file says it", name, read, read)
	}
	for _, read := range slices.Sorted(maps.Keys(roots)) {
		switch read {
		case v1.StepsRoot:
			refuse("vars.%s reads `%s`: a file's `vars:` are evaluated once when the file loads, before "+
				"any case runs, so no step has produced anything yet. Read `%s.` in `expect.check:`, "+
				"which is judged after the case has finished", name, v1.StepsRoot, v1.StepsRoot)
		case v1.InputsRoot:
			refuse("vars.%s reads `%s`: a file's `vars:` are shared by every case and a case's `%s:` are "+
				"its own, so there is no one value to read. State it in the case, or reference the var "+
				"from the case's `%s:`", name, v1.InputsRoot, v1.InputsRoot, v1.InputsRoot)
		case v1.RunRoot:
			refuse("vars.%s reads `%s`: that root describes a case that has finished, and a file's "+
				"`vars:` are evaluated once when the file loads. Read it in `expect.check:`",
				name, v1.RunRoot)
		case v1.TriggerRoot:
			refuse("vars.%s reads `%s`: a delivery belongs to the case that replays it, and a file's "+
				"`vars:` are evaluated once when the file loads, before any case runs",
				name, v1.TriggerRoot)
		}
	}
	for _, function := range slices.Sorted(maps.Keys(calls)) {
		if base.HasFunction(function) {
			continue
		}
		if varProfileFunctions()[function] {
			refuse("vars.%s calls %s(), which a workflow profile's libraries provide and the environment "+
				"a var evaluates in does not: a file's vars are not bound to a workflow — "+
				"`defaults.workflow` and a case's own `workflow:` may name different files in one suite — "+
				"so they are evaluated once, in the profile-independent environment. Write the call in "+
				"`expect.check:`, which compiles under the case's own profile, or state the value literally",
				name, function)

			continue
		}
		refuse("vars.%s calls unknown function %s(); a var is evaluated when the file loads, against "+
			"literals, operators, the standard functions and its sibling vars, and nothing else",
			name, function)
	}

	return slices.Sorted(maps.Keys(deps)), ok
}

// evaluateVar runs one expression and converts what came back into the native
// shape a fixture position and a check activation both take.
//
// Through the same pair [postRunExtras] uses on the `run` root —
// [cel.RefValueToValue] then [literalToGo] — rather than a second conversion
// beside it, so a map a var builds and a map a run produces reach a comparison
// as the same Go value.
func evaluateVar(base *cel.Env, d *varDeclaration, activation map[string]any) (any, error) {
	// Background rather than a caller's context, because this loader has none
	// to thread: the bound that matters here is cost, not time (CLAUDE.md), and
	// [maxVarCost] is enforced whatever the context says.
	out, err := varEvaluator().Eval(context.Background(), base, d.ast, activation)
	if err != nil {
		return nil, err
	}
	literal, err := cel.RefValueToValue(out)
	if err != nil {
		return nil, err
	}

	return literalToGo(literal)
}

// scrubbedVarError is what one evaluation failure may say.
//
// A CEL failure carries its operands — `no such key: <value>` — so a refusal
// about a var reading a secret-holding one could print the very material
// [taintedVars] exists to hold back. An expression reading a tainted var
// therefore has its error withheld whole, and says which var cost it that.
//
// Withheld rather than *scrubbed*, and the difference is the whole finding
// (Codex, #1197, seventh). Scrubbing worked by collecting the tainted deps'
// strings and clearing them from the message, which covers an error quoting a
// dependency and misses an error quoting something computed *from* one inside
// the same expression: `${[0][size(vars.token)]}` fails with `index out of
// bounds: 11`, a length no set ever held, and the eager unprotectable-value
// refusal never runs because the var produced no value at all — it errored.
// That is the same shape as [checkErrorText]'s, at the load-time surface, and
// it gets the same answer: a rendering either withholds what it cannot bound or
// it is a second output channel around the set.
//
// Naming the var is the useful half, as it is there: an author learns which
// dependency to break, and a name is not a value.
//
// Every diagnostic here quotes the *expression* rather than the value, which
// is the same rule stated on [varDeclaration.fence]; this is the one place a
// value could reach a message by another road, and this is that road closed.
func scrubbedVarError(err error, taint varTaint, deps []string) string {
	// deps are sorted by [checkVarExpression], so the var named here is the
	// same one on every run over the same file.
	for _, dep := range deps {
		if taint.holds(dep) {
			return fmt.Sprintf("[withheld: this expression reads vars.%s, which this file withholds]", dep)
		}
	}

	return err.Error()
}

// varOrder returns the computed vars in an order where every var's
// dependencies precede it, and every cycle that makes such an order
// impossible.
//
// Depth-first rather than Kahn's, because the diagnostic is the point: Kahn's
// answers "these vars are on cycles" and a depth-first walk answers with the
// path — `vars.a → vars.b → vars.a` — which is the sentence an author can act
// on. O(V+E) either way, and it stops early at [maxVarCycles], which is the one
// count here a document can drive past what a report can hold.
//
// Recursion is bounded by V, which is bounded by [MaxVarsPerFile]: a name
// marked in progress is never entered twice, which is the same marking that
// finds the cycle.
func varOrder(declared map[string]*varDeclaration) (order []string, cycles [][]string) {
	const (
		unvisited = iota
		inProgress
		done
	)

	state := make(map[string]int, len(declared))
	var path []string

	var visit func(name string)
	visit = func(name string) {
		if len(cycles) >= maxVarCycles {
			// Enough to fill a report; see [maxVarCycles]. The ordering this
			// abandons is not needed, because a document with a cycle in it is
			// refused whatever the rest of its block would evaluate to.
			return
		}
		d, computed := declared[name]
		if !computed {
			// A literal: it has no dependencies and needs no ordering, and a
			// name nothing declares is refused by [File.declareVars].
			return
		}
		switch state[name] {
		case done:
			return
		case inProgress:
			cycles = append(cycles, append(append([]string(nil), path[slices.Index(path, name):]...), name))

			return
		}
		state[name] = inProgress
		path = append(path, name)
		for _, dep := range d.deps {
			visit(dep)
		}
		path = path[:len(path)-1]
		state[name] = done
		order = append(order, name)
	}
	for _, name := range slices.Sorted(maps.Keys(declared)) {
		visit(name)
	}

	return order, cycles
}

// renderVarCycle renders one cycle as the hops an author walks, naming the
// document each hop was written in wherever that is not the one being parsed.
//
// A cycle can span two files: a directory's `testdefaults.yaml` vars merge
// into the suite's before anything validates ([dirDefaults.combineInto]), so
// `vars.a` in one document and `vars.b` in the other can close a loop that
// exists in neither file on its own (#1072, repair 6). The provenance is the
// fold's own — the paths it recorded, read back through [problems.fileOf] —
// rather than [DirDefaultsError], which that repair named: that type reports a
// failure to *read* the sibling file, and the per-value answer the fold
// carries since #1179 is what can name one hop of a cycle.
func (p *problems) renderVarCycle(cycle []string) string {
	hops := make([]string, 0, len(cycle))
	for _, name := range cycle {
		hop := v1.VarsRoot + "." + name
		if file := p.fileOf(site{at: at(v1.VarsRoot).field(name)}); file != "" {
			hop += " (" + file + ")"
		}
		hops = append(hops, hop)
	}

	return strings.Join(hops, " → ")
}

// secretHoldingVars are the vars a `secrets:` position references, and the
// position that references each — the seed of [taintedVars], and the far end
// of the path its diagnostics name.
//
// Syntactic, and answered before anything is evaluated or substituted. The
// walk mirrors [resolveVarsInTest]'s, because a row is a case and its
// `secrets:` are its own; its depth is the document's, already bounded by
// [checkExpansionBounds] before this walk exists. Keys are visited in sorted
// order and the first position to name a var is the one kept, so a file with
// two references to one var says the same thing every time.
func secretHoldingVars(tests []Test) map[string]string {
	holding := map[string]string{}

	var walk func(where string, tests []Test)
	walk = func(where string, tests []Test) {
		for i := range tests {
			at := fmt.Sprintf("%s[%d]", where, i)
			for _, key := range slices.Sorted(maps.Keys(tests[i].Secrets)) {
				match := varReference.FindStringSubmatch(tests[i].Secrets[key])
				if match == nil {
					continue
				}
				if _, seen := holding[match[1]]; !seen {
					holding[match[1]] = fmt.Sprintf("%s.secrets[%q]", at, key)
				}
			}
			walk(at+".cases", tests[i].Cases)
		}
	}
	walk("tests", tests)

	return holding
}

// A varTaint is which vars hold secret material, and how each one came to —
// the second half being what lets a refusal name the path rather than assert a
// verdict (#1072, repair 4; Codex on #1197).
type varTaint struct {
	// via is the next hop from a tainted var toward the `secrets:` reference
	// that taints it. A seed maps to the empty string: it *is* the reference.
	via map[string]string

	// reference is the `secrets:` position naming each seed, in the prose the
	// rest of this loader addresses a position with.
	reference map[string]string
}

// holds reports whether one var carries secret material.
func (t varTaint) holds(name string) bool {
	_, tainted := t.via[name]

	return tainted
}

// names are the tainted vars, sorted.
func (t varTaint) names() []string { return slices.Sorted(maps.Keys(t.via)) }

// path renders how one var came to be tainted: the hops to the seed, and the
// `secrets:` position that names it.
//
// `vars.port → vars.dsn, which tests[0].secrets["db"] references` — a sentence
// an author can walk, rather than a verdict they have to take on trust. The
// direction is deliberately one way for both halves of the closure: reading
// *from* a secret and contributing *to* one are the same relation seen from
// two ends, and a reader who has the chain does not need to be told which.
func (t varTaint) path(name string) string {
	hops := []string{v1.VarsRoot + "." + name}
	for hop := t.via[name]; hop != ""; hop = t.via[hop] {
		hops = append(hops, v1.VarsRoot+"."+hop)
		if len(hops) > MaxVarsPerFile {
			// Unreachable: `via` is a breadth-first tree, so it has no cycle.
			// A bound anyway, because a rendering that walks a map somebody
			// else fills in is a rendering that must terminate on its own
			// terms — the rule position.go states for its own second walls.
			break
		}
	}
	seed := hops[len(hops)-1]

	return fmt.Sprintf("%s, which %s references", strings.Join(hops, " → "),
		t.reference[strings.TrimPrefix(seed, v1.VarsRoot+".")])
}

// taintedVars closes the seeds over the dependency graph in *both* directions,
// to a fixed point.
//
// Forward — a var that reads a secret-holding one holds that secret's material
// — was the original rule (#1072, repair 4). Backward is Codex's P1 on #1197
// and the owner's call on it: `derived: "${'Bearer ' + vars.token}"` named from
// `secrets:` puts the whole `Bearer …` string in the redaction set and leaves
// `vars.token` printable, so a check witnessing it prints the token. The source
// material of a secret is secret.
//
// The two are one closure rather than two, and that is a decision worth
// stating. A var reached backward *is* secret material by the sentence above,
// and the forward rule then applies to it by its own terms — so `b` in
// `a = f(x)`, `b = g(x)`, `secrets: a` is tainted, two hops from anything the
// file called a secret. Stopping after one pass in each direction would leave
// exactly the leak shape this closure was widened to fix, one hop further out.
// Concretely the answer is the connected component of the undirected
// dependency graph containing a seed.
//
// The cost is accepted and it is real: a benign var that merely contributed to
// a secret — a `"Bearer"` prefix, a port — is withheld, and under
// [refuseUnprotectableVars] a non-string one is refused outright. Fail closed
// is the posture (CLAUDE.md), and a token minus its prefix is still a token.
//
// One breadth-first pass, O(V+E), over an adjacency built once.
func taintedVars(declared map[string]*varDeclaration, holding map[string]string) varTaint {
	adjacent := map[string][]string{}
	for name, d := range declared {
		for _, dep := range d.deps {
			// Both directions in one table: dep → name is the forward edge a
			// reader inherits along, name → dep the backward one a source is
			// reached by.
			adjacent[dep] = append(adjacent[dep], name)
			adjacent[name] = append(adjacent[name], dep)
		}
	}
	for name := range adjacent {
		slices.Sort(adjacent[name])
		adjacent[name] = slices.Compact(adjacent[name])
	}

	taint := varTaint{via: map[string]string{}, reference: holding}
	queue := slices.Sorted(maps.Keys(holding))
	for _, seed := range queue {
		taint.via[seed] = ""
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		for _, next := range adjacent[name] {
			if _, already := taint.via[next]; already {
				continue
			}
			taint.via[next] = name
			queue = append(queue, next)
		}
	}

	return taint
}

// refuseUnprotectableVar refuses one tainted var holding anything but a
// string, and reports whether it did (#1072, repair 4; four P1s from Codex on
// #1197, and the owner's rulings on them).
//
// # When, and why the timing is the fix rather than a detail
//
// Called the moment a value exists and before it is stored — for a literal
// before any expression evaluates at all, for a computed var between its own
// evaluation and its entry into the activation. Refusing afterwards, in one
// sweep at the end, still refused the file but left the value reachable in
// between: `length: "${size(vars.token)}"` followed by `bad:
// "${[0][vars.length]}"` printed the secret-derived length inside cel-go's
// `index out of bounds: <n>` before the sweep ever ran. "Refused into
// existence" has to mean it, or the promise is only about the last diagnostic
// rather than every one.
//
// The alternative — scrubbing canonical scalar renderings out of downstream
// error text — is the substring arms race this package already declined once
// (see [taintedVars] on why the taint follows references rather than values).
// A number has a dozen renderings and an attacker picks the expression.
//
// A refused var is not marked resolved, so its dependents skip on the same
// guard a failed evaluation uses and add no diagnostics of their own — the
// cascade rule #1185 set for a stub whose shape is refused: a thing whose shape
// is wrong does not have its inside judged. An *independent* problem elsewhere
// in the file is still collected, because this reports and returns rather than
// stopping the pass.
//
// # The rule, and why it is exactly this blunt
//
// The redaction set protects by *content*: a string is compared whole and
// cleared wherever it is embedded. Nothing else in the language can be reached
// that way, and the three ways a derived value carries its secret out are now
// each accounted for:
//
//   - **A string** leaks by value, and is withheld — the whole of what this
//     package's redaction machinery is for.
//   - **A scalar that is not a string** leaks by value with nothing to match:
//     `${size(vars.token)}` substituted into a case's `inputs:` reaches a
//     transcript line rooted at `steps.*`, where neither the withheld name nor
//     the withheld material can find it, and a length is a fact about a secret
//     in its own right.
//   - **A container** leaks by *shape*, which survives leaf redaction
//     completely: `${vars.token == 'guess' ? {} : {'x': 'y'}}` is an equality
//     oracle whose answer is whether the map is empty, and clearing every
//     string inside it changes nothing about that.
//
// So a tainted var is a string or it does not exist. The alternative for the
// third case was to tell fixed-shape containers from secret-dependent ones,
// which is an information-flow analysis over CEL ASTs — real machinery and
// real maintenance, guarding a corner of a load-time fixture feature.
//
// # What the refusal costs, measured rather than assumed
//
// One respelling, and the diagnostic names it. A structure whose leaves are
// computed strings is legal today at the position that *uses* it — a case's
// `inputs:`, `defaults.inputs:`, a signal payload, at any depth and inside
// lists — because [resolveVarsInValue] substitutes a whole-value `${vars.x}`
// wherever it appears in a fixture tree. So `{'Authorization': 'Bearer ' +
// vars.token}` moves from the var to the fixture and keeps the computed part
// as a string var. What is *not* legal is a fence inside a structured var
// value ([checkVars] refuses it, and says so in its own words), which is the
// expressiveness gap this refusal makes visible rather than creates.
//
// The refusal is scoped to the tainted set exactly — an untainted
// `${size(vars.hostlist)}` or a map of hostnames is an ordinary fixture and
// stays legal — which is what keeps a rule this blunt away from files that
// have nothing to do with secrets.
//
// The diagnostic names the taint path, because "this is derived from a secret"
// is a claim about a chain an author can only check if they are shown it.
func refuseUnprotectableVar(p *problems, block loc, taint varTaint, name string, value any) bool {
	if !taint.holds(name) {
		return false
	}
	kind, unprotectable := unprotectableValue(value)
	if !unprotectable {
		return false
	}
	p.report(site{at: block.field(name)},
		"vars.%s is computed from a secret and holds %s; only a non-empty string can be withheld. "+
			"A value derived from secret material carries it in a form redaction cannot reach — a "+
			"number's digits, a boolean's truth, a container's shape, an empty string's very "+
			"emptiness — none of which the redaction set can match, and each of which answers a "+
			"question about the secret. The chain: %s. Keep the derived value a non-empty string, "+
			"and express any structure where it is used — a case's `inputs:`, `defaults.inputs:`, "+
			"a signal payload — where a `${vars.x}` leaf resolves at any depth",
		name, kind, taint.path(name))

	return true
}

// unprotectableValue names what a value is, in the words the diagnostic uses,
// and reports whether redaction could never withhold it — which is everything
// that is not a non-empty string.
//
// Flat rather than recursive, and that is the decision rather than an
// omission: a container is unprotectable *as a container*, whatever its leaves
// turn out to be, because its shape is what survives clearing them. An earlier
// version walked to the first non-string leaf and called a map of strings
// protectable, which is the hole Codex's third P1 named.
//
// The empty string was excluded from this list once, on the reasoning that it
// carries no material and that refusing it would refuse a value which says
// nothing. That was wrong, and Codex's fifth P1 is why: it says exactly one
// bit, which is the only thing it can say. [collectVarStrings] declines to put
// it in the set — an empty string occurs at every position of every string, so
// redacting it would destroy the text while protecting nothing — and a value
// the set cannot hold is a value that prints. `${t == 'guess' ? ” : 'x'}`
// renders `""` in one branch and `[redacted]` in the other, which is an
// equality oracle read straight off a report. Emptiness is shape, and the
// family had already decided shape is refused.
//
// Takes its value rather than reading one out of a [File], because in a real
// document a tainted var is almost always a non-empty string and a check
// written where the values agree is one no fixture can drive (CLAUDE.md).
func unprotectableValue(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		if v == "" {
			return "the empty string", true
		}

		return "", false
	case map[string]any:
		return "a map", true
	case []any:
		return "a list", true
	case bool:
		return "a boolean", true
	case int64, int, uint64, uint:
		return "an integer", true
	case float64, float32:
		return "a number", true
	case nil:
		return "null", true
	default:
		return fmt.Sprintf("a %T", v), true
	}
}

// withheldMaterial narrows the taint to what a case has to be told about, and
// pairs each name with the strings its value holds.
//
// A literal var referenced from `secrets:` is left out. Its plaintext is that
// case's own secret and already joins that case's redaction set (run.go's
// `WithValues`), and withholding it file-wide would change what a case that
// never named the secret redacts — a behaviour change this slice has no reason
// to make. Every *other* tainted var is in, computed or not: a literal can only
// be tainted by standing on a path between expressions, which no file without
// expressions has, so nothing an existing suite can express changes meaning.
//
// A var whose evaluation failed contributes nothing, and needs to: the
// document is refused, so no case will run and there is no value to protect.
// Over [maxWithheldVarStrings] the file is refused rather than partly
// protected — the fail-closed direction every redaction seam here takes.
func withheldMaterial(p *problems, block loc, declared map[string]*varDeclaration, taint varTaint, resolved map[string]bool, values map[string]any) withheldVars {
	var names, text []string
	for _, name := range taint.names() {
		_, computed := declared[name]
		if !computed && taint.via[name] == "" {
			// A literal seed: the `secrets:` entry naming it already carries its
			// plaintext into the case that declared it, and this is the one
			// place widening would reach a file that states no expression.
			continue
		}
		names = append(names, name)
		if !resolved[name] {
			continue
		}
		var material []string
		collectVarStrings(values[name], 0, &material)
		if len(material) > maxWithheldVarStrings {
			p.report(site{at: block.field(name)},
				"vars.%s is computed from a secret and holds %d strings, more than the %d one "+
					"withheld var may contribute to a case's redaction set; keep a value derived from "+
					"a secret to the shape a fixture needs", name, len(material), maxWithheldVarStrings)

			continue
		}
		text = append(text, material...)
	}
	slices.Sort(text)

	return withheldVars{names: names, text: slices.Compact(text)}
}

// collectVarStrings appends every string one value holds — leaves and map keys
// alike, since a key is as capable of carrying a secret as a value is.
//
// Depth is bounded for the reason every walk here is: the value came out of a
// bounded evaluation, and a bound that lives only in the caller is a bound
// that moves when somebody edits the caller.
func collectVarStrings(value any, depth int, out *[]string) {
	if depth > maxDefaultsDepth {
		return
	}
	switch v := value.(type) {
	case string:
		if v != "" {
			*out = append(*out, v)
		}
	case map[string]any:
		for key, entry := range v {
			if key != "" {
				*out = append(*out, key)
			}
			collectVarStrings(entry, depth+1, out)
		}
	case []any:
		for _, entry := range v {
			collectVarStrings(entry, depth+1, out)
		}
	}
}

// resolveVars substitutes every whole-value `${vars.x}` reference in the
// file's fixture positions, in place, before tables expand and before
// `defaults:` is validated — so an inherited value resolves once and the
// fixture rule checks what the run will actually see.
//
// A reference that cannot resolve is reported and the walk carries on: the file
// is refused either way, so nothing downstream reads the half-substituted
// value, and an author gets every bad reference in one pass rather than one per
// run.
//
// Each position is named twice over, and deliberately: `where` is the prose a
// reader is given and `spot` is where the value was written. They are built one
// line apart at every call so a diagnostic cannot come to name one place and
// point at another. The two spellings differ where the prose already had its
// own (`secrets["vault:prod/db"]` quotes a key that holds a colon), which is
// the ambiguity [loc] exists to keep out of the addressing.
func (f *File) resolveVars(p *problems) {
	// With no vars, a `${vars.x}` reference is a mistake worth naming rather
	// than a string worth passing through; the walk below answers that with
	// "names no var" either way, so it runs regardless.
	for i := range f.Tests {
		resolveVarsInTest(p, fmt.Sprintf("tests[%d]", i), at("tests").item(i), &f.Tests[i], f.Vars)
	}
	if d := f.Defaults; d != nil {
		base := at("defaults")
		resolveVarsInString(p, "defaults.workflow", base.field("workflow"), &d.Workflow, f.Vars)
		resolveVarsInMap(p, "defaults.inputs", base.field("inputs"), d.Inputs, f.Vars)
		resolveVarsInIdentity(p, "defaults.sender", base.field("sender"), d.Sender, f.Vars)
		// defaults.stubs and defaults.check: deliberately untouched, the
		// stub/claim halves of the asymmetry above.
	}
}

// resolveVarsInTest covers one case's fixture positions — and its rows',
// because this runs before [expandTableEntries] and a row is a case.
func resolveVarsInTest(p *problems, where string, spot loc, test *Test, vars map[string]any) {
	resolveVarsInString(p, where+".workflow", spot.field("workflow"), &test.Workflow, vars)
	resolveVarsInMap(p, where+".inputs", spot.field("inputs"), test.Inputs, vars)
	for _, name := range slices.Sorted(maps.Keys(test.Secrets)) {
		value := test.Secrets[name]
		resolveVarsInString(p, fmt.Sprintf("%s.secrets[%q]", where, name),
			spot.field("secrets").field(name), &value, vars)
		test.Secrets[name] = value
	}
	if trigger := test.Trigger; trigger != nil {
		for _, field := range []struct {
			name   string
			target *string
		}{
			{"webhook", &trigger.Webhook}, {"payload", &trigger.Payload},
			{"kind", &trigger.Kind}, {"name", &trigger.Name},
			{"principal", &trigger.Principal}, {"delivery_id", &trigger.DeliveryID},
		} {
			resolveVarsInString(p, where+".trigger."+field.name,
				spot.field("trigger").field(field.name), field.target, vars)
		}
	}
	for i := range test.Signals {
		signal := &test.Signals[i]
		prefix := fmt.Sprintf("%s.signals[%d]", where, i)
		scripted := spot.field("signals").item(i)
		resolveVarsInString(p, prefix+".name", scripted.field("name"), &signal.Name, vars)
		resolveVarsInMap(p, prefix+".payload", scripted.field("payload"), signal.Payload, vars)
		resolveVarsInIdentity(p, prefix+".sender", scripted.field("sender"), signal.Sender, vars)
	}
	resolveVarsInIdentity(p, where+".starter", spot.field("starter"), test.Starter, vars)
	resolveVarsInMap(p, where+".expect.outputs", spot.field("expect").field("outputs"), test.Expect.Outputs, vars)
	resolveVarsInMap(p, where+".expect.inputs", spot.field("expect").field("inputs"), test.Expect.Inputs, vars)
	// expect.check: a claim position; `vars.` binds at evaluation instead.
	// stubs: the run's language; see the package comment above.
	for i := range test.Cases {
		resolveVarsInTest(p, fmt.Sprintf("%s.cases[%d]", where, i), spot.field("cases").item(i),
			&test.Cases[i], vars)
	}
}

func resolveVarsInIdentity(p *problems, where string, spot loc, identity *ScriptedIdentity, vars map[string]any) {
	if identity == nil {
		return
	}
	for _, field := range []struct {
		name   string
		target *string
	}{
		{"subject", &identity.Subject}, {"issuer", &identity.Issuer}, {"namespace", &identity.Namespace},
	} {
		resolveVarsInString(p, where+"."+field.name, spot.field(field.name), field.target, vars)
	}
	for _, name := range slices.Sorted(maps.Keys(identity.Claims)) {
		value := identity.Claims[name]
		resolveVarsInString(p, fmt.Sprintf("%s.claims[%q]", where, name),
			spot.field("claims").field(name), &value, vars)
		identity.Claims[name] = value
	}
}

// resolveVarsInMap substitutes through one decoded YAML tree, in place at the
// top level and by rebuild below it. Depth is bounded for the reason every
// walk here is ([maxDefaultsDepth]): the tree is an outside party's.
func resolveVarsInMap(p *problems, where string, spot loc, m map[string]any, vars map[string]any) {
	for _, key := range slices.Sorted(maps.Keys(m)) {
		m[key] = resolveVarsInValue(p, fmt.Sprintf("%s.%s", where, key), spot.field(key), m[key], vars, 0)
	}
}

// resolveVarsInValue returns the value with every reference in it resolved, or
// the value untouched where one could not be: a refused reference leaves what
// the author wrote in place, since the document is refused and nothing will
// read it.
func resolveVarsInValue(p *problems, where string, spot loc, value any, vars map[string]any, depth int) any {
	if depth > maxDefaultsDepth {
		p.report(site{at: spot}, "%s: nests more than %d levels deep", where, maxDefaultsDepth)

		return value
	}
	switch v := value.(type) {
	case string:
		return substituteVar(p, where, spot, v, vars)
	case map[string]any:
		for _, key := range slices.Sorted(maps.Keys(v)) {
			v[key] = resolveVarsInValue(p, fmt.Sprintf("%s.%s", where, key), spot.field(key), v[key], vars, depth+1)
		}

		return v
	case []any:
		for i, inner := range v {
			v[i] = resolveVarsInValue(p, fmt.Sprintf("%s[%d]", where, i), spot.item(i), inner, vars, depth+1)
		}

		return v
	default:
		return value
	}
}

func resolveVarsInString(p *problems, where string, spot loc, target *string, vars map[string]any) {
	resolved := substituteVar(p, where, spot, *target, vars)
	// A var holding a non-string cannot land in a position typed string: a
	// path, a subject, a signal name. Naming the type beats a later
	// far-from-here failure about a path that is a number.
	text, isText := resolved.(string)
	if !isText {
		p.report(site{at: spot}, "%s references a var holding %T, and this position takes a string", where, resolved)

		return
	}
	*target = text
}

// substituteVar resolves one string: a whole-value `${vars.x}` becomes the
// var's literal (any type — a var can hold a map a payload position wants); a
// string merely *containing* `${vars.` is refused, because a partial
// substitution would be a template language this file deliberately is not;
// every other string passes through untouched, including the `${...}`
// expressions stub positions legitimately carry.
//
// A refused reference resolves to the text as written, so that the caller's
// own type check does not then report a second problem about the same string.
func substituteVar(p *problems, where string, spot loc, s string, vars map[string]any) any {
	if match := varReference.FindStringSubmatch(s); match != nil {
		value, declared := vars[match[1]]
		if !declared {
			p.report(site{at: spot}, "%s references vars.%s, and this file's `vars:` names no %q",
				where, match[1], match[1])

			return s
		}

		return value
	}
	if strings.Contains(s, "${vars.") {
		// The advice gained its first answer with computed vars (#1072, repair
		// 5): combining text used to be something only a workflow could do, and
		// a var can now say it once — `${'https://' + vars.host + '/v1'}` — for
		// every position that references it.
		p.report(site{at: spot}, "%s mixes text with a vars reference (%q); a reference stands alone as the "+
			"whole value — build the combined text in a var (`${'a' + vars.b}`) and reference that, "+
			"build it in the workflow, or state it literally", where, s)
	}

	return s
}
