package docsgen

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// renderCELReference documents the expression surface: what a workflow's own
// language admits beyond the grammar docs/DSL.md describes.
//
// Derived from [v1.ProfileFunctions] — the same catalog `flow tasks`, MCP
// completion and hover already read — for the reason every generated document
// here exists: a function list kept by hand beside the profile that declares
// them is a second source of truth, and #573 is what this repository looked
// like with no source of truth at all. A function this build's profile does
// not admit cannot appear here, because there is nowhere for it to come from
// but the environment the profile actually builds.
//
// This is deliberately not a copy of `flow tasks`' listing. That surface
// exists to be scanned mid-authoring; this one exists to be read once, so it
// groups by library with one worked call per macro instead of a wrapped list
// of bare names, and says what a function versus a macro means for where an
// expression's meaning is settled — the terminal listing assumes a reader who
// already knows that.
func (g *Generator) renderCELReference() string {
	var b strings.Builder

	b.WriteString(generatedNotice + "\n\n")
	b.WriteString("# CEL expression reference\n\n")
	b.WriteString("An expression is written wherever a value belongs — a `vars:` binding, a task\n")
	b.WriteString("input, an `if:`, a `wait_until:` — and every position reaches the same\n")
	b.WriteString("dialect: [CEL](https://cel.dev)'s own language plus the extension libraries\n")
	b.WriteString("this build's language *profile* admits. [`docs/DSL.md`](../DSL.md) describes\n")
	b.WriteString("the grammar — what a Flowfile may contain; this describes the language an\n")
	b.WriteString("expression inside one may call.\n\n")

	renderCELProfiles(&b)
	renderCELFunctionTable(&b)
	renderCELLimits(&b)
	renderCELDurations(&b)
	renderCELIdioms(&b)

	return b.String()
}

// renderCELProfiles documents which profile a build evaluates against and
// what it admits, so a reader is never told about a function their own
// deployment refuses.
//
// Written as a loop over every known profile rather than one paragraph about
// [v1.CurrentProfile], on purpose: the day a second profile exists, this
// document is already shaped to say what each one admits instead of what
// "the" profile does, and a reader comparing two builds is not left
// wondering whether the difference is real or just unwritten.
func renderCELProfiles(b *strings.Builder) {
	b.WriteString("## Profiles\n\n")
	b.WriteString("A workflow speaks one profile, and the whole file speaks it — there is no\n")
	b.WriteString("`libs:` a step names to reach a wider vocabulary than its neighbors. Adding a\n")
	b.WriteString("library to a future build must not change what an expression already stored\n")
	b.WriteString("in a run's specification means, so a profile is a fixed, named membership\n")
	b.WriteString("rather than \"everything this build happens to have.\"\n\n")

	for _, name := range v1.ProfileNames() {
		libs, err := v1.ProfileLibraries(name)
		if err != nil {
			// ProfileNames and ProfileLibraries read the same registry, so this
			// is not a condition a caller can reach — but a generator failing
			// loudly on it beats rendering a profile section with no libraries
			// and no explanation.
			panic(fmt.Sprintf("docsgen: profile %q is listed but has no libraries: %v", name, err))
		}

		current := ""
		if name == v1.CurrentProfile {
			current = " (what this build evaluates against)"
		}

		fmt.Fprintf(b, "- **`%s`**%s admits: %s\n", cell(name), current, codeList(libs))
	}
	b.WriteString("\n")
}

// renderCELFunctionTable is the reference's core: every function and macro
// [v1.CurrentProfile] admits, grouped by the library that declares it.
//
// One table rather than one per library, because a reader comparing two
// libraries — "does `lists` or `strings` have `reverse`?" — needs them on
// the same page, and #573's own evidence (six examples spelling one
// three-way optional dispatch by hand) is about a function nobody knew to
// look for at all, not about one whose library was hard to find.
func renderCELFunctionTable(b *strings.Builder) {
	functions := v1.ProfileFunctions(v1.CurrentProfile)

	b.WriteString("## Functions and macros\n\n")
	b.WriteString("A **function** is looked up while an expression evaluates, the same as any\n")
	b.WriteString("other call — so it is resolved by whichever worker runs the workflow. A\n")
	b.WriteString("**macro** is expanded by the *parser*, so its meaning is settled when the file\n")
	b.WriteString("compiles and frozen into what a run carries; `Example` gives the macro's call\n")
	b.WriteString("form, because cel-go does not report one — `math.greatest(1, 2)` is declared\n")
	b.WriteString("as `greatest`, and `[3,1,2].sortBy(v, v)` as `sortBy`, and nothing about either\n")
	b.WriteString("name says whether it is written on a namespace or on a value.\n\n")
	b.WriteString("`Signature` is the same call form for an ordinary function: argument order,\n")
	b.WriteString("arity, and — like `Example` — whether it is written on a namespace or a value,\n")
	b.WriteString("one entry per overload. It is not written down the way `Example` is; cel-go's\n")
	b.WriteString("own compiled environment answers this for a function even though it cannot for\n")
	b.WriteString("a macro, so this column is derived from it directly.\n\n")

	b.WriteString("| Library | Name | Kind | Example | Signature |\n|---|---|---|---|---|\n")
	for _, fn := range functions {
		kind := "function"
		example := ""
		if fn.Macro {
			kind = "macro"
			example = codeOrEmpty(fn.Example)
		}
		fmt.Fprintf(b, "| `%s` | `%s` | %s | %s | %s |\n",
			cell(fn.Library), cell(fn.Name), kind, orDash(example), orDash(signatureCell(fn.Signature)))
	}
	b.WriteString("\n")
}

// signatureCell renders a function's overloads as one table cell, each on its
// own line: `<br>` rather than a semicolon, because a comma-and-semicolon
// join of `strings.replace(string, string, string) -> string` beside its own
// three-argument sibling reads as one long run-on rather than as the two
// overloads it is.
func signatureCell(signatures []string) string {
	if len(signatures) == 0 {
		return ""
	}

	out := make([]string, 0, len(signatures))
	for _, sig := range signatures {
		out = append(out, "`"+cell(sig)+"`")
	}

	return strings.Join(out, "<br>")
}

// renderCELLimits documents the cost bound every expression evaluates
// under — CLAUDE.md's own "bound anything that consumes untrusted input"
// applied to this surface, and until #702 stated nowhere a reader of this
// reference would see it, even though the Idioms section immediately below
// recommends the exact pattern (`.filter().map()` chained over a value this
// build did not produce) that spends against it.
//
// The number is read off [v1.DefaultCostLimit] rather than written here, for
// the reason every other fact in this file is derived: a budget changed in
// code and not in prose is a document that is confidently wrong, which is
// worse than one silent about the number entirely.
func renderCELLimits(b *strings.Builder) {
	b.WriteString("## Limits\n\n")
	fmt.Fprintf(b, "An expression evaluates against a **cost budget**, not a time budget:\n"+
		"`DefaultCostLimit` (%s) is what a single evaluation may spend before it aborts, in\n",
		groupThousands(v1.DefaultCostLimit))
	b.WriteString("abstract units roughly proportional to the work performed rather than\n")
	b.WriteString("wall-clock time or bytes read. This is what stands between a workflow\n")
	b.WriteString("author and a pathological expression, so it applies to a `vars:` binding, a\n")
	b.WriteString("task input, an `if:` — anywhere on this page — and not only to a `cel` step.\n\n")
	b.WriteString("It matters most for the Idioms section below. A `.filter().map()` chain\n")
	b.WriteString("costs nothing beyond the work it does per element, and is still charged per\n")
	b.WriteString("element: run over a response whose size this build does not bound, it\n")
	b.WriteString("accumulates cost across every item the chain touches, so an expression that\n")
	b.WriteString("works against a small fixture can still exceed the budget against production\n")
	b.WriteString("data. An expression that exceeds the budget fails whatever evaluation it is\n")
	b.WriteString("written in, the same as any other evaluation error: a step-scoped expression\n")
	b.WriteString("(a task input, an `if:`, a step's own `vars:`) fails that step, while a\n")
	b.WriteString("workflow-level `vars:` binding fails the run before any step starts, and a\n")
	b.WriteString("run-output expression fails it after every step has finished — there is no\n")
	b.WriteString("step for either of those to fail. The worker is never at risk, only the run.\n\n")
}

// groupThousands writes n with a comma every three digits — "1,000,000"
// rather than "1000000" — because a budget is read by a person deciding
// whether their expression might exceed it, and a bare run of seven digits
// makes that arithmetic rather than a glance.
func groupThousands(n uint64) string {
	digits := fmt.Sprintf("%d", n)

	var b strings.Builder
	for i, d := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(d)
	}

	return b.String()
}

// renderCELDurations documents the duration constructors, which are not
// library-gated — every expression has them, in `sleep:`, a signal's
// `timeout:`, and anywhere else a duration belongs.
func renderCELDurations(b *strings.Builder) {
	b.WriteString("## Durations and `now`\n\n")
	b.WriteString("Unconditional rather than opt-in: `weeks(n)`, `days(n)`, `hours(n)`,\n")
	b.WriteString("`minutes(n)` and `seconds(n)` build a duration from a count, and every\n")
	b.WriteString("expression has them because a `wait_until:` step has no `libs:` key to enable\n")
	b.WriteString("anything with — the expression is the whole of the step. `duration('72h')`,\n")
	b.WriteString("CEL's own constructor, means exactly the same thing as `days(3)`; the named\n")
	b.WriteString("units exist so a reader can scan `days(3) + hours(12)` without doing\n")
	b.WriteString("arithmetic on a string.\n\n")

	fmt.Fprintf(b, "Units, largest first: %s.\n\n", codeList(v1.DurationUnits()))

	fmt.Fprintf(b, "Inside a wait (`sleep:`, `wait_until:`, a signal's `timeout:`), `%s` is the\n",
		v1.NowIdentifier)
	fmt.Fprintf(b, "moment the wait is evaluated — so a deadline is `${%s + days(3)}` and a\n",
		v1.NowIdentifier)
	fmt.Fprintf(b, "remaining bound is `${deadline - %s}`. See examples/computed-durations and\n",
		v1.NowIdentifier)
	b.WriteString("examples/wait-until-a-moment.\n\n")
}

// renderCELIdioms is the prose a table cannot carry: which function to reach
// for, not just that it exists.
//
// #573's own framing is that a function list is a reference and an author
// needs to know *which* to reach for — the optional case above all, since
// `.orValue(false)` on a three-way dispatch silently collapses "absent" and
// "present and false" into the same branch. celenv_test.go's
// TestEveryDocumentedCELIdiomCompiles compiles every expression here against
// [v1.CurrentProfile]'s environment, so a rewording that breaks one fails
// there rather than shipping silently wrong.
func renderCELIdioms(b *strings.Builder) {
	b.WriteString("## Idioms\n\n")

	for _, idiom := range celIdioms {
		fmt.Fprintf(b, "### %s\n\n", idiom.title)
		b.WriteString(idiom.prose)
		b.WriteString("\n\n")
		b.WriteString("```cel\n")
		b.WriteString(idiom.expr)
		b.WriteString("\n```\n\n")
	}
}

// celIdiom is one worked expression: what it is for, and the expression
// itself, kept together so the doc and the compiling test read the same
// value rather than two spellings of one idea.
type celIdiom struct {
	title string
	prose string
	expr  string
}

// celIdioms is hand-written rather than derived, and says so here rather than
// only in the PR that added it: which function solves a problem is a judgment
// call a catalog cannot make, so unlike the table above this list is a place
// this document can go stale if a better idiom lands and nobody updates it.
// What keeps it from going *wrong* is CelReferenceIdiomsTest (in
// pkg/flowstate/v1), which compiles every expression below against the
// profile's own environment — an idiom that no longer parses fails there by
// name and line, the same guarantee #571 built for auth's doc comments.
var celIdioms = []celIdiom{
	{
		title: "A three-way optional dispatch",
		prose: "`payload.approved` may be true, false, or missing entirely — a signal can " +
			"arrive with no payload at all. `.orValue(false)` on that read is a bug: it makes " +
			"\"missing\" and \"present and false\" the same branch, when they mean \"nobody " +
			"answered\" and \"answered no.\" Keep the absence visible with `optional.of`/`hasValue`, " +
			"or read the optional itself and dispatch on it, rather than defaulting it away before " +
			"the question that matters gets asked. See examples/optional-dispatch.",
		expr: `payload.?approved.hasValue()
  ? (payload.approved ? "approved" : "rejected")
  : "no_response"`,
	},
	{
		title: "A safe read with a default, replacing has()",
		prose: "`has(x.y) ? x.y : d` spells the path twice and reads as a guard in front of " +
			"the read it protects. `.?` puts the default beside the read instead: `x.?y` " +
			"produces an optional, and `.orValue(d)` unwraps it to `d` when the field was never " +
			"there. `has()` still answers the question it was always for — presence itself — but " +
			"stops being the way to read a maybe-absent value.",
		expr: `{'volume': 7}.?volume.orValue(0)`,
	},
	{
		title: "Filtering and transforming a list without a loop",
		prose: "`filter` and `map` are macros — expanded when the file compiles — so a list " +
			"comprehension costs nothing at evaluation time beyond the work it does. Chained, " +
			"they read left to right: keep what matters, then compute what is kept.",
		expr: `[1, 2, 3, 4, 5].filter(n, n % 2 == 0).map(n, n * n)`,
	},
	{
		title: "Totalling a list without a loop",
		prose: "`sum` folds a list with `+`, expanded when the file compiles like `filter` and " +
			"`map` — so a numeric total is one expression instead of a `loop:` carrying an index " +
			"and a running sum through durable history, with the off-by-one that shape invites. " +
			"Chained after `map` it reads left to right: keep what matters, pick the number, add " +
			"them up. An empty list sums to `0`, and a list `+` cannot add — a string beside an " +
			"int — fails the evaluation rather than guessing. Only a list may be folded: a map's " +
			"iteration order is undefined, so both folds refuse one, and the deterministic spelling " +
			"is explicit — `m.map(k, k).sort().map(k, m[k]).sum()`. `loop:` remains the right tool " +
			"when the fold's body does real work; see examples/loop-accumulate.",
		expr: `steps.paid.value.map(o, o.amount_cents).sum()`,
	},
	{
		title: "A fold whose combiner is not +",
		prose: "`reduce` is the general form `sum` is the special case of: name the accumulator " +
			"and the element, give the seed, write the combining expression. Reach for " +
			"`map(...).sum()` first — it answers the naming and seeding questions for you — and " +
			"for `reduce` when the combiner is not `+`: a product, a running maximum, a fold " +
			"whose seed carries meaning. An empty list folds to the seed, verbatim.",
		expr: `steps.factors.value.reduce(p, v, 1, p * v)`,
	},
	{
		title: "Building one message from several values",
		prose: "`format` is an operator on a string, defined at the CEL level and pinned by " +
			"the profile, filling `%s` and `%d` verbs from the argument list in order.",
		expr: `"%s has %d item(s) over %.2f".format(["cart", 3, 19.5])`,
	},
}
