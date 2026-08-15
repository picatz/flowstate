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
	b.WriteString("this build's language *profile* admits. `docs/DSL.md` describes the grammar —\n")
	b.WriteString("what a Flowfile may contain; this describes the language an expression inside\n")
	b.WriteString("one may call.\n\n")

	renderCELProfiles(&b)
	renderCELFunctionTable(&b)
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

	b.WriteString("| Library | Name | Kind | Example |\n|---|---|---|---|\n")
	for _, fn := range functions {
		kind := "function"
		example := ""
		if fn.Macro {
			kind = "macro"
			example = codeOrEmpty(fn.Example)
		}
		fmt.Fprintf(b, "| `%s` | `%s` | %s | %s |\n", cell(fn.Library), cell(fn.Name), kind, orDash(example))
	}
	b.WriteString("\n")
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
		title: "Building one message from several values",
		prose: "`format` is an operator on a string, defined at the CEL level and pinned by " +
			"the profile, filling `%s` and `%d` verbs from the argument list in order.",
		expr: `"%s has %d item(s) over %.2f".format(["cart", 3, 19.5])`,
	},
}
