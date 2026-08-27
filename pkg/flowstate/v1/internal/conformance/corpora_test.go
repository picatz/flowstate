package conformance

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every corpus in this package holds cases.
//
// `for _, c := range Cases() { … assert … }` proves precisely nothing when
// `Cases()` returns an empty slice, and both drivers' tests are written that
// way — so an emptied corpus makes two suites green while asserting nothing at
// all. It is the `ZeroValueCases` shape `callers_test.go` was written for, one
// question over: that file asks whether both drivers *call* each corpus, and
// this one asks whether calling it is worth anything.
//
// Answered here rather than at the call sites. `tools/vacuity` reports forty of
// those — every `for _, c := range conformance.SomethingCases()` in the tree —
// and forty one-line additions would be forty places to forget the forty-first.
// A corpus is non-empty or it is not, which is a fact about the corpus.
//
// The table below is hand-written and the walk underneath refuses to let it
// rot, which is `tools/fuzztargets`' shape: a list, a walk, and a test that
// fails when they disagree.

// standIn is the base URL the parameterised corpora build their workflows
// against.
//
// Syntactically real and pointed at nothing, because none of these performs
// I/O — they interpolate it into a workflow a driver will later run against a
// server of its own. A corpus that did reach out would fail here loudly, which
// is the right outcome for a corpus that reached out.
const standIn = "http://127.0.0.1:1"

// corpusSizes calls every corpus this package exports and reports what each
// holds.
//
// Written out rather than reflected over, because Go cannot enumerate a
// package's functions at run time — and the walk below is what makes the
// written-out list trustworthy rather than a thing somebody remembered to
// update.
func corpusSizes() map[string]int {
	return map[string]int{
		"AsyncCases":                      len(AsyncCases(standIn)),
		"AsyncUnwindCases":                len(AsyncUnwindCases(standIn)),
		"AtomicBlockRefusalSubstrings":    len(AtomicBlockRefusalSubstrings()),
		"AuthorityContainmentCases":       len(AuthorityContainmentCases(standIn)),
		"AuthorityDenialCases":            len(AuthorityDenialCases()),
		"CallCases":                       len(CallCases()),
		"ContainmentProhibitedValues":     len(ContainmentProhibitedValues()),
		"ControlFlowCases":                len(ControlFlowCases(standIn)),
		"EgressIdentityCases":             len(EgressIdentityCases()),
		"DebuggerCases":                   len(DebuggerCases()),
		"ErrorKindCases":                  len(ErrorKindCases(standIn)),
		"ErrorTextCases":                  len(ErrorTextCases(standIn)),
		"ExpectedTaskSpans":               len(ExpectedTaskSpans()),
		"ForEachAtomicBlockCases":         len(ForEachAtomicBlockCases()),
		"ForEachResultsBoundCases":        len(ForEachResultsBoundCases(standIn)),
		"ForEachTripCountCases":           len(ForEachTripCountCases()),
		"InputOutputCases":                len(InputOutputCases(standIn)),
		"InputRefusalCases":               len(InputRefusalCases()),
		"InterpolationCases":              len(InterpolationCases()),
		"LogCases":                        len(LogCases()),
		"LoopCases":                       len(LoopCases()),
		"LoopExhaustionTranscriptCases":   len(LoopExhaustionTranscriptCases()),
		"NestedErrorTextCases":            len(NestedErrorTextCases()),
		"OutputShapingCases":              len(OutputShapingCases(standIn)),
		"PartialTranscriptCases":          len(PartialTranscriptCases()),
		"PendingWaitCases":                len(PendingWaitCases()),
		"PluginTaskInputCases":            len(PluginTaskInputCases()),
		"PolicyCases":                     len(PolicyCases()),
		"RehearsalSignalCases":            len(RehearsalSignalCases()),
		"ResponseScopeCases":              len(ResponseScopeCases(standIn)),
		"SwitchCases":                     len(SwitchCases()),
		"TaskOutputElementBoundCases":     len(TaskOutputElementBoundCases(standIn)),
		"TaskOutputSizeBoundCases":        len(TaskOutputSizeBoundCases(standIn)),
		"TaskPolicyCases":                 len(TaskPolicyCases()),
		"ToleratedIterationIdentityCases": len(ToleratedIterationIdentityCases(standIn)),
		"ToleratedStepFailureCases":       len(ToleratedStepFailureCases()),
		"ToleratedSuccessHasGuardCases":   len(ToleratedSuccessHasGuardCases()),
		"TriggerContextCases":             len(TriggerContextCases()),
		"UndoCallCases":                   len(UndoCallCases(standIn)),
		"UndoCancellationCases":           len(UndoCancellationCases(standIn)),
		"UndoCases":                       len(UndoCases(standIn)),
		"UndoLoopCases":                   len(UndoLoopCases(standIn)),
		"UndoPlacementCases":              len(UndoPlacementCases(standIn)),
		"ValueCases":                      len(ValueCases()),
		"VarsCases":                       len(VarsCases(standIn)),
		"VarsSecretRefusalCases":          len(VarsSecretRefusalCases()),
		"WaitCases":                       len(WaitCases()),
		"WebhookDeliveryCases":            len(WebhookDeliveryCases()),
		"WebhookTriggerCases":             len(WebhookTriggerCases()),
		"Workflows":                       len(Workflows(standIn)),
		"ZeroValueCases":                  len(ZeroValueCases(standIn)),
	}
}

// notACorpus names the exported functions that return a slice and are not a
// corpus, with the reason — the shape `oneSidedByDesign` uses next door, and
// for the same reason: an entry without a reason is a way to make the walk
// quiet rather than a decision somebody made.
var notACorpus = map[string]string{
	"PointAtStandIn": "a transformer over the caller's own nodes rather than a corpus — it " +
		"returns the http steps it could not point at a stand-in, and an empty answer is " +
		"the good outcome",
	"RenderedSpans": "a renderer over the caller's own recorder rather than a corpus — what " +
		"it returns is however many spans that caller produced",
	"ExampleVariants": "a lookup by name into the variants corpus, so it answers empty for " +
		"a name with no variants, which is legitimate; the corpus behind it is asserted " +
		"non-empty by TestEveryExampleVariantSetHoldsVariants below",
}

// unresolvedResults names the exported functions whose result type this walk
// cannot read, and says what each actually returns.
//
// A qualified type — `v1.TaskDef`, or `type Cases = other.Cases` — is an
// [ast.SelectorExpr] naming something declared in another package, and a
// syntactic walk cannot know whether it is a slice. Two answers were available
// and both are worse than this one. Ignoring it omits the function silently,
// which is precisely the failure this file exists to prevent, arriving through
// the file that prevents it (Codex, #1129). Reaching for `go/types` makes a
// test that *parses* into one that *builds*, for a question a person can answer
// in a line.
//
// So the walk refuses instead: a result it cannot read has to be written down
// here, with what it is. That turns "the checker quietly could not tell" into
// "somebody looked", which is the same trade `notACorpus` makes one table up —
// and it means every exported result in this package is now either resolved or
// named, rather than resolved or missed.
var unresolvedResults = map[string]account{
	"ErrorKindTimeoutTaskDef": {what: "a v1.TaskDef, which is a struct rather than a slice"},
	"PluginIdentityTaskDef":   {what: "a v1.TaskDef, which is a struct rather than a slice"},
	"PluginTaskInputsTaskDef": {what: "a v1.TaskDef, which is a struct rather than a slice"},
	"StepTimeoutTaskDef":      {what: "a v1.TaskDef, which is a struct rather than a slice"},
	"TotalTimeoutTaskDef":     {what: "a v1.TaskDef, which is a struct rather than a slice"},
}

// account is what an unreadable result actually is.
//
// Both fields, because saying only *what* it is left the accounting
// disconnected from the tables it exists to feed: a qualified result that is a
// corpus had nowhere to go. Listing it here satisfied the refusal and its
// emptiness went unchecked; listing it in `corpusSizes` instead was rejected,
// because the slice walk could not see it either. The diagnostic told somebody
// to do a thing that then failed, which is worse than no diagnostic
// (Codex, #1129).
//
// So the account says whether it is a slice, and the slice walk reads that —
// one answer, given once, feeding both.
type account struct {
	// what it returns, in words, for whoever reads a failure.
	what string

	// slice reports that what it returns is a slice, so a corpus returning a
	// qualified type belongs in `corpusSizes` and is checked there like any
	// other.
	slice bool
}

// TestEveryQualifiedResultIsAccountedFor is that refusal.
//
// Nothing here decides whether such a function is a corpus — the entry says
// what it returns and that is the whole of it. A qualified type that *is* a
// slice would be listed as one and then also belong in `corpusSizes`, and the
// coverage test above would say so.
func TestEveryQualifiedResultIsAccountedFor(t *testing.T) {
	found := unresolvableResultFuncs(t)

	for _, name := range unaccountedFor(found, unresolvedResults) {
		t.Errorf("%s returns a type this walk cannot read — it is qualified, so whether it is "+
			"a slice is decided in another package. Add it to unresolvedResults saying what "+
			"it returns; set `slice: true` there if it is one, and then add it to corpusSizes "+
			"too so its emptiness is checked", name)
	}

	for _, name := range staleAccounts(found, unresolvedResults) {
		t.Errorf("unresolvedResults lists %s, whose result this walk can now read (or which no "+
			"longer exists); delete the entry", name)
	}

	for name, of := range unresolvedResults {
		if strings.TrimSpace(of.what) == "" {
			t.Errorf("unresolvedResults[%q] does not say what it returns; that is the record", name)
		}
	}
}

// unaccountedFor are the names the walk found and the table does not list.
//
// Split out so the demand itself is testable. Every qualified result in this
// package is listed today, so a test that only ran it against the tree would
// pass whether the demand worked or not — which the mutation said, and is this
// file's own subject a third time.
func unaccountedFor(found []string, listed map[string]account) []string {
	var missing []string
	for _, name := range found {
		if _, ok := listed[name]; !ok {
			missing = append(missing, name)
		}
	}

	return missing
}

// staleAccounts are the names the table lists and the walk did not find.
func staleAccounts(found []string, listed map[string]account) []string {
	real := map[string]bool{}
	for _, name := range found {
		real[name] = true
	}

	var stale []string
	for name := range listed {
		if !real[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)

	return stale
}

// TestTheDemandForAnAccountWorksInBothDirections exercises it, since the tree
// itself cannot.
func TestTheDemandForAnAccountWorksInBothDirections(t *testing.T) {
	listed := map[string]account{"Known": {what: "a struct"}}

	assert.Equal(t, []string{"Unlisted"},
		unaccountedFor([]string{"Known", "Unlisted"}, listed),
		"a qualified result nobody accounted for was not demanded")

	assert.Empty(t, unaccountedFor([]string{"Known"}, listed),
		"an accounted-for result was demanded anyway")

	assert.Equal(t, []string{"Known"},
		staleAccounts([]string{"Something"}, listed),
		"an entry naming a function the walk no longer finds was not reported")

	assert.Empty(t, staleAccounts([]string{"Known"}, listed),
		"a live entry was reported stale")

	// And the half that composes the accounting with the corpus tables, driven
	// through the real functions. The first version of this built an `accounts`
	// map here and asserted it held what the line above had put into it, which
	// stayed green with both mechanisms it was written to pin deleted — this
	// file's own subject, committed inside the fix for it.
	fixture, err := parser.ParseFile(token.NewFileSet(), "fixture.go", `package conformance

type Imported = other.Cases
type Indirect = Imported

func ImportedCases() other.Cases        { panic("unused") }
func AliasedCases() Indirect            { panic("unused") }
func LocalCases() []int                 { panic("unused") }
func InstantiatedCases() other.Cases[C] { panic("unused") }
`, 0)
	require.NoError(t, err)

	slices, unknown := resolveTypeNames([]*ast.File{fixture})

	// Unreadability reaches `Indirect` only by propagating: its own declaration
	// names `Imported`, an identifier this package does declare, so a walk
	// without the propagation reads it as resolvable and never demands an
	// account of the function returning it.
	assert.True(t, unknown["Indirect"],
		"a name aliased to a name aliased to a qualified type was read as resolvable, so the "+
			"function returning it walks past the refusal built for exactly this")

	var decls []*ast.FuncDecl
	for _, d := range fixture.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok {
			decls = append(decls, fd)
		}
	}

	// Three spellings, including an *instantiated* qualified type: unwrapping
	// is what refuses that one rather than omitting it silently, which is the
	// whole argument for unwrapping at every reader instead of adding a case
	// to each.
	assert.Equal(t,
		[]string{"AliasedCases", "ImportedCases", "InstantiatedCases"},
		unreadableResults(decls, unknown),
		"every spelling of a result this walk cannot read must be demanded of the accounting")

	// The account then feeds the corpus tables. Marked a slice, a qualified
	// corpus is listed beside every other and its emptiness is checked; not
	// marked, it stays out — so this pins that the account is *read* rather
	// than that everything accounted for is admitted.
	accounts := map[string]account{
		"ImportedCases": {what: "an other.Cases", slice: true},
		"AliasedCases":  {what: "an other.Def reached through two aliases"},
	}

	assert.Equal(t, []string{"ImportedCases", "LocalCases"}, sliceReturners(decls, slices, accounts),
		"an accounted slice must reach the corpus tables, and an account not claiming to be "+
			"one must not")
}

// unresolvableResultFuncs are the exported functions returning a single
// qualified type.
func unresolvableResultFuncs(t *testing.T) []string {
	t.Helper()

	fset := token.NewFileSet()
	decls := exportedDecls(t, fset)
	_, unknown := resolveTypeNames(parsedFiles(t, fset))

	return unreadableResults(decls, unknown)
}

// unreadableResults are the declarations whose single result this walk cannot
// decide about.
//
// Takes its declarations rather than reading the package, because the tree
// contains no function returning an aliased qualified type and a walk over the
// tree therefore exercises one of its two branches. That is also why the first
// attempt at the test below was worthless: it built a map by hand and asserted
// the map held what the line above had put in it, which is this file's own
// subject committed inside the fix for it.
func unreadableResults(decls []*ast.FuncDecl, unknown map[string]bool) []string {
	var names []string
	for _, fd := range decls {
		if fd.Type.Results == nil || len(fd.Type.Results.List) != 1 {
			continue
		}

		switch result := coreType(fd.Type.Results.List[0].Type).(type) {
		case *ast.SelectorExpr:
			// `pkg.Type`. A pointer to one is a pointer and so not a slice,
			// and `[]pkg.Type` is an array node this walk reads perfectly well.
			names = append(names, fd.Name.Name)
		case *ast.Ident:
			// A name this package declares by aliasing something qualified,
			// which is the same unreadability one indirection away.
			if unknown[result.Name] {
				names = append(names, fd.Name.Name)
			}
		}
	}

	sort.Strings(names)

	return names
}

// TestEveryCorpusHoldsCases is the claim.
func TestEveryCorpusHoldsCases(t *testing.T) {
	sizes := corpusSizes()
	require := func(name string, held int) {
		if held == 0 {
			t.Errorf("%s is empty, so every `for _, c := range conformance.%s(…)` in the tree "+
				"runs zero times and asserts nothing — on both drivers, silently. Restore its "+
				"cases, or delete the corpus and its callers together", name, name)
		}
	}

	names := make([]string, 0, len(sizes))
	for name := range sizes {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		require(name, sizes[name])
	}
}

// TestEveryExampleVariantSetHoldsVariants is what `ExampleVariants`' entry in
// [notACorpus] promises.
//
// The function answers empty for an unknown name, legitimately — but the map
// behind it is a corpus like any other, and a *named* set that emptied would
// leave whichever example it belongs to asserting nothing.
func TestEveryExampleVariantSetHoldsVariants(t *testing.T) {
	if len(exampleVariants) == 0 {
		t.Fatal("the variants corpus is empty, so every example that reads a variant set " +
			"asserts nothing")
	}

	names := make([]string, 0, len(exampleVariants))
	for name := range exampleVariants {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if len(exampleVariants[name]) == 0 {
			t.Errorf("the variant set for %q is empty, so the example that reads it runs "+
				"zero variants", name)
		}
	}
}

// TestTheCorpusTableCoversEveryCorpus is what keeps the table above honest.
//
// Every exported function in this package that returns a slice is a corpus and
// belongs in [corpusSizes], or is not and belongs in [notACorpus] with the
// reason. A new corpus added without a line here would be exactly the gap this
// file exists to close, arriving through the file that closes it.
func TestTheCorpusTableCoversEveryCorpus(t *testing.T) {
	sizes := corpusSizes()

	for _, name := range exportedSliceFuncs(t) {
		_, listed := sizes[name]
		_, excused := notACorpus[name]

		switch {
		case listed && excused:
			t.Errorf("%s is both in corpusSizes and in notACorpus; it is one or the other", name)
		case !listed && !excused:
			t.Errorf("%s returns a slice and is in neither table: add it to corpusSizes so its "+
				"emptiness is checked, or to notACorpus with the reason it is not a corpus", name)
		}
	}
}

// TestANamedSliceTypeIsStillASlice is the door the walk holds shut.
//
// A corpus written `type Cases []Case` with `func NewCases() Cases` has an
// identifier for its result, not an `[]T` — so a walk matching only the literal
// syntax drops it, and it sits in neither table with nothing to say so
// (Codex, #1129). Nothing in the package is written that way today, which is
// why this asks the resolver directly rather than adding a corpus to prove it.
func TestANamedSliceTypeIsStillASlice(t *testing.T) {
	fset := token.NewFileSet()
	declared := sliceTypeNames(t, fset)

	// The types this package actually declares as slices, so the resolver is
	// asked about the tree rather than only about a fixture.
	for _, name := range []string{"Case", "UndoCase", "Refusal"} {
		if declared[name] {
			t.Errorf("%s is not a slice type and the resolver says it is", name)
		}
	}

	// And the shapes it has to recognise, resolved from source written here.
	fixture, err := parser.ParseFile(token.NewFileSet(), "fixture.go", `package conformance

type Direct []int
type Indirect Direct
type Aliased = Direct
type Array [4]int
type NotASlice struct{}
type Cyclic Cyclic
type Generic[T any] []T
type Instantiated = Generic[int]
type Pair[K comparable, V any] map[K]V
`, 0)
	require.NoError(t, err)

	resolved := resolveSliceNames([]*ast.File{fixture})

	for name, want := range map[string]bool{
		"Direct":    true,
		"Indirect":  true,
		"Aliased":   true,
		"Array":     false,
		"NotASlice": false,
		"Cyclic":    false,
		// A type parameter list sits beside the declaration rather than inside
		// it, so the declaration is an ordinary `[]T` — but an *instantiation*
		// of it wraps the name, which is the shape that broke the reader.
		"Generic":      true,
		"Instantiated": true,
		"Pair":         false,
	} {
		if resolved[name] != want {
			t.Errorf("%s: resolved as slice=%v, want %v", name, resolved[name], want)
		}
	}

	// And the resolver put to the use it exists for. Testing it alone leaves
	// the reader of its answer untested, and the reader is where the guard
	// either sees a corpus or does not — a resolver that is right and a caller
	// that ignores it is the same hole with an extra step.
	for _, results := range []struct {
		source string
		slice  bool
	}{
		{"func NewCases() Cases", true},
		{"func NewCases() []Case", true},
		{"func NewCases() Indirect", true},
		{"func NewCases() Aliased", true},
		{"func NewCases() Array", false},
		// The literal spelling of the same refusal, which is where the two
		// branches used to disagree.
		{"func NewCases() [4]int", false},
		{"func NewCases() [][]Case", true},
		{"func NewCases() NotASlice", false},
		{"func NewCases() error", false},
		// Instantiations of a generic corpus. The walk already resolved the
		// name; before `coreType` the reader saw an [ast.IndexExpr] and said
		// no, which is the resolver-and-reader split a third time (Codex,
		// #1129).
		{"func NewCases() Generic[Case]", true},
		{"func NewCases() Pair[string, Case]", false},
		{"func NewCases() Parameterised[string, Case]", true},
		// A parenthesis is not a type. Doubled, because a result list's own
		// parentheses are not an [ast.ParenExpr] — `func F() (Cases)` parses
		// to a bare identifier, so the single-paren spelling would have
		// exercised the branch above it and passed with the unwrapping
		// deleted. Checked with the parser rather than assumed, which is the
		// only reason this line is right.
		{"func NewCases() ((Cases))", true},
	} {
		parsed, err := parser.ParseFile(token.NewFileSet(), "use.go",
			"package conformance\n\n"+results.source+" { panic(\"unused\") }\n", 0)
		require.NoError(t, err)

		fn := parsed.Decls[0].(*ast.FuncDecl)
		got := isSliceResult(fn.Type.Results.List[0].Type, map[string]bool{
			"Cases": true, "Indirect": true, "Aliased": true,
			"Generic": true, "Parameterised": true,
		})
		if got != results.slice {
			t.Errorf("%s: read as returning a slice=%v, want %v", results.source, got, results.slice)
		}
	}
}

// TestTheCorpusTablesNameOnlyRealFunctions is the other direction, which is
// what makes a deletion visible.
//
// A name left in either table after its function is gone reads as coverage that
// is not there — and in [notACorpus] it is worse, because a stale excuse is the
// next gap's cover, which is the sentence `callers_test.go` already earned.
func TestTheCorpusTablesNameOnlyRealFunctions(t *testing.T) {
	real := map[string]bool{}
	for _, name := range exportedSliceFuncs(t) {
		real[name] = true
	}

	for name := range corpusSizes() {
		if !real[name] {
			t.Errorf("corpusSizes lists %s, which this package no longer exports as a "+
				"slice-returning function; delete the entry", name)
		}
	}

	for name, reason := range notACorpus {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("notACorpus[%q] has no reason; the reason is the record", name)
		}
		if !real[name] {
			t.Errorf("notACorpus lists %s, which this package no longer exports as a "+
				"slice-returning function; delete the entry", name)
		}
	}
}

// exportedSliceFuncs are this package's exported functions that return a
// slice, which is what a corpus looks like from the outside.
//
// "Returns a slice" means the *underlying* type, not the syntax: a corpus
// written `type Cases []Case` and `func NewCases() Cases` has an [ast.Ident]
// for its result, and a walk matching only literal `[]T` drops it — leaving it
// out of both tables with nothing to say so, which is the guard failing
// silently at the one job it has (Codex, #1129). Nothing in the package is
// written that way today, so this is a door held shut rather than one closed.
func exportedSliceFuncs(t *testing.T) []string {
	t.Helper()

	fset := token.NewFileSet()
	names := sliceReturners(exportedDecls(t, fset), sliceTypeNames(t, fset), unresolvedResults)

	if len(names) < 20 {
		t.Fatalf("the walk found %d exported slice-returning function(s), which is too few to "+
			"be this package — a checker that walks nothing and reports every corpus covered "+
			"is the failure this file is about", len(names))
	}

	return names
}

// sliceReturners are the declarations returning a slice — by the walk's own
// reading, or by the account somebody wrote where the walk could not read.
//
// One list either way, which is the whole point. An account marked a slice that
// did not reach here left a corpus refused by `corpusSizes` and excused from
// every emptiness check, with a diagnostic sending its author to the table that
// then rejected it (Codex, #1129).
func sliceReturners(decls []*ast.FuncDecl, slices map[string]bool, accounts map[string]account) []string {
	var names []string
	for _, fd := range decls {
		if fd.Type.Results == nil || len(fd.Type.Results.List) != 1 {
			continue
		}
		if !isSliceResult(fd.Type.Results.List[0].Type, slices) && !accounts[fd.Name.Name].slice {
			continue
		}
		names = append(names, fd.Name.Name)
	}

	sort.Strings(names)

	return names
}

// isSliceResult reports whether a result type is a slice, directly or through
// a name this package declares.
func isSliceResult(result ast.Expr, slices map[string]bool) bool {
	result = coreType(result)

	if isSliceType(result) {
		return true
	}
	// A name declared in this package as a slice, of either spelling — `type
	// Cases []Case` and `type Cases = []Case` both reach here as an identifier.
	name, ok := result.(*ast.Ident)

	return ok && slices[name.Name]
}

// coreType strips what an instantiation or a parenthesis wraps around the
// expression that actually decides the question.
//
// `Cases[Case]` is an [ast.IndexExpr] and `Cases[K, V]` an [ast.IndexListExpr],
// so a generic corpus written `type Cases[T any] []T` was resolved as a slice by
// the walk and then not recognised at the one place that reads the answer
// (Codex, #1129). That is the fixed-array finding again — two branches
// disagreeing about one question — one type-parameter list along.
//
// Unwrapping is what closes the class rather than the instance, and the
// difference is not stylistic. Three shapes are the whole decision: a literal
// slice, a name this package declares, and a name it cannot read. Adding an
// `IndexExpr` case to each reader would leave `pkg.Cases[Case]` matching
// nothing — omitted silently, which is the failure the refusal exists to
// prevent. Unwrapping first means every reader sees the core, so that spelling
// is refused rather than dropped.
func coreType(expr ast.Expr) ast.Expr {
	for {
		switch e := expr.(type) {
		case *ast.ParenExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		default:
			return expr
		}
	}
}

// isSliceType reports whether a type expression is a slice rather than a
// fixed-size array.
//
// One function because two branches asking one question is how they come to
// disagree, and these two did: this one accepted `[4]int` while
// [resolveSliceNames] rejected it, two dozen lines apart, so a helper returning
// a literal array would have been demanded of the corpus table while a named
// one was excused (Codex, #1129). The test written to prove those two agreed
// had the same hole — it exercised the named path and not the literal one.
func isSliceType(expr ast.Expr) bool {
	array, ok := expr.(*ast.ArrayType)

	// A length makes it an array, which is not a corpus by any reading.
	return ok && array.Len == nil
}

// sliceTypeNames are the names this package declares whose underlying type is
// a slice.
//
// Resolved transitively, because `type Cases []Case` followed by `type Many
// Cases` is two hops to the same answer and one hop would stop at the first.
// Iterated to a fixpoint rather than recursed, for the reason the vacuity
// checker iterates its own: a cycle in type declarations does not compile, but
// a walk that assumes so and recurses does not come back if it ever meets one.
func sliceTypeNames(t *testing.T, fset *token.FileSet) map[string]bool {
	t.Helper()

	return resolveSliceNames(parsedFiles(t, fset))
}

// resolveSliceNames is [sliceTypeNames] over files already parsed, so a fixture
// can be handed to it — which is the only way to exercise a shape this package
// does not currently contain.
func resolveSliceNames(files []*ast.File) map[string]bool {
	slices, _ := resolveTypeNames(files)

	return slices
}

// resolveTypeNames are the names this package declares as slices, and the names
// whose declaration it cannot read.
//
// The second half is `type Cases = other.Cases`: an alias to a qualified type,
// which the selector-only refusal missed because a *function* returning `Cases`
// has an identifier for its result, not a selector. One indirection was enough
// to walk past the refusal built for exactly this (Codex, #1129).
func resolveTypeNames(files []*ast.File) (slices, unknown map[string]bool) {
	// name -> the identifier it is declared as, for the indirect case.
	aliasOf := map[string]string{}
	slices = map[string]bool{}
	unknown = map[string]bool{}

	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}

			// Read once. Two lines reaching for `spec.Type` and unwrapping it
			// differently is the divergence this file has already paid for
			// twice, and it would land here first: `type Cases = other.Cases[T]`
			// is a slice question at one line and an unreadability question at
			// the next.
			core := coreType(spec.Type)

			if isSliceType(core) {
				slices[spec.Name.Name] = true

				return true
			}
			switch declared := core.(type) {
			case *ast.Ident:
				aliasOf[spec.Name.Name] = declared.Name
			case *ast.SelectorExpr:
				// Declared in another package, so whether it is a slice is
				// decided there.
				unknown[spec.Name.Name] = true
			}

			return true
		})
	}

	for range len(aliasOf) + 1 {
		changed := false
		for name, of := range aliasOf {
			if !slices[name] && slices[of] {
				slices[name] = true
				changed = true
			}
			// Unreadability propagates the same way an answer does: a name
			// aliased to one this package cannot read is one it cannot read.
			if !unknown[name] && unknown[of] {
				unknown[name] = true
				changed = true
			}
		}
		if !changed {
			break
		}
	}

	return slices, unknown
}

// exportedDecls are this package's exported top-level functions, parsed from
// its non-test files.
//
// Shared with `callers_test.go`, which asks a different question of the same
// set — whether both drivers call each one. Two walks over one directory would
// be two answers to "what does this package export", and this package's whole
// subject is what happens when one thing is written down twice.
func exportedDecls(t *testing.T, fset *token.FileSet) []*ast.FuncDecl {
	t.Helper()

	var decls []*ast.FuncDecl
	for _, f := range parsedFiles(t, fset) {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.IsExported() {
				decls = append(decls, fd)
			}
		}
	}

	// A floor rather than a zero-check, because zero is not the only way a walk
	// goes wrong and it is the least likely one. Both checkers iterate whatever
	// this finds and judge each entry, so a walk that silently *narrows* leaves
	// them green while asking about fewer things.
	//
	// What it does and does not buy, measured rather than assumed. Dropping
	// every `Undo…` export from this walk — five of fifty-three — fails the
	// corpus tables, because those name what they expect and notice a name
	// going missing. It does *not* trip this floor, and no floor that survives
	// somebody adding an export would: the two-driver rule stays insensitive to
	// a partial narrowing, which is a limit of asking "does each thing I found
	// have callers" rather than something this line fixes.
	//
	// So the floor is for gross breakage — a walk pointed at the wrong
	// directory, or a filter that matches nothing — where it is the difference
	// between a loud failure and a clean report about nothing.
	if len(decls) < 30 {
		t.Fatalf("the walk found %d exported function(s) in this package, which is too few to "+
			"be it — the checkers reading this are broken, not the tree", len(decls))
	}

	return decls
}

// parsedFiles are this package's non-test files, parsed.
//
// The one place the directory is read, so every question asked of this package
// — what it exports, what types it declares — is asked of the same set of
// files.
func parsedFiles(t *testing.T, fset *token.FileSet) []*ast.File {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	var files []*ast.File
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}

	return files
}

// TestEveryTableRowCallsTheCorpusItNames closes the one way [corpusSizes] can
// lie while every other check in this file stays green.
//
// The table's key is a name and its value is a call, and nothing tied the two
// together. A row copied and half-edited — `"FooCases": len(BarCases())` —
// satisfies every guard here: the coverage walk sees `FooCases` listed, the
// staleness walk sees `FooCases` exported, and the emptiness check sees a
// positive number, which is Bar's. Foo could then be emptied and both driver
// suites would go vacuous with this file reporting full coverage (Codex, #1129).
//
// It is the file's own subject one level up. A hand-written table is made
// trustworthy by a walk that fails when the table and the tree disagree, and
// until now the walk only compared *names* — so the table could name everything
// correctly and still count the wrong things.
func TestEveryTableRowCallsTheCorpusItNames(t *testing.T) {
	fset := token.NewFileSet()

	// This file, read as source. Every other walk here reads the package's
	// non-test files, which is where the corpora live; the table lives here.
	file, err := parser.ParseFile(fset, corpusTableFile, nil, 0)
	require.NoError(t, err)

	table := funcNamed(file, "corpusSizes")
	require.NotNil(t, table,
		"corpusSizes is gone, renamed, or does something other than return its table in one "+
			"statement — this compares identifier spellings rather than resolving them, so a "+
			"binding before the return would let a key agree with a call that is not it")

	rows, unreadable := tableRows(table)

	for _, what := range unreadable {
		t.Errorf("corpusSizes has a row this check cannot read (%s); every row must be "+
			"`\"XCases\": len(XCases(...))` so the name and the call can be compared — a row "+
			"whose shape is unreadable is a row that could be counting anything", what)
	}

	// A floor, for the same reason [exportedDecls] carries one: a reader that
	// silently matched nothing would report a clean table with total confidence.
	require.GreaterOrEqual(t, len(rows), 40,
		"the table reader found too few rows to be this table, so it is broken rather than the table")

	for _, row := range misnamedRows(rows) {
		t.Errorf("corpusSizes lists %q and counts %s — so %s's size is never checked and it "+
			"may be emptied silently; make the key and the call the same name",
			row.key, row.calls, row.key)
	}
}

// misnamedRows are the rows whose key and call disagree.
//
// Extracted rather than compared in place, for the reason every other decision
// in this file was extracted: the real table agrees with itself, so a
// comparison written inline is one no test can reach, and a mutation deleting
// it survives. That was true of the first version of this and the mutation
// said so.
func misnamedRows(rows []corpusRow) []corpusRow {
	var wrong []corpusRow
	for _, row := range rows {
		if row.key != row.calls {
			wrong = append(wrong, row)
		}
	}

	return wrong
}

// TestAMisnamedTableRowIsCaught exercises the reader, because the tree cannot.
//
// Every row in the real table agrees with itself today, so a check that only
// ran against the table would pass whether the comparison worked or not —
// which is the vacuity this whole file is about, and which caught three of its
// own fixtures already.
func TestAMisnamedTableRowIsCaught(t *testing.T) {
	fixture, err := parser.ParseFile(token.NewFileSet(), "fixture.go", `package conformance

func corpusSizes() map[string]int {
	return map[string]int{
		"AgreeCases":    len(AgreeCases()),
		"ParamCases":    len(ParamCases(standIn)),
		"MisnamedCases": len(SomethingElseCases()),
		"NotACallAtAll": 7,
		"BuiltinCases":  len(make([]int, 3)),
		"ComposedCases": len(ComposedCases(Extra())),
		"CountedCases":  size(CountedCases()),
		constantKey:     len(ConstantKeyCases()),
	}
}
`, 0)
	require.NoError(t, err)

	rows, unreadable := tableRows(funcNamed(fixture, "corpusSizes"))

	var mismatched []string
	for _, row := range misnamedRows(rows) {
		mismatched = append(mismatched, row.key)
	}

	assert.Equal(t, []string{"MisnamedCases"}, mismatched,
		"a row naming one corpus and counting another was read as agreeing with itself")

	// Five unreadable shapes, and they are listed separately because each is
	// refused by a different rule. A single row that tripped two of them —
	// `len(append(X(), y()...))`, which is both a builtin and composed — made
	// the two rules mask each other under mutation: deleting either left the
	// other still refusing the row, and both mutations survived. So each rule
	// gets a row only it can refuse.
	//
	// Refused rather than skipped, in every case. A row that is not a `len` of
	// a call cannot be compared at all, and passing over it would make this
	// check quietly optional — which is the failure `unresolvedResults` one
	// table up exists to prevent, wearing different clothes.
	assert.ElementsMatch(t,
		[]string{
			"NotACallAtAll", "BuiltinCases", "ComposedCases", "CountedCases",
			// A key that is not a string literal, named by what it says rather
			// than by what it means — this cannot evaluate a constant, which is
			// the whole reason the row is refused.
			"constantKey",
		}, unreadable,
		"a row whose shape this cannot read was passed over rather than refused")

	assert.Len(t, rows, 3, "an unreadable row must not also be counted as read")

	// An element that is not a key-value pair at all. Unreachable through a map
	// literal, which is what the table is — Go will not parse a bare element
	// there — so it is exercised through the shape that can carry one. The
	// branch stays because the reader takes whatever composite literal a
	// function returns, and "the table is a map today" is precisely the kind of
	// premise this file exists to stop relying on.
	// A body that does anything before returning its table. This compares
	// identifier spellings rather than resolving them — it parses, it does not
	// type-check — so an alias bound here would make a key agree with a call
	// that is not it, and the agreement would be perfect (Codex, #1141).
	//
	// Refused whole rather than resolved, which is the deliberate stopping
	// point: resolving bindings is the road to a type checker, and what this
	// check exists to catch is a row copied and half-edited, not an author
	// working to defeat it. That threat model is what makes "refuse every shape
	// I cannot read" a complete answer rather than an arms race.
	t.Run("a table with anything before it is refused whole", func(t *testing.T) {
		aliased, err := parser.ParseFile(token.NewFileSet(), "aliased.go", `package conformance

func corpusSizes() map[string]int {
	ValueCases := WaitCases

	return map[string]int{
		"ValueCases": len(ValueCases()),
	}
}
`, 0)
		require.NoError(t, err)

		rows, unreadable := tableRows(funcNamed(aliased, "corpusSizes"))

		assert.Empty(t, rows,
			"a row was read out of a table the walk cannot trust, and it agreed with itself")
		assert.Empty(t, unreadable)
		assert.Nil(t, returnedLiteral(funcNamed(aliased, "corpusSizes")),
			"the table must be refused whole, so the check fails loudly rather than reporting nothing")

		// And the contract is "the body is exactly one return", not "the body
		// begins with one". Without a case where the return comes *first* and
		// something follows it, relaxing the count is invisible: the fixture
		// above refuses on its first statement not being a return, and would go
		// on refusing however the count were weakened. Two rules, one fixture,
		// masking each other — the third time on this pull request.
		trailing, err := parser.ParseFile(token.NewFileSet(), "trailing.go", `package conformance

func corpusSizes() map[string]int {
	return map[string]int{
		"ValueCases": len(ValueCases()),
	}
	ValueCases := WaitCases
	_ = ValueCases
}
`, 0)
		require.NoError(t, err)

		assert.Nil(t, returnedLiteral(funcNamed(trailing, "corpusSizes")),
			"a body whose return is merely first was accepted, so the count is not what is checked")
	})

	t.Run("an element that is not a row is refused", func(t *testing.T) {
		bare, err := parser.ParseFile(token.NewFileSet(), "bare.go", `package conformance

func corpusSizes() []int {
	return []int{1, two}
}
`, 0)
		require.NoError(t, err)

		rows, unreadable := tableRows(funcNamed(bare, "corpusSizes"))

		assert.Empty(t, rows)
		assert.Equal(t, []string{"1", "two"}, unreadable)
	})
}

// corpusTableFile is where [corpusSizes] is written, read as source by the
// check above. Named rather than discovered, because a check that hunted for
// the table would find nothing after a rename and report success.
const corpusTableFile = "corpora_test.go"

// corpusRow is one row of the size table: the name it claims and the corpus it
// actually counts.
type corpusRow struct {
	key   string
	calls string
}

// tableRows reads the rows of the `map[string]int` literal a function returns,
// and names the ones it cannot read.
//
// Takes a declaration rather than reading the file, so the fixture above can
// hand it shapes this package does not contain — the same reason
// [unreadableResults] and [sliceReturners] take their inputs.
func tableRows(fn *ast.FuncDecl) (rows []corpusRow, unreadable []string) {
	literal := returnedLiteral(fn)
	if literal == nil {
		return nil, nil
	}

	for _, element := range literal.Elts {
		entry, ok := element.(*ast.KeyValueExpr)
		if !ok {
			unreadable = append(unreadable, types.ExprString(element))

			continue
		}

		name, ok := literalKey(entry.Key)
		if !ok {
			// A key that is not a string literal is a key this cannot compare,
			// and skipping it would make the check quietly optional for exactly
			// the row somebody wrote cleverly. `fooCasesName: len(BarCases())`
			// with a constant for the key is invisible here and perfectly
			// visible to the runtime checks, which see the resulting `FooCases`
			// and are satisfied — so FooCases could be emptied with this check
			// reporting a clean table (Codex, #1141).
			unreadable = append(unreadable, types.ExprString(entry.Key))

			continue
		}

		called, ok := lenOfCall(entry.Value)
		if !ok {
			unreadable = append(unreadable, name)

			continue
		}
		rows = append(rows, corpusRow{key: name, calls: called})
	}

	return rows, unreadable
}

// returnedLiteral is the composite literal a function returns, when returning it
// is the only thing the function does.
//
// The table's own elements, rather than every key-value pair anywhere inside
// the function. An [ast.Inspect] over the whole declaration would descend into
// a value's own composite literals and read their entries as rows — nothing in
// the table is written that way today, which is the condition under which this
// file's mistakes have always been made.
//
// "The only thing the function does" is the second refusal, and it is the one
// that makes the comparison mean anything. This parses rather than type-checks,
// so it compares identifier *spellings*: a binding introduced before the return
// — `ValueCases := WaitCases` — makes `"ValueCases": len(ValueCases())` agree
// with itself while counting something else entirely (Codex, #1141). Rather than
// resolve bindings, which is the road to a type checker, a body that is anything
// but one return statement is refused and the check says so.
func returnedLiteral(fn *ast.FuncDecl) *ast.CompositeLit {
	if fn == nil || fn.Body == nil || len(fn.Body.List) != 1 {
		return nil
	}
	ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return nil
	}
	literal, _ := ret.Results[0].(*ast.CompositeLit)

	return literal
}

// literalKey is the string a key spells, when it spells one literally.
func literalKey(expr ast.Expr) (string, bool) {
	key, ok := expr.(*ast.BasicLit)
	if !ok || key.Kind != token.STRING {
		return "", false
	}
	name, err := strconv.Unquote(key.Value)
	if err != nil {
		return "", false
	}

	return name, true
}

// lenOfCall reads `len(X(...))` and reports X.
//
// Deliberately exact. `len(append(X(), y()...))` names two functions and there
// is no honest answer to which one the row counts, so it is refused and its
// author writes the row plainly — a reader that guessed would be inventing the
// agreement this check exists to verify.
func lenOfCall(expr ast.Expr) (string, bool) {
	outer, ok := expr.(*ast.CallExpr)
	if !ok {
		return "", false
	}
	if name, ok := outer.Fun.(*ast.Ident); !ok || name.Name != "len" || len(outer.Args) != 1 {
		return "", false
	}
	inner, ok := outer.Args[0].(*ast.CallExpr)
	if !ok {
		return "", false
	}
	// Exported, which is what a corpus is and what a builtin is not. Without
	// this, `len(append(X(), y()...))` reads as a row calling `append` — a
	// perfectly good identifier — and is recorded rather than refused. The
	// fixture caught that in the first version of this reader, which is the
	// second time on this file that writing the negative case found the bug.
	corpus, ok := inner.Fun.(*ast.Ident)
	if !ok || !corpus.IsExported() {
		return "", false
	}
	// And nothing composed. A row whose call takes another call as an argument
	// names two corpora and there is no honest answer to which one it counts,
	// so it is refused and its author writes the row plainly. Every real row
	// passes a bare `standIn` or nothing at all.
	for _, arg := range inner.Args {
		if composed := containsCall(arg); composed {
			return "", false
		}
	}

	return corpus.Name, true
}

// containsCall reports whether an expression calls anything.
func containsCall(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if _, ok := node.(*ast.CallExpr); ok {
			found = true
		}

		return !found
	})

	return found
}

// funcNamed is the top-level function declaration of that name, or nil.
func funcNamed(file *ast.File, name string) *ast.FuncDecl {
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return fn
		}
	}

	return nil
}
