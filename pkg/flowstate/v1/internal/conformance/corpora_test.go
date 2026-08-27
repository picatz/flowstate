package conformance

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
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
func exportedSliceFuncs(t *testing.T) []string {
	t.Helper()

	var names []string
	for _, fd := range exportedDecls(t, token.NewFileSet()) {
		if fd.Type.Results == nil || len(fd.Type.Results.List) != 1 {
			continue
		}
		if _, ok := fd.Type.Results.List[0].Type.(*ast.ArrayType); !ok {
			continue
		}
		names = append(names, fd.Name.Name)
	}

	sort.Strings(names)

	if len(names) < 20 {
		t.Fatalf("the walk found %d exported slice-returning function(s), which is too few to "+
			"be this package — a checker that walks nothing and reports every corpus covered "+
			"is the failure this file is about", len(names))
	}

	return names
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

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	var decls []*ast.FuncDecl
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
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
