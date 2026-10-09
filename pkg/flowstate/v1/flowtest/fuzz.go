package flowtest

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/goccy/go-yaml"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// MaxFuzzRuns bounds `--fuzz N`: every run executes a whole case, so the cost
// is N times the suite's.
const MaxFuzzRuns = 10_000

// DefaultFuzzSeed0 is the seed the first generated case of every fuzzed case
// uses; run k uses DefaultFuzzSeed0 + k.
const DefaultFuzzSeed0 uint64 = 1

// maxGeneratedString bounds a generated string, whatever `max_len:` allows.
const maxGeneratedString = 4096

// FuzzOptions asks [RunOptions] to run each case over generated inputs.
type FuzzOptions struct {
	// Runs is how many generated cases to run per authored case; zero asks for
	// no fuzzing.
	Runs int

	// Seed, when Pinned, replays exactly the generated case that seed names.
	Seed   uint64
	Pinned bool
}

func (o FuzzOptions) enabled() bool { return o.Runs > 0 || o.Pinned }

// fuzzer accumulates one file's fuzzing.
type fuzzer struct {
	opts         FuzzOptions
	runs         int
	cases        int
	inconclusive int
	skipped      map[string]bool
	finding      *v1.FuzzFinding
}

func newFuzzer(opts FuzzOptions) *fuzzer {
	if !opts.enabled() {
		return nil
	}

	// The bound lives here as well as in the command: a library caller of
	// [RunOptions] must not be able to ask for more than the command would.
	opts.Runs = min(opts.Runs, MaxFuzzRuns)

	return &fuzzer{opts: opts, skipped: map[string]bool{}}
}

// report is the machine account, or nil when nothing was asked.
func (f *fuzzer) report() *v1.FuzzReport {
	if f == nil {
		return nil
	}

	return &v1.FuzzReport{
		Runs:          int32(f.runs),
		Cases:         int32(f.cases),
		Inconclusive:  int32(f.inconclusive),
		SkippedInputs: slices.Sorted(maps.Keys(f.skipped)),
		Finding:       f.finding,
	}
}

// run fuzzes one authored case: it generates inputs for the workflow's declared
// inputs around the case's own, runs the case over each, and stops at the first
// generated case that breaks a property.
//
// A generated case is judged by what must hold of every input: the run does not
// fail with an Internal or Expression error (a `no such key` or `no such
// overload` is a defect of the program, found by an input rather than by a
// reviewer), and the case's `invariants:` hold. The case's `expect:` describes
// the authored inputs and is not applied, and `faults:` are not injected: this
// is the input dimension, run apart from the failure dimension.
func (f *fuzzer) run(ctx context.Context, test *Test, spec *v1.Workflow, deliveryPath string,
	load func() (*v1.Workflow, error), vars fileVars, timeout time.Duration) {
	if f == nil || f.finding != nil {
		return
	}
	// A workflow with nothing to generate (no declared input of a generated
	// type) is named in the report and not counted as fuzzed: running the
	// authored inputs again N times would read as N generated cases that
	// proved something.
	if slots, skipped := inputSlots(spec); len(slots) == 0 {
		for _, s := range skipped {
			f.skipped[s] = true
		}

		return
	}
	f.cases++

	// Computed as it goes, so a large Runs allocates nothing up front.
	count := f.opts.Runs
	if f.opts.Pinned {
		count = 1
	}

	sensitive := v1.SensitiveInputNames(spec)
	// judge runs the case once over inputs and says what, if anything, is
	// wrong with the run: the one verdict the search and the shrink share.
	judge := func(inputs map[string]any) ([]string, caseShown, judgeVerdict) {
		generated := *test
		generated.Inputs = inputs
		generated.Expect = Expectation{}
		generated.Faults = nil
		runCtx, cancel := caseContextWithin(ctx, timeout)
		result, _, _, _, shown, runErr := runCase(runCtx, &generated, deliveryPath, load, false, vars)
		cancel()
		// A cancelled suite is not a verdict about the workflow: the run's
		// error would classify as Internal and read as a defect an input found.
		if ctx.Err() != nil {
			return nil, shown, judgeCancelled
		}

		// An invocation no stub answers says nothing about the workflow: the
		// case's stubs were written for its own inputs, and a generated one can
		// route a call past every `where:`. Inconclusive, like a case that
		// errored before the run.
		var unanswered *stubDiagnostic
		if result.GetError() != "" || errors.As(runErr, &unanswered) {
			return nil, shown, judgeInconclusive
		}
		var problems []string
		if kind := v1.ClassifyError(runErr); runErr != nil && (kind == v1.ErrorKindInternal || kind == v1.ErrorKindExpression) {
			problems = append(problems, fmt.Sprintf("the run failed with an %s error: %s", kind, shown.runErrorUnder(shown.sensitive)))
		}
		// Only the invariants: the case's `expect:` was cleared because it
		// describes the authored inputs, and what is left of it (a run that
		// failed with nothing declaring it should) is not a property of every
		// input.
		for _, failure := range result.GetFailures() {
			if strings.HasPrefix(failure.GetField(), "invariants") {
				problems = append(problems, failure.GetField()+": "+failure.GetMessage())
			}
		}

		return problems, shown, judgeJudged
	}
	for k := range count {
		seed := DefaultFuzzSeed0 + uint64(k)
		if f.opts.Pinned {
			seed = f.opts.Seed
		}
		if ctx.Err() != nil {
			return
		}
		inputs, skipped, ok := generateInputs(spec, test.Inputs, seed)
		for _, s := range skipped {
			f.skipped[s] = true
		}
		if !ok {
			f.inconclusive++

			continue
		}

		problems, shown, verdict := judge(inputs)
		switch verdict {
		case judgeCancelled:
			return
		case judgeInconclusive:
			f.inconclusive++

			continue
		}
		f.runs++
		if len(problems) == 0 {
			continue
		}

		// The last violating probe is the one the shrink ended on, since the
		// search only moves to a probe that violated: its failure text and
		// redaction state are the ones to report, with no replay to spend.
		var lastProblems []string
		lastShown := shown
		shrunk := shrinkInputs(test.Inputs, inputs, sensitive, func(candidate map[string]any) (violated, ok, conclusive bool) {
			again, shownAgain, verdict := judge(candidate)
			switch {
			case verdict == judgeCancelled:
				return false, false, false
			case len(again) > 0:
				lastProblems, lastShown = again, shownAgain

				return true, true, true
			}

			return false, true, verdict == judgeJudged
		})
		if shrunk.Reproduced && lastProblems != nil {
			problems, shown, inputs = lastProblems, lastShown, shrunk.Inputs
		}

		f.finding = &v1.FuzzFinding{
			Case:       redactedErrorText(test.Name, shown.sensitive),
			Seed:       seed,
			Inputs:     redactedErrorText(pasteableInputs(overlayOf(test.Inputs, inputs), sensitive), shown.sensitive),
			Absent:     redactedNames(absentInputs(test.Inputs, inputs, sensitive), shown.sensitive),
			Failure:    redactedErrorText(strings.Join(problems, "\n"), shown.sensitive),
			Changed:    int32(shrunk.From),
			ShrinkRuns: int32(shrunk.Runs),
			Minimal:    shrunk.Minimal,
		}

		return
	}
}

// judgeVerdict is whether one generated run said anything about the workflow.
type judgeVerdict int

const (
	// judgeJudged: the run reached a verdict, clean or not.
	judgeJudged judgeVerdict = iota
	// judgeInconclusive: the run errored before it ran, or hit an invocation
	// no stub answers, so it says nothing.
	judgeInconclusive
	// judgeCancelled: the suite was cancelled; nothing was learned.
	judgeCancelled
)

// maxInputShrinkRuns bounds the re-runs spent shrinking one finding's inputs,
// the same budget a violating seed's faults get.
const maxInputShrinkRuns = MaxShrinkRuns

// inputShrink is what [shrinkInputs] found.
type inputShrink struct {
	// Inputs is the generated set reduced to the inputs that matter: every
	// other input is back at the case's own value.
	Inputs map[string]any
	// From is how many inputs the seed changed from the case's own.
	From int
	// Runs is how many probes were spent.
	Runs int
	// Reproduced reports that the full generated set violated again when
	// replayed; when it did not nothing was shrunk.
	Reproduced bool
	// Minimal reports that putting any one remaining input back at its own
	// value stopped the failure.
	Minimal bool
}

// shrinkInputs reduces a failing generated input set to the inputs that cause
// the failure, by [ddmin] over the inputs the seed changed: each candidate puts
// every other input back at the case's own value (or leaves it out when the
// case never supplied it). It is 1-minimal, not the smallest set there is, and
// reproduces *a* failure, which need not be the first. Sensitive inputs are
// never generated, so they are never among the changed ones.
//
// violates reports whether a run over exactly that input set breaks the case,
// whether it answered at all (ok false: the suite was cancelled, and the search
// ends), and whether it was a verdict about the workflow (conclusive false: the
// candidate was refused at submit or errored before the run, which does not
// reproduce the failure but does not prove the inputs it put back matter, so
// the result is reported as not minimal). The empty set, every input put back,
// is probed last: a failure the case's own inputs already have is not caused by
// any generated one.
func shrinkInputs(base, generated map[string]any, sensitive map[string]bool, violates func(map[string]any) (violated, ok, conclusive bool)) inputShrink {
	changed := changedInputs(base, generated, sensitive)
	unconfirmed := false
	probe := func(names []string) (bool, bool) {
		violated, ok, conclusive := violates(withInputs(base, generated, names))
		if ok && !violated && !conclusive {
			unconfirmed = true
		}

		return violated, ok
	}
	// One probe is kept back for the empty set.
	r := ddmin(changed, maxInputShrinkRuns-1, probe)
	out := inputShrink{Inputs: generated, From: len(changed), Runs: r.Runs, Reproduced: r.Reproduced}
	if !r.Reproduced {
		return out
	}
	kept, minimal := r.Kept, r.Minimal
	if len(kept) > 0 && minimal {
		out.Runs++
		violated, ok := probe(nil)
		switch {
		case !ok:
			minimal = false
		case violated:
			kept = nil
		}
	}
	out.Inputs = withInputs(base, generated, kept)
	out.Minimal = minimal && !unconfirmed

	return out
}

// redactedNames withholds each name as the case's own report withholds text.
func redactedNames(names []string, sensitive sensitiveInputs) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = redactedErrorText(name, sensitive)
	}

	return out
}

// changedInputs names, sorted, the non-sensitive inputs generated differs from
// base in: a different value, or a value base never supplied, or a value base
// supplied that generated left out.
func changedInputs(base, generated map[string]any, sensitive map[string]bool) []string {
	var names []string
	for name := range maps.Keys(generated) {
		if !sensitive[name] && !sameInput(base, generated, name) {
			names = append(names, name)
		}
	}
	for name := range maps.Keys(base) {
		if _, kept := generated[name]; !kept && !sensitive[name] {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	return slices.Compact(names)
}

func sameInput(a, b map[string]any, name string) bool {
	av, aok := a[name]
	bv, bok := b[name]

	return aok == bok && reflect.DeepEqual(av, bv)
}

// withInputs is base with the named inputs taken from generated: set where
// generated has them, deleted where it left them out.
func withInputs(base, generated map[string]any, names []string) map[string]any {
	out := maps.Clone(base)
	if out == nil {
		out = map[string]any{}
	}
	for _, name := range names {
		if v, ok := generated[name]; ok {
			out[name] = v
		} else {
			delete(out, name)
		}
	}

	return out
}

// overlayOf is the part of inputs the case's own inputs do not already say: the
// `inputs:` overlay to merge over them.
func overlayOf(base, inputs map[string]any) map[string]any {
	overlay := map[string]any{}
	for name, v := range inputs {
		if !sameInput(base, inputs, name) {
			overlay[name] = v
		}
	}

	return overlay
}

// absentInputs names, sorted, the non-sensitive inputs the case supplies and
// the run left out, which an overlay cannot say.
func absentInputs(base, inputs map[string]any, sensitive map[string]bool) []string {
	var names []string
	for name := range maps.Keys(base) {
		if _, kept := inputs[name]; !kept && !sensitive[name] {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	return names
}

// pasteableInputs renders inputs as the `inputs:` overlay that reproduces a
// failure, leaving out every input the workflow declares sensitive: merge it
// over the case's own `inputs:`, which keep their sensitive entries.
func pasteableInputs(inputs map[string]any, sensitive map[string]bool) string {
	shown := map[string]any{}
	for name, value := range inputs {
		if !sensitive[name] {
			shown[name] = value
		}
	}
	out, err := yaml.Marshal(map[string]any{"inputs": shown})
	if err != nil {
		return ""
	}

	return string(out)
}

// generateInputs draws one input set for spec around base, deterministically
// from seed. It reports the declared inputs it generated nothing for, and false
// when no draw the declaration accepts at submit was found.
//
// The first seeds walk a corpus: every boundary value of every input in turn,
// and every optional input absent, with the others left at the case's own
// values, so a small `--fuzz N` still exercises each boundary and a failure is
// localized to the one input that moved. Past the corpus, and for a corpus
// entry the declaration refuses, the seed draws a random combination.
//
// Every candidate set is bound through [v1.BindRunInputs], the path a real
// submit takes, so a generated value the declaration refuses (a `must:` it
// fails, a length out of bounds) is never run: a refused input is a test of the
// binder, not of the workflow.
func generateInputs(spec *v1.Workflow, base map[string]any, seed uint64) (map[string]any, []string, bool) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))

	slots, skipped := inputSlots(spec)

	if seed >= DefaultFuzzSeed0 {
		if inputs, ok := corpusInputs(spec, base, slots, seed-DefaultFuzzSeed0); ok {
			return inputs, skipped, true
		}
	}

	for range 16 {
		inputs := maps.Clone(base)
		if inputs == nil {
			inputs = map[string]any{}
		}
		for _, s := range slots {
			switch roll := rng.IntN(8); {
			case roll == 0 && !s.required:
				delete(inputs, s.name)
			case roll == 1:
				// The case's own value, so one input moves at a time often enough
				// to localize a failure to it.
			default:
				inputs[s.name] = s.candidates[rng.IntN(len(s.candidates))]
			}
		}
		if _, err := v1.BindRunInputs(spec, v1.NewNamedValues(inputs)); err == nil {
			return inputs, skipped, true
		}
	}

	return nil, skipped, false
}

// corpusInputs is the n-th deterministic corpus entry: one input at one of its
// boundary values, or absent when optional, the rest as the case wrote them.
// False when n is past the corpus or the declaration refuses that entry.
func corpusInputs(spec *v1.Workflow, base map[string]any, slots []inputSlot, n uint64) (map[string]any, bool) {
	for _, s := range slots {
		variants := len(s.candidates)
		if !s.required {
			variants++
		}
		if n >= uint64(variants) {
			n -= uint64(variants)

			continue
		}
		inputs := maps.Clone(base)
		if inputs == nil {
			inputs = map[string]any{}
		}
		switch {
		case !s.required && n == 0:
			delete(inputs, s.name)
		case !s.required:
			inputs[s.name] = s.candidates[n-1]
		default:
			inputs[s.name] = s.candidates[n]
		}
		if _, err := v1.BindRunInputs(spec, v1.NewNamedValues(inputs)); err != nil {
			return nil, false
		}

		return inputs, true
	}

	return nil, false
}

// inputSlot is one declared input a draw can vary.
type inputSlot struct {
	name       string
	candidates []any
	required   bool
}

// inputSlots are the declared inputs something is generated for, and why
// nothing is for each of the rest, in declaration order.
func inputSlots(spec *v1.Workflow) (slots []inputSlot, skipped []string) {
	table := v1.TypesOf(spec)
	for _, d := range spec.GetDeclaredInputs() {
		if d.GetSensitive() {
			skipped = append(skipped, fmt.Sprintf("%s: declared sensitive, never generated", d.GetName()))

			continue
		}
		// A record that holds a sensitive field is sensitive whole, as everywhere
		// else a run is shown, even when the input was not marked so.
		if table.HoldsSensitive(d.DeclaredType()) {
			skipped = append(skipped, fmt.Sprintf("%s: its type holds a sensitive field, never generated", d.GetName()))

			continue
		}
		candidates, why := inputCandidates(table, d)
		if len(candidates) == 0 {
			skipped = append(skipped, fmt.Sprintf("%s: %s", d.GetName(), why))

			continue
		}
		slots = append(slots, inputSlot{d.GetName(), candidates, d.GetRequired()})
	}

	return slots, skipped
}

const (
	// maxTypeCandidates bounds the candidates generated for one list, map or
	// record, whatever its parts offer: a nested type picks from its parts' sets
	// and never takes their product.
	maxTypeCandidates = 6

	// maxCandidateNodes bounds the size of one generated composite, counted in
	// nodes, so a list of lists of lists cannot double its way past memory.
	maxCandidateNodes = 1024

	// maxGeneratorSteps bounds the types one declaration's generator visits. A
	// record is expanded once per field that names it, so records that each name the
	// next twice cost twice as much per level; the caps above bound what is kept,
	// and this bounds the work of producing it.
	maxGeneratorSteps = 4096

	// maxGeneratedItems bounds a list built to a declaration's `min_items:`.
	maxGeneratedItems = 64
)

// inputCandidates are the boundary values worth trying for one declaration,
// or why there are none. table resolves the records the declaration names.
func inputCandidates(table v1.TypeTable, d *v1.InputDeclaration) ([]any, string) {
	g := &typeGenerator{table: table, expanding: map[string]bool{}}

	candidates, why := g.candidates(d.DeclaredType(), d, 0)
	if g.steps > maxGeneratorSteps {
		// Not a partial answer: a record with fields quietly dropped would be
		// generated as though it were whole.
		return nil, fmt.Sprintf("its type nests more than %d levels of records and lists to draw from", maxGeneratorSteps)
	}

	return candidates, why
}

// typeGenerator draws candidates from a structural type.
type typeGenerator struct {
	table v1.TypeTable
	// expanding holds the records being expanded, so a record that reaches itself
	// ends instead of recursing.
	expanding map[string]bool
	// steps counts the types visited, against [maxGeneratorSteps].
	steps int
}

// candidates are the values worth trying for type t. d, which may be nil, is
// the declaration that carries t's constraints (an input, or a record's field);
// an element of a list or map has none.
func (g *typeGenerator) candidates(t *v1.Type, d *v1.InputDeclaration, depth int) ([]any, string) {
	if len(d.GetValues()) > 0 {
		out := make([]any, 0, len(d.GetValues()))
		for _, v := range d.GetValues() {
			out = append(out, v)
		}

		return out, ""
	}
	if depth > v1.MaxStructureDepth {
		return nil, fmt.Sprintf("nested deeper than %d levels", v1.MaxStructureDepth)
	}
	if g.steps++; g.steps > maxGeneratorSteps {
		return nil, "too many nested types"
	}

	switch kind := t.GetKind().(type) {
	case *v1.Type_Scalar_:
		return scalarCandidates(kind.Scalar, d)
	case *v1.Type_Enum:
		return nil, "an enum that declares no values"
	case *v1.Type_List:
		elems, why := g.candidates(kind.List, nil, depth+1)
		if len(elems) == 0 {
			return nil, fmt.Sprintf("%s has no element to draw: %s", v1.TypeString(t), why)
		}
		picked := pick(elems, 2)
		out := []any{[]any{}}
		for _, c := range picked {
			out = append(out, []any{c})
		}
		out = append(out, []any{picked[0], picked[len(picked)-1]})
		if n := d.GetMinItems(); n > 1 {
			out = append(out, slices.Repeat([]any{picked[0]}, int(min(n, maxGeneratedItems))))
		}

		return bounded(out)
	case *v1.Type_Map_:
		elems, why := g.candidates(kind.Map.GetValue(), nil, depth+1)
		if len(elems) == 0 {
			return nil, fmt.Sprintf("%s has no value to draw: %s", v1.TypeString(t), why)
		}
		out := []any{map[string]any{}}
		for _, c := range pick(elems, 2) {
			out = append(out, map[string]any{"k": c})
		}

		return bounded(out)
	case *v1.Type_Message:
		return g.record(kind.Message, depth)
	default:
		return nil, fmt.Sprintf("%s inputs are not generated: no declared type to draw from", v1.TypeString(t))
	}
}

// record are the values of the record type name: closed objects built from its
// fields, some with every field and some with only the required ones.
func (g *typeGenerator) record(name string, depth int) ([]any, string) {
	declared, ok := g.table[name]
	switch {
	case !ok:
		return nil, fmt.Sprintf("record %s is not declared under `types:`", name)
	case g.expanding[name]:
		return nil, fmt.Sprintf("record %s refers to itself", name)
	}
	g.expanding[name] = true
	defer delete(g.expanding, name)

	type fieldSet struct {
		name       string
		required   bool
		candidates []any
	}
	var fields []fieldSet
	for _, f := range declared.GetFields() {
		candidates, why := g.candidates(f.DeclaredType(), f, depth+1)
		switch {
		case len(candidates) > 0:
			fields = append(fields, fieldSet{f.GetName(), f.GetRequired(), candidates})
		case f.GetRequired():
			return nil, fmt.Sprintf("record %s, field %s: %s", name, f.GetName(), why)
		}
		// An optional field nothing is generated for stays absent.
	}

	build := func(i int, all bool) map[string]any {
		object := map[string]any{}
		for _, f := range fields {
			if all || f.required {
				object[f.name] = f.candidates[i%len(f.candidates)]
			}
		}

		return object
	}
	var out []any
	for i := range 3 {
		for _, object := range []map[string]any{build(i, true), build(i, false)} {
			if !slices.ContainsFunc(out, func(seen any) bool { return reflect.DeepEqual(seen, object) }) {
				out = append(out, object)
			}
		}
	}

	return bounded(out)
}

// pick is at most n of candidates, spread from the first to the last.
func pick(candidates []any, n int) []any {
	if len(candidates) <= n {
		return candidates
	}
	out := make([]any, n)
	for i := range n {
		out[i] = candidates[i*(len(candidates)-1)/(n-1)]
	}

	return out
}

// bounded drops the candidates larger than [maxCandidateNodes] and keeps at
// most [maxTypeCandidates] of the rest.
func bounded(candidates []any) ([]any, string) {
	out := slices.DeleteFunc(candidates, func(c any) bool { return nodes(c) > maxCandidateNodes })
	if len(out) == 0 {
		return nil, fmt.Sprintf("every generated value is larger than %d nodes", maxCandidateNodes)
	}

	return out[:min(len(out), maxTypeCandidates)], ""
}

// nodes counts the values in v, a composite's members included.
func nodes(v any) int {
	switch v := v.(type) {
	case []any:
		n := 1
		for _, e := range v {
			n += nodes(e)
		}

		return n
	case map[string]any:
		n := 1
		for _, e := range v {
			n += nodes(e)
		}

		return n
	}

	return 1
}

// scalarCandidates are the boundary values of a scalar, shaped under d's length
// bounds. A timestamp is the RFC 3339 text, a duration the Go duration text and
// bytes the standard base64 text, the spellings a test file and a submit bind.
func scalarCandidates(s v1.Type_Scalar, d *v1.InputDeclaration) ([]any, string) {
	switch s {
	case v1.Type_SCALAR_STRING:
		out := []any{"", "x", "é✓ ", "${1 + 1}"}
		if d.GetMinLen() > 0 {
			out = append(out, strings.Repeat("a", int(min(d.GetMinLen(), maxGeneratedString))))
		}
		longest := uint64(64)
		if d != nil && d.MaxLen != nil {
			longest = d.GetMaxLen()
		}
		out = append(out, strings.Repeat("a", int(min(longest, maxGeneratedString))))

		return out, ""
	case v1.Type_SCALAR_INT:
		return []any{int64(0), int64(1), int64(-1), int64(2), int64(100), int64(1 << 31), int64(-(1 << 31)), int64(1<<53 - 1)}, ""
	case v1.Type_SCALAR_DOUBLE:
		return []any{0.0, 1.5, -1.5, 1e-9, 1e9, 0.1}, ""
	case v1.Type_SCALAR_BOOL:
		return []any{true, false}, ""
	case v1.Type_SCALAR_TIMESTAMP:
		return []any{"1970-01-01T00:00:00Z", "2026-02-28T12:30:00Z", "2024-02-29T23:59:59+05:30", "9999-12-31T23:59:59Z"}, ""
	case v1.Type_SCALAR_DURATION:
		return []any{"0s", "1ns", "90m", "-1s", "8760h"}, ""
	case v1.Type_SCALAR_BYTES:
		return []any{"", "AA==", "aGVsbG8=", base64.StdEncoding.EncodeToString(make([]byte, 256))}, ""
	default:
		return nil, fmt.Sprintf("%s inputs are not generated yet", strings.ToLower(strings.TrimPrefix(s.String(), "SCALAR_")))
	}
}
