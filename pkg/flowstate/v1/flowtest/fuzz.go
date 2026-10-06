package flowtest

import (
	"context"
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

		shrunk := shrinkInputs(test.Inputs, inputs, sensitive, func(candidate map[string]any) (bool, bool) {
			again, _, verdict := judge(candidate)

			return len(again) > 0, verdict != judgeCancelled
		})
		if shrunk.Reproduced {
			// The finding names the smaller input set's failure: the probe
			// that reproduced it is the last violating run, and its text is
			// what the author will see when they paste the overlay.
			if final, shownFinal, v := judge(shrunk.Inputs); v == judgeJudged && len(final) > 0 {
				problems, shown = final, shownFinal
			}
			inputs = shrunk.Inputs
		}

		f.finding = &v1.FuzzFinding{
			Case:       redactedErrorText(test.Name, shown.sensitive),
			Seed:       seed,
			Inputs:     redactedErrorText(pasteableInputs(overlayOf(test.Inputs, inputs), sensitive), shown.sensitive),
			Absent:     absentInputs(test.Inputs, inputs, sensitive),
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
func shrinkInputs(base, generated map[string]any, sensitive map[string]bool, violates func(map[string]any) (violated, ok bool)) inputShrink {
	changed := changedInputs(base, generated, sensitive)
	r := ddmin(changed, maxInputShrinkRuns, func(subset []string) (bool, bool) {
		return violates(withInputs(base, generated, subset))
	})
	out := inputShrink{Inputs: generated, From: len(changed), Runs: r.Runs, Reproduced: r.Reproduced, Minimal: r.Minimal}
	if r.Reproduced {
		out.Inputs = withInputs(base, generated, r.Kept)
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
	for _, d := range spec.GetDeclaredInputs() {
		if d.GetSensitive() {
			skipped = append(skipped, fmt.Sprintf("%s: declared sensitive, never generated", d.GetName()))

			continue
		}
		candidates, why := inputCandidates(d)
		if len(candidates) == 0 {
			skipped = append(skipped, fmt.Sprintf("%s: %s", d.GetName(), why))

			continue
		}
		slots = append(slots, inputSlot{d.GetName(), candidates, d.GetRequired()})
	}

	return slots, skipped
}

// inputCandidates are the boundary values worth trying for one declaration,
// or why there are none.
func inputCandidates(d *v1.InputDeclaration) ([]any, string) {
	if len(d.GetValues()) > 0 {
		out := make([]any, 0, len(d.GetValues()))
		for _, v := range d.GetValues() {
			out = append(out, v)
		}

		return out, ""
	}
	switch d.GetType() {
	case v1.InputDeclaration_TYPE_STRING:
		out := []any{"", "x", "é✓ ", "${1 + 1}"}
		if d.MinLen != nil && d.GetMinLen() > 0 {
			out = append(out, strings.Repeat("a", int(min(d.GetMinLen(), maxGeneratedString))))
		}
		longest := uint64(64)
		if d.MaxLen != nil {
			longest = d.GetMaxLen()
		}
		out = append(out, strings.Repeat("a", int(min(longest, maxGeneratedString))))

		return out, ""
	case v1.InputDeclaration_TYPE_INT:
		return []any{int64(0), int64(1), int64(-1), int64(2), int64(100), int64(1 << 31), int64(-(1 << 31)), int64(1<<53 - 1)}, ""
	case v1.InputDeclaration_TYPE_FLOAT:
		return []any{0.0, 1.5, -1.5, 1e-9, 1e9, 0.1}, ""
	case v1.InputDeclaration_TYPE_BOOL:
		return []any{true, false}, ""
	default:
		return nil, fmt.Sprintf("%s inputs are not generated yet", strings.ToLower(strings.TrimPrefix(d.GetType().String(), "TYPE_")))
	}
}
