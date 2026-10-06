package flowtest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
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
			return
		}

		// An invocation no stub answers says nothing about the workflow: the
		// case's stubs were written for its own inputs, and a generated one can
		// route a call past every `where:`. Inconclusive, like a case that
		// errored before the run.
		var unanswered *stubDiagnostic
		if result.GetError() != "" || errors.As(runErr, &unanswered) {
			f.inconclusive++

			continue
		}
		f.runs++
		var problems []string
		if kind := v1.ClassifyError(runErr); runErr != nil && (kind == v1.ErrorKindInternal || kind == v1.ErrorKindExpression) {
			problems = append(problems, fmt.Sprintf("the run failed with a %s error: %s", kind, shown.runErrorUnder(shown.sensitive)))
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
		if len(problems) == 0 {
			continue
		}

		f.finding = &v1.FuzzFinding{
			Case:    redactedErrorText(test.Name, shown.sensitive),
			Seed:    seed,
			Inputs:  redactedErrorText(pasteableInputs(inputs, sensitive), shown.sensitive),
			Failure: redactedErrorText(strings.Join(problems, "\n"), shown.sensitive),
		}

		return
	}
}

// pasteableInputs renders inputs as the `inputs:` stanza that reproduces a
// failure, leaving out every input the workflow declares sensitive.
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
// Every candidate set is bound through [v1.BindRunInputs], the path a real
// submit takes, so a generated value the declaration refuses (a `must:` it
// fails, a length out of bounds) is never run: a refused input is a test of the
// binder, not of the workflow.
func generateInputs(spec *v1.Workflow, base map[string]any, seed uint64) (map[string]any, []string, bool) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))

	slots, skipped := inputSlots(spec)

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
