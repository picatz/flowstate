package flowtest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-yaml"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// MaxFaultsPerTest bounds one case's `faults:` list. A fault is consulted on
// every matching invocation, so the list is work spent per task call.
const MaxFaultsPerTest = 32

// maxFaultAtMost bounds [Fault.AtMost]: a fault that may fire without limit is
// a stub that always fails, which `stubs:` already says without a coin flip.
const maxFaultAtMost = 100

// defaultFaultRate is the chance a matching invocation fails when a fault
// states no `rate:`: even odds, so a few seeds reach both the failed and the
// untouched path.
const defaultFaultRate = 0.5

// A Fault is a failure `flow test --seeds` may inject into a task invocation
// that the case's stubs would otherwise answer.
//
// Where a stub says what a task does, a fault says what can go wrong with it:
// each invocation that matches draws from the seed's fault stream, and a draw
// under [Fault.Rate] fails that attempt as the declared error instead of
// consulting the stubs. The written-order run, which is the one a plain
// `flow test` reports, never injects, so `faults:` can neither turn a green
// case red nor hide a red one; it asks the *seeded* runs whether the
// case's [Test.Invariants] still hold when the world misbehaves.
//
// A fault is consulted before the stubs and spends none of their `times:`
// budgets, so a stubbed answer scripted for the second call is still the
// second call's answer whatever happens to the first attempt.
type Fault struct {
	// Task is the task name whose invocations may fail, anywhere in the run,
	// callees and compensations included. Exactly one of Task and Step.
	Task string `yaml:"task,omitempty"`

	// Step is the step id, in the workflow under test, whose invocations may
	// fail, one draw per attempt.
	Step string `yaml:"step,omitempty"`

	// Fails is the injected failure. Its kind defaults to Upstream, the
	// ordinary transient one, and must be a kind a task can honestly report:
	// Internal and Expression describe a bug in the engine or the workflow, not
	// a fault in the world, and an injected one would make the invariant that
	// watches for them unfalsifiable.
	Fails StubFailure `yaml:"fails,omitempty"`

	// Rate is the chance, in (0, 1], that a matching invocation fails.
	// Absent means [defaultFaultRate]; a pointer because zero is not a rate a
	// fault can usefully have, and silently reading it as the default would
	// say something the author did not write.
	Rate *float64 `yaml:"rate,omitempty"`

	// AtMost caps how many times this fault fires in one run. Absent means
	// once, the shape a retry is built to survive; a fault that may fail every
	// attempt is stated as `at_most: N`. A pointer so an explicit `at_most: 0`
	// is refused rather than read as the default.
	AtMost *int `yaml:"at_most,omitempty"`

	// On pins the fault: it fires on exactly these invocations of its target,
	// numbered from 1 in the order the run makes them, and no seed is
	// consulted. A pinned fault fires in every run, the plain written-order
	// one included, which is what turns a violating seed into a regression
	// case `flow test` replays with no `--seeds` at all: a violation prints
	// the faults it fired in this form. Exclusive with Rate and AtMost, which
	// describe a draw. A run that makes fewer invocations than the largest
	// number named has drifted from the script and fails.
	On []int `yaml:"on,flow,omitempty"`
}

func (f *Fault) rate() float64 {
	if f.Rate == nil {
		return defaultFaultRate
	}

	return *f.Rate
}

func (f *Fault) limit() int {
	if f.AtMost == nil {
		return 1
	}

	return *f.AtMost
}

// kind is the failure's error kind, defaulted the way a stub's `fails:` is.
func (f *Fault) kind() v1.ErrorKind {
	if f.Fails.Kind == "" {
		return v1.ErrorKindUpstream
	}

	return v1.ErrorKind(f.Fails.Kind)
}

// checkFaultShape refuses a malformed `faults:` entry, named by its position.
// It is the part of the judgment that needs no workflow.
func checkFaultShape(i int, f *Fault) error {
	where := fmt.Sprintf("faults[%d]", i)
	if (f.Task == "") == (f.Step == "") {
		return fmt.Errorf("%s: name exactly one of `task:` or `step:`", where)
	}
	if f.Fails.Kind != "" {
		kind, ok := v1.ParseErrorKind(f.Fails.Kind)
		switch {
		case !ok:
			return fmt.Errorf("%s: fails.kind %q is not an error kind; it is one of %s",
				where, f.Fails.Kind, kindList())
		case kind == v1.ErrorKindInternal || kind == v1.ErrorKindExpression:
			return fmt.Errorf("%s: fails.kind %q is a defect in the engine or the workflow, not a fault in the "+
				"world, so injecting it would hide the very finding it stands for; use a kind a task reports", where, kind)
		case kind == v1.ErrorKindRunTimeout:
			return fmt.Errorf("%s: fails.kind %q is synthesized for a whole-run timeout and no task can report it; "+
				"use Timeout for an attempt that ran out of time", where, kind)
		}
	}
	if len(f.On) > 0 {
		if f.Rate != nil || f.AtMost != nil {
			return fmt.Errorf("%s: `on:` pins the invocations that fail, so it takes no `rate:` or `at_most:`", where)
		}
		if len(f.On) > maxFaultAtMost {
			return fmt.Errorf("%s: on names %d invocations, more than the limit of %d", where, len(f.On), maxFaultAtMost)
		}
		seen := map[int]bool{}
		for _, n := range f.On {
			if n < 1 {
				return fmt.Errorf("%s: on: %d is not an invocation number; they count from 1", where, n)
			}
			if seen[n] {
				return fmt.Errorf("%s: on names invocation %d twice", where, n)
			}
			seen[n] = true
		}
	}
	if f.Rate != nil && (!(*f.Rate > 0) || *f.Rate > 1) {
		return fmt.Errorf("%s: rate %v is outside (0, 1]; a fault that never fires tests nothing, "+
			"and one that always fires is a `stubs:` entry with `fails:`", where, *f.Rate)
	}
	if f.AtMost != nil && (*f.AtMost < 1 || *f.AtMost > maxFaultAtMost) {
		return fmt.Errorf("%s: at_most %d is outside 1..%d", where, *f.AtMost, maxFaultAtMost)
	}

	return nil
}

func kindList() string {
	var kinds []string
	for _, k := range v1.ErrorKinds() {
		if k != v1.ErrorKindInternal && k != v1.ErrorKindExpression && k != v1.ErrorKindRunTimeout {
			kinds = append(kinds, string(k))
		}
	}

	return strings.Join(kinds, ", ")
}

// checkFaultNames refuses a fault aimed at a task or step the workflow under
// test cannot invoke, before the run, for the reason [checkInvocationNames]
// does: a fault at a ghost is a no-op that reads as resilience.
func checkFaultNames(faults []Fault, spec *v1.Workflow) error {
	if len(faults) == 0 {
		return nil
	}
	all := map[string]bool{}
	collectAllStepIDs(spec.GetSteps(), all)
	containers := map[string]bool{}
	parallelContainers(spec.GetSteps(), containers)
	names := slices.Sorted(maps.Keys(all))

	var tasks []string
	for i := range faults {
		f := &faults[i]
		where := fmt.Sprintf("faults[%d]", i)
		if f.Task != "" {
			if tasks == nil {
				var err error
				if tasks, err = v1.RequiredTaskNames(spec); err != nil {
					return fmt.Errorf("faults: %w", err)
				}
			}
			if !slices.Contains(tasks, f.Task) {
				if suggestion, ok := nearest.Name(f.Task, tasks); ok {
					return fmt.Errorf("%s: task names unknown task %q; did you mean %q?", where, f.Task, suggestion)
				}

				return fmt.Errorf("%s: task names unknown task %q, which this workflow, its callees and its compensations never invoke", where, f.Task)
			}
		}
		if f.Step != "" {
			switch {
			case containers[f.Step]:
				return fmt.Errorf("%s: step names %q, a parallel container that invokes no task itself; "+
					"name the branch steps instead", where, f.Step)
			case !all[f.Step]:
				if suggestion, ok := nearest.Name(f.Step, names); ok {
					return fmt.Errorf("%s: step names unknown step %q; did you mean %q?", where, f.Step, suggestion)
				}

				return fmt.Errorf("%s: step names unknown step %q, which this workflow has no step for", where, f.Step)
			}
		}
	}

	return nil
}

// faultPlan is one run's fault state: the declared faults, how many
// invocations each was eligible for, and how many times each fired.
//
// Fresh per run, so a seeded run's `at_most:` budgets never carry over from
// another schedule, and held on the context beside the invocation log because
// the stub function that consults it is called by the engine, which knows
// nothing of the case.
type faultPlan struct {
	root   string
	faults []Fault

	mu    sync.Mutex
	seen  []int
	fired []int
	// drawn counts the fires a seed decided, which a pinned fault's fires are
	// not: only a drawn fire makes a seeded run a faulted one.
	drawn []int
	// pinnedFired is the invocation numbers each pinned fault fired on. A pin
	// can be counted by [faultPlan.seen] and still not fire, when an earlier
	// fault answered the call first, and that is a script that did not run.
	pinnedFired [][]int
	// script is the invocation numbers each drawn fault fired on, in order,
	// from which [faultPlan.pinned] writes the regression case.
	script [][]int
}

type faultPlanKey struct{}

func newFaultPlan(root string, faults []Fault) *faultPlan {
	return &faultPlan{
		root:   root,
		faults: faults,
		seen:   make([]int, len(faults)),
		fired:  make([]int, len(faults)),
		drawn:  make([]int, len(faults)),
		script: make([][]int, len(faults)),

		pinnedFired: make([][]int, len(faults)),
	}
}

type faultProbeKey struct{}

// contextWithFaultProbe makes the run under ctx a shrink probe: the case runs
// with exactly faults, all pinned, in place of its own.
func contextWithFaultProbe(ctx context.Context, faults []Fault) context.Context {
	return context.WithValue(ctx, faultProbeKey{}, faults)
}

func faultProbeFrom(ctx context.Context) ([]Fault, bool) {
	faults, ok := ctx.Value(faultProbeKey{}).([]Fault)

	return faults, ok
}

func contextWithFaultPlan(ctx context.Context, p *faultPlan) context.Context {
	return context.WithValue(ctx, faultPlanKey{}, p)
}

// attempt consults the plan for one invocation of task, and returns the error
// to fail it with, or nil to let the stubs answer. It records the invocation
// as one the fault was eligible for whether or not it fires, which is how a
// fault no invocation ever reached is told from one that merely drew
// "no" ([faultPlan.unreached]).
//
// Under a scheduler that is not a [v1.FaultChooser], which is the written-order
// run, nothing is drawn; a pinned fault ([Fault.On]) fires regardless.
func (p *faultPlan) attempt(ctx context.Context, task string) error {
	ref, _ := v1.TaskStepRefFromContext(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()

	// Every matching fault counts the invocation before any of them decides, so
	// "invocation n of this fault's target" does not depend on which other fault
	// fired on the calls before it. A pinned script names calls by that number,
	// and removing one fault from a script (see [shrinkFaults]) must not renumber
	// the calls another fault is aimed at.
	matches := make([]int, 0, len(p.faults))
	for i := range p.faults {
		f := &p.faults[i]
		if f.Task != "" && f.Task != task || f.Step != "" && (f.Step != ref.Step || ref.Workflow != p.root) {
			continue
		}
		p.seen[i]++
		matches = append(matches, i)
	}
	for _, i := range matches {
		f := &p.faults[i]
		switch {
		case len(f.On) > 0:
			if !slices.Contains(f.On, p.seen[i]) {
				continue
			}
			p.pinnedFired[i] = append(p.pinnedFired[i], p.seen[i])
		case p.fired[i] >= f.limit():
			continue
		case !v1.InjectFault(ctx, fmt.Sprintf("faults[%d]", i), f.rate()):
			continue
		default:
			p.drawn[i]++
			p.script[i] = append(p.script[i], p.seen[i])
		}
		p.fired[i]++
		message := f.Fails.Message
		if message == "" {
			message = fmt.Sprintf("injected fault faults[%d]", i)
		}

		return v1.NewTaskError(task, f.kind(), errors.New(message))
	}

	return nil
}

// unreached lists the step faults no invocation was ever eligible for, as
// diagnostics. Judged on the run that injects nothing: a step the written-order
// run never reached is almost always a step the case cannot reach, and a fault
// there would report resilience to a failure it never saw. Task faults are not
// judged here: a task may be a compensation that only a fault elsewhere
// activates, and a task no run can invoke is refused by name before the run
// ([checkFaultNames]).
func (p *faultPlan) unreached() []*v1.Diagnostic {
	p.mu.Lock()
	defer p.mu.Unlock()

	var out []*v1.Diagnostic
	for i := range p.faults {
		f := &p.faults[i]
		if last := slices.Max(append([]int{0}, f.On...)); last > p.seen[i] {
			out = append(out, &v1.Diagnostic{
				Step:  f.Step,
				Field: fmt.Sprintf("faults[%d].on", i),
				Message: fmt.Sprintf("is pinned to invocation %d of its target, but the run made only %d; "+
					"the script has drifted from the workflow, so re-derive it from a fresh `--seeds` finding", last, p.seen[i]),
			})

			continue
		}
		if missed := slices.DeleteFunc(slices.Clone(f.On), func(n int) bool { return slices.Contains(p.pinnedFired[i], n) }); len(missed) > 0 {
			out = append(out, &v1.Diagnostic{
				Step:  f.Step,
				Field: fmt.Sprintf("faults[%d].on", i),
				Message: fmt.Sprintf("is pinned to invocation %d of its target, but an earlier fault answered that call "+
					"first, so this one never fired; the script has drifted, so re-derive it from a fresh `--seeds` finding", missed[0]),
			})

			continue
		}
		if p.seen[i] > 0 || f.Step == "" {
			continue
		}
		target := "step " + f.Step
		out = append(out, &v1.Diagnostic{
			Step:  f.Step,
			Field: fmt.Sprintf("faults[%d]", i),
			Message: fmt.Sprintf("%s was never invoked by this case, so the fault could not fire under any seed; "+
				"make the case reach it, or drop the fault", target),
		})
	}

	return out
}

// faultedErrorClass is the one oracle a faulted run owes without being told:
// a failure the world caused must not surface as a defect. An Internal error
// under injected faults means the engine mishandled a failure it was handed.
func faultedErrorClass(runErr error) []*v1.Diagnostic {
	if runErr == nil || v1.ClassifyError(runErr) != v1.ErrorKindInternal {
		return nil
	}

	return []*v1.Diagnostic{{
		Field:   "faults",
		Message: "the run failed as an Internal error under injected faults; a failure the world causes must surface as the failure it is",
	}}
}

// checkFaults refuses a malformed `faults:` list at the key the author wrote
// it, judged once at a table entry and not again per row. It reports whether
// the list is sound enough to be bounded and named against a workflow.
func checkFaults(p *problems, r site, test *Test, at loc) {
	if len(test.Faults) > MaxFaultsPerTest {
		p.report(r.in(at), "test %q declares %d faults, more than the limit of %d",
			test.Name, len(test.Faults), MaxFaultsPerTest)

		return
	}
	for i := range test.Faults {
		if err := checkFaultShape(i, &test.Faults[i]); err != nil {
			p.report(r.in(at.item(i)), "test %q %s", test.Name, err)
		}
	}
}

// checkInvariants refuses a malformed `invariants:` list, the claims being
// judged exactly as an `expect.check:` list is. Like the faults it is judged
// where the author wrote it.
func checkInvariants(p *problems, r site, test *Test, at loc) {
	if len(test.Invariants) > MaxChecksPerTest {
		p.report(r.in(at), "test %q declares %d invariants, more than the limit of %d",
			test.Name, len(test.Invariants), MaxChecksPerTest)

		return
	}
	test.Invariants = slices.Clone(test.Invariants)
	checkClaimList(p, r.in(at), fmt.Sprintf("test %q invariants", test.Name),
		test.Invariants, len(test.Invariants), "")
}

// assertInvariants evaluates the case's invariants against a finished run, as
// the claims they are: the same evaluation as `expect.check:`, reported under
// the key the author wrote.
func assertInvariants(ctx context.Context, started time.Time, claims []CheckClaim, spec *v1.Workflow, bound map[string]*v1.Value, vars fileVars, outputs *v1.Workflow_StepOutputs, facts runFacts, sensitive sensitiveInputs) []*v1.Diagnostic {
	failures := assertChecks(ctx, started, claims, spec, bound, vars, outputs, facts, sensitive)
	for _, d := range failures {
		d.Field = "invariants" + strings.TrimPrefix(d.Field, "expect.check")
	}

	return failures
}

// firedAny reports whether a seed made any fault fire in this run. A seeded
// run no draw fired in is an ordinary schedule and is judged and compared as
// one; pinned faults are part of the case and do not count.
func (p *faultPlan) firedAny() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.ContainsFunc(p.drawn, func(n int) bool { return n > 0 })
}

// firedPinned reports whether any pinned fault fired in this run. A probe
// ([shrinkFaults]) runs only pinned faults, so this is what makes its run a
// faulted one.
func (p *faultPlan) firedPinned() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.ContainsFunc(p.fired, func(n int) bool { return n > 0 })
}

// pins are the faults that fired, each pinned to the invocation numbers it fired
// on and everything else about it as declared. authored[i] is true for a pin
// the case declared itself, which is part of the case and not something a seed
// found. Empty when no draw fired.
func (p *faultPlan) pins() (pins []Fault, authored []bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, on := range p.script {
		pin := p.faults[i]
		switch {
		case len(pin.On) > 0 && p.fired[i] > 0:
			// Already a script; kept as declared so replacing `faults:` with
			// this list loses none of the case's own pins.
			pin.On = slices.Clone(pin.On)
			authored = append(authored, true)
		case len(on) > 0:
			pin.Rate, pin.AtMost, pin.On = nil, nil, slices.Clone(on)
			authored = append(authored, false)
		default:
			continue
		}
		pins = append(pins, pin)
	}

	return pins, authored
}

// pinnedScript writes pins as the `faults:` list that replays them without a
// seed, or "" for none.
func pinnedScript(pins []Fault) string {
	if len(pins) == 0 {
		return ""
	}
	out, err := yaml.Marshal(map[string][]Fault{"faults": pins})
	if err != nil {
		return ""
	}

	return string(out)
}
