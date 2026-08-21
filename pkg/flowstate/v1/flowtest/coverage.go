package flowtest

import (
	"fmt"
	"sort"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Coverage is which of a workflow's steps at least one case in a `*.test.yaml`
// ran, and which no case ever reached (issue #420).
//
// It is a property of the harness, not of the workflow language: no schema
// field carries it, and it never travels a durable boundary. It is derived
// from the same transcript `expect.ran` is checked against (the
// [v1.Workflow_StepOutputs] a run hands back), so a step counts as reached on
// exactly the evidence `expect.ran` counts on, and nothing weaker.
//
// A run that fails is measured from the partial transcript it hands back
// ([v1.PartialTranscript]): the steps it ran before it stopped, and the step it
// stopped on. That is issue #453, a case whose only exercise of an error branch
// is `expect.failed: true` used to contribute its workflow's steps to the universe
// and reach none of them, so an author had to record a reason under
// `coverage.allow_unreached` for a branch a case really did run. It stays
// consistent with `expect.ran` because both read that same record.
//
// Two measurement rules, matching #420:
//
//   - A step skipped by `if:` in every case is unreached, not covered. A
//     skipped step produces no recorded outputs, so it is simply absent from
//     every transcript, and absence is what unreached means here. Being named
//     in `expect.skipped` is an assertion about one case, never evidence the
//     branch works.
//   - A step inside a `for_each` or `loop` body counts as reached if any
//     iteration ran it. The body's per-iteration outputs travel in the loop
//     node's `results` output ([v1.LoopResultsField]), so the walk descends
//     into that list and unions the step ids it finds across iterations.
type Coverage struct {
	// Workflow is the workflow this coverage accounts for, the path a case's
	// `workflow:` resolved to ([WorkflowPath]). It is the identity coverage is
	// keyed by, so a file that targets more than one workflow keeps a step id
	// one workflow reaches from masking the same id left unreached in another
	// (issue #420, Finding 3).
	Workflow string

	// Reached is every step id that ran in at least one case, sorted.
	Reached []string

	// Unreached is the complement: every step id in the workflow that no case
	// ran, sorted. This is the line #420 exists to report.
	Unreached []string

	// Accepted maps an unreached step id to the reason the file recorded for it
	// (`coverage.allow_unreached`). Every key here is also in Unreached: it is
	// genuinely not reached, and Accepted is why that is a decision rather than
	// a gap. A step in Accepted does not fail `--coverage-required`.
	Accepted map[string]string

	// Stale is every `coverage.allow_unreached` entry that does not describe a
	// real residual: it names a step or switch arm some case did reach, or one
	// no targeted workflow has. Such a record is a false statement about the
	// suite, so it fails `--coverage-required` the same way an unrecorded gap
	// does. Each entry is a sentence naming the entry and what is wrong.
	Stale []string

	// Arms is every arm of every `switch:` step the workflow has, in written
	// order, and whether any case took it (issue #801).
	//
	// A second unit beside the step one rather than more entries in Reached and
	// Unreached, because an arm is not a step: [Coverage.Total] would otherwise
	// count two different things, and the reason this exists at all is that an
	// arm's body may hold no steps. `steps: []` is how a switch writes down
	// deliberately ignoring a value — a documented, reviewable pattern
	// (proto/flowstate/v1/workflow.proto, and examples/webhook-routing) — and it
	// contributes nothing to the step universe, so before this no suite could
	// fail to cover it. Deleting the one case that exercised it left
	// `--coverage-required` reporting full coverage.
	Arms []*SwitchArm
}

// SwitchArm is one arm of one `switch:` step — one `case:` literal, or the
// `default:` — and whether any case took it.
//
// Read from the transcript, never inferred from what ran. A switch records the
// literal that matched under [v1.SwitchCaseOutput], so this reads the arm that
// was taken; deducing it from the body steps that appeared is what the step
// universe does, and that cannot see an empty body and cannot tell which member
// of `case: [closed, merged]` matched. #420 said so when it proposed switch
// coverage — "the matched case is in the transcript, so the harness reads it
// rather than inferring it" — and #452 shipped the inference.
type SwitchArm struct {
	// Key is the coverage identity: `<step>:case[<i>]` for a case holding one
	// literal, `<step>:case[<i>][<j>]` for member j of a case listing several,
	// and `<step>:default`. It is what a file names under
	// `coverage.allow_unreached` to record an arm it cannot reach.
	Key string

	// Step is the id of the `switch:` step this arm belongs to.
	Step string

	// Label is the arm as an author reads it: `case "synchronize"`, `case 3`, or
	// `default`. Rendered by [flowfile.SwitchLiteralText], the same function the
	// validator's own case diagnostics use, so one arm reads as one sentence
	// wherever it is named.
	Label string

	// Reached reports whether any case took this arm.
	Reached bool

	// Reason is what the file recorded for this arm under
	// `coverage.allow_unreached`, and is empty when it recorded none.
	Reason string

	// Where is where the arm was written. Invalid when the workflow was not
	// parsed from a file that recorded positions — a workflow submitted as bytes
	// ([RunSource]) has no source to point at.
	//
	// The reason issue #801 asks for positions at all: a step has an id every
	// other surface resolves, and `on_event:case[2]` has nothing but this.
	Where flowfile.Span
}

// Total is how many steps the workflow has that coverage accounts for.
func (c *Coverage) Total() int { return len(c.Reached) + len(c.Unreached) }

// ArmsReached is how many of the workflow's switch arms at least one case took.
func (c *Coverage) ArmsReached() int {
	n := 0
	for _, arm := range c.Arms {
		if arm.Reached {
			n++
		}
	}
	return n
}

// Gaps is every unreached step the file did not record a reason for: the holes
// in the suite, as opposed to the residuals it accepted. This is what
// `--coverage-required` fails on, together with [Coverage.ArmGaps] and
// [Coverage.Stale].
func (c *Coverage) Gaps() []string {
	var gaps []string
	for _, id := range c.Unreached {
		if _, accepted := c.Accepted[id]; !accepted {
			gaps = append(gaps, id)
		}
	}
	return gaps
}

// ArmGaps is [Coverage.Gaps] for switch arms: every arm no case took and no
// `coverage.allow_unreached` entry explains.
//
// Kept apart from Gaps rather than merged into it, because the two are reported
// differently and have to be. A gap names a step, and an author finds a step by
// its id; an arm can only be found by the position [SwitchArm.Where] carries.
func (c *Coverage) ArmGaps() []*SwitchArm {
	var gaps []*SwitchArm
	for _, arm := range c.Arms {
		if !arm.Reached && arm.Reason == "" {
			gaps = append(gaps, arm)
		}
	}
	return gaps
}

// Report renders this coverage as the schema message that crosses the machine
// boundary ([v1.CoverageReport]), so `flow test -o json` emits it through
// protojson like every other part of the report rather than through a second,
// hand-shaped encoder that could disagree with the first.
//
// Nil slices and a nil Accepted map are left as they are: protojson's
// EmitUnpopulated renders them as `[]` and `{}`, so a consumer indexing the
// arrays finds a list to range over rather than a null to guard, matching the
// posture of the schema fields beside them.
func (c *Coverage) Report() *v1.CoverageReport {
	report := &v1.CoverageReport{
		Workflow:     c.Workflow,
		StepsTotal:   int32(c.Total()),
		StepsReached: int32(len(c.Reached)),
		Reached:      c.Reached,
		Unreached:    c.Unreached,
		Gaps:         c.Gaps(),
		Accepted:     c.Accepted,
		Stale:        c.Stale,
	}

	for _, arm := range c.Arms {
		report.Arms = append(report.Arms, &v1.SwitchArmCoverage{
			Arm:     arm.Key,
			Step:    arm.Step,
			Label:   arm.Label,
			Reached: arm.Reached,
			Reason:  arm.Reason,
			Line:    int32(arm.Where.Start.Line),
			Column:  int32(arm.Where.Start.Column),
		})
	}

	return report
}

// coverageAccumulator gathers, per workflow a file's cases target, the universe
// of that workflow's steps and the union of the steps any case reached.
//
// Coverage is keyed by workflow identity rather than unioned across the file,
// because a `*.test.yaml`'s cases each name their own `workflow:` and could
// target different files (issue #420, Finding 3). Unioning them lets a step one
// workflow reaches mask the same step id left unreached in another, a false
// pass in a fail-closed gate, so each workflow keeps its own universe and
// reached set. In the ordinary case every case targets one workflow and there
// is exactly one entry.
type coverageAccumulator struct {
	// workflows holds one universe/reached pair per workflow identity observed.
	workflows map[string]*workflowCoverage

	// allowUnreached is the file's `coverage.allow_unreached`, the residuals it
	// recorded a reason for. Read at result time to partition each workflow's
	// Unreached into accepted residuals and unrecorded gaps, and to catch a
	// record that describes a real residual of no targeted workflow. It is a
	// file-level record, so an entry is judged against every targeted workflow
	// together: it explains a residual as long as some workflow leaves that step
	// unreached, and is stale only when none does.
	allowUnreached map[string]string
}

// workflowCoverage is one workflow's accumulated universe and reached set.
type workflowCoverage struct {
	universe map[string]bool
	reached  map[string]bool

	// arms is every switch arm the workflow declares, in written order, and
	// armIndex is where each key sits in it. A slice rather than a map because
	// written order is the order an author reads their own file in, and a
	// coverage line listing arms in map order would be a different list every
	// run.
	arms     []*SwitchArm
	armIndex map[string]int

	// armsReached is the union across every case of the arms taken. Kept beside
	// the arms rather than on them because arms are collected once, from the
	// first case that compiled the workflow, and reached is accumulated over all
	// of them.
	armsReached map[string]bool
}

// arm returns the workflow's arm with the given key, or nil when it has none.
func (w *workflowCoverage) arm(key string) *SwitchArm {
	i, ok := w.armIndex[key]
	if !ok {
		return nil
	}

	return w.arms[i]
}

func newCoverageAccumulator(allowUnreached map[string]string) *coverageAccumulator {
	return &coverageAccumulator{
		workflows:      map[string]*workflowCoverage{},
		allowUnreached: allowUnreached,
	}
}

// observe folds one case's compiled workflow and its transcript into the
// coverage for the workflow it targeted, named by identity ([WorkflowPath]).
// spec may be nil for a case that never compiled a workflow; outputs may be nil
// for a case that never reached a run at all, one refused before submission, or
// one whose stubs did not resolve, in which case the case widens that workflow's
// universe but reaches nothing. A case whose *run* failed is no longer one of
// those: it arrives with the partial transcript ([v1.PartialTranscript]).
// positions is where that workflow's source was written, as
// [flowfile.ParseFile] handed it back, and may be nil — a workflow submitted as
// bytes has no source, and a nil *Positions answers every question with "not
// known" rather than needing a guard here. It is what gives a switch arm the
// only identity it has (issue #801).
func (a *coverageAccumulator) observe(identity string, spec *v1.Workflow, outputs *v1.Workflow_StepOutputs, positions *flowfile.Positions) {
	wc := a.workflows[identity]
	if wc == nil {
		wc = &workflowCoverage{
			universe:    map[string]bool{},
			reached:     map[string]bool{},
			armIndex:    map[string]int{},
			armsReached: map[string]bool{},
		}
		a.workflows[identity] = wc
	}
	if spec != nil {
		collectStepUniverse(spec.GetSteps(), wc.universe)
		collectSwitchArms(spec.GetSteps(), positions, wc)
	}
	if spec == nil || outputs == nil {
		return
	}
	markReached(spec.GetSteps(), outputs.GetStepValues(), wc.reached)
	markArmsReached(spec.GetSteps(), outputs.GetStepValues(), wc.armsReached)
}

// result renders the accumulated coverage, one [Coverage] per workflow the file
// targeted, sorted by workflow identity for determinism. It returns nil when no
// case contributed a workflow to account for (a refused file, or one whose
// every case failed to compile), the signal to report no coverage line at all
// rather than a misleading "0/0 steps reached". A workflow whose every case
// failed to compile likewise contributes no entry.
func (a *coverageAccumulator) result() []*Coverage {
	// Only workflows that saw a compiled spec produce coverage; an identity
	// whose every case failed to compile has an empty universe and is dropped.
	identities := make([]string, 0, len(a.workflows))
	for id, wc := range a.workflows {
		if len(wc.universe) == 0 {
			continue
		}
		identities = append(identities, id)
	}
	if len(identities) == 0 {
		return nil
	}
	sort.Strings(identities)

	// A `coverage.allow_unreached` entry describes a real residual as long as
	// some targeted workflow leaves that step — or, since #801, that switch arm —
	// unreached. Judged across every workflow at once so an entry explaining
	// workflow A's residual is not reported stale against workflow B, which never
	// had that step.
	residual := map[string]bool{}
	for entry := range a.allowUnreached {
		for _, id := range identities {
			wc := a.workflows[id]
			if wc.universe[entry] && !wc.reached[entry] {
				residual[entry] = true
				break
			}
			if wc.arm(entry) != nil && !wc.armsReached[entry] {
				residual[entry] = true
				break
			}
		}
	}

	covs := make(map[string]*Coverage, len(identities))
	for _, id := range identities {
		wc := a.workflows[id]
		cov := &Coverage{Workflow: id}
		for step := range wc.universe {
			if wc.reached[step] {
				cov.Reached = append(cov.Reached, step)
			} else {
				cov.Unreached = append(cov.Unreached, step)
			}
		}
		sort.Strings(cov.Reached)
		sort.Strings(cov.Unreached)

		// Accept every recorded residual this workflow genuinely leaves
		// unreached. An entry that also explains another workflow is accepted in
		// each workflow it is a residual of; that is not double-counting, it is
		// the record doing its job for every branch it names.
		for step, reason := range a.allowUnreached {
			if wc.universe[step] && !wc.reached[step] {
				if cov.Accepted == nil {
					cov.Accepted = map[string]string{}
				}
				cov.Accepted[step] = reason
			}
		}

		// Arms carry their own reason rather than joining Accepted, because
		// Accepted is documented as a map of *step* ids and a reader indexing it
		// by one must not find something else there.
		cov.Arms = make([]*SwitchArm, 0, len(wc.arms))
		for _, declared := range wc.arms {
			arm := *declared
			arm.Reached = wc.armsReached[arm.Key]
			if !arm.Reached {
				arm.Reason = a.allowUnreached[arm.Key]
			}
			cov.Arms = append(cov.Arms, &arm)
		}

		covs[id] = cov
	}

	// A recorded residual of no targeted workflow is stale, a false statement
	// about the suite that fails the same way a gap does. The staleness check is
	// the "assert a bound was reached as well as not exceeded" discipline
	// (CLAUDE.md) applied to the record itself: a reason kept past the branch it
	// explained is how the record stops meaning anything. Each stale entry is
	// reported once, on a deterministic workflow: the first (sorted) whose
	// universe holds the step, with the "a case reached it" wording, or the
	// first workflow overall with the "not a step" wording when no workflow has
	// it at all. For a file targeting one workflow this is exactly the old
	// per-workflow partition, wording unchanged.
	for entry := range a.allowUnreached {
		if residual[entry] {
			continue
		}
		home := ""
		for _, id := range identities {
			if a.workflows[id].universe[entry] || a.workflows[id].arm(entry) != nil {
				home = id
				break
			}
		}
		var msg string
		if home != "" {
			msg = fmt.Sprintf(
				"coverage.allow_unreached names %q, but a case reached it; remove the entry", entry)
		} else {
			home = identities[0]
			msg = fmt.Sprintf(
				"coverage.allow_unreached names %q, which is not a step or switch arm in this workflow; "+
					"fix the id or remove the entry", entry)
		}
		covs[home].Stale = append(covs[home].Stale, msg)
	}

	out := make([]*Coverage, 0, len(identities))
	for _, id := range identities {
		sort.Strings(covs[id].Stale)
		out = append(out, covs[id])
	}
	return out
}

// collectStepUniverse walks a workflow's node tree, adding every step id
// coverage accounts for to universe.
//
// It descends into the bodies of `for_each` and `loop` and into the branches
// of `parallel`, because those hold steps an author wrote and a test can leave
// unreached. It does not descend into a `call:`, whose steps belong to the
// callee's own file and are that file's coverage to account for.
//
// A `parallel` container is not itself counted: it records no outputs of its
// own (the engine merges its branches' outputs and returns nothing under the
// container's id), so counting it would report a step that no transcript can
// ever show as reached. Its branch steps are counted, and they are what
// `parallel:` coverage is about.
func collectStepUniverse(nodes []*v1.Node, universe map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				collectStepUniverse(branch.GetSteps(), universe)
			}
		case *v1.Node_ForEach:
			universe[node.GetId()] = true
			collectStepUniverse(kind.ForEach.GetBody(), universe)
		case *v1.Node_Loop:
			universe[node.GetId()] = true
			collectStepUniverse(kind.Loop.GetBody(), universe)
		case *v1.Node_Switch:
			// The switch itself records outputs (the observed value and the case
			// that took it), so it is a countable step; its body steps merge into
			// the enclosing scope the way parallel branch steps do, so they count
			// in the same universe — which is what makes `flow test`'s coverage
			// report the case body no test reaches.
			universe[node.GetId()] = true
			for _, body := range v1.SwitchBodies(kind.Switch) {
				collectStepUniverse(body, universe)
			}
		default:
			// Task, Wait, Call: a leaf for coverage. A call's own steps are the
			// callee file's to account for, so the call node is counted but not
			// descended into.
			universe[node.GetId()] = true
		}
	}
}

// markReached walks a workflow's node tree against a transcript, marking every
// step the transcript shows as having run.
//
// present is a map from step id to the outputs recorded for it. A top-level
// step and a `parallel` branch step both appear here directly (the engine
// merges a parallel's branch outputs into the enclosing scope). A `for_each` or
// `loop` body step does not: its outputs travel inside the loop node's own
// `results` output, so reaching it means descending into that list.
func markReached(nodes []*v1.Node, present map[string]*v1.Node_Outputs, reached map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			// The container records nothing; its branch steps are merged into
			// present directly, so recurse with the same map.
			for _, branch := range kind.Parallel.GetBranches() {
				markReached(branch.GetSteps(), present, reached)
			}
		case *v1.Node_ForEach:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			reached[node.GetId()] = true
			markReachedInResults(kind.ForEach.GetBody(), resultsList(out.GetNamedValues()), reached)
		case *v1.Node_Loop:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			reached[node.GetId()] = true
			markReachedInResults(kind.Loop.GetBody(), resultsList(out.GetNamedValues()), reached)
		case *v1.Node_Switch:
			// The switch records its own outputs, and the taken body's steps
			// merge into the enclosing scope, so both are read from present
			// directly, exactly as a parallel branch's are.
			if _, ok := present[node.GetId()]; ok {
				reached[node.GetId()] = true
			}
			for _, body := range v1.SwitchBodies(kind.Switch) {
				markReached(body, present, reached)
			}
		default:
			if _, ok := present[node.GetId()]; ok {
				reached[node.GetId()] = true
			}
		}
	}
}

// markReachedInResults descends into a `for_each` or `loop`'s per-iteration
// `results` list, marking every body step any iteration ran.
//
// Each iteration is a map literal from body step id to that step's outputs, so
// a step is reached if it appears in any iteration. A nested `for_each`/`loop`
// carries its own `results` field inside its entry, which is why this recurses
// through [markReachedInLiteral] rather than only unioning the top-level keys:
// a loop two levels down is still a step some case either reached or did not.
func markReachedInResults(body []*v1.Node, iterations []*expr.Value, reached map[string]bool) {
	for _, iteration := range iterations {
		lit := mapEntries(iteration)
		if lit == nil {
			continue
		}
		markReachedInLiteral(body, lit, reached)
	}
}

// markReachedInLiteral is [markReached] over a literal transcript rather than a
// [v1.Node_Outputs] one: inside a loop's `results`, a step's outputs are a CEL
// map literal, not a [v1.Node_Outputs]. The two walks are the same shape over
// two encodings of the same fact.
func markReachedInLiteral(nodes []*v1.Node, present map[string]*expr.Value, reached map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				markReachedInLiteral(branch.GetSteps(), present, reached)
			}
		case *v1.Node_ForEach:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			reached[node.GetId()] = true
			markReachedInResults(kind.ForEach.GetBody(), resultsListLiteral(out), reached)
		case *v1.Node_Loop:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			reached[node.GetId()] = true
			markReachedInResults(kind.Loop.GetBody(), resultsListLiteral(out), reached)
		case *v1.Node_Switch:
			if _, ok := present[node.GetId()]; ok {
				reached[node.GetId()] = true
			}
			for _, body := range v1.SwitchBodies(kind.Switch) {
				markReachedInLiteral(body, present, reached)
			}
		default:
			if _, ok := present[node.GetId()]; ok {
				reached[node.GetId()] = true
			}
		}
	}
}

// resultsList pulls the `results` list out of a loop node's recorded outputs.
func resultsList(named map[string]*v1.Value) []*expr.Value {
	return named[v1.LoopResultsField].GetLiteral().GetListValue().GetValues()
}

// resultsListLiteral pulls the `results` list out of a loop node's outputs when
// those outputs are themselves a CEL map literal, as they are one level down
// inside another loop's `results`.
func resultsListLiteral(v *expr.Value) []*expr.Value {
	for _, entry := range v.GetMapValue().GetEntries() {
		if entry.GetKey().GetStringValue() == v1.LoopResultsField {
			return entry.GetValue().GetListValue().GetValues()
		}
	}
	return nil
}

// mapEntries reads a CEL map literal into a lookup by string key, or nil when
// the value is not a map.
func mapEntries(v *expr.Value) map[string]*expr.Value {
	m := v.GetMapValue()
	if m == nil {
		return nil
	}
	out := make(map[string]*expr.Value, len(m.GetEntries()))
	for _, entry := range m.GetEntries() {
		out[entry.GetKey().GetStringValue()] = entry.GetValue()
	}
	return out
}

// The switch-arm coverage unit (issue #801).
//
// Two walks, mirroring [collectStepUniverse] and [markReached]: one that
// enumerates every arm the workflow declares, and one that reads the transcript
// to see which of them a case actually took. They descend the same way those do
// — into parallel branches, into loop bodies, into switch bodies, and never into
// a `call:`, whose arms belong to the callee's own file.
//
// The measurement is the whole point of the pair. `--coverage-required` used to
// infer that an arm ran from the body steps that appeared, which is wrong in
// three ways an author can hit and one they cannot: an arm whose body is `steps:
// []` contributes no step to infer from, so it was uncoverable; `case: [closed,
// merged]` is one body, so which literal matched was unknowable; and the enclosing
// scope can run a step of the same id. A switch records the literal it matched
// under [v1.SwitchCaseOutput], so this reads the record instead.

// collectSwitchArms enumerates every arm of every switch in nodes, in written
// order, into wc.
//
// Idempotent: [coverageAccumulator.observe] runs once per case and every case
// compiles the same workflow, so an arm already enumerated is left as it is
// rather than appended twice.
func collectSwitchArms(nodes []*v1.Node, positions *flowfile.Positions, wc *workflowCoverage) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				collectSwitchArms(branch.GetSteps(), positions, wc)
			}
		case *v1.Node_ForEach:
			collectSwitchArms(kind.ForEach.GetBody(), positions, wc)
		case *v1.Node_Loop:
			collectSwitchArms(kind.Loop.GetBody(), positions, wc)
		case *v1.Node_Switch:
			for _, arm := range switchArms(node.GetId(), kind.Switch, positions) {
				if _, already := wc.armIndex[arm.Key]; already {
					continue
				}
				wc.armIndex[arm.Key] = len(wc.arms)
				wc.arms = append(wc.arms, arm)
			}
			for _, body := range v1.SwitchBodies(kind.Switch) {
				collectSwitchArms(body, positions, wc)
			}
		default:
			// Task, Wait, Call: no arms of their own, and a call's are the
			// callee file's to account for.
		}
	}
}

// switchArms is one switch's arms: one per `case:` literal, then the `default:`
// when it has one.
//
// One arm per *literal* rather than per case entry, which is the finer of the
// two readings #801 offers and the one the record supports. `case: [closed,
// merged]` shares a body, so a per-case unit cannot tell a suite that exercises
// only `merged` from one that exercises both — and the transcript names the
// member that matched, so the finer unit costs nothing to measure. That matters
// most exactly where [v1.SwitchLiteralsEqual]'s cross-type numeric matching
// lives: `case: [1, 1.0]` is refused as a duplicate, but `case: [1, 2]` with a
// double discriminant is a pair a per-case unit would report covered on either.
//
// A case value that is not a literal contributes no arm. The parser compiles a
// computed `case:` so that [validateSwitch] can refuse it with a position, and
// `flow test` runs files that validated, so this is a backstop rather than a
// path: an arm nothing could ever match is not a gap worth reporting on top of
// the diagnostic that already refuses the file.
func switchArms(step string, sw *v1.Switch, positions *flowfile.Positions) []*SwitchArm {
	arms := make([]*SwitchArm, 0, len(sw.GetCases())+1)

	for i, c := range sw.GetCases() {
		values := c.GetValues()
		for j, candidate := range values {
			literal, ok := candidate.GetKind().(*v1.Value_Literal)
			if !ok {
				continue
			}

			key := fmt.Sprintf("%s:case[%d]", step, i)
			if len(values) > 1 {
				key += fmt.Sprintf("[%d]", j)
			}

			// The same address the validator's own case diagnostics use, so the
			// position under an arm and the position under a `case:` diagnostic
			// are one path rather than two spellings of it.
			where, _ := positions.Locate(step, flowfile.SwitchCaseField(i, len(values), j))

			arms = append(arms, &SwitchArm{
				Key:   key,
				Step:  step,
				Label: "case " + flowfile.SwitchLiteralText(literal.Literal),
				Where: where,
			})
		}
	}

	if sw.GetDefault() != nil {
		where, _ := positions.Locate(step, "default")
		arms = append(arms, &SwitchArm{
			Key:   step + ":default",
			Step:  step,
			Label: "default",
			Where: where,
		})
	}

	return arms
}

// markArmsReached walks nodes against a transcript, marking every switch arm the
// run took.
//
// A switch's own outputs sit under its id, exactly as [markReached] reads them,
// and [v1.SwitchCaseOutput] holds the literal that matched — or null, which means
// no case did. Null therefore marks the `default:` arm when the switch has one,
// and marks nothing when it does not, which is the switch that matched nothing
// and ran nothing. The two are not ambiguous: `validateSwitch` refuses a null
// `case:` outright, in those words, precisely so this record stays readable.
func markArmsReached(nodes []*v1.Node, present map[string]*v1.Node_Outputs, reached map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				markArmsReached(branch.GetSteps(), present, reached)
			}
		case *v1.Node_ForEach:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			markArmsReachedInResults(kind.ForEach.GetBody(), resultsList(out.GetNamedValues()), reached)
		case *v1.Node_Loop:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			markArmsReachedInResults(kind.Loop.GetBody(), resultsList(out.GetNamedValues()), reached)
		case *v1.Node_Switch:
			if out, ok := present[node.GetId()]; ok {
				markArmTaken(node.GetId(), kind.Switch,
					out.GetNamedValues()[v1.SwitchCaseOutput].GetLiteral(), reached)
			}
			for _, body := range v1.SwitchBodies(kind.Switch) {
				markArmsReached(body, present, reached)
			}
		default:
		}
	}
}

// markArmsReachedInResults descends into a loop's per-iteration `results` list,
// marking the arms any iteration took.
func markArmsReachedInResults(body []*v1.Node, iterations []*expr.Value, reached map[string]bool) {
	for _, iteration := range iterations {
		lit := mapEntries(iteration)
		if lit == nil {
			continue
		}
		markArmsReachedInLiteral(body, lit, reached)
	}
}

// markArmsReachedInLiteral is [markArmsReached] over a literal transcript rather
// than a [v1.Node_Outputs] one: inside a loop's `results`, a step's outputs are a
// CEL map literal. The same walk over the second encoding of the same fact, the
// way [markReachedInLiteral] mirrors [markReached].
func markArmsReachedInLiteral(nodes []*v1.Node, present map[string]*expr.Value, reached map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				markArmsReachedInLiteral(branch.GetSteps(), present, reached)
			}
		case *v1.Node_ForEach:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			markArmsReachedInResults(kind.ForEach.GetBody(), resultsListLiteral(out), reached)
		case *v1.Node_Loop:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			markArmsReachedInResults(kind.Loop.GetBody(), resultsListLiteral(out), reached)
		case *v1.Node_Switch:
			if out, ok := present[node.GetId()]; ok {
				markArmTaken(node.GetId(), kind.Switch, mapEntries(out)[v1.SwitchCaseOutput], reached)
			}
			for _, body := range v1.SwitchBodies(kind.Switch) {
				markArmsReachedInLiteral(body, present, reached)
			}
		default:
		}
	}
}

// markArmTaken records the one arm a switch's recorded `case` output says ran.
//
// Matched through [v1.SwitchLiteralsEqual], the same function
// [v1.SelectSwitchCase] used to decide which arm to run, so "this value took
// that case" means one thing in the engine and in the account of it. Spelling
// the comparison a second time here is how the two would come to disagree about
// `case: 1` against a discriminant of `1.0`.
func markArmTaken(step string, sw *v1.Switch, took *expr.Value, reached map[string]bool) {
	if took == nil {
		return
	}

	if _, isNull := took.GetKind().(*expr.Value_NullValue); isNull || took.GetKind() == nil {
		// No case matched. The default ran if there is one; otherwise the step
		// ran nothing, and there is no arm to credit.
		if sw.GetDefault() != nil {
			reached[step+":default"] = true
		}

		return
	}

	for i, c := range sw.GetCases() {
		values := c.GetValues()
		for j, candidate := range values {
			literal, ok := candidate.GetKind().(*v1.Value_Literal)
			if !ok {
				continue
			}
			if !v1.SwitchLiteralsEqual(took, literal.Literal) {
				continue
			}

			key := fmt.Sprintf("%s:case[%d]", step, i)
			if len(values) > 1 {
				key += fmt.Sprintf("[%d]", j)
			}
			reached[key] = true

			return
		}
	}
}
