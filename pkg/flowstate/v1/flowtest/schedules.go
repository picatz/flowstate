package flowtest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
)

// Seeded schedule exploration for `flow test` (issue #800, #477 slice 2).
//
// The deterministic simulation tier (pkg/flowstate/v1/dst) has been able to run
// one workflow under many schedules since #512, and until now only a Go test
// could ask it to: `runCase` never installed a [v1.Scheduler], so every case
// `flow test` has ever run took [v1.WrittenOrder]. This is the half an author
// reaches. Every case runs once under written order — the run whose verdict and
// whose coverage the file reports, unchanged — and then once per seed, and the
// first case whose observables depended on which schedule ran is reported with
// the seed that produced it.
//
// # What a green here means, exactly
//
// The local driver's schedule space, and nothing else. `dst` is local-only by
// design (its package doc's "Which driver") and `flow test` runs the local
// driver only by design (#155), so the two compose into one narrow claim: this
// file's observables do not depend on the orderings *this* engine is free to
// choose — the order a `parallel:` block advances its branches in, and whether
// an `async:` step's work happens where it is written or at the position that
// joins it. It is not a claim about Temporal's orderings. Those are Temporal's,
// and what keeps them honest is replay against its own history.
//
// # Why the decision count is reported even when nothing failed
//
// A workflow with no `parallel:` and no `async:` reaches no junction, so every
// one of its schedules *is* written order and a thousand seeds prove exactly
// what one run proved. A green with no decision count attached is therefore
// ambiguous in the worst direction — it reads as "seeded exploration found
// nothing" when the truth is "there was nothing to explore" — so
// [ScheduleReport.Decisions] travels with every result and the command says so
// in words when it is zero. The same reasoning as [dst.Report.Decisions], moved
// to the surface an author actually reads.

// ScheduleReport is what running one `*.test.yaml`'s cases under more than one
// schedule found.
//
// Nil for a run that explored nothing, which is the ordinary case: `flow test`
// defaults to no seeds and one written-order run per case.
type ScheduleReport struct {
	// Schedules is how many seeded schedules each case ran under, beyond the
	// written-order run.
	Schedules int

	// Cases is how many of the file's cases were explored.
	Cases int

	// Decisions is the largest number of scheduling choices any one schedule of
	// any case asked for.
	//
	// Zero means no case reached a junction: the file's workflows have no
	// `parallel:` and no `async:`, so every schedule explored was written order
	// and the exploration proved nothing beyond what the ordinary run already
	// proved. Reported rather than inferred, because that distinction is
	// invisible from the outcome alone.
	Decisions int

	// Truncated reports that some schedule spent its whole
	// [v1.MaxScheduleDecisions] budget and took written order for the rest of
	// its run, so the interleaving it explored stopped partway.
	Truncated bool

	// Divergence is the first case whose observables were not the same under a
	// seeded schedule as under written order, or nil when every case agreed with
	// itself under every schedule explored.
	Divergence *ScheduleDivergence
}

// ScheduleDivergence is one case that observed a difference the schedule made.
type ScheduleDivergence struct {
	// Case is the name of the test case, as the file spells it.
	Case string

	// Seed is the schedule that disagreed — the whole of what a replay needs.
	Seed uint64

	// Decisions is how many scheduling choices that seed's run made, and
	// WrittenOrderDecisions how many the baseline made (always zero: written
	// order asks a scheduler for nothing).
	Decisions int

	// Truncated reports that the diverging schedule spent its whole decision
	// budget, so what it explored is only the part before the bound.
	Truncated bool

	// WrittenOrder and Seeded are the two runs' renderings for a person to
	// read in a failure. The comparison was made over the runs themselves;
	// where either withholds anything, both are shown under what either
	// withholds ([dst.Withholding]).
	WrittenOrder string
	Seeded       string
}

// Report renders this exploration as the schema message the machine report
// carries ([v1.ScheduleExploration]), so `flow test -o json` emits it through
// protojson like every other part of the report rather than through a second,
// hand-shaped encoder that could disagree with the first — the same reasoning
// as [Coverage.Report], which this mirrors (issue #931).
func (s *ScheduleReport) Report() *v1.ScheduleExploration {
	report := &v1.ScheduleExploration{
		Schedules: int32(s.Schedules),
		Cases:     int32(s.Cases),
		Decisions: int32(s.Decisions),
		Truncated: s.Truncated,
	}

	if d := s.Divergence; d != nil {
		report.Divergence = &v1.ScheduleDivergenceReport{
			Case:         d.Case,
			Seed:         d.Seed,
			Decisions:    int32(d.Decisions),
			Truncated:    d.Truncated,
			WrittenOrder: d.WrittenOrder,
			Seeded:       d.Seeded,
		}
	}

	return report
}

// scheduleAccumulator runs each of a file's cases under the schedules a budget
// describes and keeps what the whole file's exploration found.
//
// One accumulator per file rather than per case, because the question an author
// asked is about the file: the first case that diverges is the one reported, and
// the decision count is the largest any case reached, so a file where one case
// has a `parallel:` and nine do not still reports honestly that something was
// explored.
type scheduleAccumulator struct {
	budget dst.Budget

	// explores is false for the default budget, and is what keeps `flow test`
	// with no seeds byte-for-byte what it was: one run per case, no digest
	// computed, no second schedule.
	explores bool

	cases      int
	schedules  int
	decisions  int
	truncated  bool
	divergence *ScheduleDivergence
}

// caseRun is one invocation of a case, as [scheduleAccumulator.run] drives it:
// the context carries whatever scheduler this schedule is, and everything else
// about the case is already bound.
type caseRun func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, caseShown, error)

// newScheduleAccumulator returns the accumulator for one file's run.
func newScheduleAccumulator(budget dst.Budget) *scheduleAccumulator {
	return &scheduleAccumulator{
		budget: budget,
		// A pinned seed is a search of exactly one schedule — the replay a
		// divergence tells you to run — so it explores even though Schedules is
		// zero. See [dst.Budget.Pinned].
		explores: budget.Pinned != nil || budget.Schedules > 0,
	}
}

// run runs one case under every schedule the budget describes, and returns what
// the *written-order* run produced: the verdict the file reports and the spec
// and transcript its coverage is measured from.
//
// Written order is the reported run on purpose. A seeded schedule is evidence
// about the file, not a second opinion about the case: letting one change a
// verdict would make `flow test --seeds N` report a different pass/fail set than
// `flow test`, so an author could not tell a real regression from a schedule
// they happened to draw. What a seeded schedule can do is fail the *command*, by
// producing a divergence, which is a finding of its own kind and is reported as
// one.
func (a *scheduleAccumulator) run(ctx context.Context, once caseRun) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine) {
	if !a.explores {
		result, spec, transcript, account, _, _ := once(ctx)

		return result, spec, transcript, account
	}

	a.cases++

	var (
		result     *v1.TestCase
		spec       *v1.Workflow
		transcript *v1.Workflow_StepOutputs
		account    []TranscriptLine
	)

	report := dst.Explore(ctx, a.budget, func(ctx context.Context) dst.Result {
		caseResult, caseSpec, caseTranscript, caseAccount, shown, runErr := once(ctx)

		// Identified by the scheduler on the context rather than by which
		// invocation this is: [dst.Explore] documents that it runs the baseline
		// first, and this stays correct whatever order it comes to run its
		// schedules in. The account kept is the written-order run's, exactly
		// as the verdict and the coverage are.
		if v1.SchedulerFromContext(ctx) == v1.WrittenOrder {
			result, spec, transcript, account = caseResult, caseSpec, caseTranscript, caseAccount
		}

		// Compared as it is, shown as the case's own report would show it: a
		// divergence is printed and emitted with `-o json`, and it must not be
		// the one place a withheld value appears (#2214). Every run can be
		// shown, since a divergence shows it beside a run that may withhold
		// what this one does not.
		observed := dst.Result{
			Transcript: caseTranscript,
			Err:        caseObservables(caseResult, runErr),
			Show: func(withheld dst.Withholding) dst.Result {
				return shownCase(caseResult, caseTranscript, shown, withheld)
			},
		}
		if !shown.sensitive.Empty() {
			observed.Withheld = shownPosture{sensitive: shown.sensitive}
		}

		return observed
	})

	// Counted from the report rather than from the budget, so the number printed
	// is how many schedules were really run and not how many were asked for.
	a.schedules = report.Schedules()

	if decisions := report.Decisions(); decisions > a.decisions {
		a.decisions = decisions
	}
	a.truncated = a.truncated || report.Truncated()

	if report.Divergence != nil && a.divergence == nil {
		a.divergence = &ScheduleDivergence{
			Case:         result.GetName(),
			Seed:         report.Divergence.Diverged.Seed,
			Decisions:    report.Divergence.Diverged.Decisions,
			Truncated:    report.Divergence.Diverged.Truncated,
			WrittenOrder: report.Divergence.Baseline.Rendering,
			Seeded:       report.Divergence.Diverged.Rendering,
		}
	}

	return result, spec, transcript, account
}

// result renders what this file's exploration found, or nil when it explored
// nothing because nobody asked it to.
func (a *scheduleAccumulator) result() *ScheduleReport {
	if !a.explores {
		return nil
	}

	return &ScheduleReport{
		Schedules:  a.schedules,
		Cases:      a.cases,
		Decisions:  a.decisions,
		Truncated:  a.truncated,
		Divergence: a.divergence,
	}
}

// caseObservables renders everything one case produced that a schedule is not
// allowed to change, as the single error [dst.Result] compares by text.
//
// Two things, and both are things an author can see. The verdict — the case's
// own error and its unmet expectations — because that is what `flow test`
// prints. And the run's own failure, because `expect.error_contains` and
// `expect.compensated` are read out of it, so a failure whose text depends on
// the schedule is a claim about the file that holds under some orderings and not
// others, whether or not this particular case happens to assert on it. The
// transcript travels separately, on [dst.Result.Transcript].
//
// Nil for a case that passed with nothing to say, which is how a whole file of
// passing cases renders identically under every schedule.
func caseObservables(result *v1.TestCase, runErr error) error {
	lines := make([]string, 0, len(result.GetFailures()))
	for _, failure := range result.GetFailures() {
		lines = append(lines, fmt.Sprintf("failure %s (step %q, value %q): %s",
			failure.GetField(), failure.GetStep(), failure.GetValue(), failure.GetMessage()))
	}

	// Sorted, and this is not cosmetic. assertExpectation produces the
	// `expect.outputs` diagnostics by ranging over the case's own map of
	// expectations, so two runs of one case can report the same set of
	// diagnostics in a different order with nothing about the schedule involved.
	// Comparing that order would make this property report the harness's own map
	// iteration as an engine defect — a false divergence, which for a check like
	// this is worse than a missed one, because nobody can act on it. Content is
	// still compared exactly; only the order it is listed in is not a claim.
	sort.Strings(lines)

	if harnessErr := result.GetError(); harnessErr != "" {
		lines = append([]string{"error: " + harnessErr}, lines...)
	}
	if runErr != nil {
		lines = append([]string{"run: " + runErr.Error()}, lines...)
	}

	if len(lines) == 0 {
		return nil
	}

	return errors.New(strings.Join(lines, "\n"))
}

// shownPosture is a case run's posture as [dst.Withholding], so a divergence
// shows each of its two runs under what either withholds.
type shownPosture struct {
	sensitive sensitiveInputs
}

// Join is both postures, each value held once. A withholding this package did
// not make withholds everything, since nothing says what it holds.
func (p shownPosture) Join(other dst.Withholding) dst.Withholding {
	o, ok := other.(shownPosture)
	if !ok {
		return shownPosture{sensitive: v1.WithheldSensitiveValues()}
	}

	return shownPosture{sensitive: widenedBy(p.sensitive, o.sensitive)}
}

// shownCase is one run of a case as a divergence shows it under withheld: its
// transcript ([withheldTranscript]), its failure, and its verdict, each with
// what withheld holds taken out.
//
// The verdict was rendered under the run's own posture. Where that posture
// withheld everything, it was already rendered under the most any posture
// withholds and is kept; otherwise each line is withheld again under
// withheld, which holds at least as much as the run's own posture did.
func shownCase(result *v1.TestCase, transcript *v1.Workflow_StepOutputs, shown caseShown, withheld dst.Withholding) dst.Result {
	sensitive := v1.WithheldSensitiveValues()
	if posture, ok := withheld.(shownPosture); ok {
		sensitive = posture.sensitive
	}

	verdict := result
	if !shown.sensitive.WithholdAll() && !sensitive.Empty() {
		verdict = proto.CloneOf(result)
		if verdict.GetError() != "" {
			verdict.Error = redactedErrorText(verdict.GetError(), sensitive)
		}
		for _, failure := range verdict.GetFailures() {
			failure.Message = redactedErrorText(failure.GetMessage(), sensitive)
			if failure.GetValue() != "" {
				failure.Value = redactedErrorText(failure.GetValue(), sensitive)
			}
		}
	}

	return dst.Result{
		Transcript: withheldTranscript(transcript, sensitive),
		Err:        caseObservables(verdict, shown.runErrorUnder(sensitive)),
	}
}

// withheldTranscript is transcript with every value sensitive withholds taken
// out, at any depth, for a schedule divergence to show in its place.
//
// By content, as every other rendering of a case is: the same set the case's
// own transcript withholds, so a divergence shows no more of a run than the
// case's report does. A value with nothing to withhold is kept exactly as
// recorded, so where two schedules differ in what may be shown, what is
// shown still differs.
func withheldTranscript(transcript *v1.Workflow_StepOutputs, sensitive sensitiveInputs) *v1.Workflow_StepOutputs {
	if transcript == nil {
		return nil
	}

	withheld := &v1.Workflow_StepOutputs{
		StepValues: make(map[string]*v1.Node_Outputs, len(transcript.GetStepValues())),
	}
	for id, outputs := range transcript.GetStepValues() {
		withheld.StepValues[id] = &v1.Node_Outputs{NamedValues: withheldNamedValues(outputs.GetNamedValues(), sensitive)}
	}
	if run := transcript.GetRunOutputs(); run != nil {
		withheld.RunOutputs = &v1.RunOutputs{Values: withheldNamedValues(run.GetValues(), sensitive)}
	}

	return withheld
}

// withheldNamedValues is values with each value withheld ([withheldValue]) and
// each name withheld as the case's transcript withholds an output name: whole
// where it is sensitive ([redactedKeyText]), then at each substring it
// carries, since an author can write a sensitive value into a name.
//
// Two names that withhold to one spelling are made distinct again, in the
// order of the names as recorded, so neither is lost and the rendering does
// not depend on map order. Step ids are not names a run chose, and are kept.
func withheldNamedValues(values map[string]*v1.Value, sensitive sensitiveInputs) map[string]*v1.Value {
	withheld := make(map[string]*v1.Value, len(values))
	for _, name := range slices.Sorted(maps.Keys(values)) {
		spelled := sensitive.RedactSubstrings(redactedKeyText(name, sensitive))
		distinct := spelled
		for n := 2; taken(withheld, distinct); n++ {
			distinct = fmt.Sprintf("%s#%d", spelled, n)
		}
		withheld[distinct] = withheldValue(values[name], sensitive)
	}

	return withheld
}

// taken reports whether values already holds name, nil or not.
func taken(values map[string]*v1.Value, name string) bool {
	_, ok := values[name]

	return ok
}

// withheldValue is [withheldTranscript] for one recorded value.
//
// A literal is withheld the way the case's transcript withholds it: whole where
// it is sensitive, at each leaf that is, and at each substring it carries. One
// that will not convert is withheld whole, since nothing can be shown to be
// safe in it. An error's message is text the run wrote, often quoting a value.
// An expression is the author's own source, and a secret reference names a
// secret without holding it, so both are kept.
func withheldValue(value *v1.Value, sensitive sensitiveInputs) *v1.Value {
	switch kind := value.GetKind().(type) {
	case *v1.Value_Literal:
		native, err := literalToGo(kind.Literal)
		if err != nil {
			return v1.NewLiteral(sensitiveMarker)
		}
		redacted := redactSubstringsTree(sensitive.RedactTree(native), sensitive)
		if reflect.DeepEqual(redacted, native) {
			return value
		}

		return v1.NewValue(redacted)
	case *v1.Value_Error_:
		withheld := proto.Clone(value).(*v1.Value)
		withheld.GetError().Message = redactedErrorText(kind.Error.GetMessage(), sensitive)

		return withheld
	case *v1.Value_Structure_:
		withheld := proto.Clone(value).(*v1.Value)
		structure := withheld.GetStructure()
		for i, item := range structure.GetList().GetValues() {
			structure.GetList().Values[i] = withheldValue(item, sensitive)
		}
		if entries := structure.GetMap(); entries != nil {
			// A key is text a run chose, as an output name is (Codex, #2214).
			entries.Entries = withheldNamedValues(entries.GetEntries(), sensitive)
		}

		return withheld
	default:
		return value
	}
}
