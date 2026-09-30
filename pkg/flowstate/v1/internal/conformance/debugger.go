package conformance

import (
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// DebuggerCase is a workflow, the answer it must produce, and the step
// boundaries a debugger is offered while it runs.
//
// The two halves are deliberately asymmetric, because the drivers are. See
// [DebuggerCases].
type DebuggerCase struct {
	Case

	// Offered is the step ids a debugger is asked about, in the order it is
	// asked — one entry per *offer*, so a loop body appears once per
	// iteration rather than once.
	//
	// A sequence rather than a set, and every case here runs its steps
	// sequentially so that the sequence is a fact rather than a race. A
	// `for_each:` with `max_parallel:` above one offers its body from several
	// goroutines and the order is the schedule's business; asserting one would
	// be asserting something neither driver promises.
	Offered []string

	// Held is the step ids a *durable* debug lease can hold this run at, in
	// order — #928's stage 2.
	//
	// Always a subsequence of [Offered], never a superset, and the two callers
	// hold it to that from opposite sides: the local one asserts every id here
	// was really offered, and the durable one asserts a lease takes effect
	// where this says it does. A durable driver holding somewhere the local
	// driver does not stop would be the drivers disagreeing about which
	// boundaries a run has, which is exactly what this corpus exists to catch.
	//
	// # Why it is ever shorter
	//
	// A lease names one holder and one position, and the durable driver takes
	// one only where the run *has* a single representable position — the
	// `susp == 0` boundaries, the same set Continue-As-New may suspend at and
	// the same set `RunProgress` answers about. A `switch:` arm, a `parallel:`
	// branch and a loop body are all at several positions at once as far as
	// suspension is concerned, and [v1.DebugPosition] carries no `path` for
	// precisely that reason ("a position that needed one would be a run held in
	// two places, which is not a state this seam can be in", debug.proto).
	//
	// The local driver has no such constraint: it holds a goroutine, so it can
	// stop each branch where that branch is. So the asymmetry is a real
	// property of the two mechanisms rather than a gap, and it is written down
	// here — beside the list it differs from — rather than left to be
	// rediscovered by whoever notices the two counts do not match.
	Held []string
}

// DebuggerCases hold both drivers to one answer about a run that is being
// debugged, and hold the local driver to which boundaries it offers.
//
// # Why this corpus is shaped unlike its neighbours
//
// [v1.Debugger] is a local-driver seam and will stay one until #928's second
// slice: pausing a durable run is a different mechanism for a different reason,
// and a per-process callback would hold one worker's goroutine while the run
// itself is free to continue on another ([v1.Debugger] says so at length).
// `observe.go` states the rule that makes that legitimate rather than a
// violation — the both-drivers rule governs what a *workflow* can observe, and
// no workflow can observe its observer.
//
// So the corpus asks each driver the question it can answer:
//
//   - **Both drivers** run these workflows and must produce
//     [Case.ExpectedOutputs]. That is the claim that matters and the one a
//     debugger could break: a session may hold a run, and may end it, but it
//     may never change what the run computes. `debugger.go` names that as the
//     one thing a debugger must never do — a debugger turning a red case green
//     — and the local caller runs each case *twice*, once plain and once with a
//     session stepping through every boundary, asserting the same outputs both
//     times. The durable driver runs the same workflows with no debugger at
//     all, which is what makes "the same answer" a cross-driver fact rather
//     than one driver agreeing with itself.
//   - **The local driver alone** is held to [DebuggerCase.Offered].
//
// Written now rather than during slice 2, and that is the whole argument for
// this file existing before the feature it describes. The conformance package
// is where an agreement between the drivers is *stated*; an asymmetry that is
// nowhere written down is indistinguishable from an oversight, and the cost of
// discovering during slice 2 that the two drivers had quietly disagreed about
// which boundaries exist is the cost this package was built to avoid. #1111
// item 12.
func DebuggerCases() []DebuggerCase {
	return []DebuggerCase{
		{
			// The ordinary path, and the baseline the other two are read
			// against: a debugger sees each step where its author wrote it.
			Name: "steps are offered in the order they are written",
			Workflow: &v1.Workflow{
				Name:    "debug-order",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					says("second", "two"),
					says("third", "three"),
				},
			},
			ExpectedOutputs: held("first", "second", "third"),
			Offered:         []string{"first", "second", "third"},
			Held:            []string{"first", "second", "third"},
		},
		{
			// The sharp one. [v1.Debugger.BeforeStep] is documented as being
			// called *after* the condition decided the step runs, and the call
			// site honours it — `eval.go` evaluates the condition, `continue`s
			// on a skip, and offers the boundary only below that.
			//
			// A driver that offered a skipped step would stop an author at a
			// step that is not going to run, and `inspect` there would answer
			// about a scope no work will ever be done in. It is also the
			// difference between "the debugger shows the workflow" and "the
			// debugger shows the run", and only the second is useful.
			Name: "a step the condition skipped is never offered",
			Workflow: &v1.Workflow{
				Name:    "debug-skip",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("before", "one"),
					guarded("skipped", "false", "never"),
					says("after", "two"),
				},
			},
			// Absent rather than present and empty, which is the ordinary
			// `if:` rule this corpus inherits rather than restates.
			ExpectedOutputs: held("before", "after"),
			Offered:         []string{"before", "after"},

			// The same two, because a skipped step is not a boundary on either
			// driver: the condition decides first, and only a step that is
			// going to run reaches the seam. A lease held at a step nothing
			// will do would stop a run at a place it is not.
			Held: []string{"before", "after"},
		},
		{
			// Once per iteration, which is the claim a set could not make and
			// a count of distinct steps would get wrong. A session stepping
			// through a loop stops three times in a three-item loop, because
			// that is what the run does — and a driver offering the body once
			// would be describing the *text* rather than the execution.
			//
			// `max_parallel: 1` so the sequence is deterministic; see
			// [DebuggerCase.Offered].
			Name: "a loop is offered once and its body once per iteration",
			Workflow: &v1.Workflow{
				Name:    "debug-loop",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:       v1.NewExpr(`["a", "b", "c"]`),
							MaxParallel: 1,
							Body:        []*v1.Node{says("touch", "visited")},
						}},
					},
				},
			},
			// A loop records one entry per iteration under
			// [v1.LoopResultsField], each holding that iteration's own step
			// outputs — so a three-item loop over one `log:` step is three
			// maps of one empty entry.
			//
			// Stated exactly rather than loosely, even though this corpus is
			// about boundaries and not about loop encoding. The encoding is a
			// cross-driver contract like any other, and a case in this package
			// that declined to pin it would be the one place the two drivers
			// could quietly diverge while a test watched.
			ExpectedOutputs: withStep(held(), "each", map[string]*v1.Value{
				v1.LoopResultsField: v1.NewLiteralList(
					map[string]any{"touch": map[string]any{}},
					map[string]any{"touch": map[string]any{}},
					map[string]any{"touch": map[string]any{}},
				),
			}),
			Offered: []string{"each", "touch", "touch", "touch"},

			// The loop, and not its body. A `for_each:` body runs at a deeper
			// suspend level than the step that declares it — the engine will
			// not Continue-As-New inside one — so it has no representable
			// position for a lease to name. The local driver stops three times
			// here and a durable lease stops once, which is the asymmetry
			// [DebuggerCase.Held] exists to state rather than leave for
			// somebody to find.
			Held: []string{"each"},
		},
		{
			// The asymmetry with nothing else going on, so that the difference
			// between the two lists is the whole of what this case is about.
			//
			// A `switch:` is the sequential member of the family — one arm
			// runs, deterministically, so the offers are a fact rather than a
			// race — and its body is at a deeper suspend level exactly as a
			// loop's is. Written with a `parallel:` this case could not state
			// an order at all; written with a `for_each:` it would repeat the
			// one above.
			Name: "a switch is a boundary and the arm it takes is not",
			Workflow: &v1.Workflow{
				Name:    "debug-switch",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("before", "one"),
					{
						Id: "route",
						Kind: &v1.Node_Switch{Switch: &v1.Switch{
							Value: v1.NewLiteral("go"),
							Cases: []*v1.Switch_Case{{
								Values: []*v1.Value{v1.NewLiteral("go")},
								Steps:  []*v1.Node{says("chosen", "two")},
							}},
						}},
					},
				},
			},
			// A `switch:` records which arm it took beside the value it
			// matched, so the step that is not a pause point still has
			// outputs. Stated exactly rather than loosely, for the reason
			// the loop case above states its encoding: this corpus is
			// about boundaries, and a case that declined to pin what a
			// step produced would be a place the two drivers could
			// quietly diverge while a test watched.
			ExpectedOutputs: withStep(held("before", "chosen"), "route", map[string]*v1.Value{
				"value": v1.NewLiteral("go"),
				"case":  v1.NewLiteral("go"),
			}),
			Offered: []string{"before", "route", "chosen"},
			Held:    []string{"before", "route"},
		},
	}
}

// MissedUntilCase is a run both drivers hold at HeldAt and then resume with
// `until Until`, a target the run never reaches from there. Each driver must
// record the missed-`until` notice once, in the same words, when the run
// completes (#2201): the local session from its run's return, the durable run
// in the snapshot it answers afterwards.
//
// One program and one `until` for both, rather than a test per driver that
// happens to share a string: two drivers disagreeing about whether an `until`
// is still armed at the end of a given run is what this exists to catch.
type MissedUntilCase struct {
	// Name labels the case.
	Name string

	// Workflow is the program, with no `debug:` policy: the durable caller
	// adds the one its harness attaches under.
	Workflow *v1.Workflow

	// HeldAt is where both drivers hold the run before the resume: the
	// first boundary, which a local session holds at on entry and a durable
	// pause asked before the run starts holds at too.
	HeldAt string

	// Until is the target the resume names.
	Until string
}

// MissedPauseCase is a run both drivers are asked to pause while its last
// step, Sleeping, a `sleep:`, is under way. The pause holds at the next step
// boundary, and there is none: the run completes. Each driver must record
// the missed-pause notice once, in the same words, rather than answer the
// pause and then say nothing (#1297).
//
// A sleep because it is the one step each driver's harness can hold under
// way without spending real time on it: the local driver's on a clock the
// test releases, the durable driver's on its test environment's skipped
// time, with the ask arriving partway through.
type MissedPauseCase struct {
	// Name labels the case.
	Name string

	// Workflow is the program, with no `debug:` policy: the durable caller
	// adds the one its harness attaches under.
	Workflow *v1.Workflow

	// Sleeping is the last step, a `sleep:`, the pause is asked during.
	Sleeping string

	// Sleep is how long Sleeping sleeps.
	Sleep time.Duration
}

// MissedPauseCases is the corpus for [MissedPauseCase].
func MissedPauseCases() []MissedPauseCase {
	const sleep = 4 * time.Second

	return []MissedPauseCase{{
		Name: "a pause asked while the last step sleeps",
		Workflow: &v1.Workflow{
			Name:    "missed-pause",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nap", Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(sleep)}}}},
			},
		},
		Sleeping: "nap",
		Sleep:    sleep,
	}}
}

// MissedUntilCases is the corpus for [MissedUntilCase].
func MissedUntilCases() []MissedUntilCase {
	return []MissedUntilCase{{
		Name: "an until naming the step the run is already past",
		Workflow: &v1.Workflow{
			Name:    "missed-until",
			Profile: v1.CurrentProfile,
			Steps:   []*v1.Node{says("first", "one"), says("second", "two"), says("third", "three")},
		},
		HeldAt: "first",
		Until:  "first",
	}}
}

// HeldSensitiveCase is a run both drivers hold inside a callee that declares
// one of its inputs sensitive, where the caller passed it a value it does not
// itself declare sensitive. At that hold, inspecting Expression must withhold
// Secret on both drivers (#2208): the local session from what the engine
// records for the held position, the durable run from its own sensitiveAt.
type HeldSensitiveCase struct {
	// Name labels the case.
	Name string

	// Workflow is the program, with no `debug:` policy: the durable caller
	// adds the one its harness attaches under.
	Workflow *v1.Workflow

	// Until is the target both drivers run to from their first stop, and
	// HeldAt the address they must then be held at.
	Until, HeldAt string

	// Expression is inspected at that hold, and Secret must not appear in
	// the answer.
	Expression, Secret string
}

// HeldSensitiveCases is the corpus for [HeldSensitiveCase].
func HeldSensitiveCases() []HeldSensitiveCase {
	const secret = "hunter2-callee-only-secret"
	child := &v1.Workflow{
		Name:           "child",
		Profile:        v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
		Steps:          []*v1.Node{says("use", "hi")},
	}

	return []HeldSensitiveCase{{
		Name: "a callee's own sensitive input, passed a plain value",
		Workflow: &v1.Workflow{
			Name:    "held-sensitive",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow:  child,
					Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
				}}},
			},
		},
		Until:      "nested/use",
		HeldAt:     "nested(child)/use",
		Expression: "inputs.api_key",
		Secret:     secret,
	}, {
		Name: "a middle workflow's sensitive input, forwarded to a leaf under a plain name",
		Workflow: &v1.Workflow{
			Name:    "held-sensitive-forwarded",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "outer", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:           "middle",
						Profile:        v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{Name: "token", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
						Steps: []*v1.Node{{Id: "inner", Kind: &v1.Node_Call{Call: &v1.Call{
							Workflow: &v1.Workflow{
								Name:           "leaf",
								Profile:        v1.CurrentProfile,
								DeclaredInputs: []*v1.InputDeclaration{{Name: "who", Type: v1.InputDeclaration_TYPE_STRING}},
								Steps:          []*v1.Node{says("use", "hi")},
							},
							Arguments: map[string]*v1.Value{"who": v1.NewExpr("inputs.token")},
						}}}},
					},
					Arguments: map[string]*v1.Value{"token": v1.NewLiteral(secret)},
				}}},
			},
		},
		Until:      "outer/inner/use",
		HeldAt:     "outer(middle)/inner(leaf)/use",
		Expression: "inputs.who",
		Secret:     secret,
	}, {
		// #2213: the value crosses back into the caller's scope, as an output
		// the callee does not declare sensitive and as a later caller step's
		// copy of it, and the caller's own declarations never named it.
		Name: "a callee's sensitive input, handed back as a plain output and read later in the caller",
		Workflow: &v1.Workflow{
			Name:    "returned-sensitive",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:            "child",
						Profile:         v1.CurrentProfile,
						DeclaredInputs:  []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
						Steps:           []*v1.Node{says("use", "hi")},
						DeclaredOutputs: []*v1.OutputDeclaration{{Name: "key", Value: v1.NewExpr("inputs.api_key")}},
					},
					Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
				}}},
				{Id: "copied", Kind: &v1.Node_Value{Value: v1.NewExpr(`"Bearer " + steps.nested.key`)}},
				says("later", "two"),
			},
		},
		Until:      "later",
		HeldAt:     "later",
		Expression: "[steps.nested.key, steps.copied]",
		Secret:     secret,
	}, {
		// #2213: a tolerated call's failure is kept for later steps to read,
		// and it quotes what the callee declared sensitive.
		Name: "a tolerated call's recorded failure, quoting the callee's sensitive input",
		Workflow: &v1.Workflow{
			Name:    "tolerated-sensitive",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Policy: &v1.StepPolicy{ContinueOnError: true}, Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:           "child",
						Profile:        v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
						Steps:          []*v1.Node{{Id: "boom", Kind: &v1.Node_Value{Value: v1.NewExpr(`{"a": 1}[inputs.api_key]`)}}},
					},
					Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
				}}},
				says("later", "two"),
			},
		},
		Until:      "later",
		HeldAt:     "later",
		Expression: "steps.nested",
		Secret:     secret,
	}, {
		// #2213: what a caller took back, passed on to a later callee under a
		// plain name, is withheld at a hold inside that callee.
		Name: "a callee's sensitive input, handed back and passed on to another callee under a plain name",
		Workflow: &v1.Workflow{
			Name:    "returned-passed-on",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:            "child",
						Profile:         v1.CurrentProfile,
						DeclaredInputs:  []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
						Steps:           []*v1.Node{says("use", "hi")},
						DeclaredOutputs: []*v1.OutputDeclaration{{Name: "key", Value: v1.NewExpr("inputs.api_key")}},
					},
					Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
				}}},
				{Id: "passed", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:           "leaf",
						Profile:        v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{Name: "who", Type: v1.InputDeclaration_TYPE_STRING}},
						Steps:          []*v1.Node{says("greet", "hi")},
					},
					Arguments: map[string]*v1.Value{"who": v1.NewExpr("steps.nested.key")},
				}}},
			},
		},
		Until:      "passed/greet",
		HeldAt:     "passed(leaf)/greet",
		Expression: "inputs.who",
		Secret:     secret,
	}, {
		// #2213: an output the callee declares sensitive, computed from
		// nothing it declares sensitive, is withheld where it is handed back.
		Name: "a callee's declared-sensitive output, read later in the caller",
		Workflow: &v1.Workflow{
			Name:    "sensitive-output",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:            "child",
						Profile:         v1.CurrentProfile,
						DeclaredInputs:  []*v1.InputDeclaration{{Name: "seed", Type: v1.InputDeclaration_TYPE_STRING}},
						Steps:           []*v1.Node{says("use", "hi")},
						DeclaredOutputs: []*v1.OutputDeclaration{{Name: "token", Sensitive: true, Value: v1.NewExpr(`"token-" + inputs.seed`)}},
					},
					Arguments: map[string]*v1.Value{"seed": v1.NewLiteral(secret)},
				}}},
				says("later", "two"),
			},
		},
		Until:      "later",
		HeldAt:     "later",
		Expression: "steps.nested.token",
		Secret:     secret,
	}}
}

// FailedSensitiveCase is a run whose callee fails with an error quoting a
// value that callee, or a workflow on the way to it, declares sensitive,
// where the root declares nothing sensitive. Every step the failure passes
// through is reported failed to an attached session, and on both drivers
// none of those reports may show Secret (#2210): the failing step's from its
// own position, each calling step's from what the failure carries out of
// its callee, since the caller's position knows nothing of the callee's
// declarations.
type FailedSensitiveCase struct {
	// Name labels the case.
	Name string

	// Workflow is the program, with no `debug:` policy: the durable caller
	// adds the one its harness attaches under.
	Workflow *v1.Workflow

	// Failed are the steps, innermost first, whose failure each driver must
	// report, and Quoted a fragment of the error every one of those reports
	// carries, so a report that says nothing does not pass for one that
	// withheld the secret.
	Failed []string
	Quoted string

	// Secret must appear in none of the reports.
	Secret string
}

// FailedSensitiveCases is the corpus for [FailedSensitiveCase].
func FailedSensitiveCases() []FailedSensitiveCase {
	const secret = "hunter2-callee-only-secret"
	fails := func(id, reads string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr(`{"a": 1}[` + reads + `]`)}}
	}

	return []FailedSensitiveCase{{
		Name: "a callee's own sensitive input, quoted by its failure",
		Workflow: &v1.Workflow{
			Name:    "failed-sensitive",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:           "child",
						Profile:        v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
						Steps:          []*v1.Node{fails("boom", "inputs.api_key")},
					},
					Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
				}}},
			},
		},
		Failed: []string{"boom", "nested"},
		Quoted: "no such key",
		Secret: secret,
	}, {
		Name: "a middle workflow's sensitive input, forwarded to a leaf whose failure quotes it",
		Workflow: &v1.Workflow{
			Name:    "failed-sensitive-forwarded",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "outer", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:           "middle",
						Profile:        v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{Name: "token", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
						Steps: []*v1.Node{{Id: "inner", Kind: &v1.Node_Call{Call: &v1.Call{
							Workflow: &v1.Workflow{
								Name:           "leaf",
								Profile:        v1.CurrentProfile,
								DeclaredInputs: []*v1.InputDeclaration{{Name: "who", Type: v1.InputDeclaration_TYPE_STRING}},
								Steps:          []*v1.Node{fails("boom", "inputs.who")},
							},
							Arguments: map[string]*v1.Value{"who": v1.NewExpr("inputs.token")},
						}}}},
					},
					Arguments: map[string]*v1.Value{"token": v1.NewLiteral(secret)},
				}}},
			},
		},
		Failed: []string{"boom", "inner", "outer"},
		Quoted: "no such key",
		Secret: secret,
	}, {
		// The failure is the step's `if:`, which records no outcome of its
		// own, so the only report of it is the one the guard seam gives
		// (#2124), and it has to withhold what a failed step's does.
		Name: "a callee's sensitive input, quoted by an if: that could not be evaluated",
		Workflow: &v1.Workflow{
			Name:    "failed-sensitive-guard",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:           "child",
						Profile:        v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
						Steps:          []*v1.Node{guarded("boom", `{"a": 1}[inputs.api_key] == 1`, "never")},
					},
					Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
				}}},
			},
		},
		Failed: []string{"boom", "nested"},
		Quoted: "no such key",
		Secret: secret,
	}, {
		// Only the leaf declares anything: what its failure carries has to
		// survive the middle's own report of it to reach the root's.
		Name: "a leaf's own sensitive input, quoted two calls deep",
		Workflow: &v1.Workflow{
			Name:    "failed-sensitive-deep",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "outer", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:           "middle",
						Profile:        v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{Name: "key", Type: v1.InputDeclaration_TYPE_STRING}},
						Steps: []*v1.Node{{Id: "inner", Kind: &v1.Node_Call{Call: &v1.Call{
							Workflow: &v1.Workflow{
								Name:           "leaf",
								Profile:        v1.CurrentProfile,
								DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
								Steps:          []*v1.Node{fails("boom", "inputs.api_key")},
							},
							Arguments: map[string]*v1.Value{"api_key": v1.NewExpr("inputs.key")},
						}}}},
					},
					Arguments: map[string]*v1.Value{"key": v1.NewLiteral(secret)},
				}}},
			},
		},
		Failed: []string{"boom", "inner", "outer"},
		Quoted: "no such key",
		Secret: secret,
	}, {
		// No step of the callee fails: its declared output does, computed
		// from the callee's scope and reported by the call.
		Name: "a callee's output quoting its sensitive input",
		Workflow: &v1.Workflow{
			Name:    "failed-sensitive-output",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:           "child",
						Profile:        v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
						Steps:          []*v1.Node{says("use", "hi")},
						DeclaredOutputs: []*v1.OutputDeclaration{{
							Name: "bad", Value: v1.NewExpr(`{"a": 1}[inputs.api_key]`),
						}},
					},
					Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)},
				}}},
			},
		},
		Failed: []string{"nested"},
		Quoted: "no such key",
		Secret: secret,
	}, {
		// Refused while the callee's inputs are bound, before it has a
		// position of its own.
		Name: "a callee's sensitive input its constraint refuses",
		Workflow: &v1.Workflow{
			Name:    "failed-sensitive-binding",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				says("first", "one"),
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:    "child",
						Profile: v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{{
							Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true,
							Must: new(`this.startsWith("never-")`),
						}},
						Steps: []*v1.Node{says("use", "hi")},
					},
					Arguments: map[string]*v1.Value{"api_key": v1.NewExpr(`"` + secret + `"`)},
				}}},
			},
		},
		Failed: []string{"nested"},
		Quoted: "must satisfy",
		Secret: secret,
	}}
}

// OutputCase is a run both drivers debug through while its steps finish. Each
// driver must give the same account of every step that produced outputs: the
// values it produced, named and in name order (`flowdebug.FinishedText`),
// withheld as a hold at that step would withhold them.
type OutputCase struct {
	// Name labels the case.
	Name string

	// Workflow is the program, with no `debug:` policy: the durable caller
	// adds the one its harness attaches under. Its first step always runs,
	// so both drivers hold there before the resume.
	Workflow *v1.Workflow

	// Finished is the account of each step that finished, by step id, which
	// the case keeps unique across the workflows it calls. Nil for a case
	// about addresses alone.
	Finished map[string]string

	// Addresses, when set, is the occurrence address of every step that
	// reported an outcome (finished, skipped, failed or tolerated), sorted:
	// where in the run each did, as an author reads it (`each[1]/touch`,
	// `fan#0/left`, `route?0/chosen`). A step in a body is named by the
	// iteration, branch or arm it ran in on both drivers, whether it ran,
	// was skipped, or failed.
	Addresses []string

	// Secret, when set, is a value a callee declares sensitive, which no
	// account may show.
	Secret string
}

// Problems is how a driver's observations differ from the case's account: a
// step whose account is not the expected sentence, one missing, one extra, and
// any account that shows the case's secret. Empty means the driver agrees.
func (c OutputCase) Problems(observations []*v1.DebugObservation) []string {
	var problems []string
	finished := map[string]string{}
	var addresses []string
	for _, observation := range observations {
		switch observation.GetKind() {
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED:
			finished[observation.GetStepId()] = observation.GetText()
			addresses = append(addresses, observation.GetAddress())
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_SKIPPED,
			v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED,
			v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TOLERATED:
			addresses = append(addresses, observation.GetAddress())
		}
		if c.Secret != "" && strings.Contains(observation.GetText(), c.Secret) {
			problems = append(problems, observation.GetStepId()+"'s account showed the secret")
		}
	}
	if c.Finished != nil {
		for id, want := range c.Finished {
			if got, ok := finished[id]; !ok {
				problems = append(problems, "no account of "+id+", want "+want)
			} else if got != want {
				problems = append(problems, "account of "+id+" is "+got+", want "+want)
			}
		}
		for id := range finished {
			if _, ok := c.Finished[id]; !ok {
				problems = append(problems, "unexpected account of "+id+": "+finished[id])
			}
		}
	}
	if c.Addresses != nil {
		slices.Sort(addresses)
		if !slices.Equal(addresses, c.Addresses) {
			problems = append(problems, "finished at "+strings.Join(addresses, ", ")+", want "+strings.Join(c.Addresses, ", "))
		}
	}
	slices.Sort(problems)

	return problems
}

// OutputCases is the corpus for [OutputCase].
func OutputCases() []OutputCase {
	// Longer than any bound an observation is cut to, so a driver that cut
	// the account before withholding it would keep the value's start.
	longSecret := "hunter2-output-secret-" + strings.Repeat("x", 1024)

	return []OutputCase{
		{
			Name: "a step's account carries the outputs it produced",
			Workflow: &v1.Workflow{
				Name:    "output-plain",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "price", Kind: &v1.Node_Value{Value: v1.NewExpr("40 + 2")}},
					{Id: "shape", Kind: &v1.Node_Value{Value: v1.NewExpr(`{"tags": ["a", "b"], "count": 2}`)}},
					{Id: "background", Async: true, Kind: says("background", "two").GetKind()},
				},
			},
			Finished: map[string]string{
				"first":      "first completed",
				"price":      "price -> value: 42",
				"shape":      `shape -> value: {"count":2,"tags":["a","b"]}`,
				"background": "background completed",
			},
		},
		{
			// Where a step ran is part of what a session says of it. A step
			// in a body is named by the iteration, branch or arm it ran in,
			// and a body inside a callee by the call first, as the local
			// driver names them; the durable driver named none of them.
			Name: "a step in a body is addressed by the iteration, branch or arm it ran in",
			Workflow: &v1.Workflow{
				Name:    "output-addresses",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:       v1.NewExpr(`["a", "b"]`),
							MaxParallel: 1,
							Body:        []*v1.Node{says("touch", "visited")},
						}},
					},
					{
						Id: "count",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							State:         "n",
							Initial:       v1.NewLiteral(int64(2)),
							Update:        v1.NewExpr("n - 1"),
							Until:         v1.NewExpr("n <= 1"),
							MaxIterations: 10,
							Body:          []*v1.Node{says("tick", "ticked")},
						}},
					},
					{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
						{Steps: []*v1.Node{says("left", "l")}},
						{Steps: []*v1.Node{says("right", "r")}},
					}}}},
					{
						Id: "route",
						Kind: &v1.Node_Switch{Switch: &v1.Switch{
							Value: v1.NewLiteral("go"),
							Cases: []*v1.Switch_Case{
								{Values: []*v1.Value{v1.NewLiteral("stop")}, Steps: []*v1.Node{says("halted", "no")}},
								{Values: []*v1.Value{v1.NewLiteral("go")}, Steps: []*v1.Node{says("chosen", "yes")}},
							},
						}},
					},
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:    "child",
							Profile: v1.CurrentProfile,
							Steps: []*v1.Node{
								says("inner", "i"),
								{
									Id: "sweep",
									Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
										Items:       v1.NewExpr(`["x"]`),
										MaxParallel: 1,
										Body:        []*v1.Node{says("deep", "d")},
									}},
								},
							},
						},
					}}},
				},
			},
			Addresses: []string{
				"count",
				"count[0]/tick",
				"count[1]/tick",
				"each",
				"each[0]/touch",
				"each[1]/touch",
				"fan",
				"fan#0/left",
				"fan#1/right",
				"first",
				"nested",
				"nested(child)/inner",
				"nested(child)/sweep",
				"nested(child)/sweep[0]/deep",
				"route",
				"route?1/chosen",
			},
		},
		{
			// A step that never ran, or ran and failed, is placed by the run
			// and not by the last step to arrive: the second iteration's
			// skipped `maybe` is `each[1]/maybe`, not the first's address
			// left behind (review of #2236). A concurrent `for_each:` names
			// its iterations too.
			Name: "a skipped, tolerated or concurrent step in a body is addressed by where it ran",
			Workflow: &v1.Workflow{
				Name:    "output-addresses-outcomes",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:       v1.NewExpr(`["a", "b"]`),
							Iterator:    "item",
							MaxParallel: 1,
							Body: []*v1.Node{
								guarded("maybe", `item == "a"`, "visited"),
								{
									Id:     "flaky",
									Policy: &v1.StepPolicy{ContinueOnError: true},
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name:   "log",
										Inputs: map[string]*v1.Value{"message": v1.NewExpr(`{"a": 1}["missing"]`)},
									}},
								},
							},
						}},
					},
					{
						Id: "wide",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:       v1.NewExpr(`["x", "y"]`),
							MaxParallel: 2,
							Body:        []*v1.Node{says("touch", "visited")},
						}},
					},
				},
			},
			Addresses: []string{
				"each",
				"each[0]/flaky",
				"each[0]/maybe",
				"each[1]/flaky",
				"each[1]/maybe",
				"first",
				"wide",
				"wide[0]/touch",
				"wide[1]/touch",
			},
		},
		{
			// A failure the run tolerates is reported as one, and as nothing
			// else: the step did not finish (Copilot, #2233).
			Name: "a tolerated async failure is not also reported as finished",
			Workflow: &v1.Workflow{
				Name:    "output-async-tolerated",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{
						Id:     "flaky",
						Async:  true,
						Policy: &v1.StepPolicy{ContinueOnError: true},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "log",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr(`{"a": 1}["missing"]`)},
						}},
					},
					says("last", "two"),
				},
			},
			Finished: map[string]string{
				"first": "first completed",
				"last":  "last completed",
			},
		},
		{
			// #2213's shape: the callee declares the input sensitive, hands it
			// back as an output it does not, and the caller copies it on. None
			// of the three accounts may show it, in the callee or after it.
			Name: "a step inside a callee, its call, and a copy of what it handed back withhold the callee's sensitive input",
			Workflow: &v1.Workflow{
				Name:    "output-sensitive",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:           "child",
							Profile:        v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
							Steps: []*v1.Node{
								{Id: "header", Kind: &v1.Node_Value{Value: v1.NewExpr(`"Bearer " + inputs.api_key`)}},
							},
							DeclaredOutputs: []*v1.OutputDeclaration{{Name: "key", Value: v1.NewExpr("inputs.api_key")}},
						},
						Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(longSecret)},
					}}},
					{Id: "copied", Kind: &v1.Node_Value{Value: v1.NewExpr(`"Bearer " + steps.nested.key`)}},
				},
			},
			Finished: map[string]string{
				"first":  "first completed",
				"header": `header -> value: "Bearer [redacted]"`,
				"nested": `nested -> key: "[redacted]"`,
				"copied": `copied -> value: "Bearer [redacted]"`,
			},
			// Its first runes, which an account cut before it was withheld
			// would keep.
			Secret: longSecret[:24],
		},
		{
			// A short leaf of a sensitive structure is found by value: no
			// substring of the rendered line finds `7` in `{"pin":7}`.
			// (`who` is not the structure's, so it shows.)
			Name: "a step inside a callee withholds a sensitive structure's leaves by value",
			Workflow: &v1.Workflow{
				Name:    "output-sensitive-struct",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:           "child",
							Profile:        v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{{Name: "creds", Type: v1.InputDeclaration_TYPE_STRUCT, Sensitive: true}},
							Steps: []*v1.Node{
								{Id: "echo", Kind: &v1.Node_Value{Value: v1.NewExpr(`{"who": "ops", "pin": inputs.creds.pin}`)}},
							},
						},
						Arguments: map[string]*v1.Value{"creds": v1.NewExpr(`{"pin": 7, "token": "hunter2-struct"}`)},
					}}},
				},
			},
			Finished: map[string]string{
				"first": "first completed",
				// The key too: a sensitive structure's field names are what its
				// set withholds.
				"echo":   `echo -> value: {"[redacted]":"[redacted]","who":"ops"}`,
				"nested": "nested completed",
			},
			Secret: "hunter2-struct",
		},
	}
}

// GuardCase is a run both drivers debug through while its steps' `if:`s
// decide against them. Each driver must give the same account of every
// decision, taken from the evaluation that made it (#2124): a skip quotes the
// condition that decided it, in [v1.SkippedText]'s words, and a condition
// that could not be evaluated is reported as its step failing, which a
// session would otherwise show as a step the run never reached.
type GuardCase struct {
	// Name labels the case.
	Name string

	// Workflow is the program, with no `debug:` policy: the durable caller
	// adds the one its harness attaches under. Its first step always runs,
	// so both drivers hold there before the resume.
	Workflow *v1.Workflow

	// Skipped is the account of each skip, in order.
	Skipped []string

	// Failed is the step whose `if:` could not be evaluated, which ends the
	// run, or "" for a run that completes. Quoted is what its report must
	// say of the error.
	Failed, Quoted string

	// Secret, when set, is a value a callee declares sensitive and its `if:`
	// quotes, which no account may show.
	Secret string
}

// GuardCases is the corpus for [GuardCase].
func GuardCases() []GuardCase {
	// Longer than any bound an observation is cut to, so a driver that cut
	// the sentence before withholding it would keep the value's start
	// (Codex, #2227).
	longSecret := "hunter2-guard-secret-" + strings.Repeat("x", 1024)

	return []GuardCase{
		{
			Name: "a skip quotes the if: that decided it",
			Workflow: &v1.Workflow{
				Name:    "guard-skip",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					guarded("discount", "size(['a']) > 1", "never"),
					// Written back from the macro call the parse keeps.
					guarded("macro", "['a'].exists(x, x == 'b')", "never"),
					// A compiled `if: false` is a literal, not an expression.
					{Id: "literal", Condition: v1.NewLiteral(false), Kind: says("literal", "never").GetKind()},
					says("last", "two"),
				},
			},
			Skipped: []string{
				"discount skipped: `if: size([\"a\"]) > 1` was false",
				"macro skipped: `if: [\"a\"].exists(x, x == \"b\")` was false",
				"literal skipped (`if: false`)",
			},
		},
		{
			// The condition is the author's text, and here quotes the value a
			// callee declares sensitive, which the root does not. Each driver
			// withholds it where the skip is, as it would any rendering there.
			Name: "a skip inside a callee withholds what the callee declares sensitive",
			Workflow: &v1.Workflow{
				Name:    "guard-sensitive",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:           "child",
							Profile:        v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
							Steps:          []*v1.Node{guarded("rotate", `inputs.api_key != "`+longSecret+`"`, "never")},
						},
						Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(longSecret)},
					}}},
				},
			},
			Skipped: []string{"rotate skipped: `if: inputs.api_key != \"[redacted]\"` was false"},
			// Its first runes, which a sentence cut before it was withheld
			// would keep.
			Secret: longSecret[:24],
		},
		{
			// A bytes literal is written in octal escapes, so a sensitive
			// value in one is withheld by value before the condition is
			// written, or no match for its text finds it (Codex, #2227).
			Name: "a skip inside a callee withholds a sensitive bytes value it quotes",
			Workflow: &v1.Workflow{
				Name:    "guard-sensitive-bytes",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:           "child",
							Profile:        v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{{Name: "creds", Type: v1.InputDeclaration_TYPE_STRUCT, Sensitive: true}},
							Steps: []*v1.Node{
								guarded("rotate", `inputs.creds.token != b"hunter2-bytes"`, "never"),
								// The same bytes written as a string literal.
								guarded("recheck", `string(inputs.creds.token) != "hunter2-bytes"`, "never"),
							},
						},
						Arguments: map[string]*v1.Value{"creds": v1.NewExpr(`{"token": b"hunter2-bytes"}`)},
					}}},
				},
			},
			// The field is withheld too: a sensitive structure's keys are
			// what its set withholds, and the renderer quotes a field name
			// that is not an identifier.
			Skipped: []string{
				"rotate skipped: `if: inputs.creds.`[redacted]` != \"[redacted]\"` was false",
				"recheck skipped: `if: string(inputs.creds.`[redacted]`) != \"[redacted]\"` was false",
			},
			// "hun" as the renderer writes it in a bytes literal; the
			// string literal is checked by the expected sentences.
			Secret: `\150\165\156`,
		},
		{
			// A string that merely contains a sensitive structure's bytes is
			// withheld from the bytes' text, which the set holds a spelling
			// of (#2231).
			Name: "a skip inside a callee withholds a literal containing a nested sensitive bytes value",
			Workflow: &v1.Workflow{
				Name:    "guard-sensitive-bytes-contained",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:           "child",
							Profile:        v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{{Name: "creds", Type: v1.InputDeclaration_TYPE_STRUCT, Sensitive: true}},
							Steps: []*v1.Node{
								guarded("rotate", `"Bearer hunter2-contained" != "Bearer " + string(inputs.creds.token)`, "never"),
							},
						},
						Arguments: map[string]*v1.Value{"creds": v1.NewExpr(`{"token": b"hunter2-contained"}`)},
					}}},
				},
			},
			Skipped: []string{
				"rotate skipped: `if: \"[redacted]\" != \"Bearer \" + string(inputs.creds.`[redacted]`)` was false",
			},
			Secret: "hunter2-contained",
		},
		{
			// The renderer escapes the quote, so the value's own text is
			// no longer in the sentence to be matched; a literal that merely
			// contains it is withheld from its unescaped text (Copilot,
			// #2227).
			Name: "a skip inside a callee withholds a literal containing a sensitive value the renderer escapes",
			Workflow: &v1.Workflow{
				Name:    "guard-sensitive-escaped",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:           "child",
							Profile:        v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
							Steps:          []*v1.Node{guarded("rotate", `"key: hunter2\"quoted" != "key: " + inputs.api_key`, "never")},
						},
						Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(`hunter2"quoted`)},
					}}},
				},
			},
			Skipped: []string{"rotate skipped: `if: \"[redacted]\" != \"key: \" + inputs.api_key` was false"},
			Secret:  "hunter2",
		},
		{
			// A value written as a literal of another type is spelled the
			// renderer's own way, a string in octal inside a bytes literal and
			// an int in exponent form as a double, which no match for the
			// value's text finds, so each is asked about as every type it
			// could be held as (#2227).
			Name: "a skip inside a callee withholds sensitive values written as literals of another type",
			Workflow: &v1.Workflow{
				Name:    "guard-sensitive-retyped",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:    "child",
							Profile: v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{
								{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true},
								{Name: "pin", Type: v1.InputDeclaration_TYPE_INT, Sensitive: true},
								{Name: "vault", Type: v1.InputDeclaration_TYPE_STRUCT, Sensitive: true},
							},
							Steps: []*v1.Node{
								guarded("bearer", `bytes("Bearer " + inputs.api_key) != b"Bearer hunter2-retyped"`, "never"),
								guarded("unlock", `double(inputs.pin) != 918273645.0`, "never"),
								// Past the signed range, where only the unsigned
								// form holds it.
								guarded("vault", `double(inputs.vault.n) != 9223372036854775808.0`, "never"),
								// A null leaf, which the set holds as nil.
								guarded("absent", `inputs.vault.gone != null`, "never"),
							},
						},
						Arguments: map[string]*v1.Value{
							"api_key": v1.NewLiteral("hunter2-retyped"),
							"pin":     v1.NewLiteral(int64(918273645)),
							"vault":   v1.NewExpr(`{"n": 9223372036854775808u, "gone": null}`),
						},
					}}},
				},
			},
			Skipped: []string{
				"bearer skipped: `if: bytes(\"Bearer \" + inputs.api_key) != \"[redacted]\"` was false",
				"unlock skipped: `if: double(inputs.pin) != \"[redacted]\"` was false",
				"vault skipped: `if: double(inputs.vault.`[redacted]`) != \"[redacted]\"` was false",
				"absent skipped: `if: inputs.vault.`[redacted]` != \"[redacted]\"` was false",
			},
			// "hun" as the renderer writes it in a bytes literal; the pin's
			// exponent form is checked by the same substring.
			Secret: `\150\165\156`,
		},
		{
			// A field name is not a constant, and a one-rune key is too short
			// for any text match to look for, so a sensitive structure's key
			// is withheld where the condition selects it (Codex, #2227).
			Name: "a skip inside a callee withholds a one-rune key of a sensitive structure",
			Workflow: &v1.Workflow{
				Name:    "guard-sensitive-key",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:           "child",
							Profile:        v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{{Name: "creds", Type: v1.InputDeclaration_TYPE_STRUCT, Sensitive: true}},
							Steps:          []*v1.Node{guarded("rotate", `inputs.creds.k != "hunter2-onerune"`, "never")},
						},
						Arguments: map[string]*v1.Value{"creds": v1.NewValue(map[string]any{"k": "hunter2-onerune"})},
					}}},
				},
			},
			Skipped: []string{"rotate skipped: `if: inputs.creds.`[redacted]` != \"[redacted]\"` was false"},
			Secret:  "hunter2-onerune",
		},
		{
			// A sensitive structure's bool leaf is asked about by a literal
			// `if:` too, and the words the text pass withholds read alike on
			// both drivers (Codex, #2227).
			Name: "a skip inside a callee withholds a literal condition a sensitive structure holds",
			Workflow: &v1.Workflow{
				Name:    "guard-sensitive-literal",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
						Workflow: &v1.Workflow{
							Name:           "child",
							Profile:        v1.CurrentProfile,
							DeclaredInputs: []*v1.InputDeclaration{{Name: "flags", Type: v1.InputDeclaration_TYPE_STRUCT, Sensitive: true}},
							Steps: []*v1.Node{{
								Id:        "rotate",
								Condition: v1.NewLiteral(false),
								Kind:      says("rotate", "never").GetKind(),
							}},
						},
						Arguments: map[string]*v1.Value{"flags": v1.NewExpr(`{"enabled": false}`)},
					}}},
				},
			},
			// "false" is a word the text pass withholds here as well.
			Skipped: []string{"rotate skipped: `if: \"[redacted]\"` was [redacted]"},
		},
		{
			Name: "an if: that cannot be evaluated is its step failing",
			Workflow: &v1.Workflow{
				Name:    "guard-error",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("first", "one"),
					guarded("bad", "['a'][5] == 'never'", "never"),
					says("after", "two"),
				},
			},
			Failed: "bad",
			Quoted: "evaluating condition",
		},
	}
}
