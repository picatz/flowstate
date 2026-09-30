package conformance

import (
	"cmp"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// CapabilityCase proves one field of [v1.DebugCapabilities] on both drivers.
//
// The debugger has one contract, and a snapshot's capabilities are what a
// surface believes about the backend behind it: a front advertises only what
// they say and refuses the rest by name. Two hand-kept constructors say it
// (`flowdebug.Session` for the local driver and [v1.DurableDebugCapabilities]
// for the durable one), and nothing else ties what they say to what each driver
// does. A case is that tie: a small workflow, the commands that exercise one
// capability, a reading of what became of them, and what each driver is
// expected to do.
//
// Both drivers' tests collect a [CapabilityObserved] for each case and hand it
// to [AssertCapabilityCase], which is the one place the two sides meet: the
// value the snapshot advertised must equal what the commands did. A driver that
// advertises a capability must apply the command, and one that does not must
// say so by name rather than accept it silently. The same expectations are the
// per-driver table in docs/DEBUGGING.md, so the document is derived from what
// both drivers were held to.
//
// Adding a field to `DebugCapabilities` without adding a case here fails
// `TestEveryCapabilityFieldHasACase`.
type CapabilityCase struct {
	// Field is the proto name of the capability, as `debug.proto` spells it.
	Field string

	// Exercise says in one line what the commands do, for the docs table.
	Exercise string

	// Workflow is the program, with no `debug:` policy: the durable caller
	// adds the one its harness attaches under and a trailing sleep that keeps
	// the run alive to be read.
	Workflow *v1.Workflow

	// SourceMap, when set, maps Workflow's sites to a source document. Only the
	// local driver is handed it: a durable run resolves no source lines.
	SourceMap *v1.DebugSourceMap

	// Probe is what a session sends once the run is held at its first boundary.
	Probe CapabilityProbe

	// Read says what became of the probe. It is written once, over what a
	// driver observed, so both drivers are judged by one reading.
	Read func(CapabilityObserved) CapabilityOutcome

	// Local and Durable are what each driver is expected to do.
	Local, Durable CapabilityOutcome
}

// CapabilityProbe is the commands one case sends, at the run's first hold, in
// the order they are listed here.
type CapabilityProbe struct {
	// Breakpoints is sent first as the whole breakpoint set.
	Breakpoints *v1.DebugSetBreakpointsRequest

	// Pause asks the held run to pause.
	Pause bool

	// Inspect is evaluated against the held scope. Its revision is the hold's.
	Inspect *v1.DebugInspectRequest

	// Moves are resumes, each sent once the previous one has settled at the
	// run's next stop or end.
	Moves []*v1.DebugResumeRequest
}

// CapabilityObserved is what one driver answered to a probe, collected without
// judgement: the case's Read is what interprets it.
type CapabilityObserved struct {
	// Advertised is the capabilities of the snapshot the run was first held at,
	// before any command was sent.
	Advertised *v1.DebugCapabilities

	// Set is the receipt of the breakpoint set, and Breakpoints its states.
	Set         *v1.DebugReceipt
	Breakpoints []*v1.DebugBreakpointState

	// Pause is the receipt of the pause.
	Pause *v1.DebugReceipt

	// Inspect and InspectError are the inspection's answer.
	Inspect      *v1.DebugInspectResponse
	InspectError error

	// Moves are the receipts of the resumes.
	Moves []*v1.DebugReceipt

	// After is where the run stands once the probe has settled.
	After *v1.DebugSnapshot
}

// CapabilityOutcome is what became of a capability's commands.
//
// Expected, Applied says whether the driver does the thing and Says is text an
// unapplied outcome's own words must contain, so that a refusal is named
// rather than silent. Observed, Says is those words in full.
type CapabilityOutcome struct {
	Applied bool
	Says    string
}

// CapabilityCases is the corpus for [CapabilityCase], one per field of
// [v1.DebugCapabilities] that is a behavior.
func CapabilityCases() []CapabilityCase {
	const (
		cont    = v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE
		stepIn  = v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN
		stepOut = v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT
		over    = v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER
	)

	// Three steps at the top level: every boundary a durable run holds at, and
	// nothing a durable run cannot.
	straight := func(name string) *v1.Workflow {
		return &v1.Workflow{
			Name:    name,
			Profile: v1.CurrentProfile,
			Steps:   []*v1.Node{says("first", "one"), says("second", "two"), says("third", "three")},
		}
	}

	// A call, held at before it is entered, with a step after it.
	calling := func(name string) *v1.Workflow {
		return &v1.Workflow{
			Name:    name,
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
					Name:    "child",
					Profile: v1.CurrentProfile,
					Steps:   []*v1.Node{says("greet", "hello")},
				}}}},
				says("last", "done"),
			},
		}
	}

	// A `for_each:` running one iteration at a time, so a step in its body is
	// arrived at three times and a durable run holds at each.
	visiting := &v1.Workflow{
		Name:    "capability-hit",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			says("first", "one"),
			{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:       v1.NewExpr(`["a", "b", "c"]`),
				MaxParallel: 1,
				Body:        []*v1.Node{says("touch", "visited")},
			}}},
		},
	}

	// A step that fails and is tolerated, so the run goes on past a failure a
	// failure stop would hold at.
	failing := &v1.Workflow{
		Name:    "capability-failure",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{says("first", "one"), failsWhen("flaky", "true"), says("last", "done")},
	}

	sourced := straight("capability-source")

	applied := CapabilityOutcome{Applied: true}

	return []CapabilityCase{
		{
			Field:    "step_in",
			Exercise: "`step` at a call enters the callee's first step",
			Workflow: calling("capability-step-in"),
			Probe:    CapabilityProbe{Moves: []*v1.DebugResumeRequest{{Action: stepIn}}},
			Read:     stopsAt("nested(child)/greet"),
			Local:    applied,
			Durable:  applied,
		},
		{
			Field:    "step_over",
			Exercise: "`next` at a call runs the callee whole and stops after it",
			Workflow: calling("capability-step-over"),
			Probe:    CapabilityProbe{Moves: []*v1.DebugResumeRequest{{Action: over}}},
			Read:     stopsAt("last"),
			Local:    applied,
			Durable:  applied,
		},
		{
			Field:    "step_out",
			Exercise: "`finish` inside a callee runs it to its end and stops after the call",
			Workflow: calling("capability-step-out"),
			Probe:    CapabilityProbe{Moves: []*v1.DebugResumeRequest{{Action: stepIn}, {Action: stepOut}}},
			Read:     stopsAt("last"),
			Local:    applied,
			Durable:  applied,
		},
		{
			Field: "pause",
			// A pause asked of a run already held is answered applied rather
			// than pending, which is the only pause both drivers answer without
			// racing the run; a pause taking effect at the next boundary is
			// the missed-pause corpus's claim.
			Exercise: "`pause` is accepted by a session attached to the run",
			Workflow: straight("capability-pause"),
			Probe:    CapabilityProbe{Pause: true},
			Read:     acceptedPause,
			Local:    applied,
			Durable:  applied,
		},
		{
			Field:    "run_until",
			Exercise: "`until third` runs to that step and stops there",
			Workflow: straight("capability-until"),
			Probe:    CapabilityProbe{Moves: []*v1.DebugResumeRequest{{Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "third"}}},
			Read:     stopsAt("third", v1.DebugStopReason_DEBUG_STOP_REASON_UNTIL),
			Local:    applied,
			Durable:  applied,
		},
		{
			Field:    "conditional_breakpoints",
			Exercise: "a breakpoint whose condition is false is passed, and the next breakpoint stops the run",
			Workflow: straight("capability-condition"),
			Probe: CapabilityProbe{
				Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{
					{Id: "never", Step: "second", Condition: "1 == 2"},
					{Id: "gate", Step: "third"},
				}},
				Moves: []*v1.DebugResumeRequest{{Action: cont}},
			},
			Read:    stopsAtBreakpoint("third", "gate"),
			Local:   applied,
			Durable: applied,
		},
		{
			Field:    "hit_conditions",
			Exercise: "a breakpoint with `== 2` stops at the step's second arrival only",
			Workflow: visiting,
			Probe: CapabilityProbe{
				Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{
					{Id: "twice", Step: "touch", HitCondition: "== 2"},
				}},
				Moves: []*v1.DebugResumeRequest{{Action: cont}},
			},
			Read:    stopsAtBreakpoint("each[1]/touch", "twice"),
			Local:   applied,
			Durable: applied,
		},
		{
			Field:    "logpoints",
			Exercise: "a breakpoint with `log` records its message and does not stop the run",
			Workflow: straight("capability-logpoint"),
			Probe: CapabilityProbe{
				Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{
					{Id: "note", Step: "second", LogMessage: "second reached"},
				}},
				Moves: []*v1.DebugResumeRequest{{Action: cont}},
			},
			Read:    recordsLogpoint("second reached"),
			Local:   applied,
			Durable: CapabilityOutcome{Says: "logpoints are not supported"},
		},
		{
			Field:    "failure_breakpoints",
			Exercise: "failure mode `all` holds the run at a step whose failure `continue_on_error:` tolerates",
			Workflow: failing,
			Probe: CapabilityProbe{
				Breakpoints: &v1.DebugSetBreakpointsRequest{FailureMode: v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL},
				Moves:       []*v1.DebugResumeRequest{{Action: cont}},
			},
			Read:    stopsOnFailure("flaky"),
			Local:   applied,
			Durable: CapabilityOutcome{Says: "failure stops are not supported"},
		},
		{
			Field:     "source_breakpoints",
			Exercise:  "a breakpoint on a source line stops at the step written there",
			Workflow:  sourced,
			SourceMap: sourceMapOf(sourced, "second", 2),
			Probe: CapabilityProbe{
				Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{
					{Id: "line", Line: &v1.DebugSourceLine{Uri: capabilitySource, Line: 2}},
				}},
				Moves: []*v1.DebugResumeRequest{{Action: cont}},
			},
			Read:    stopsAtBreakpoint("second", "line"),
			Local:   applied,
			Durable: CapabilityOutcome{Says: "resolves no source lines"},
		},
		{
			Field:    "inspect",
			Exercise: "`inspect 1 + 1` evaluates against the held scope",
			Workflow: straight("capability-inspect"),
			Probe:    CapabilityProbe{Inspect: &v1.DebugInspectRequest{Expression: "1 + 1"}},
			Read:     inspected("2", 0),
			Local:    applied,
			Durable:  applied,
		},
		{
			Field:    "value_expansion",
			Exercise: "`expand [1, 2, 3]` lists the list's three children",
			Workflow: straight("capability-expand"),
			Probe:    CapabilityProbe{Inspect: &v1.DebugInspectRequest{Expression: "[1, 2, 3]", Children: true}},
			Read:     inspected("", 3),
			Local:    applied,
			Durable:  applied,
		},
		{
			Field:    "observations",
			Exercise: "a step that ran is reported between stops as finished",
			Workflow: straight("capability-observe"),
			Probe:    CapabilityProbe{Moves: []*v1.DebugResumeRequest{{Action: stepIn}}},
			Read:     observedFinished("first"),
			Local:    applied,
			Durable:  applied,
		},
		{
			// There is no command to send: the contract's only resume that
			// leaves a run is a detach, and that never ends it. So the case
			// exercises the nearest thing, and reads whether the run was ended.
			Field:    "terminate",
			Exercise: "`detach` releases the run and never ends it",
			Workflow: straight("capability-terminate"),
			Probe:    CapabilityProbe{Moves: []*v1.DebugResumeRequest{{Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH}}},
			Read:     endedTheRun,
			Local:    CapabilityOutcome{Says: "the run continues"},
			Durable:  CapabilityOutcome{Says: "the run continues"},
		},
		{
			// Likewise: nothing here can be sent, so the case reads the resume
			// vocabulary the contract has.
			Field:    "reverse",
			Exercise: "no resume action moves a run backwards",
			Workflow: straight("capability-reverse"),
			Read:     movesBackwards,
			Local:    CapabilityOutcome{Says: "no resume action"},
			Durable:  CapabilityOutcome{Says: "no resume action"},
		},
	}
}

// capabilitySource is the document [CapabilityCase.SourceMap] names.
const capabilitySource = "capability.yaml"

// sourceMapOf maps the site of the step id in workflow to one line of a
// single document, bound to the workflow by its digest.
func sourceMapOf(workflow *v1.Workflow, id string, line uint32) *v1.DebugSourceMap {
	sites, _ := v1.DebugStaticSites(workflow)
	for _, site := range sites {
		if len(site.Site.GetPath()) == 1 && site.Site.GetPath()[0] == id {
			return &v1.DebugSourceMap{
				IrDigest:  v1.WorkflowIRDigest(workflow),
				Documents: []*v1.DebugSourceDocument{{Uri: capabilitySource, Language: "flowfile"}},
				Entries: []*v1.DebugSourceEntry{{
					Site:     site.Site,
					Location: &v1.DebugSourceLocation{Range: &v1.SourceRange{StartLine: line, EndLine: line}},
				}},
			}
		}
	}

	panic("conformance: no step " + id + " in " + workflow.GetName())
}

// accepted reports whether a receipt is one the run took: applied, or delivered
// and waiting for its next boundary.
func accepted(receipt *v1.DebugReceipt) bool {
	switch receipt.GetStatus() {
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE,
		v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING:
		return true
	default:
		return false
	}
}

// refusal is the words of the first command a driver did not take, so an
// outcome that was not applied names why.
func refusal(receipts ...*v1.DebugReceipt) string {
	for _, receipt := range receipts {
		if receipt != nil && !accepted(receipt) {
			return receipt.GetMessage()
		}
	}

	return ""
}

// heldAt reports whether the run is held at address, for one of reasons when
// any are given.
func heldAt(after *v1.DebugSnapshot, address string, reasons ...v1.DebugStopReason) bool {
	if after.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD || after.GetOccurrence().GetAddress() != address {
		return false
	}

	return len(reasons) == 0 || reasons[0] == after.GetReason()
}

// stopsAt reads a run that a resume moved to address.
func stopsAt(address string, reasons ...v1.DebugStopReason) func(CapabilityObserved) CapabilityOutcome {
	return func(o CapabilityObserved) CapabilityOutcome {
		says := refusal(o.Moves...)

		return CapabilityOutcome{Applied: says == "" && heldAt(o.After, address, reasons...), Says: says}
	}
}

// stopsAtBreakpoint reads a run that a breakpoint set stopped at address, at
// exactly the breakpoint named id. A breakpoint the driver did not arm says why
// in its own state.
func stopsAtBreakpoint(address, id string) func(CapabilityObserved) CapabilityOutcome {
	return func(o CapabilityObserved) CapabilityOutcome {
		says := unarmed(o)
		stopped := heldAt(o.After, address, v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT) &&
			len(o.After.GetBreakpointIds()) == 1 && o.After.GetBreakpointIds()[0] == id

		return CapabilityOutcome{Applied: says == "" && stopped, Says: says}
	}
}

// unarmed is the words a driver used for the first breakpoint it did not arm,
// or for the set or resume it did not take.
func unarmed(o CapabilityObserved) string {
	for _, state := range o.Breakpoints {
		if !state.GetVerified() {
			return state.GetMessage()
		}
	}

	return refusal(append([]*v1.DebugReceipt{o.Set}, o.Moves...)...)
}

// recordsLogpoint reads a run whose logpoint was armed, recorded message and
// never stopped the run.
func recordsLogpoint(message string) func(CapabilityObserved) CapabilityOutcome {
	return func(o CapabilityObserved) CapabilityOutcome {
		says := unarmed(o)
		recorded := false
		for _, observation := range o.After.GetObservations() {
			if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_LOG &&
				strings.Contains(observation.GetText(), message) {
				recorded = true
			}
		}

		return CapabilityOutcome{
			Applied: says == "" && recorded && o.After.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD,
			Says:    says,
		}
	}
}

// stopsOnFailure reads a run that a failure stop held at step id.
func stopsOnFailure(id string) func(CapabilityObserved) CapabilityOutcome {
	return func(o CapabilityObserved) CapabilityOutcome {
		says := unarmed(o)

		return CapabilityOutcome{
			Applied: says == "" && heldAt(o.After, id, v1.DebugStopReason_DEBUG_STOP_REASON_FAILURE),
			Says:    says,
		}
	}
}

// acceptedPause reads a pause the run took.
func acceptedPause(o CapabilityObserved) CapabilityOutcome {
	says := refusal(o.Pause)

	return CapabilityOutcome{Applied: says == "" && o.Pause != nil, Says: says}
}

// inspected reads an inspection that evaluated to rendered, when that is
// given, and listed children.
func inspected(rendered string, children int) func(CapabilityObserved) CapabilityOutcome {
	return func(o CapabilityObserved) CapabilityOutcome {
		if o.InspectError != nil {
			return CapabilityOutcome{Says: o.InspectError.Error()}
		}
		if o.Inspect == nil || o.Inspect.GetError() != "" {
			return CapabilityOutcome{Says: o.Inspect.GetError()}
		}

		return CapabilityOutcome{
			Applied: (rendered == "" || o.Inspect.GetValue().GetRendered() == rendered) &&
				len(o.Inspect.GetChildren()) == children,
		}
	}
}

// observedFinished reads a session that was told the step id ran.
func observedFinished(id string) func(CapabilityObserved) CapabilityOutcome {
	return func(o CapabilityObserved) CapabilityOutcome {
		says := refusal(o.Moves...)
		for _, observation := range o.After.GetObservations() {
			if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED &&
				observation.GetStepId() == id {
				return CapabilityOutcome{Applied: says == "", Says: says}
			}
		}

		return CapabilityOutcome{Says: says}
	}
}

// endedTheRun reads a detach that ended the run rather than releasing it. A
// run ended by its session is failed; one released is detached, or has since
// finished on its own.
func endedTheRun(o CapabilityObserved) CapabilityOutcome {
	after := o.After.GetState()

	return CapabilityOutcome{
		Applied: after == v1.DebugRunState_DEBUG_RUN_STATE_FAILED,
		Says:    cmp.Or(refusal(o.Moves...), o.After.GetMessage()),
	}
}

// movesBackwards reads whether any resume action the contract has goes back.
func movesBackwards(CapabilityObserved) CapabilityOutcome {
	actions := v1.DebugResumeAction(0).Descriptor().Values()
	for i := range actions.Len() {
		name := string(actions.Get(i).Name())
		if strings.Contains(name, "REVERSE") || strings.Contains(name, "BACK") || strings.Contains(name, "PREVIOUS") {
			return CapabilityOutcome{Applied: true}
		}
	}

	return CapabilityOutcome{Says: "no resume action is a backwards move"}
}

// capabilityField returns the field of [v1.DebugCapabilities] a case names, or
// nil.
func capabilityField(name string) protoreflect.FieldDescriptor {
	return capabilityFields().ByName(protoreflect.Name(name))
}

// capabilityFields is every field of [v1.DebugCapabilities], read from the
// schema so that a field added there is one the corpus must cover.
func capabilityFields() protoreflect.FieldDescriptors {
	return (*v1.DebugCapabilities)(nil).ProtoReflect().Descriptor().Fields()
}

// AssertCapabilityCase holds one driver to one case: what the snapshot
// advertised must equal what the commands did, and both must be what the case
// says of that driver. driver is "local" or "durable".
func AssertCapabilityCase(tb testing.TB, driver string, c CapabilityCase, o CapabilityObserved) {
	tb.Helper()

	field := capabilityField(c.Field)
	if field == nil || field.Kind() != protoreflect.BoolKind {
		tb.Fatalf("capability case %q does not name a bool field of DebugCapabilities", c.Field)
	}
	if o.Advertised == nil {
		tb.Fatalf("%s driver: %s: the run's first snapshot carried no capabilities", driver, c.Field)
	}
	advertised := o.Advertised.ProtoReflect().Get(field).Bool()

	want := c.Local
	if driver == "durable" {
		want = c.Durable
	}
	got := c.Read(o)

	if advertised != got.Applied {
		tb.Errorf("%s driver: %s is advertised as %t, and the commands were %s (%q); a driver advertising a capability must apply "+
			"its command, and one that does not must refuse it by name", driver, c.Field, advertised,
			appliedWord(got.Applied), got.Says)
	}
	if got.Applied != want.Applied {
		tb.Errorf("%s driver: %s: the case says %s, and the commands were %s (%q)", driver, c.Field,
			appliedWord(want.Applied), appliedWord(got.Applied), got.Says)
	}
	if !got.Applied && (want.Says == "" || !strings.Contains(got.Says, want.Says)) {
		tb.Errorf("%s driver: %s is not applied, and its refusal %q does not say %q: an unsupported capability "+
			"is named, never accepted silently", driver, c.Field, got.Says, want.Says)
	}
}

func appliedWord(applied bool) string {
	if applied {
		return "applied"
	}

	return "not applied"
}
