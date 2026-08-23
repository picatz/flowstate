package flowtest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The case transcript (#929 slice 2): what one run *did*, step by step, so a
// failing case's unmet expectation arrives with the account it was judged
// against — which stub answered each step, when virtual time moved, what each
// step produced, which switch arm was taken, when a wait parked and what
// delivery ended it. The events come from [v1.RunObserver] (the engine's own
// single recording points) plus the two facts only this harness knows: which
// stub matcher answered an invocation, and when a scripted signal was
// delivered and as whom.
//
// The rendered lines are the public shape, not the events: the CLI and the
// flowtesting package both print an account, and neither re-derives it. The
// event stream itself stays unexported until the debugger (#928) needs it,
// so its shape is not frozen by a reader it does not have yet.

// TranscriptLine is one rendered line of a case's transcript, with the tone
// the package doc's color rule assigns it: Danger for what failed the run,
// Warning for a fact worth reading that is not a verdict (a tolerated
// failure, a truncation), Info for everything the run simply did.
type TranscriptLine struct {
	Text string
	Tone TranscriptTone
}

// TranscriptTone classifies a line for whoever renders it; a plain-text
// consumer may ignore it entirely.
type TranscriptTone int

const (
	ToneInfo TranscriptTone = iota
	ToneWarning
	ToneDanger
)

// maxTranscriptEvents bounds one case's recorded account. A `loop:` may run
// [v1.MaxLoopIterations] iterations with several steps each, and the
// transcript exists to debug a failure, not to be one: past the bound the
// run continues unrecorded and the rendering ends with a line saying exactly
// that, so a truncated account never reads as a complete one.
const maxTranscriptEvents = 10_000

// maxTranscriptOutputBytes bounds the serialized step-output bytes one case's
// account may retain, beside the event count — because bounding one resource
// does not bound another the submitter controls the ratio to (CLAUDE.md): ten
// thousand events each retaining a megabyte of cloned outputs is gigabytes
// held by a count that reads as bounded. Sized far above any account a person
// debugs by reading and far below what a hostile submission could hold; past
// it the run continues unrecorded, exactly as past the event count.
const maxTranscriptOutputBytes = 8 << 20

// transcriptEvent is one recorded fact; kind decides which fields mean
// anything.
type transcriptEvent struct {
	at   time.Duration
	kind transcriptEventKind

	step      string
	outputs   *v1.Node_Outputs
	failure   string
	tolerated bool

	signal  string
	timeout time.Duration
	bounded bool

	payload map[string]any
	sender  string

	task        string
	stubOrdinal int
	stubStep    string
}

type transcriptEventKind int

const (
	eventStepFinished transcriptEventKind = iota
	eventStepSkipped
	eventWaitStarted
	eventSignalDelivered
	eventSignalRefused
	eventStubAnswered
	eventStubUnmatched
)

// runRecorderKey carries the recorder to the two harness-side recording
// sites that only see a context: the stub function each invocation runs
// through, and nothing else — the engine's own facts arrive through
// [v1.RunObserver] instead.
type runRecorderKey struct{}

func contextWithRunRecorder(ctx context.Context, r *runRecorder) context.Context {
	return context.WithValue(ctx, runRecorderKey{}, r)
}

func runRecorderFromContext(ctx context.Context) *runRecorder {
	r, _ := ctx.Value(runRecorderKey{}).(*runRecorder)
	return r
}

// runRecorder accumulates one case's transcript. It implements
// [v1.RunObserver] for the engine's share and carries two harness-side entry
// points for the rest. Locked, because the observer contract says callbacks
// arrive on the goroutine running the step, and a `parallel:` workflow has
// several.
type runRecorder struct {
	clock *v1.VirtualClock

	mu sync.Mutex

	events []transcriptEvent

	// eventsFull and bytesFull record which bound stopped the account — kept
	// apart because the truncation line names its cause, and blaming the
	// event count for a byte-budget stop sends a reader counting events
	// toward the wrong bound.
	eventsFull  bool
	bytesFull   bool
	outputBytes int

	// sensitive is the redaction set every rendered value passes through —
	// the same [sensitiveInputs] the unmatched-stub diagnostic uses, built
	// from the same declarations, so what `flow test` refuses to print in one
	// place it refuses to print everywhere.
	sensitive sensitiveInputs

	// switches records, per step id, that the compiled workflow declares a
	// `switch:` there and whether it has a `default:` — carried from the spec
	// rather than inferred from a `case` output's presence (Codex, #1052):
	// an ordinary task may name an output `case`, and a null record means
	// "the default ran" only where a default exists. Two isolated bodies may
	// legally declare switches under one id; when their shapes disagree the
	// entry is ambiguous and the renderer falls back to plain outputs rather
	// than guessing.
	switches map[string]switchFact
}

// switchFact is what the renderer may honestly say about one switch step id.
type switchFact struct {
	hasDefault bool
	ambiguous  bool
}

func newRunRecorder(clock *v1.VirtualClock) *runRecorder {
	return &runRecorder{clock: clock, switches: map[string]switchFact{}}
}

// noteSwitches records every `switch:` step whose events this run's observer
// can report — the compiled workflow's own tree at every depth, and the
// callee a `call:` embeds, whose steps run through the same observer under
// their own ids. An id also declared by any non-switch node in an isolated
// sibling body is marked ambiguous, so the renderer falls back to plain
// outputs rather than describing one declaration in the other's words.
// Called once per case, with the same spec every other account is measured
// against.
func (r *runRecorder) noteSwitches(nodes []*v1.Node) {
	nonSwitch := map[string]bool{}
	r.noteSwitchNodes(nodes, nonSwitch, 0)
	for id := range nonSwitch {
		if fact, isSwitch := r.switches[id]; isSwitch {
			fact.ambiguous = true
			r.switches[id] = fact
		}
	}
}

// maxNoteSwitchesDepth mirrors the engine's own call-depth bound: the walk
// descends embedded callees exactly as the run will, and a bound on one side
// without the other would be a walk an adversarial spec can wedge.
const maxNoteSwitchesDepth = 16

func (r *runRecorder) noteSwitchNodes(nodes []*v1.Node, nonSwitch map[string]bool, depth int) {
	if depth > maxNoteSwitchesDepth {
		return
	}
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Switch:
			fact := switchFact{hasDefault: kind.Switch.GetDefault() != nil}
			if existing, seen := r.switches[node.GetId()]; seen && existing != fact {
				fact.ambiguous = true
			}
			r.switches[node.GetId()] = fact
			for _, body := range v1.SwitchBodies(kind.Switch) {
				r.noteSwitchNodes(body, nonSwitch, depth)
			}
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				r.noteSwitchNodes(branch.GetSteps(), nonSwitch, depth)
			}
		case *v1.Node_ForEach:
			nonSwitch[node.GetId()] = true
			r.noteSwitchNodes(kind.ForEach.GetBody(), nonSwitch, depth)
		case *v1.Node_Loop:
			nonSwitch[node.GetId()] = true
			r.noteSwitchNodes(kind.Loop.GetBody(), nonSwitch, depth)
		case *v1.Node_Call:
			// The compiled call embeds its callee, and the callee's steps —
			// its switches included — report through this same run's observer
			// under their own ids (Codex, #1052).
			nonSwitch[node.GetId()] = true
			r.noteSwitchNodes(kind.Call.GetWorkflow().GetSteps(), nonSwitch, depth+1)
		default:
			nonSwitch[node.GetId()] = true
		}
	}
}

// record appends one event under the bound; the timestamp is read here, once,
// so every recording site reports the same clock the run waits against.
func (r *runRecorder) record(event transcriptEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recordLocked(event)
}

func (r *runRecorder) recordLocked(event transcriptEvent) {
	if len(r.events) >= maxTranscriptEvents {
		r.eventsFull = true
		return
	}
	if event.outputs != nil {
		size := proto.Size(event.outputs)
		if r.outputBytes+size > maxTranscriptOutputBytes {
			r.bytesFull = true
			return
		}
		r.outputBytes += size
	}
	event.at = r.clock.Now().Sub(epoch)
	r.events = append(r.events, event)
}

func (r *runRecorder) StepFinished(id string, outputs *v1.Node_Outputs, err error, tolerated bool) {
	event := transcriptEvent{kind: eventStepFinished, step: id, outputs: outputs, tolerated: tolerated}
	if err != nil {
		event.failure = err.Error()
	}
	r.record(event)
}

func (r *runRecorder) StepSkipped(id string) {
	r.record(transcriptEvent{kind: eventStepSkipped, step: id})
}

func (r *runRecorder) WaitStarted(id, signal string, timeout time.Duration, bounded bool) {
	r.record(transcriptEvent{kind: eventWaitStarted, step: id, signal: signal, timeout: timeout, bounded: bounded})
}

// stubAnswered records that one matcher answered an invocation: the ordinal
// every stub diagnostic already numbers stubs by, the step the engine says
// was being served ("" for a compensation, which runs off the run-level
// context), and the task the stub replaced.
func (r *runRecorder) stubAnswered(task string, ordinal int, stubStep, servingStep string) {
	r.record(transcriptEvent{kind: eventStubAnswered, task: task, stubOrdinal: ordinal, step: servingStep, stubStep: stubStep})
}

// stubUnmatched records that an invocation ended with no matcher answering —
// the drained/unmatched fall-through. Its whole job is to clear a stale
// attribution (Codex, #1052): a retried step whose earlier attempt a stub
// answered, and whose final attempt nothing did, must not render that stub's
// identity beside the failure the *unanswered* attempt produced. The failure
// text itself already names what did not match; this event draws no line of
// its own.
func (r *runRecorder) stubUnmatched(servingStep string) {
	r.record(transcriptEvent{kind: eventStubUnmatched, step: servingStep})
}

// deliverRecorded runs one scripted delivery and records its outcome — the
// delivery, or the refusal a declared signal policy or the queue's bound
// answers with — *under the recorder's own lock, around the send itself*.
// That ordering is the point (Codex, #1052): the moment [v1.LocalSignals.DeliverFrom]
// makes a payload visible, the parked run goroutine can wake and try to
// record the wait step's completion, and that record takes this same lock —
// so holding it across the send is what keeps the account deterministic, the
// delivery always before the completion it caused. Safe because DeliverFrom
// buffers or refuses and returns; it never waits on the run's progress.
//
// A refusal is recorded as its own fact rather than dropped, because a
// policy denial is precisely the outcome a case scripting a sender needs to
// see; an account that showed it as delivered would be a false transcript in
// exactly the runs that need debugging.
func (r *runRecorder) deliverRecorded(name string, payload map[string]any, sender string, deliver func() error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := deliver(); err != nil {
		r.recordLocked(transcriptEvent{kind: eventSignalRefused, signal: name, failure: err.Error()})
		return
	}
	r.recordLocked(transcriptEvent{kind: eventSignalDelivered, signal: name, payload: payload, sender: sender})
}

// render turns the account into lines. Called once, after the run, on the
// goroutine that owns the case — the lock is taken only to be correct if a
// signal goroutine is still winding down.
func (r *runRecorder) render() []TranscriptLine {
	r.mu.Lock()
	events := r.events
	eventsFull, bytesFull := r.eventsFull, r.bytesFull
	sensitive := r.sensitive
	switches := r.switches
	r.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	// The step column fits the longest name it will hold, capped so one long
	// id does not push every other line's account off the screen.
	width := 0
	for _, e := range events {
		if label := transcriptLabel(e); len(label) > width && len(label) <= 24 {
			width = len(label)
		}
	}

	// A stub answer annotates the step line it served rather than taking a
	// line of its own — `stub 2 (step: build)` beside what the step produced
	// — except one serving no step (a compensation), which has no line to
	// join. Retries make several answers per step legitimate; the last one is
	// the one whose answer the recorded outcome came from.
	pendingStub := map[string]transcriptEvent{}

	var lines []TranscriptLine
	for _, e := range events {
		switch e.kind {
		case eventStubAnswered:
			if e.step == "" {
				lines = append(lines, transcriptLine(e.at, width, e.task,
					fmt.Sprintf("compensation answered by %s", stubIdentity(e)), ToneInfo))
				continue
			}
			pendingStub[e.step] = e

		case eventStubUnmatched:
			delete(pendingStub, e.step)

		case eventStepFinished:
			text, tone := stepOutcomeText(e, sensitive, switches)
			if stub, ok := pendingStub[e.step]; ok {
				delete(pendingStub, e.step)
				text += "  " + stubIdentity(stub)
			}
			lines = append(lines, transcriptLine(e.at, width, e.step, text, tone))

		case eventStepSkipped:
			lines = append(lines, transcriptLine(e.at, width, e.step, "skipped by its if:", ToneInfo))

		case eventWaitStarted:
			text := fmt.Sprintf("sleeping %s", shortDuration(e.timeout))
			if e.signal != "" {
				text = fmt.Sprintf("waiting: %s (timeout %s)", e.signal, shortDuration(e.timeout))
				if !e.bounded {
					text = fmt.Sprintf("waiting: %s (no timeout)", e.signal)
				}
			}
			lines = append(lines, transcriptLine(e.at, width, e.step, text, ToneInfo))

		case eventSignalDelivered:
			text := fmt.Sprintf("signal %s %s", e.signal, redactedGoValue(e.payload, sensitive))
			if e.sender != "" {
				// Through the same redaction as every other value: a case may
				// spell its sender from the same sensitive input the policy
				// resolves its subject from, and the account must not be the
				// one surface that prints it.
				text += "  sender: " + redactedBareText(e.sender, sensitive)
			}
			lines = append(lines, transcriptLine(e.at, width, "", text, ToneInfo))

		case eventSignalRefused:
			lines = append(lines, transcriptLine(e.at, width, "",
				fmt.Sprintf("signal %s refused: %s", e.signal, redactedBareText(e.failure, sensitive)), ToneWarning))
		}
	}

	// The truncation line names the bound that stopped the account, because a
	// reader counting events toward the wrong bound is a reader misled by the
	// line meant to keep the account honest.
	if eventsFull || bytesFull {
		var reasons []string
		if eventsFull {
			reasons = append(reasons, fmt.Sprintf("the %d-event bound", maxTranscriptEvents))
		}
		if bytesFull {
			reasons = append(reasons, fmt.Sprintf("the %d MiB step-output bound", maxTranscriptOutputBytes>>20))
		}
		lines = append(lines, TranscriptLine{
			Text: fmt.Sprintf("  … transcript truncated at %s; the run continued unrecorded", strings.Join(reasons, " and ")),
			Tone: ToneWarning,
		})
	}

	return lines
}

// transcriptLabel is what an event puts in the step column.
func transcriptLabel(e transcriptEvent) string {
	if e.kind == eventStubAnswered && e.step == "" {
		return e.task
	}
	return e.step
}

func transcriptLine(at time.Duration, width int, label, text string, tone TranscriptTone) TranscriptLine {
	return TranscriptLine{
		Text: fmt.Sprintf("  t=%-6s %-*s  %s", shortDuration(at), width, label, text),
		Tone: tone,
	}
}

// stubIdentity renders which stub answered, in the numbering every other stub
// diagnostic uses.
func stubIdentity(e transcriptEvent) string {
	target := fmt.Sprintf("task %q", e.task)
	if e.stubStep != "" {
		target = fmt.Sprintf("step %q", e.stubStep)
	}
	return fmt.Sprintf("stub %d (%s)", e.stubOrdinal, target)
}

// stepOutcomeText renders what a finished step's line says after its name.
func stepOutcomeText(e transcriptEvent, sensitive sensitiveInputs, switches map[string]switchFact) (string, TranscriptTone) {
	fact, isSwitch := switches[e.step]
	isSwitch = isSwitch && !fact.ambiguous

	if e.failure != "" {
		// withholdAll withholds the failure text too (Codex, #1052): a failed
		// expression's message can embed the very value the set could not
		// enumerate, and an empty substring list applied when redaction has
		// decided nothing is provably safe is the fail-open inverse of what
		// the flag means. The case's own diagnostics still carry the failure;
		// only this rendered account withholds.
		text := "[failure withheld: a sensitive input could not be enumerated, so no text is provably safe]"
		if !sensitive.withholdAll {
			text = redactSensitiveSubstrings(e.failure, sensitive.substrings)
		}
		// A failed switch body's record deliberately preserves the arm that
		// was taken ([v1.StepFailureRecord]), and the decision is most worth
		// showing exactly when the branch it chose is what failed (Codex,
		// #1052).
		if arm, took := takenSwitchArm(e, sensitive, isSwitch, fact); took {
			text += "  (" + arm + ")"
		}
		if e.tolerated {
			return "failed (tolerated by continue_on_error): " + text, ToneWarning
		}
		return "FAILED: " + text, ToneDanger
	}

	named := e.outputs.GetNamedValues()
	if len(named) == 0 {
		return "completed", ToneInfo
	}

	// A switch's record reads as the decision it is, not as two opaque
	// outputs: the arm that took the observed value, in the words an author
	// asserts on (`steps.<id>.case`). Only for a step the compiled workflow
	// really declares a `switch:` at — an ordinary task may name an output
	// `case`, and inferring from the name alone rendered it as a decision it
	// never made (Codex, #1052).
	if isSwitch {
		if arm, took := takenSwitchArm(e, sensitive, isSwitch, fact); took {
			return arm, ToneInfo
		}
	}

	// The whole fragment is withheld when the redaction set could not be
	// built, and the *joined* fragment passes the substring backstop before
	// the cap — keys included, because a stub's `returns:` keys and a
	// payload's keys are authored text a sensitive value can be spelled into,
	// and per-value redaction alone would print them (Codex, #1052).
	if sensitive.withholdAll {
		return "-> [withheld]", ToneInfo
	}

	names := make([]string, 0, len(named))
	for name := range named {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+": "+redactedValueText(named[name], sensitive))
	}
	return "-> " + capRunes(redactSensitiveSubstrings(strings.Join(parts, ", "), sensitive.substrings), 120), ToneInfo
}

// takenSwitchArm renders the decision a known switch step's record carries,
// or reports that the record carries none. A null `case` means "no case
// matched", which took the `default:` only where the workflow declares one —
// a no-default switch that matched nothing ran nothing, and saying "took
// default" about it would be a body that never existed (Codex, #1052).
func takenSwitchArm(e transcriptEvent, sensitive sensitiveInputs, isSwitch bool, fact switchFact) (string, bool) {
	if !isSwitch {
		return "", false
	}
	caseValue, recorded := e.outputs.GetNamedValues()[v1.SwitchCaseOutput]
	if !recorded {
		return "", false
	}
	took, err := literalToGo(caseValue.GetLiteral())
	switch {
	case err != nil:
		return "", false
	case took == nil && fact.hasDefault:
		return "took default (no case matched)", true
	case took == nil:
		return "matched no case (and there is no default:)", true
	default:
		return "took " + fmt.Sprintf("case %s", redactedScalarText(took, sensitive)), true
	}
}

// redactedValueText renders one recorded output value through the same
// redaction the stub diagnostics apply.
func redactedValueText(value *v1.Value, sensitive sensitiveInputs) string {
	lit := value.GetLiteral()
	if lit == nil {
		return "…"
	}
	native, err := literalToGo(lit)
	if err != nil {
		return "…"
	}
	return redactedScalarText(native, sensitive)
}

// redactedGoValue renders a native map (a signal payload) compactly,
// redacted — the joined fragment passing the substring backstop keys
// included, for [stepOutcomeText]'s reason.
func redactedGoValue(payload map[string]any, sensitive sensitiveInputs) string {
	if sensitive.withholdAll {
		return "[withheld]"
	}
	if len(payload) == 0 {
		return "{}"
	}
	names := make([]string, 0, len(payload))
	for name := range payload {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+": "+redactedScalarText(payload[name], sensitive))
	}
	return capRunes(redactSensitiveSubstrings("{"+strings.Join(parts, ", ")+"}", sensitive.substrings), 120)
}

// redactedScalarText is the one spelling of "a value, safe to print": the
// tree redaction first (values compared by content, the blunt rule
// [sensitiveNativeValues] documents), the textual backstop second, a rune cap
// last. withholdAll withholds, exactly as the stub diagnostics do when the
// redaction set could not be built.
func redactedScalarText(native any, sensitive sensitiveInputs) string {
	if sensitive.withholdAll {
		return "[withheld]"
	}
	redacted := redactSensitiveTree(native, sensitive.values)
	var text string
	if s, ok := redacted.(string); ok {
		text = fmt.Sprintf("%q", s)
	} else {
		text = fmt.Sprintf("%v", redacted)
	}
	return capRunes(redactSensitiveSubstrings(text, sensitive.substrings), 48)
}

// redactedBareText is [redactedScalarText] for a string rendered into the
// line without quoting — a sender's subject, a refusal's reason — through the
// same two passes, so no string reaches the account un-redacted by virtue of
// its position in the sentence.
func redactedBareText(s string, sensitive sensitiveInputs) string {
	if sensitive.withholdAll {
		return "[withheld]"
	}
	text, ok := redactSensitiveTree(s, sensitive.values).(string)
	if !ok {
		return sensitiveMarker
	}
	return capRunes(redactSensitiveSubstrings(text, sensitive.substrings), 120)
}

// capRunes bounds one rendered fragment, marking the cut.
func capRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}

// shortDuration renders a virtual duration the way an author writes one: 5m,
// 1h30m, 0s — [time.Duration.String]'s spelling minus the zero units it
// appends ("5m0s", "1h0m0s") that say nothing.
func shortDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	// Truncation is cosmetic and must stay that way: a positive duration
	// below a millisecond — `sleep: 500us` is legal — renders exactly rather
	// than collapsing to "0s", which would report that no virtual time passed
	// in the one account whose job is the timing (Codex, #1052).
	if d >= time.Millisecond {
		d = d.Truncate(time.Millisecond)
	}
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}
