package flowdebug

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Asking a paused session a question, without typing at it.
//
// # Why this exists
//
// The session's whole control surface is [Session.dispatch], which takes a
// *line of text*. That is right for a person at a prompt and for a script over
// MCP, and it is the only thing there is — so every other front has to compose
// a command string and then read the session's printed output back to learn
// what happened. A debug adapter doing that is parsing a human-readable
// rendering it also has to keep stable; a Go test doing it cannot ask "what is
// the value" at all, only "what did it print".
//
// That is the gap between "the core is protocol-agnostic" and what is actually
// true, which is that the core is agnostic about *where the text comes from*.
//
// [Session.Complete] already proved the seam: it answers from a console's own
// goroutine, against the scope the run is parked in, by reading the same
// [promptSubject] the prompt is holding. Everything here is that pattern
// applied to the questions the prompt can already answer — so a caller that is
// not a terminal gets values, and the printing verbs become one renderer of
// them rather than the only way to reach them.
//
// # What is not here
//
// Control. [Session.Evaluate] and its neighbours are questions about a session
// that is *already* paused, and answering one changes nothing — which is what
// makes them safe to call from another goroutine while the run is held. Making
// a run *move* from another goroutine is a different problem, because the
// parked [Session.BeforeStep] is blocked reading text, and it is deliberately
// not solved here.

// ErrNotPaused reports that the session is not holding a run, so there is no
// scope to answer against.
//
// A distinct error rather than an empty answer, because "nothing is in scope"
// and "there is no scope" are different facts and only the second is a caller's
// mistake.
var ErrNotPaused = errors.New("flowdebug: the session is not paused at a step")

// Position is where a paused session is holding the run.
type Position struct {
	// Step is the id of the step the run is stopped before, empty at an
	// autopsy — where the run is over and there is no step to be at.
	Step string

	// Kind is what that step is, in the same words `info` prints.
	Kind string

	// Workflow names the workflow whose steps are running here, which is what
	// tells a caller's `build` from a called workflow's `build`.
	//
	// The engine's own answer, read at the boundary through
	// [v1.TaskWorkflowFromContext] — `runCall` moves the runtime position
	// across a call precisely so that "a consumer of the runtime position
	// [cannot confuse] equal step ids in two different workflow files"
	// (`eval.go:1804-1812`), and a debugger is such a consumer.
	//
	// Empty where the run carries no runtime position: an embedder driving
	// [v1.Run] directly, and most tests. A consumer must treat that as "not
	// said" rather than as a name — see [Session.StepPosition], which refuses
	// to guess.
	Workflow string

	// Autopsy reports which of the two prompts is holding: a breakpoint before
	// a step, or the account of a case that has already failed.
	Autopsy bool
}

// Paused reports whether the session is holding a run right now, and where.
//
// The boolean rather than a nil Position, because an autopsy is a real pause
// with no step to name — so "there is a position" and "the position has a
// step" have to stay separable.
func (s *Session) Paused() (Position, bool) {
	s.mu.Lock()
	subject := s.at
	s.mu.Unlock()

	if subject.scope == nil {
		return Position{}, false
	}

	return Position{
		Step:     subject.step,
		Kind:     subject.kind,
		Workflow: subject.workflow,
		Autopsy:  subject.autopsy,
	}, true
}

// Evaluate answers one CEL expression against the scope the run is paused in,
// which is what `inspect` prints.
//
// Both halves of the answer are redacted, and that is the whole point of the
// method existing rather than a caution around it. A caller reaching a session
// this way is no more entitled to a secret than one at a terminal: the front
// changes and the withholding does not.
//
// The first draft returned the raw [ref.Val] beside the redacted text, on the
// reasoning that redaction is a property of what is *displayed*. That is wrong
// here, and wrong in the one direction that matters. `flow test` installs
// [Session.SetValueRedactor] precisely so that a structured value a debugger
// hands out does not carry a sensitive input or a resolved secret
// (`flowtest/run.go:755-790`), and an adapter expanding a variable in a pane
// reads exactly that value — so returning it raw would open, on a new surface,
// the hole the seam exists to close (Codex, #1120).
//
// The [ref.Val] is still returned, because a caller does need the value's shape
// to decide whether a variable is expandable. It is the redacted one — or, on a
// session that has a text redactor and no value redactor, nothing at all: see
// the withholding branch below for why that case fails closed rather than
// handing back what it cannot redact.
//
// An expression that does not compile is an ordinary answer here rather than an
// error on the session, for the reason the prompt treats it as one: somebody
// asking questions will ask some that do not parse. The error is redacted too —
// see [withheld], because an error names what caused it and what caused it can
// be the value being withheld.
func (s *Session) Evaluate(ctx context.Context, expression string) (string, ref.Val, error) {
	s.mu.Lock()
	subject := s.at
	s.mu.Unlock()

	if subject.scope == nil {
		return "", nil, ErrNotPaused
	}

	// Bounded before the parser sees it. A console reader owes
	// [MaxCommandBytes] and enforces it on the way in, so the text path was
	// bounded by the surface it arrives on — and a caller reaching this method
	// arrives on no such surface. [v1.DefaultCostLimit] bounds *evaluation*,
	// which is work that happens after a parse, so an expression large enough
	// to be a problem is one the cost limit never gets to see (Codex, #1120).
	if len(expression) > MaxCommandBytes {
		return "", nil, fmt.Errorf(
			"%w: an expression may be %d bytes and this one is %d",
			ErrExpressionTooLarge, MaxCommandBytes, len(expression))
	}

	libs, err := v1.ProfileLibraries(subject.scope.GetProfile())
	if err != nil {
		return "", nil, fmt.Errorf("cannot inspect: %w", err)
	}

	activation := subject.scope.Activation(ctx)
	if len(subject.extra) > 0 {
		activation = subject.scope.ActivationWith(ctx, subject.extra)
	}

	out, err := v1.DefaultEvaluator().EvalString(ctx, expression, libs, activation)
	if err != nil {
		return "", nil, withheld(subject.redactText, err)
	}

	// Redacted with the redactors this *pause* began under, taken from the
	// subject snapshotted at the top rather than read from the session now.
	// Evaluation takes time, and a console exiting the pause meanwhile clears
	// them — so reading them here would hand back exactly what they existed to
	// withhold. See [promptSubject.redactText].
	//
	// One conversion, shared with the printing path, because a second one that
	// reached for [ref.Val.Value] directly would redact a scalar and hand a map
	// straight through. See [redactedNative].
	native, converted := redactedNative(out, subject.redactValue)
	if !converted {
		// Nothing structured to offer for a value the conversion cannot read,
		// and the text redactor still covers what comes back as prose.
		return capRunes(applyText(subject.redactText, fmt.Sprint(out.Value())), MaxInspectRunes), nil, nil
	}

	text := capRunes(applyText(subject.redactText, nativeText(native)), MaxInspectRunes)

	// The structured half is withheld entirely when this session cannot redact
	// one. Told there is something to withhold, with no way to withhold it
	// structurally, and a component that allows when it cannot decide will
	// eventually allow everything — CLAUDE.md's rule, which bites here because
	// the two redactors are installed independently
	// (`flowtest/run.go:755-790`). The redacted text still comes back, so what
	// is withheld is a representation rather than the answer.
	if subject.redactValue == nil && subject.redactText != nil {
		return text, nil, nil
	}

	// The second seam, which the structured half was missing. A value redactor
	// matches by *equality* — `flowtest`'s does (`stub.go:935-963`) — so a
	// composed string like `"Bearer " + inputs.token` is not the secret and
	// passes it through whole. The rendered text has never had that problem,
	// because the text redactor is a substring backstop applied to the whole
	// rendering; the structured answer got only the equality half, so the same
	// expression withheld the token in prose and handed it over as a value
	// (Codex, #1120).
	//
	// Both seams, then, exactly as [Session.SetValueRedactor] says there are
	// two questions: is this the value, and does this text contain it.
	return text, v1.TypeAdapter.NativeToValue(withheldLeaves(subject.redactText, native)), nil
}

// withheldLeaves is native with the text redactor applied to every string in
// it, keys included.
//
// The traversal mirrors `flowtest`'s `redactSensitiveTree` and is deliberately
// not shared with it: that one asks whether a value *is* the secret, this one
// asks whether a string *contains* it, and the two live on opposite sides of an
// import direction that serves neither (`flowtest` drives a session; a session
// cannot reach back into it). What they must agree on is the shape they walk,
// which is the two containers [v1.LiteralToGo] can produce.
//
// Unbounded on purpose, and this is the one place in this package where that is
// the answer rather than an oversight. The value handed here has already been
// through [cel.RefValueToValue] and [v1.LiteralToGo], both of which recurse
// over this exact structure — so anything deep enough to be a problem is deep
// enough to have been one before this function was reached, and a bound here
// would be a bound nothing can reach, which CLAUDE.md is explicit is a bound
// nothing tests.
func withheldLeaves(redact func(string) string, native any) any {
	if redact == nil {
		return native
	}

	switch value := native.(type) {
	case string:
		return redact(value)

	case map[string]any:
		withheld := make(map[string]any, len(value))
		for name, element := range value {
			// Keys too. A sensitive value used as a key is the material just
			// as much as one used as a value, which is the case
			// `redactSensitiveTree` learned by being wrong about it first.
			withheld[redact(name)] = withheldLeaves(redact, element)
		}

		return withheld

	case []any:
		withheld := make([]any, len(value))
		for i, element := range value {
			withheld[i] = withheldLeaves(redact, element)
		}

		return withheld

	default:
		return native
	}
}

// withheld is err with the pause's text redactor applied to its message.
//
// A CEL runtime error interpolates the value that caused it — `hours(n)` past
// the duration ceiling prints n itself (`celenv.go:642-645`) — so an error is a
// way for a value to leave, and the prompt has always known this: it prints one
// through `printfTone`, which redacts. This surface returned the error raw, so
// the same failing expression withheld its value when typed and disclosed it
// when asked for (Codex, #1120).
//
// Rebuilt rather than wrapped, which is the part worth stating. Wrapping keeps
// the original reachable through [errors.Unwrap], and the whole message is what
// had to be withheld — the same leak CLAUDE.md records for Temporal's failure
// converter, which walks the unwrap chain and persists every level. So the
// redacted sentence is the whole of what comes back.
//
// With no redactor installed the error is returned exactly as it is: identity
// is preserved precisely where there is nothing to withhold, and the sentinels
// this method returns are all returned before evaluation, so none of them
// passes through here.
func withheld(redact func(string) string, err error) error {
	if redact == nil || err == nil {
		return err
	}

	return errors.New(redact(err.Error()))
}

// applyText is text through redact, or text.
func applyText(redact func(string) string, text string) string {
	if redact == nil {
		return text
	}

	return redact(text)
}

// ErrExpressionTooLarge reports an expression past [MaxCommandBytes].
//
// The same bound a console reader enforces on a typed line, because the two are
// the same resource reached two ways — and named rather than anonymous so a
// caller can tell a refusal to look at an expression from an expression that
// did not compile.
var ErrExpressionTooLarge = errors.New("flowdebug: expression too large")

// Names are the bindings the paused run can reach, which is what `scope`
// prints.
//
// Grouped as the prompt groups them rather than flattened, because the grouping
// is the answer: a debug adapter renders one pane per group, and a person
// reading `scope` is looking for which *kind* of name they can reach.
type Names struct {
	// Group is the root the names hang from — `steps`, `vars`, the workflow's
	// declared vars, and the bare bindings an autopsy adds.
	Group string

	// Names are the members of that group, sorted as the prompt lists them,
	// and all of them.
	//
	// [MaxScopeNames] is a property of a line somebody reads rather than of
	// what a run can name, so it is applied by the renderer and not here: a
	// caller filling a variables pane wants every name and does its own
	// paging, and a value surface quietly narrower than the run would be a
	// worse lie than a long line.
	Names []string

	// Root is the expression prefix these names hang from — `steps`,
	// `inputs`, `vars`, `run`, `trigger` — or "" where they are bound bare
	// and resolve under no root at all (a loop's `as:`, a step's own `vars:`,
	// the autopsy's bindings).
	//
	// Exported because it is what turns a name into a question: a renderer
	// filling a pane asks [Session.Evaluate] for `Root + "." + name`, and
	// without this it has to keep its own switch over the group names. Two
	// did — `flowdap.rootOf` was one, and its own comment said it was "the
	// same fact read for a different renderer" — which is the parallel
	// declaration CLAUDE.md says always eventually drifts. It is derived from
	// the same value listing is, so the prompt's spelling and a pane's cannot
	// disagree.
	Root string

	// listing is the command that enumerates these names, for the renderer.
	// Unexported because it is a fact about the text prompt rather than about
	// the scope — Root is the part that is about the scope.
	listing string
}

// Scope lists what the paused run can name.
func (s *Session) Scope() ([]Names, error) {
	s.mu.Lock()
	subject := s.at
	s.mu.Unlock()

	if subject.scope == nil {
		return nil, ErrNotPaused
	}

	return s.scopeNames(subject.scope, subject.extra), nil
}

// StepState is what a session last watched one step do.
//
// The vocabulary is [v1.RunObserver]'s, read once: a step is entered, and then
// it finishes, is absorbed, fails, or is skipped by its `if:`. It is
// deliberately not [Tone] and deliberately not flowtest's transcript kinds —
// a tone says how a line should *read* and this says what a step *did*, and
// the two only look alike because a failure is worth colouring.
type StepState int

const (
	// StepPending is a step the session has not watched do anything. The zero
	// value, so a step named by a caller and never reached reads as not yet
	// rather than as finished.
	StepPending StepState = iota

	// StepRunning is a step the run entered and whose outcome has not
	// arrived. The step a session is paused at is one of these; so is a step
	// still in flight on another branch.
	StepRunning

	// StepDone is a step that finished without an error.
	StepDone

	// StepTolerated is a step that failed and whose failure the run absorbed
	// (`continue_on_error`). Distinct from [StepDone] because a run that
	// carried on is not a step that worked, and that is usually the thing
	// somebody opened a debugger to find.
	StepTolerated

	// StepFailed is a step that failed and whose failure the run did not
	// absorb.
	StepFailed

	// StepSkipped is a step whose `if:` was false. It never reaches a
	// boundary — there is no work to hold — so it can only ever arrive
	// through [Session.StepSkipped].
	StepSkipped
)

// String names a state in the words the prompt already uses for it.
func (s StepState) String() string {
	switch s {
	case StepRunning:
		return "running"
	case StepDone:
		return "ok"
	case StepTolerated:
		return "tolerated"
	case StepFailed:
		return "failed"
	case StepSkipped:
		return "skipped"
	default:
		return "pending"
	}
}

// Step is one entry in the run's step list.
type Step struct {
	// Workflow is the workflow that declares this step. It is what a reader
	// sees, and it is *not* on its own an identity.
	//
	// A callee's step ids belong to the callee and not to its caller
	// (`eval.go:1804-1812`), so a caller and a called workflow may both
	// declare `build` — and an inventory of the two flattened together holds
	// two rows nothing can tell apart. Empty where the caller listing the
	// steps did not say, and where the list is the ids this session has merely
	// watched go past.
	Workflow string

	// Declaration identifies the workflow *instance* this step belongs to.
	//
	// A name is not a declaration. One callee invoked from two `call:` steps
	// appears twice in an inventory under one name, and so do two genuinely
	// different embedded workflows that happen to share a `name:` — in both
	// cases the rows are separate declarations whose outcomes belong to
	// separate invocations, and grouping them by name says the session can
	// attribute an outcome it cannot.
	//
	// The engine's own structure, read statically: `runNodes` descends into a
	// callee once per `call:` node (`eval.go:1734`), so a walk's descents and
	// that call's invocations are one-to-one by construction. Numbered rather
	// than named because the only question ever asked of it is whether two
	// rows are the same declaration.
	//
	// Zero is the root workflow and the value a caller that says nothing
	// leaves. That is why the grouping key is this *and* [Step.Workflow]: an
	// inventory built by hand distinguishes its workflows by name, and one
	// built by a walk distinguishes them here, and neither has to know about
	// the other.
	Declaration int

	// Via is the id of the `call:` step that reaches this declaration, empty
	// at the root.
	//
	// For the reader rather than for the grouping: where two rows share a name
	// *and* an id, the name cannot tell them apart on screen, and the call
	// step an author wrote is the thing that can.
	Via string

	// ID is the step's id, which is the only name the *run* has for it: both
	// [v1.Debugger] and [v1.RunObserver] are handed bare ids.
	ID string

	// State is what this session last watched it do.
	//
	// Ignored on the way in: [Options.Steps] is an inventory of what a run may
	// reach, and nothing has happened to any of it yet.
	State StepState
}

// StepList is a window onto the run's step list, with the facts about the whole
// that a window cannot carry.
//
// A window rather than the list, because the list is the author's file: a
// workflow may declare thousands of nodes, a pane draws a dozen, and copying
// every entry at every stop is O(N) allocation per stop and O(N²) across a walk
// of the run. The shape is offset-and-total deliberately — it is what a pane
// needs and what slice 2's wire message will need, so a network-attached
// session answers the same question the same way.
type StepList struct {
	// Steps is the window, in the order an author wrote them.
	Steps []Step

	// Offset is where the window starts in the whole list, after clamping.
	Offset int

	// Total is how long the whole list is, so an elision can say how much it
	// elided rather than that it elided some.
	Total int

	// Unattributed counts the rows whose outcome this session cannot
	// attribute — see [Session.Steps] on what fails closed and why.
	Unattributed int

	// Truncated reports that this session stopped recording what it watched,
	// so a state above may understate what a step actually did.
	//
	// The same flag completion reads, rather than a second one: what truncates
	// is [Session.sawStep]'s one cache, so a second notice would be a second
	// thing to keep true.
	Truncated bool
}

// StepPosition reports where a step sits in the run's step list, and how long
// that list is.
//
// Separate from [Session.Steps] and copying nothing, because a caller windowing
// the list needs both numbers *before* it can say which slice it wants.
//
// The workflow is what disambiguates, and it may be empty. An empty one matches
// by id alone and is the honest answer for a run whose position carries no
// workflow ([v1.TaskWorkflowFromContext] reports none) — but where the id it
// names is declared by more than one workflow, this reports -1 rather than the
// first match. Pointing at a step the run is not at is the one thing a debugger
// must never do, and "I cannot tell which" is an answer a renderer can draw.
func (s *Session) StepPosition(workflow, id string) (index, total int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	order := s.inventory()

	return positionIn(order, workflow, id), len(order)
}

// positionIn is the one resolution of "which row is this", shared by
// [Session.StepPosition] and [Session.Steps].
//
// One function because the two were two, and the second was subtly wrong: it
// marked the held row by comparing the position's workflow *name* to each
// row's, which marks both rows when one callee is invoked twice — the very
// defect the name-versus-declaration correction is about, wearing the other
// hat (Codex, #1186). A single resolver cannot disagree with itself.
//
// -1 where no row answers, and -1 where more than one does. The second is the
// load-bearing one: a boundary is told the callee's *name*
// ([v1.TaskWorkflowFromContext]) and nothing about which invocation of it is
// running, so two indistinguishable rows are answered with "cannot tell"
// rather than with the first. Pointing at a step the run is not at is the one
// thing a debugger must never do.
func positionIn(order []Step, workflow, id string) int {
	if id == "" {
		return -1
	}

	index := -1
	for i, step := range order {
		if step.ID != id {
			continue
		}
		if workflow != "" && step.Workflow != "" && step.Workflow != workflow {
			continue
		}
		if index >= 0 {
			return -1
		}
		index = i
	}

	return index
}

// Steps returns at most limit entries of the run's step list, starting at
// offset, and what each has done.
//
// Two sources for the list itself, exactly as [Session.reachableSteps] has two
// and for the same reason: a caller that holds the workflow said so
// ([Options.Steps]), and that is the list worth drawing — it carries the steps
// the run has *not* reached, which is most of a step list's value. A caller
// that does not gets the ids this session has watched go past, in arrival
// order, which is at least the run so far rather than nothing.
//
// Not gated on the session being paused, unlike [Session.Scope] and
// [Session.Evaluate]. Those answer against a scope that only exists while a run
// is held; this answers about the workflow and about what has happened, both of
// which are as true between stops as at one. A pane that could only be drawn at
// a pause would be a pane that vanished the moment somebody typed `continue`.
//
// # An outcome nothing can attribute is not attributed
//
// Outcomes arrive through [v1.RunObserver] as bare ids, so where two workflows
// in this inventory both declare `build`, a `StepFinished("build")` names
// neither of them. The rows for such an id therefore report [StepPending] — the
// state meaning this session has watched nothing happen here — rather than one
// workflow's outcome painted onto the other's row, and [StepList.Unattributed]
// counts them so a renderer can say so. Fail closed: under-claiming is a gap a
// reader can see, and mis-claiming is a debugger pointing at the wrong step.
//
// The one exception is the step the run is *held* at, which the position names
// exactly when it carries a workflow — that row reads [StepRunning], because
// there the session does know.
func (s *Session) Steps(offset, limit int) StepList {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, _ := s.stepWindow(offset, limit)

	return list
}

// stepWindow is [Session.Steps]' whole answer, plus where the held row sits in
// the *whole* list, held under s.mu by every caller.
//
// Split out rather than copied because [Session.StepWindowProto] needs both
// halves and computing them twice is how the two would come to disagree — the
// same argument [positionIn] itself is, one level up. The index is absolute so
// that a caller can say whether the held row falls inside a window it chose;
// -1 where nothing is held, which is an autopsy, a session between stops, and a
// position no row can be attributed to.
func (s *Session) stepWindow(offset, limit int) (StepList, int) {
	order := s.inventory()

	list := StepList{
		Total:     len(order),
		Truncated: s.seenShort,
	}

	// Counted in [New] rather than here: it is a property of an inventory that
	// does not change, and recomputing it per call would put the O(N) pass
	// back that the window exists to remove.
	list.Unattributed = s.sharedCount

	// Where the run is, resolved once and by index. The earlier draft compared
	// the position's workflow name against each row's, which marks *both* rows
	// when one callee is invoked from two call sites — see [positionIn].
	held := -1
	if at := s.at; at.scope != nil && !at.autopsy {
		held = positionIn(order, at.workflow, at.step)
	}

	offset = max(0, min(offset, len(order)))

	// The end is measured from what is *left* rather than from offset+limit,
	// which overflows: this API is shaped for a caller that does not exist yet
	// (slice 2's wire client), so the limit is untrusted, and `Steps(1,
	// math.MaxInt)` wraps that sum negative — a negative end slices backwards
	// and makes a negative capacity, both of which are a panic rather than a
	// refusal. Saturating here means every limit past the end is the same
	// answer as a limit exactly at it (Codex, #1186).
	end := len(order)
	if limit >= 0 {
		end = offset + min(limit, len(order)-offset)
	}
	list.Offset = offset

	list.Steps = make([]Step, 0, end-offset)
	for i, step := range order[offset:end] {
		// A declared id the session has watched nothing do is StepPending, the
		// zero value — which is the answer, not a gap.
		step.State = s.seen[step.ID]

		if s.ambiguous(step.ID) {
			step.State = StepPending
			if offset+i == held {
				// The one row an ambiguous id can still be said something
				// about: the position named exactly this row, so the run is
				// here whatever the outcomes cannot say.
				step.State = StepRunning
			}
		}

		list.Steps = append(list.Steps, step)
	}

	return list, held
}

// inventory is the list [Session.Steps] and [Session.StepPosition] read, held
// under s.mu by both.
//
// The caller's own list where there is one, and the ids this session watched go
// past otherwise. The second carries no workflow — nothing that reaches
// [Session.sawStep] knows one — which is why [Session.ambiguous] can only ever
// fire on the first.
func (s *Session) inventory() []Step {
	if len(s.steps) > 0 {
		return s.steps
	}

	return s.seenOrder
}

// ambiguous reports that more than one workflow in this session's inventory
// declares id, so an outcome naming it names neither.
//
// Read off a set computed once in [New], because the inventory is a caller's
// answer and does not change: rebuilding it per call would put an O(N) map
// where the point of the window is to keep a stop off O(N).
func (s *Session) ambiguous(id string) bool {
	_, shared := s.sharedIDs[id]

	return shared
}
