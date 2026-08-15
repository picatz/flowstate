package flowstatev1

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/google/cel-go/common/types/ref"
)

// How a run started, as the run itself may read it, and what a workflow may say
// about being started by a person.
//
// The two halves are here together because they are two sides of one decision and
// keeping them apart is what makes the wrong shape tempting. [TriggerContext] is
// for *behaviour*: a nightly sweep that does not page anyone, a human start that
// takes a second pair of eyes. [ManualTrigger] is for *authorization*: who may
// start this at all, and whether they have to say why.
//
// The line between them is the one rule in this file worth restating wherever it
// is read. This is the right shape:
//
//	- id: notify
//	  if: ${trigger.kind != "schedule"}
//
// and this is the wrong one:
//
//	- id: delete_everything
//	  if: ${trigger.principal == "admin"}
//
// The second is an expression a test can fake, one `flow validate` cannot reason
// about, and one a later refactor can reorder past the step it was guarding. The
// decision it is trying to make belongs on the trigger, taken fail-closed when
// configuration loads, which is what [CheckManualStart] is. That is the same rule
// that keeps egress policy out of the validator: each decision lives where the
// thing that owns it lives.

// TriggerRoot is the name a run reads its own trigger through, as
// `trigger.kind`, `trigger.name`, `trigger.principal` and `trigger.delivery_id`.
//
// The fifth rooted namespace, added on the same argument that made `run` one and
// costing the same nothing: a name under a root is a field selection rather than
// an identifier, so it cannot collide with a step id, a loop's `as:`, a step's
// own `vars:` key or `now`, and a file that did not reference it before means
// exactly what it meant. What it *does* cost is the root itself, refused as any
// of those four names by the compiler exactly as `steps`, `vars`, `inputs` and
// `run` already are — a check that runs regardless of a file's declared edition,
// because invariant 10 protects what a run already compiled and replays, not what
// a file sitting in a repository may name a step.
//
// # Why a root of its own rather than `run.trigger`
//
// `run` answers "which run is this" — its address, and the identity it acts as.
// A trigger answers "why is this run happening", which is a different question
// with a different lifetime: the address is minted by the substrate and the
// trigger is established by whichever entry path admitted the submission. Nesting
// would also have put a five-field selection (`run.trigger.delivery_id`) on the
// most common branch anybody writes, for no gain.
//
// # Replay-safe, which is not true of everything a run might want to know
//
// Every field is fixed when the run starts and carried in [RunState.trigger], so
// it reads identically on every replay and after every Continue-As-New. That is
// exactly what the clock and randomness are not, and it is why `now` is bound
// only inside a wait while this is readable everywhere: a value that cannot drift
// can be read anywhere, and one that can drift may only be read where the
// substrate makes it durable.
const TriggerRoot = "trigger"

// The kinds a run can have been started by.
//
// A closed set of three, which is the three that are request-shaped and
// implemented: a person, a delivery, a cadence. `workflow:` and `queue:` are
// deferred (#490) — the first is accepted in principle and the second needs a
// long-lived consumer that does not exist — and neither is spelled here, because
// a kind an author can compare against and never receive is a branch that is
// never taken and a diagnostic that never fires.
const (
	// TriggerKindManual is a start by a person: `flow run`, `flow run local`,
	// the `Run` RPC, an agent over MCP. The default for every path that starts a
	// run without a declared trigger, which is what makes `if: ${trigger.kind ==
	// "manual"}` a branch an author can reach on their own machine.
	TriggerKindManual = "manual"

	// TriggerKindWebhook is a start by a delivery to a declared
	// [WebhookTrigger]. [TriggerContext.name] is the webhook's name and
	// [TriggerContext.delivery_id] names the arrival.
	TriggerKindWebhook = "webhook"

	// TriggerKindSchedule is a firing of a schedule created from a
	// [ScheduleTrigger]. [TriggerContext.name] is the schedule's id.
	TriggerKindSchedule = "schedule"
)

// triggerKinds is the set above, in the order a diagnostic lists them: the one
// every file has first, then the two a file declares.
var triggerKinds = []string{TriggerKindManual, TriggerKindWebhook, TriggerKindSchedule}

// TriggerKinds returns the values `trigger.kind` can take.
//
// A copy, so a caller rendering it into a diagnostic cannot reorder the set every
// other reader depends on — the rule [WebhookVerificationSchemes] follows.
func TriggerKinds() []string { return slices.Clone(triggerKinds) }

// KnownTriggerKind reports whether kind is one this build can start a run with.
func KnownTriggerKind(kind string) bool { return slices.Contains(triggerKinds, kind) }

// triggerContextFields are the fields `trigger` has, in the order a diagnostic
// lists them.
//
// Closed, and the closure is the feature: `trigger` is metadata and never data,
// so there is no `payload` and no `headers` here and none will be added.
// Everything a workflow operates on arrives through a trigger's `with:` into
// `inputs:`, where declarations exist for `flow validate` to check it against. A
// second input path reachable under this root would be one the validator is blind
// to, which is the whole reason the set is stated once, here, and read by
// [TriggerContextValue] and by the compiler's own unknown-field diagnostic.
var triggerContextFields = []string{"kind", "name", "principal", "delivery_id"}

// TriggerContextFields returns the fields an expression may select on `trigger`.
func TriggerContextFields() []string { return slices.Clone(triggerContextFields) }

// NewManualTriggerContext is the context every manual start records: the kind,
// and who asked.
//
// principal is the attested subject of the caller, empty where nothing attested
// one — the local driver, which has no server in front of it at all. Empty is
// honest there for the reason [LocalRunAddress] is a sentinel rather than a blank
// elsewhere: `run.local` is what tells an unattested rehearsal apart from a
// server that genuinely attested an anonymous caller, and this field does not
// need to answer that question twice.
func NewManualTriggerContext(principal string) *TriggerContext {
	return &TriggerContext{Kind: TriggerKindManual, Principal: principal}
}

// NewWebhookTriggerContext is the context a delivery records: which webhook, and
// which arrival.
//
// The principal is the trigger a delivery was admitted by rather than the sender,
// because a sender holds no Flowstate credential — the same value the receiver
// already records as the run's identity, restated so a step asking "how was I
// started" does not have to know that a webhook's principal is spelled one way
// under `run` and another under `trigger`.
func NewWebhookTriggerContext(name, principal, deliveryID string) *TriggerContext {
	return &TriggerContext{
		Kind:       TriggerKindWebhook,
		Name:       name,
		Principal:  principal,
		DeliveryId: deliveryID,
	}
}

// NewScheduleTriggerContext is the context a scheduled firing records: which
// schedule, and whose.
func NewScheduleTriggerContext(name, principal string) *TriggerContext {
	return &TriggerContext{Kind: TriggerKindSchedule, Name: name, Principal: principal}
}

// TriggerContextValue renders a trigger as the map an expression reads under
// [TriggerRoot].
//
// Every field is rendered, including the empty ones, and a nil context renders
// with all four empty. That is the rule [runRootValue] follows and it buys the
// same thing: a reference to a field that is simply blank on this run resolves to
// an empty string rather than failing as an unresolved reference, so an author
// reading `${trigger.name}` on a manual start sees "" instead of being sent to
// look for a root that is always there.
//
// A run started before this field existed renders the same way, which is correct:
// "no trigger recorded" is one fact, and there is nothing a workflow could
// usefully do with a second one distinguishing it from "recorded as nothing".
func TriggerContextValue(trigger *TriggerContext) ref.Val {
	return TypeAdapter.NativeToValue(map[string]any{
		"kind":        trigger.GetKind(),
		"name":        trigger.GetName(),
		"principal":   trigger.GetPrincipal(),
		"delivery_id": trigger.GetDeliveryId(),
	})
}

// triggerContextKey carries a local run's trigger on its context.
type triggerContextKey struct{}

// NewContextWithTrigger returns a context carrying the trigger a local run is to
// report under [TriggerRoot].
//
// A context value rather than a parameter on [RunWithInputs], because the local
// driver's submit boundary is a function four commands and one library call, and
// widening it for a value all but one of them would pass nil for is a change to
// every caller for the benefit of none. The durable driver has no equivalent
// need: its trigger arrives in [RunState], which is already the message every
// entry path fills.
//
// This is how `flow test` sets a case's `trigger:` — see flowtest — which is the
// whole reason a branch guarded by `if: ${trigger.kind == "manual"}` is
// exercisable both ways with no real trigger involved.
func NewContextWithTrigger(ctx context.Context, trigger *TriggerContext) context.Context {
	return context.WithValue(ctx, triggerContextKey{}, trigger)
}

// TriggerFromContext returns the trigger a local run is to report, and the
// manual one every local start has by default.
//
// Defaulting to manual rather than to nothing, because that is what a local run
// *is*: a person typing `flow run local`. A run reporting an empty kind there
// would make `if: ${trigger.kind == "manual"}` false on the one machine an author
// can see it on, which is the failure mode this whole slice exists to avoid —
// conditional behaviour that only manifests in production.
func TriggerFromContext(ctx context.Context) *TriggerContext {
	if trigger, ok := ctx.Value(triggerContextKey{}).(*TriggerContext); ok && trigger != nil {
		return trigger
	}

	return NewManualTriggerContext("")
}

// CheckTriggerContext reports a context this build cannot have produced.
//
// The one rule is that a kind, when there is one, is a kind that exists: a
// specification submitted by hand could otherwise put `kind: "admin"` into
// durable history, where every `${trigger.kind == "..."}` in the file would then
// compare against a word nothing can ever produce — an authorization-shaped hole
// opened by a value nobody checked.
//
// An empty kind is accepted, because that is what a run predating the field
// carries and what a path recording nothing produces; see [TriggerContextValue].
func CheckTriggerContext(trigger *TriggerContext) error {
	kind := trigger.GetKind()
	if kind == "" || KnownTriggerKind(kind) {
		return nil
	}

	return fmt.Errorf("a run records trigger kind %q, which is not a kind Flowstate starts runs with; the kinds are %s",
		kind, strings.Join(TriggerKinds(), ", "))
}

// CheckManualTrigger reports what is wrong with a declared `manual:` block, in
// sentences meant to be read by whoever wrote it.
//
// Everything here is a property of the file, so it is the same check `flow
// validate` reports with a position and [BindRunInputs] applies to a
// specification that never was a Flowfile — the shape [CheckWebhookTrigger]
// already has, and for the same reason: a rule with two implementations
// eventually has two meanings.
//
// The refusals are all contradictions rather than typos, because the schema
// already bounds the shapes. A `denied` block that also says who may start the
// workload is two sentences that cannot both be true, and resolving it by
// precedence would mean one of the two lines an author wrote silently does
// nothing.
func CheckManualTrigger(manual *ManualTrigger) error {
	if manual == nil {
		return nil
	}

	if !manual.GetDenied() {
		return nil
	}

	if manual.GetRequireReason() {
		return fmt.Errorf("`manual:` both refuses manual starts and requires a reason for one, which cannot " +
			"both hold; write `manual: denied` to refuse them, or `require_reason: true` to allow them with a " +
			"reason recorded")
	}

	if len(manual.GetAllowedPrincipals()) > 0 {
		return fmt.Errorf("`manual:` both refuses manual starts and names %d principal(s) allowed to make one, "+
			"which cannot both hold; write `manual: denied` to refuse them, or list the principals to allow "+
			"only those", len(manual.GetAllowedPrincipals()))
	}

	return nil
}

// CheckManualStart decides whether a person may start this workload, against the
// caller a server attested and the reason they gave.
//
// This is the authorization half of the trigger design, and it is the whole
// reason a `trigger.principal` comparison does not belong in a step's `if:`. It
// is decided here, at the boundary, from configuration that was loaded and
// checked — never from an expression the workflow evaluates about itself.
//
// # What "narrows" means precisely
//
// A workflow with no `manual:` block is startable by any authenticated caller,
// which is what every workflow in existence already does and must keep doing:
// `triggers:` is not exhaustive, so adding a webhook cannot silently take
// `flow run` away from the author who added it.
//
// # Fail closed, in the two places it matters
//
// A malformed block ([CheckManualTrigger]) denies rather than being ignored, so a
// contradiction that reached a server is a refusal and not a permit. And an empty
// subject against a non-empty `allowed_principals` is denied: a deployment with
// no identity provider attests every caller as nobody in particular, and a policy
// that admitted that would admit everyone while reading as if it named three
// people.
//
// # Where this is deliberately not called
//
// `flow run local` and `flow test`. The author's machine is not a deployment, it
// has nothing to attest a principal with, and a workflow that cannot be developed
// locally is not one anybody will maintain. Gating a rehearsal on a policy whose
// inputs only exist in production would make every regulated workflow untestable
// — the same reasoning that keeps an egress policy out of the validator.
func CheckManualStart(wf *Workflow, principal, reason string) error {
	manual := wf.GetTriggers().GetManual()
	if manual == nil {
		return nil
	}

	if err := CheckManualTrigger(manual); err != nil {
		return fmt.Errorf("workflow %q cannot be started manually because its `manual:` block is refused: %w",
			wf.GetName(), err)
	}

	if manual.GetDenied() {
		return fmt.Errorf("workflow %q declares `manual: denied`, so it is started by its other declared "+
			"triggers and not by a person; %s", wf.GetName(), manualStartAlternative(wf))
	}

	if len(manual.GetAllowedPrincipals()) > 0 && !slices.Contains(manual.GetAllowedPrincipals(), principal) {
		if principal == "" {
			// Named separately because the remedy is different in kind: the caller
			// is not the wrong person, they are nobody, and the fix is on the
			// deployment rather than in the request.
			return fmt.Errorf("workflow %q allows a manual start only by %s, and this caller was attested with "+
				"no subject at all; a start by nobody in particular is refused rather than admitted, so "+
				"authenticate as one of those principals",
				wf.GetName(), strings.Join(manual.GetAllowedPrincipals(), ", "))
		}

		return fmt.Errorf("workflow %q allows a manual start only by %s, and this caller is %q",
			wf.GetName(), strings.Join(manual.GetAllowedPrincipals(), ", "), principal)
	}

	if manual.GetRequireReason() && strings.TrimSpace(reason) == "" {
		return fmt.Errorf("workflow %q requires a reason for a manual start, and this one gave none; "+
			"pass `--reason` (or `reason` on the request) saying why this run is being started",
			wf.GetName())
	}

	return nil
}

// manualStartAlternative says what does start a workflow that refuses people, so
// a refusal is an answer rather than a wall.
//
// A refusal with no alternative is the diagnostic this repository's own standard
// calls half-written: it names what is wrong and not what to do instead. A
// workload that refuses manual starts and declares nothing else is a separate
// mistake, said plainly here rather than left for somebody to discover by
// deploying it.
func manualStartAlternative(wf *Workflow) string {
	names := WebhookTriggerNames(wf)
	if wf.GetTriggers().GetSchedule() != nil {
		names = append(names, "its schedule")
	}

	if len(names) == 0 {
		return "and it declares no other trigger either, so nothing can start it at all — remove " +
			"`manual: denied`, or declare the source that is meant to"
	}

	return "it is started by " + strings.Join(names, ", ")
}
