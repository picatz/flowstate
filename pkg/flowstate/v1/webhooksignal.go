package flowstatev1

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
)

// The bridge from a verified delivery to a declared gate.
//
// Everything a *file* decides about it lives here: which signal a delivery
// answers, which run it addresses, what it carries, and — the one rule this
// bridge adds rather than reuses — that the gate's `signals:` policy names the
// trigger. The receiver (`server/webhook.go`) decides the half a deployment
// owns, and the engine decides what a run does with a delivery it has already
// consumed. None of the three re-derives the others' rules.

// WebhookPrincipalIssuer is the issuer half of the principal a delivery acts
// as, for a run it starts and for a gate it answers alike.
//
// A delivery holds no Flowstate credential — a payments provider does not have
// one — so what is attested is the *trigger* that admitted it, written in the
// same `issuer#subject` form every other caller is recorded in. It is a URL
// shape on purpose: an OIDC issuer is a URL too, and `flowstate://` is a scheme
// no identity provider can mint, so a `signals:` rule naming a webhook cannot
// collide with one naming a person.
//
// Here rather than in the server package because the *validator* has to name
// the same string: refusing a `signal:` whose policy could not admit the
// trigger means composing the principal the receiver will mint, and two copies
// of that string is exactly the drift invariant 2 refuses.
const WebhookPrincipalIssuer = "flowstate://webhook"

// WebhookTriggerSubject is the subject half of that principal: which webhook,
// on which workflow.
//
// `<workflow>/<trigger>` — the same pair a delivery is addressed by
// (`/webhooks/<workflow>/<trigger>`), so a rule an operator writes and a URL a
// provider is configured with read alike.
func WebhookTriggerSubject(workflow, trigger string) string {
	return workflow + "/" + trigger
}

// WebhookTriggerPrincipal is the identity a delivery to this trigger attests
// as, which is what a `signals:` rule is matched against.
//
// namespace is the receiver's own tenant, and is empty in the one caller that
// does not have one: `flow validate`, which is answering a question about the
// file and has no deployment to ask. See [CheckWebhookSignalPolicy] for what
// that omission does and does not let it conclude.
func WebhookTriggerPrincipal(namespace, workflow, trigger string) *WorkloadIdentity {
	return &WorkloadIdentity{
		Issuer:    WebhookPrincipalIssuer,
		Subject:   WebhookTriggerSubject(workflow, trigger),
		Namespace: namespace,
	}
}

// CheckWebhookSignalBridges reports what is wrong with every `signal:` a
// workflow's webhooks declare.
//
// Called from [BindRunInputs] beside [CheckWebhookTriggers], for that
// function's reason: it is the one place every submit path already passes
// through, so a specification that never was a Flowfile is held to these rules
// too. A bridge is a trust boundary, and a rule only `flow validate` enforced
// would be a rule a hand-built `RunRequest` walks around.
func CheckWebhookSignalBridges(wf *Workflow) error {
	for _, trigger := range wf.GetTriggers().GetWebhooks() {
		if trigger.GetSignal() == nil {
			continue
		}
		if err := CheckWebhookSignalExclusive(trigger); err != nil {
			return err
		}
		if err := CheckWebhookSignalName(wf, trigger.GetName(), trigger.GetSignal()); err != nil {
			return err
		}
		if err := CheckWebhookSignalCorrelate(trigger.GetName(), trigger.GetSignal()); err != nil {
			return err
		}
		if err := CheckWebhookSignalPolicy(wf, trigger.GetName(), trigger.GetSignal()); err != nil {
			return err
		}
		if err := CheckWebhookSignalAddressing(trigger); err != nil {
			return err
		}
	}

	return nil
}

// CheckWebhookSignalExclusive refuses a webhook that both starts a run and
// answers a gate.
//
// Refused rather than ordered by precedence, for the reason
// [ManualTrigger.denied] refuses its own contradiction: a delivery that binds
// `inputs:` starts something, a delivery that answers a gate joins something
// that is already running, and a trigger claiming both is two sentences that
// cannot both be true. Whichever one a precedence rule picked would be the
// other one silently doing nothing, which is the "accepted and ignored" shape
// docs/STYLE.md's R6 refuses outright.
//
// # Presence, not length
//
// A non-nil `arguments` map with nothing in it is still the construct: the
// specification carries both, and `arguments: {}` is what protojson produces
// for a written-but-empty mapping and what a hand-built [RunRequest] can carry
// directly. Judging by `len` accepted exactly that, which is the contradiction
// this refuses arriving through the one door that does not go through a
// Flowfile. The Flowfile layer asks the same question in its own terms — was
// the `with:` key written — because a compiler drops an empty mapping before
// this function could ever see it; see webhookTrigger in `flowfile`.
func CheckWebhookSignalExclusive(trigger *WebhookTrigger) error {
	if trigger.GetArguments() == nil {
		return nil
	}

	return WebhookSignalExclusiveError(trigger.GetName())
}

// WebhookSignalExclusiveError is the one sentence both seams say it with.
//
// Exported because the compiler decides the same contradiction from a different
// fact — a written key rather than a populated map — and two spellings of one
// refusal is what makes a search for either of them miss half the files that
// have it.
func WebhookSignalExclusiveError(webhook string) error {
	return fmt.Errorf("webhook %q declares both `with:` and `signal:`; the first binds a new run's "+
		"`inputs:` and the second answers a run that is already waiting, so one delivery cannot do "+
		"both. Keep `signal:` to answer a gate, or `with:` to start a run",
		webhook)
}

// CheckWebhookSignalName refuses a bridge to a name nothing in this file waits
// for.
//
// The same refusal `SignalWithStart` makes out loud and for the same reason: a
// delivery to a name no `wait_for_signal:` declares is carried in the run's own
// state forever, occupying the budget [CheckRunStateSize] weighs, waiting for a
// step that does not exist. Far more often it is a misspelling, and this is the
// moment somebody is present to fix it.
func CheckWebhookSignalName(wf *Workflow, webhook string, signal *WebhookTrigger_Signal) error {
	name := signal.GetName()
	if name == "" {
		return fmt.Errorf("webhook %q declares a `signal:` with no `name:`; name the signal a "+
			"`wait_for_signal:` in this workflow waits for", webhook)
	}

	waited := SignalNames(wf)
	if slices.Contains(waited, name) {
		return nil
	}

	if len(waited) == 0 {
		return fmt.Errorf("webhook %q answers signal %q, and this workflow has no `wait_for_signal:` at "+
			"all; a delivery would be carried by the run forever without ever being consumed",
			webhook, name)
	}

	return fmt.Errorf("webhook %q answers signal %q, which no `wait_for_signal:` in this workflow waits "+
		"for; the names it waits for are %s",
		webhook, name, strings.Join(waited, ", "))
}

// CheckWebhookSignalCorrelate refuses a correlation that cannot name a run.
//
// It asks [CheckWebhookIdempotencyKey]'s question, about the other expression a
// delivery is addressed by, and it is worth the second answer: a `correlate:`
// that does not read the delivery names one run for every delivery this webhook
// will ever receive, so every approval anybody sends lands on whichever run that
// constant happens to address. The gap that check documents applies here
// unchanged — naming `event` is provable, depending on it is not.
func CheckWebhookSignalCorrelate(webhook string, signal *WebhookTrigger_Signal) error {
	correlate := signal.GetCorrelate()
	if correlate == nil {
		return fmt.Errorf("webhook %q declares a `signal:` with no `correlate:`; write the expression over "+
			"`%s` that yields the entity key of the run this delivery answers, such as "+
			"`correlate: ${%s.body.order_id}`",
			webhook, EventRoot, EventRoot)
	}

	expression, computed := correlate.GetKind().(*Value_Expr)
	if !computed || !celExprReferencesIdentifier(expression.Expr.GetExpr(), EventRoot) {
		return fmt.Errorf("webhook %q writes a `correlate:` that does not depend on the delivery, so every "+
			"delivery would address one run; write an expression over `%s`, such as `${%s.body.order_id}`",
			webhook, EventRoot, EventRoot)
	}

	return nil
}

// CheckWebhookSignalPolicy closes the signal zero case on the one route that
// cannot afford it.
//
// # What the zero case is, and why it is fine everywhere else
//
// A signal name with no `signals:` entry admits any sender: authorization is
// opt-in per name, because the alternative was every existing workflow's next
// `flow signal` failing for a policy nobody had written. That is defensible
// behind the server's own authentication, where "any sender" already means "any
// caller this deployment authenticated and admitted to this tenant".
//
// It is not defensible on a public route. A webhook endpoint is reachable by
// anybody, and a delivery is admitted by holding one trigger's signing key — so
// under the zero case, one leaked Slack secret answers every unpoliced gate in
// every workflow that deployment serves. The gap is closed in the file, where
// an author can see it: a `signal:` requires an explicit policy for its name,
// and that policy must have a rule this trigger's principal could satisfy.
//
// # What "could satisfy" means, and what it deliberately does not check
//
// The principal is [WebhookTriggerPrincipal]: this issuer, this
// `<workflow>/<trigger>` subject, no claims. A rule is reachable when its
// `subject:` is empty or names exactly that pair, and when it requires no
// claims — the receiver mints this principal from its own configuration and
// attaches no claims to it, so a rule demanding one can never match, however
// the deployment is set up.
//
// `namespace:` is not checked, and that is the honest line between what a file
// knows and what a deployment answers: the receiver's tenant is chosen by
// `--webhook-namespace`, `flow validate` performs no deployment lookups, and
// refusing a namespace rule here would refuse a file that works. A rule naming
// only a namespace therefore passes this check and is still enforced in full at
// delivery, by [SignalPolicyCheck], which is the same function and the only
// enforcement point either driver has.
func CheckWebhookSignalPolicy(wf *Workflow, webhook string, signal *WebhookTrigger_Signal) error {
	name := signal.GetName()
	principal := WebhookTriggerPrincipal("", wf.GetName(), webhook)
	qualified := QualifiedSubject(principal.GetIssuer(), principal.GetSubject())

	policy, declared := wf.GetSignals()[name]
	if !declared {
		return fmt.Errorf("webhook %q answers signal %q, which declares no `signals:` policy; a signal with "+
			"no policy admits any sender, and this one is answerable by whoever holds this webhook's "+
			"signing key. Declare it: `signals: {%s: {allow: [{subject: %q}]}}`",
			webhook, name, name, qualified)
	}

	for _, rule := range policy.GetAllow() {
		if len(rule.GetClaims()) > 0 {
			// The receiver attaches no claims to a webhook principal — there is
			// nothing to attach, since a signature attests a key rather than a
			// person — so a rule requiring one is unreachable from this route
			// whatever the deployment does.
			continue
		}
		if subject := rule.GetSubject(); subject != "" && subject != qualified {
			continue
		}
		if rule.GetSubjectFrom() != nil {
			// Resolved at submit, from the run's own inputs, to a literal this
			// file cannot know. It may well resolve to the trigger's subject, so
			// it is not refused — it simply cannot be counted as the rule that
			// makes the bridge reachable, which is the fail-closed reading.
			continue
		}

		return nil
	}

	return fmt.Errorf("webhook %q answers signal %q, and none of that signal's `allow:` rules can admit a "+
		"webhook delivery: this trigger attests as %q and carries no claims. Add a rule naming it — "+
		"`- subject: %q` — or answer a different signal",
		webhook, name, qualified, qualified)
}

// WebhookAddressingField names which of a trigger's two addressing expressions
// a [WebhookAddressingError] is about.
//
// A typed value rather than a sentence a caller matches against: the diagnostic
// layer has to underline one expression or the other, and deriving that from
// the refusal's prose makes rewording the sentence silently move the squiggle
// to the wrong line. The rule decides; the presentation reads what it decided.
type WebhookAddressingField string

const (
	// WebhookAddressingCorrelate is the `signal.correlate:` expression, which
	// chooses which run a delivery answers.
	WebhookAddressingCorrelate WebhookAddressingField = "signal.correlate"

	// WebhookAddressingIdempotencyKey is the trigger's `idempotency_key:`,
	// which on a bridge mints the delivery id the run's replay ring recognizes.
	WebhookAddressingIdempotencyKey WebhookAddressingField = "idempotency_key"
)

// WebhookAddressingError is [CheckWebhookSignalAddressing]'s refusal, carrying
// the expression it is about beside the sentence a person reads.
type WebhookAddressingError struct {
	// Field is which expression must change.
	Field WebhookAddressingField

	// Webhook is the trigger the expression belongs to.
	Webhook string

	// Schemes are the declared schemes whose signatures do not cover headers,
	// named in the sentence so an author knows what would have to change for
	// the expression to be admissible.
	Schemes []string

	// Construct names what the expression did with `event` that this rule could
	// not prove came from the signed body — "a bare `event`", "`event.headers`",
	// "`event` as a comprehension's range". An author has to be told which
	// construct to rewrite, because "address it from the body" is not actionable
	// against an expression that already mentions the body somewhere.
	Construct string
}

func (e *WebhookAddressingError) Error() string {
	return fmt.Sprintf("webhook %q writes a `%s:` this deployment cannot prove reads only signed "+
		"bytes: it uses %s, and %s signs a delivery's body and not its headers. Anybody who has "+
		"seen one valid delivery could replay that body and signature with a header rewritten, and "+
		"this expression decides which run the replay answers — so every `%s` in it has to be a "+
		"`%s.%s` read (`%s.%s.order_id`, `%s.%s[\"order\"]`), which is what makes the value it "+
		"produces attested",
		e.Webhook, e.Field, e.Construct, strings.Join(e.Schemes, " and "),
		EventRoot, EventRoot, EventBodyField,
		EventRoot, EventBodyField, EventRoot, EventBodyField)
}

// CheckWebhookSignalAddressing refuses a bridge that decides *which run* from
// bytes its own `verify:` does not sign.
//
// # The asymmetry this closes
//
// A signature attests the part of a delivery it covers and nothing else.
// [WebhookSchemeHMACSHA256] covers the raw body; [WebhookSchemeStripe] covers
// `<timestamp>.<body>`. Neither covers arbitrary request headers — see
// [webhookSchemesSigningHeaders] — so anybody who has once seen a valid
// delivery can replay its exact body and signature with a header rewritten: a
// proxy log, a mirrored request, a captured retry.
//
// For a trigger that *starts* a run that is a bounded nuisance, because the two
// things a header can move are the run's own id and its inputs, and both stay
// inside what the key holder could have sent anyway. For a bridge it is not:
// `correlate:` chooses which parked run is answered, and `idempotency_key:`
// mints the delivery id the run's replay ring recognizes — so a header-derived
// pair lets a replayer answer a *different* gate, as many times as they like,
// with a body and a signature they never had to be able to produce.
//
// So a bridge addresses itself from signed bytes or it does not compile. Both
// expressions are checked, because they are two halves of one address: the run
// and the delivery's identity within it.
//
// # Provenance is proved, not searched for
//
// The check is an allow-list ([unprovenEventUse]): every occurrence of `event`
// must be the operand of a `body` read. It is not a search for `event.headers`,
// which is what it was and which proved nothing about the expressions it
// accepted — `${[event].map(e, e.headers["x-order"])[0]}` reaches a header
// with no `headers` selection over the `event` identifier anywhere in it, and
// passed. Ternaries, `has()`, map literals and function composition all launder
// the root the same way; a deny-list has to enumerate them and an attacker has
// to find one. Requiring the proof instead makes aliasing unexpressible, since
// an alias must first mention the root somewhere this refuses.
//
// The rule is read off the scheme table rather than written against scheme
// names, so a scheme that does cover headers admits header-derived addressing
// the day it lands, with nothing here to edit.
func CheckWebhookSignalAddressing(trigger *WebhookTrigger) error {
	if trigger.GetSignal() == nil {
		return nil
	}

	var unsigned []string
	for _, scheme := range slices.Sorted(maps.Keys(trigger.GetVerify())) {
		if !WebhookSchemeSignsHeaders(scheme) {
			unsigned = append(unsigned, scheme)
		}
	}
	if len(unsigned) == 0 {
		return nil
	}

	for _, addressed := range []struct {
		field WebhookAddressingField
		value *Value
	}{
		{field: WebhookAddressingCorrelate, value: trigger.GetSignal().GetCorrelate()},
		{field: WebhookAddressingIdempotencyKey, value: trigger.GetIdempotencyKey()},
	} {
		construct, unproven := unprovenEventUse(addressed.value.GetExpr().GetExpr())
		if !unproven {
			continue
		}

		return &WebhookAddressingError{
			Field:     addressed.field,
			Webhook:   trigger.GetName(),
			Schemes:   unsigned,
			Construct: construct,
		}
	}

	return nil
}

// BindWebhookTriggerSignal turns a verified delivery into the answer it
// carries: which run it addresses, what payload reaches the gate, and the key
// that names the delivery.
//
// [BindWebhookTriggerInputs]' sibling, and deliberately its twin down to the
// order — the trigger's own declaration first, then the refusal of an
// unverified delivery, and only then is anything attacker-chosen evaluated.
// Both are pure functions of a stored delivery, which is what lets `flow test`
// replay a bridge offline exactly as it replays a start.
//
// The payload is bounded by [CheckSignalPayloadSize] here rather than at the
// receiver's door for the reason the size check exists at all: it is the piece
// of a run's carried state a sender chooses the size of, and the refusal has to
// land on the party who can shrink it. A caller that skips this function would
// be a second binding path, which is what the receiver calling it rather than
// restating it prevents.
func BindWebhookTriggerSignal(
	ctx context.Context, wf *Workflow, trigger *WebhookTrigger, delivery WebhookDelivery,
) (entityKey string, payload *Node_Outputs, idempotencyKey string, err error) {
	if err := CheckWebhookTrigger(trigger); err != nil {
		return "", nil, "", err
	}

	signal := trigger.GetSignal()
	if signal == nil {
		return "", nil, "", fmt.Errorf("webhook %q declares no `signal:`, so a delivery to it starts a run "+
			"rather than answering one", trigger.GetName())
	}
	if err := CheckWebhookSignalBridges(wf); err != nil {
		return "", nil, "", err
	}

	if !delivery.Verified {
		// The same sentence [BindWebhookTriggerInputs] refuses with, and for the
		// same reason: whether the signature was wrong, absent or impossible to
		// check is exactly what a prober is trying to learn.
		return "", nil, "", fmt.Errorf("delivery to webhook %q did not verify against %s, so it is refused; "+
			"an unverifiable delivery is never accepted on the grounds that it could not be checked",
			trigger.GetName(), strings.Join(slices.Sorted(maps.Keys(trigger.GetVerify())), ", "))
	}

	event := NewWebhookEvent(delivery.Headers, delivery.Body)
	if err := event.Error(); err != nil {
		return "", nil, "", fmt.Errorf("webhook %q: reading the delivery: %w", trigger.GetName(), err)
	}

	// One scope holding `event` and nothing else, through the same [Scope]
	// machinery every other evaluation uses, so the cost limit and the profile
	// libraries are the ones the rest of the system has (invariant 2).
	scope := NewScope(wf.GetProfile(), nil)
	bound := map[string]ref.Val{EventRoot: eventRefValue(event)}
	activation := scope.ActivationWith(ctx, bound)
	evaluator := DefaultEvaluator()

	key, err := evaluator.EvalParsedBase(ctx, scope.GetProfile(), trigger.GetIdempotencyKey().GetExpr(), activation)
	if err != nil {
		return "", nil, "", fmt.Errorf("webhook %q: evaluating `idempotency_key:`: %w", trigger.GetName(), err)
	}
	idempotencyKey, ok := key.Value().(string)
	if !ok {
		return "", nil, "", fmt.Errorf("webhook %q: `idempotency_key:` evaluated to %s rather than to a "+
			"string; a delivery is named by text, so convert it with string(...)", trigger.GetName(), key.Type())
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return "", nil, "", fmt.Errorf("webhook %q: `idempotency_key:` evaluated to an empty string, which "+
			"would name every delivery alike; write an expression reaching a value the sender repeats on "+
			"a retry", trigger.GetName())
	}

	correlated, err := evaluator.EvalParsedBase(ctx, scope.GetProfile(), signal.GetCorrelate().GetExpr(), activation)
	if err != nil {
		return "", nil, "", fmt.Errorf("webhook %q: evaluating `signal.correlate:`: %w", trigger.GetName(), err)
	}
	entityKey, ok = correlated.Value().(string)
	if !ok {
		return "", nil, "", fmt.Errorf("webhook %q: `signal.correlate:` evaluated to %s rather than to a "+
			"string; a run is addressed by an entity key, so convert it with string(...)",
			trigger.GetName(), correlated.Type())
	}

	// The grammar the run's own id is composed under, checked here so a payload
	// that cannot name a run is refused for what it is rather than through a
	// workflow id that failed to compose. [ValidateEntityKey] is the same rule
	// `RunRequest.entity_key` is held to — a delivery reaches no address a
	// caller could not have reached.
	if err := ValidateEntityKey(entityKey); err != nil {
		return "", nil, "", fmt.Errorf("webhook %q: `signal.correlate:` produced %q, which is not an entity "+
			"key: %w", trigger.GetName(), entityKey, err)
	}

	values := make(map[string]*Value, len(signal.GetArguments()))
	for _, name := range slices.Sorted(maps.Keys(signal.GetArguments())) {
		argument := signal.GetArguments()[name]
		if _, isExpr := argument.GetKind().(*Value_Expr); !isExpr {
			values[name] = argument

			continue
		}

		out, evalErr := evaluator.EvalParsedBase(ctx, scope.GetProfile(), argument.GetExpr(), activation)
		if evalErr != nil {
			return "", nil, "", fmt.Errorf("webhook %q: `signal.with.%s:`: %w", trigger.GetName(), name, evalErr)
		}
		literal, convErr := cel.RefValueToValue(out)
		if convErr != nil {
			return "", nil, "", fmt.Errorf("webhook %q: `signal.with.%s:`: converting result: %w",
				trigger.GetName(), name, convErr)
		}
		values[name] = &Value{Kind: &Value_Literal{Literal: literal}}
	}

	payload = &Node_Outputs{NamedValues: values}
	if err := CheckSignalPayloadSize(payload); err != nil {
		return "", nil, "", fmt.Errorf("webhook %q: %w", trigger.GetName(), err)
	}

	return entityKey, payload, idempotencyKey, nil
}

// ConsumeDeliveryID records that a run has taken the delivery id at a gate, and
// reports whether it was new.
//
// This is the dedupe, and it is one function because both drivers have to
// answer it identically: durably the set is [RunState.consumed_delivery_ids]
// carried across every Continue-As-New, locally it is a field of the process's
// own [LocalSignals], and neither may decide "already consumed" a different way
// from the other. Membership is the whole of it — no clock, no verification, no
// I/O — which is what makes calling it from workflow code determinate under
// replay (invariant 4).
//
// An empty id is always fresh and is never recorded: every sender that is not a
// webhook carries one, and a `flow signal` that reported success must reach the
// gate whether or not somebody else has sent one before.
//
// The set is add-only within its bound and a ring past it: at
// [MaxPendingSignals] the oldest id is evicted to make room. That is safe in
// the direction that matters — evicting narrows the window a replay is caught
// in, and unsays no delivery — and it is what keeps a run that answers gates
// forever from carrying an unbounded set across every suspension.
func ConsumeDeliveryID(consumed []string, id string) (updated []string, fresh bool) {
	if id == "" {
		return consumed, true
	}
	if slices.Contains(consumed, id) {
		return consumed, false
	}

	consumed = append(consumed, id)
	if len(consumed) > MaxPendingSignals {
		consumed = consumed[len(consumed)-MaxPendingSignals:]
	}

	return consumed, true
}

// DeliveryWasConsumed reports whether a run has already taken this delivery at
// a gate, without recording anything.
//
// The read half of [ConsumeDeliveryID], for the intake point that has to drop a
// delivery rather than take it: a duplicate arriving on a channel is not
// carried across a suspension, and carrying it would leave the run holding a
// delivery no gate will ever consume.
func DeliveryWasConsumed(consumed []string, id string) bool {
	return id != "" && slices.Contains(consumed, id)
}
