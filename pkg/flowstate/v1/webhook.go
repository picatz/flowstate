package flowstatev1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// What a declared webhook can be wrong about, and how a delivery becomes a run's
// inputs — the two halves that have to agree wherever a webhook is read.
//
// Three surfaces read a [WebhookTrigger] and must say the same thing about it:
// `flow validate` in an author's editor, `flow test` replaying a stored delivery
// offline, and the receiver that will one day accept a live one. Two of those have
// a line to point at and one does not, which is the shape [CheckScheduleTrigger]
// already has and the reason these checks live here rather than in the compiler:
// a rule with two implementations eventually has two meanings.
//
// # The bound the receiver will inherit rather than invent
//
// A delivery body is attacker-chosen input, so it gets a byte bound *before* it is
// read into memory — [MaxWebhookPayloadBytes]. That bound is enforced today by the
// only reader there is, `flow test` reading a stored delivery off disk
// (`flowtest.loadDelivery`), and it is the number the live receiver will apply to
// a request body when the receiver lands: the cap belongs on the reader, below
// whatever framework serves the request, for the same reason `plugin/transport.go`
// puts the response cap on the RoundTripper rather than on the RPC library.
//
// Nothing here is the receiver. There is no endpoint, no routing, and no signature
// arithmetic in this repository yet; what exists is the declaration, its checks,
// and the mapping from a delivery to a run's inputs — which is the part a file
// controls and the part that is testable with no network at all.

// EventRoot is the name a trigger's expressions read the delivery through.
//
// Bound inside a trigger's `arguments` (`with:` in a Flowfile) and its
// `idempotency_key`, and nowhere else in the language. That narrowness is the
// design's own rule rather than an implementation limit: everything a workflow
// *operates on* arrives through `with:` into `inputs:`, so a step body that could
// read the payload directly would be a second input path with no contract, which
// `flow validate` could not check.
//
// It is bound bare, like [NowIdentifier] and a wait's own result, which is why the
// `flow fix` rewriter has to know about it: a file may legitimately contain a step
// called `event`, and a rewriter that rooted the name inside a trigger would turn
// a working mapping into a reference to that step — the exact corruption class
// CLAUDE.md's rewriter section records. Unlike `now` it is *not* reserved as a step
// id, because inside a trigger there is no step scope at all for it to shadow.
const EventRoot = "event"

// The two fields a delivery carries, which is the whole of what `event` is.
//
// Named here rather than left as strings in three files, so an editor's completion,
// the mapping below and whatever documents the shape cannot disagree about the
// spelling. Headers are the transport's metadata (a signature header is the usual
// idempotency key), body is the decoded payload.
const (
	EventHeadersField = "headers"
	EventBodyField    = "body"
)

// MaxWebhookPayloadBytes bounds a delivery body before it is read into memory.
//
// A megabyte is far past any legitimate webhook — Stripe, GitHub and Shopify all
// document payloads two orders of magnitude smaller — and far short of anything
// that costs a worker. It matches [flowfile]'s own bound on a Flowfile and
// flowtest's on a test file, deliberately: they are all "a document somebody else
// wrote, read whole before it can be parsed", and one number for that class is one
// number to reason about.
//
// Enforced by every reader of a delivery, at the read, never after: a bound applied
// to an already-decoded body has already lost.
const MaxWebhookPayloadBytes = 1 << 20

// The verification schemes a webhook may name.
//
// A closed set, checked when the file compiles, because the alternative fails
// closed in the worst possible way: an unverifiable delivery is refused, so a
// scheme nobody implements is a trigger that can never fire — better said with a
// line and a column than discovered by an integration that silently never runs.
//
// It is a property of the *file* rather than of a deployment, which is the standing
// rule for what a validator may report. Whether this deployment has the signing key
// for `stripe` configured is the deployment's answer and is not asked here; whether
// `stripe` is a thing Flowstate can verify at all is a fact about Flowstate,
// exactly as "the http task speaks HTTP" is a fact about that task.
//
// Two entries because two are what the design settled on (#490). A third is a line
// here plus the code that implements it, and adding one is not a schema change.
const (
	// WebhookSchemeHMACSHA256 is the generic scheme: an HMAC-SHA256 of the raw
	// body under a shared secret, compared against a header. What most providers
	// do, spelled without naming one.
	WebhookSchemeHMACSHA256 = "hmac_sha256"

	// WebhookSchemeStripe is Stripe's `Stripe-Signature` construction, which is an
	// HMAC-SHA256 over a timestamp and the body with its own replay window — a
	// named scheme rather than the generic one because getting the payload
	// construction wrong is how signature checks come to pass on forged bodies.
	WebhookSchemeStripe = "stripe"
)

// webhookVerificationSchemes is the set above, in the order a diagnostic lists
// them: the generic one first, because it is the one an unfamiliar provider is
// spelled with.
var webhookVerificationSchemes = []string{WebhookSchemeHMACSHA256, WebhookSchemeStripe}

// WebhookVerificationSchemes returns the schemes a `verify:` block may name.
//
// A copy, so a caller rendering it into a diagnostic cannot reorder the set every
// other reader depends on.
func WebhookVerificationSchemes() []string {
	return slices.Clone(webhookVerificationSchemes)
}

// KnownWebhookVerificationScheme reports whether name is a scheme this build can
// verify a delivery with.
func KnownWebhookVerificationScheme(name string) bool {
	return slices.Contains(webhookVerificationSchemes, name)
}

// CheckWebhookTrigger reports what is wrong with a declared webhook, in sentences
// meant to be read by whoever wrote it.
//
// Everything here is a property of the file: a missing name, a `verify:` block that
// names no scheme or names one nothing implements, a missing `idempotency_key`. It
// resolves nothing and performs no I/O — whether the secret behind a scheme exists
// is a deployment's answer, and asking would put a secret backend on an editor's
// keystroke path.
//
// It is composed of the four checks below rather than written as one block, and
// the split is not tidiness: `flow validate` reports each of them against a
// *different* position — the name key, one scheme's entry, the `idempotency_key:`
// line — and a caller with only a whole-trigger check would have to re-derive which
// part was at fault from the sentence it got back. One rule, two moments, is the
// shape [CheckInputDefault] already has.
func CheckWebhookTrigger(trigger *WebhookTrigger) error {
	if trigger == nil {
		return fmt.Errorf("a webhook trigger needs a name, a `verify:` block and an `idempotency_key:`")
	}

	if err := CheckWebhookName(trigger.GetName()); err != nil {
		return err
	}

	// Verification first, because it decides whether a delivery can ever be
	// accepted, and a trigger that can never fire is worth saying before anything
	// about its arguments.
	if err := CheckWebhookVerify(trigger.GetName(), trigger.GetVerify()); err != nil {
		return err
	}
	for _, scheme := range slices.Sorted(maps.Keys(trigger.GetVerify())) {
		if err := CheckWebhookVerifyScheme(trigger.GetName(), scheme, trigger.GetVerify()[scheme]); err != nil {
			return err
		}
	}

	return CheckWebhookIdempotencyKey(trigger.GetName(), trigger.GetIdempotencyKey())
}

// CheckWebhookName reports whether a webhook is named at all.
//
// The shape of the name — letters, digits, `-` and `_` — is the schema's rule
// ([WebhookTrigger.name]'s pattern); what is checked here is the part protovalidate
// cannot say usefully in an editor, which is that there is a name to address this
// source by at all.
func CheckWebhookName(name string) error {
	if name == "" {
		return fmt.Errorf("a webhook trigger has no name; write `- webhook: <name>`, which is how a " +
			"diagnostic and a `flow test` case address this source")
	}

	return nil
}

// CheckWebhookVerify reports a webhook that names no signing scheme.
//
// Fail closed, and this is the sentence that says why: there is no spelling that
// means "accept anything", so a webhook with no scheme is not permissive, it is
// inert.
func CheckWebhookVerify(name string, verify map[string]*Value) error {
	if len(verify) == 0 {
		return fmt.Errorf("webhook %q declares no `verify:`; an unverifiable delivery is refused rather than "+
			"accepted, so a webhook with no scheme can never start a run. Write `verify: {%s: ${secret('...')}}` "+
			"with the key the sender signs with",
			name, WebhookSchemeHMACSHA256)
	}

	return nil
}

// CheckWebhookVerifyScheme reports what is wrong with one entry of a `verify:`
// block: a scheme nothing implements, or signing material written into the file
// instead of referenced.
func CheckWebhookVerifyScheme(name, scheme string, key *Value) error {
	if !KnownWebhookVerificationScheme(scheme) {
		return fmt.Errorf("webhook %q verifies with %q, which is not a scheme Flowstate can check a "+
			"delivery against; the schemes are %s",
			name, scheme, strings.Join(WebhookVerificationSchemes(), ", "))
	}

	if _, isSecret := key.GetKind().(*Value_SecretRef); !isSecret {
		return fmt.Errorf("webhook %q verifies with %q using a value written in the file; write the signing "+
			"key as a secret reference — `%s: ${secret('env:WEBHOOK_SECRET')}` — so it is resolved where the "+
			"delivery is checked rather than committed here",
			name, scheme, scheme)
	}

	return nil
}

// CheckWebhookIdempotencyKey reports a webhook with no dedupe key.
//
// Required rather than optional, and the reason is stated where somebody meets it:
// webhook delivery is at-least-once by nature, so a trigger with no key turns every
// retried delivery into a second run. An integration that genuinely has no stable
// key has to write that down rather than inherit it from a default nobody chose.
//
// # What this refuses, and the wider thing it does not
//
// It asks one question — does the expression *name* the delivery, a free
// occurrence of `event` no enclosing comprehension has shadowed
// ([celExprReferencesIdentifier])? That question is answerable from the syntax
// and its answer is a proof: an expression with no free `event` cannot read the
// delivery, so it cannot vary with it, on any delivery that will ever arrive.
//
// The wider question — does the key's *value* actually move when the delivery
// does — is not answered here, and #733 is the record of why. Naming the delivery
// is not the same as depending on it: `${true ? "all-events" : event.body.id}`
// carries a real free `event` in a branch nothing takes, passes this check, and
// still collapses every delivery onto one run.
//
// Two ways to close that were tried and rejected, and the second is the one worth
// knowing about because it looks like it works:
//
//   - Constant folding, partially. A special case for a literal condition closes
//     the example above and leaves `1 == 1 ? … : …`, a constant `has()`, a
//     comparison of two literals and a macro over a literal list all passing — a
//     check that reads complete when it is not.
//   - Evaluating the key against synthetic deliveries and refusing one that comes
//     out the same string every time. This was built, reviewed and backed out: a
//     finite sample can witness that a key *varies* and can never prove that it
//     does not. `${event.body.type == "invoice.paid" ? event.body.id : "ignored"}`
//     is identical across any set of synthetic deliveries that happen not to carry
//     that type, and distinct across the ones the author cares about — so the
//     refusal lands on a working file, which is the outcome CLAUDE.md rates worse
//     than the gap it closes. This function is also reached from [BindRunInputs],
//     from the webhook mount and from [BindWebhookTriggerInputs], so such a
//     refusal is not an editor's squiggle: it rejects a live delivery to a webhook
//     that had been working.
//
// So the check refuses what it can prove and stays silent about the rest, and the
// residual is documented in docs/DSL.md rather than left for somebody to discover
// by reading this function and assuming it is complete. Closing it needs a place
// to put a finding that is evidence rather than proof — a diagnostic severity this
// package does not have, or a rehearsal over the author's own recorded deliveries,
// where two distinct deliveries colliding on one key is a fact rather than a
// sample.
func CheckWebhookIdempotencyKey(name string, key *Value) error {
	if key == nil {
		return fmt.Errorf("webhook %q declares no `idempotency_key:`; delivery is at-least-once, so without one "+
			"every redelivery starts a second run. Write the value the sender repeats on a retry, such as "+
			"`idempotency_key: ${event.headers[\"stripe-signature\"]}` or an id from the body",
			name)
	}

	// A constant is the same failure arrived at the long way: every delivery would
	// be named alike, so either all of them dedupe against each other or none does.
	// Refused where it is written rather than at the first redelivery, which is the
	// one moment nobody is watching.
	expression, computed := key.GetKind().(*Value_Expr)
	if !computed || !celExprReferencesIdentifier(expression.Expr.GetExpr(), EventRoot) {
		return fmt.Errorf("webhook %q writes an `idempotency_key:` that does not depend on the delivery, so "+
			"every delivery would be named alike; write an expression over `%s`, such as "+
			"`${%s.%s[\"stripe-signature\"]}` or an id from the body",
			name, EventRoot, EventRoot, EventHeadersField)
	}

	return nil
}

// celExprReferencesIdentifier reports whether an expression has a *free*
// occurrence of name — one not shadowed by an enclosing comprehension's own
// bound variable. Trigger expressions are already type-checked separately;
// this walk exists to distinguish a delivery-derived key from a CEL
// expression that merely wraps a constant.
//
// Scope-aware for the same reason [collectFreeIdentifiers] is: a
// comprehension can bind an iteration variable of the same name it is asked
// about, and once it does, every occurrence inside the comprehension's
// loop_condition, loop_step and result refers to that local binding, not to
// the identifier this is checking for. `[1].map(event, string(event))[0]`
// type-checks to the constant `"1"` — its `event` is the comprehension's own
// loop variable, never the delivery root — and treating it as a reference to
// the outer `event` would accept an idempotency key that names the same
// value on every delivery.
func celExprReferencesIdentifier(expression *expr.Expr, name string) bool {
	return celExprReferencesFreeIdentifier(expression, name, false, maxIdentifierWalkDepth)
}

// maxIdentifierWalkDepth bounds [celExprReferencesFreeIdentifier]'s recursion.
//
// This walk reads an AST an outside party chose: the submit path accepts a
// hand-built `RunRequest` carrying a `ParsedExpr` that never went through the
// CEL parser, so nothing upstream has limited how deeply it nests. The 1 MiB
// specification cap bounds bytes, and bytes are not depth — compact nested
// nodes buy thousands of levels inside it, which is CLAUDE.md's rule about
// bounding the resource the attacker actually controls. Depth is that
// resource here, because the walk is recursive and the exhaustible thing is
// the goroutine stack.
//
// Exceeding it answers "no free occurrence", which refuses the idempotency
// key. That is the fail-closed direction: the alternative accepts a key on an
// expression this function gave up reading.
//
// 32, the same bound and the same argument as [maxActivationDepth]. A real
// idempotency key is a header lookup or a field selection, nowhere near it.
const maxIdentifierWalkDepth = 32

// celExprReferencesFreeIdentifier is [celExprReferencesIdentifier]'s walk.
//
// shadowed records whether an enclosing comprehension has bound `name` — one
// bool rather than the set of every enclosing binding, because that set was
// only ever consulted about `name`. Carrying the set meant copying it at each
// comprehension, which made the walk quadratic in nesting depth on exactly
// the hand-built input described above: thousands of nested comprehensions
// with distinct iterator names, each level copying every name bound above it.
// One bool has nothing to copy, and shadowing is monotone — once `name` is
// bound it stays bound for everything inside — so there is no pop to get
// wrong either.
func celExprReferencesFreeIdentifier(expression *expr.Expr, name string, shadowed bool, depth int) bool {
	if expression == nil || depth <= 0 {
		return false
	}
	depth--
	switch kind := expression.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		if shadowed {
			return false
		}
		return kind.IdentExpr.GetName() == name
	case *expr.Expr_SelectExpr:
		return celExprReferencesFreeIdentifier(kind.SelectExpr.GetOperand(), name, shadowed, depth)
	case *expr.Expr_CallExpr:
		if celExprReferencesFreeIdentifier(kind.CallExpr.GetTarget(), name, shadowed, depth) {
			return true
		}
		for _, argument := range kind.CallExpr.GetArgs() {
			if celExprReferencesFreeIdentifier(argument, name, shadowed, depth) {
				return true
			}
		}
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			if celExprReferencesFreeIdentifier(element, name, shadowed, depth) {
				return true
			}
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			if celExprReferencesFreeIdentifier(entry.GetMapKey(), name, shadowed, depth) ||
				celExprReferencesFreeIdentifier(entry.GetValue(), name, shadowed, depth) {
				return true
			}
		}
	case *expr.Expr_ComprehensionExpr:
		comprehension := kind.ComprehensionExpr

		// iter_range and accu_init are evaluated in the *outer* scope — the
		// comprehension's own variables are not bound yet when either runs.
		if celExprReferencesFreeIdentifier(comprehension.GetIterRange(), name, shadowed, depth) ||
			celExprReferencesFreeIdentifier(comprehension.GetAccuInit(), name, shadowed, depth) {
			return true
		}

		// The loop's own scope: iteration variables and the accumulator are
		// all bound while loop_condition and loop_step run.
		loop := shadowed
		for _, bound := range []string{comprehension.GetIterVar(), comprehension.GetIterVar2(), comprehension.GetAccuVar()} {
			if bound == name && bound != "" {
				loop = true
			}
		}

		// The result's scope is *not* the loop's. It is evaluated once the
		// iteration has ended, with only the accumulator still bound — so an
		// expression that names `event` in a comprehension's result names the
		// delivery root, even when the comprehension iterates over a variable
		// of that same name.
		//
		// Carrying the loop's shadowing into the result would refuse a key
		// that genuinely does depend on the delivery, and CLAUDE.md rates a
		// false diagnostic worse than a missing one: a refusal an author
		// cannot act on, about a file that is right.
		result := shadowed
		if comprehension.GetAccuVar() == name && name != "" {
			result = true
		}

		return celExprReferencesFreeIdentifier(comprehension.GetLoopCondition(), name, loop, depth) ||
			celExprReferencesFreeIdentifier(comprehension.GetLoopStep(), name, loop, depth) ||
			celExprReferencesFreeIdentifier(comprehension.GetResult(), name, result, depth)
	}
	return false
}

// CheckWebhookTriggers reports what is wrong across a workflow's whole set of
// webhooks: each one on its own terms, and the one rule that is about the set.
//
// Two sources sharing a name are two mappings a reader cannot tell apart and a
// `flow test` case cannot address — it would replay against whichever came first,
// silently — so the duplicate is refused rather than resolved.
//
// Called from [BindRunInputs], beside [CheckVarsHoldNoSecretRef] and
// [CheckWaitPromptsAreAskable] and for the reason they are: it is the one function
// every submit path already calls, so a specification that never was a Flowfile is
// held to these rules too. Without that, a hand-built `RunRequest` could write a
// literal signing key into durable history — see the call site for the full
// argument.
func CheckWebhookTriggers(triggers *Triggers) error {
	seen := make(map[string]bool, len(triggers.GetWebhooks()))
	for _, trigger := range triggers.GetWebhooks() {
		if err := CheckWebhookTrigger(trigger); err != nil {
			return err
		}
		if seen[trigger.GetName()] {
			return fmt.Errorf("webhook %q is declared twice; each source needs its own name, because a "+
				"diagnostic and a `flow test` case address a webhook by name", trigger.GetName())
		}
		seen[trigger.GetName()] = true
	}

	return nil
}

// FindWebhookTrigger returns the webhook a workflow declares under name.
//
// One lookup, so `flow test` and whatever addresses a delivery later cannot
// disagree about which trigger a name means — including about the duplicate
// [CheckWebhookTriggers] refuses, which this would otherwise resolve silently.
func FindWebhookTrigger(wf *Workflow, name string) (*WebhookTrigger, bool) {
	for _, trigger := range wf.GetTriggers().GetWebhooks() {
		if trigger.GetName() == name {
			return trigger, true
		}
	}

	return nil, false
}

// WebhookTriggerNames returns the names a workflow's webhooks are declared under,
// in the order they were written, for a diagnostic that has to say what *is*
// available when somebody names one that is not.
func WebhookTriggerNames(wf *Workflow) []string {
	names := make([]string, 0, len(wf.GetTriggers().GetWebhooks()))
	for _, trigger := range wf.GetTriggers().GetWebhooks() {
		names = append(names, trigger.GetName())
	}

	return names
}

// NewWebhookEvent builds the value a trigger's expressions read as `event`.
//
// Headers and a decoded body, and deliberately nothing else. A delivery's method,
// path and remote address are the receiver's business — routing and policy, decided
// where the request arrives — and putting them in an author's namespace now would
// be inventing surface the design has not settled.
//
// Header names are lower-cased, which is the one normalization the mapping performs
// and the reason it is performed here rather than by each caller: HTTP header names
// are case-insensitive, so `${event.headers["stripe-signature"]}` has to find a
// header a sender spelled `Stripe-Signature`. A stored delivery replayed by
// `flow test` and a live one therefore agree about what an expression sees.
func NewWebhookEvent(headers map[string]string, body any) *Value {
	lowered := make(map[string]any, len(headers))
	for name, value := range headers {
		lowered[strings.ToLower(name)] = value
	}

	return NewLiteralMap(map[string]any{
		EventHeadersField: lowered,
		EventBodyField:    body,
	})
}

// NormalizeDeliveryNumbers reads a decoded JSON body the way an author expects a
// payload to read: a number written without a fraction is an integer.
//
// Necessary because `encoding/json` decodes every number as a float64 by default,
// so `"amount": 4200` arrives as 4200.0 and an input declared `int` refuses it —
// a mapping that is correct in the file and fails on arrival, which is the worst
// kind of failure this design exists to move earlier. Decoding with
// [json.Decoder.UseNumber] and passing the result through here is the fix, and it
// belongs beside the trigger rather than in whichever reader got there first: a
// stored delivery replayed by `flow test` and a live one must produce the same
// value or the rehearsal lies.
//
// A number with a fraction, or one too large for an int64, stays a float — the same
// order [NewLiteral] would reach for, and honest about what the sender wrote.
func NormalizeDeliveryNumbers(v any) any {
	switch value := v.(type) {
	case json.Number:
		if whole, err := value.Int64(); err == nil {
			return whole
		}
		if approximate, err := value.Float64(); err == nil {
			return approximate
		}
		// Neither, which JSON's own grammar makes very hard to write: kept as the
		// text the sender sent rather than dropped, so an expression reading it
		// gets something to fail on rather than a missing key.
		return value.String()
	case map[string]any:
		normalized := make(map[string]any, len(value))
		for name, entry := range value {
			normalized[name] = NormalizeDeliveryNumbers(entry)
		}
		return normalized
	case []any:
		normalized := make([]any, len(value))
		for i, entry := range value {
			normalized[i] = NormalizeDeliveryNumbers(entry)
		}
		return normalized
	default:
		return v
	}
}

// A WebhookDelivery is one arrival at a declared webhook, as much of it as the
// mapping is allowed to see.
//
// A plain struct rather than a schema message, and this is the exception CLAUDE.md's
// proto-first rule names: the schema describes what travels, and a delivery is
// consumed where it lands. It is the receiver's input and the mapping's argument,
// never a field of a specification and never something written to history — what
// *does* travel out of this is the run's bound inputs and the idempotency key, both
// of which are already schema types. Making it a message would put an
// attacker-supplied body in a shape everything downstream could carry by accident.
type WebhookDelivery struct {
	// Headers are the delivery's headers, matched case-insensitively by
	// [NewWebhookEvent].
	Headers map[string]string

	// Body is the decoded payload — whatever a JSON document decodes to.
	Body any

	// Verified says whether the delivery satisfied one of the trigger's declared
	// schemes. False refuses the delivery outright in [BindWebhookTriggerInputs],
	// including when it is false because verification could not be *attempted*:
	// there is no arm that means "unchecked, allow anyway", by design.
	Verified bool
}

// BindWebhookTriggerInputs turns a delivery into the inputs a run would start with,
// and the key that names the delivery.
//
// This is the whole of what a webhook trigger *does*, and it is deliberately
// separable from any receiver: given a stored payload it is a pure function, which
// is what makes an argument mapping unit-testable rather than debuggable only in
// production. `flow test` calls it with a delivery read off disk; the live receiver
// will call it with one read off a socket, having decided [WebhookDelivery.Verified]
// with the material behind the trigger's `verify:` block.
//
// The order is fail-closed and matters:
//
//  1. The trigger's own declaration is checked ([CheckWebhookTrigger]), because a
//     malformed trigger cannot be a basis for accepting anything.
//  2. The delivery is refused unless it verified.
//  3. Only then is anything attacker-chosen evaluated.
//
// Each expression is evaluated against `event` alone, bounded by
// [DefaultCostLimit] like every other expression this system evaluates — the
// bound that matches the resource an attacker controls here, since a payload
// decides how large the values an expression walks are.
//
// The returned inputs have been through [BindRunInputs], so a webhook lands in the
// same `inputs:` contract with the same diagnostics as `flow run --input` and an
// MCP invocation: a trigger that could bypass that check would make the contract
// decorative.
func BindWebhookTriggerInputs(ctx context.Context, wf *Workflow, trigger *WebhookTrigger, delivery WebhookDelivery) (inputs map[string]*Value, idempotencyKey string, err error) {
	if err := CheckWebhookTrigger(trigger); err != nil {
		return nil, "", err
	}

	if !delivery.Verified {
		// Refused before the payload is read by anything, and refused with the same
		// sentence whether the signature was wrong, absent, or impossible to check.
		// Distinguishing them here would tell whoever is probing which of the three
		// they achieved.
		return nil, "", fmt.Errorf("delivery to webhook %q did not verify against %s, so it is refused; "+
			"an unverifiable delivery is never accepted on the grounds that it could not be checked",
			trigger.GetName(), strings.Join(slices.Sorted(maps.Keys(trigger.GetVerify())), ", "))
	}

	event := NewWebhookEvent(delivery.Headers, delivery.Body)
	if err := event.Error(); err != nil {
		return nil, "", fmt.Errorf("webhook %q: reading the delivery: %w", trigger.GetName(), err)
	}

	// The scope a trigger evaluates in holds `event` and nothing else — no steps,
	// no vars, no inputs — because a trigger runs before there is a run to have any
	// of those. Built through the same [Scope] machinery every other evaluation
	// uses so that the cost limit, the cancellation contract and the profile
	// libraries are the ones the rest of the system has, rather than a second
	// environment that drifts (invariant 2).
	scope := NewScope(wf.GetProfile(), nil)
	bound := map[string]ref.Val{EventRoot: eventRefValue(event)}
	activation := scope.ActivationWith(ctx, bound)

	evaluator := DefaultEvaluator()

	key, err := evaluator.EvalParsedBase(ctx, scope.GetProfile(), trigger.GetIdempotencyKey().GetExpr(), activation)
	if err != nil {
		return nil, "", fmt.Errorf("webhook %q: evaluating `idempotency_key:`: %w", trigger.GetName(), err)
	}
	text, ok := key.Value().(string)
	if !ok {
		return nil, "", fmt.Errorf("webhook %q: `idempotency_key:` evaluated to %s rather than to a string; "+
			"a delivery is named by text, so convert it with string(...)", trigger.GetName(), key.Type())
	}
	if strings.TrimSpace(text) == "" {
		// An empty key is the failure the required field exists to prevent, arrived
		// at the long way: every delivery would share it, so either all of them
		// dedupe against each other or none does. Refused rather than accepted as
		// "no key", because the file declared one.
		return nil, "", fmt.Errorf("webhook %q: `idempotency_key:` evaluated to an empty string, which would "+
			"name every delivery alike; write an expression reaching a value the sender repeats on a retry",
			trigger.GetName())
	}

	arguments := make(map[string]*Value, len(trigger.GetArguments()))
	for _, name := range slices.Sorted(maps.Keys(trigger.GetArguments())) {
		argument := trigger.GetArguments()[name]
		if _, isExpr := argument.GetKind().(*Value_Expr); !isExpr {
			// A literal argument is already a value — `amount: 0` under a schedule's
			// sweep — and there is nothing to evaluate.
			arguments[name] = argument
			continue
		}

		out, evalErr := evaluator.EvalParsedBase(ctx, scope.GetProfile(), argument.GetExpr(), activation)
		if evalErr != nil {
			return nil, "", fmt.Errorf("webhook %q: input %q: %w", trigger.GetName(), name, evalErr)
		}
		literal, convErr := cel.RefValueToValue(out)
		if convErr != nil {
			return nil, "", fmt.Errorf("webhook %q: input %q: converting result: %w", trigger.GetName(), name, convErr)
		}
		arguments[name] = &Value{Kind: &Value_Literal{Literal: literal}}
	}

	// The same bind a submit performs, reached rather than restated: defaults
	// filled in, types checked, constraints applied, an undeclared name refused.
	bounded, err := BindRunInputs(wf, arguments)
	if err != nil {
		return nil, "", fmt.Errorf("webhook %q: %w", trigger.GetName(), err)
	}

	return bounded, text, nil
}

// eventRefValue converts the built event into the CEL value bound as `event`.
//
// An unconvertible event becomes an error value rather than being dropped, for the
// reason [refValues] gives: a dropped name looks simply unbound, which sends an
// author hunting for a typo in the one case where the name is right and the
// delivery is not.
func eventRefValue(event *Value) ref.Val {
	converted, err := cel.ValueToRefValue(TypeAdapter, event.GetLiteral())
	if err != nil {
		return TypeAdapter.NativeToValue(fmt.Errorf("the delivery could not be read as a value: %w", err))
	}

	return converted
}

// WebhookDeliveryID names one delivery, for provenance and for a log line.
//
// A digest of the evaluated idempotency key rather than the key itself, and that
// is the whole substance of the function: the usual key *is* a signature header,
// and this value is written into a memo and into the run's own
// [TriggerContext.delivery_id], both of which are durable and broadly readable
// (invariant 8). A digest reveals nothing about the key it names.
//
// Truncated to sixteen bytes because this is an identifier a human correlates
// across a log line, a memo and a `${trigger.delivery_id}` in a workflow — not a
// security boundary. The receiver puts the full digest in the run's workflow id
// for whoever needs collision resistance.
//
// Here rather than in the receiver, because it is not only the receiver's: `flow
// test` replaying a stored delivery has to produce the identical value, or a
// rehearsal asserting on `${trigger.delivery_id}` would assert against something
// production never answers with — one meaning, one definition, which is the rule
// CLAUDE.md states for every value both drivers read.
func WebhookDeliveryID(key string) string {
	digest := sha256.Sum256([]byte(key))

	return hex.EncodeToString(digest[:16])
}
