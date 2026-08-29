package flowstatev1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Mutual exclusion across runs, answered at submit.
//
// [Workflow.Concurrency]'s own doc comment carries the design — why the permit is
// a workflow id, why there are exactly three answers, and why nothing inside a run
// ever reads the block. This file is the derivation and the checks: composing the
// id, resolving the key, and refusing the shapes a workflow id cannot honour.

// concurrencyWorkflowIDPrefix keeps a concurrency-addressed run's id in its own
// namespace, distinct from [EntityWorkflowID]'s `flowstate-entity-` and from the
// webhook receiver's `flowstate-webhook-`.
//
// Distinct prefixes are what make "a key can never address, join or block a run
// created by some other addressing scheme" a fact about the strings rather than a
// hope about the values: no digest and no entity key can produce a string carrying
// another scheme's prefix, because the prefix is prepended after the derivation.
const concurrencyWorkflowIDPrefix = "flowstate-lock-"

// MaxConcurrencyKeyLen bounds the resolved key.
//
// A bound rather than "the digest absorbs anything", which is true of the *id* and
// beside the point: the key is a value an outside party chooses (a hand-built
// specification reaches `Run` directly, and an interpolated key resolves from
// caller-submitted inputs), and every such value in this repository carries an
// explicit bound. The resource this one bounds is the memory the resolution holds,
// not the id's alphabet.
//
// 1024 rather than something tighter because a legitimate key is a composed
// business identifier — a tenant, a region and a cluster joined together — and
// refusing one for length would send an author to hash it themselves, which is
// what this already does for them.
const MaxConcurrencyKeyLen = 1024

// ConcurrencyWorkflowID composes the workflow id that *is* the permit for one key.
//
// namespace must come from the authenticated caller's identity — never from a
// request field — the same rule [EntityWorkflowID] and the webhook receiver's
// `webhookWorkflowID` already state, and for the same reason: a workload able to
// name the tenant its id is composed under is a workload able to block, join or
// terminate another tenant's run. That is the negative direction
// `TestConcurrencyKeysDoNotCrossTenants` asserts.
//
// Hashed rather than interpolated, exactly as `webhookWorkflowID` is, and the two
// arguments are the same ones. A key is an author's expression over caller-chosen
// inputs, so it can contain anything at all — characters Temporal refuses, a length
// past its limit, or a value crafted to collide with the id somebody else's run is
// addressed by. A digest is fixed-length and alphabet-safe, and the tenant and the
// workflow name are *inside* it, so the same key in two tenants, or on two
// workflows, is two permits. The NUL separators are what stop
// `("a", "bc")` and `("ab", "c")` digesting alike.
//
// It also keeps the key out of the id, which matters because the id is durable and
// broadly readable (invariant 8) while a key is frequently a customer's name.
func ConcurrencyWorkflowID(namespace, workflow, key string) string {
	digest := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s", namespace, workflow, key))

	return concurrencyWorkflowIDPrefix + hex.EncodeToString(digest[:])
}

// ResolveConcurrencyKey evaluates a workflow's `concurrency:` key against the
// run's bound inputs, returning the literal the permit is composed from.
//
// Called once, by `FlowstateServer.Run`, after [BindRunInputs] and before the run
// exists — the resolve-once-at-submit discipline [SignalPolicyRule.subject_from]
// established. inputs must already be [BindRunInputs]'s output.
//
// The difference from that discipline, stated because the shape is deliberately
// not identical: a signal policy has a field to clear, because its resolved value
// has to survive to an enforcement path that runs on every later delivery. This
// one has no such path. The literal is used here, immediately, to compose an id,
// and is then discarded — so there is nothing carried forward that could still
// hold an expression, and nothing downstream that could evaluate one. That is the
// same guarantee `CheckSignalPolicyShape` has to enforce at run time, obtained
// instead by there being no run-time reader at all.
//
// Returns "" with no error when the workflow declares no `concurrency:` block,
// which the caller reads as "compose an ordinary id".
func ResolveConcurrencyKey(ctx context.Context, wf *Workflow, inputs map[string]*Value) (string, error) {
	concurrency := wf.GetConcurrency()
	if concurrency == nil {
		return "", nil
	}

	value := concurrency.GetKey()
	if value == nil {
		// Refused rather than treated as "no key": protovalidate marks the field
		// required, so a specification reaching here without one has bypassed
		// validation, and inventing an empty key for it would compose a permit
		// every keyless run of this workflow shares (invariant 6).
		return "", fmt.Errorf("concurrency.key is unset")
	}

	key, err := concurrencyKeyLiteral(ctx, value, &Scope{Profile: wf.GetProfile(), Inputs: inputs})
	if err != nil {
		return "", fmt.Errorf("concurrency.key: %w", err)
	}

	return key, nil
}

// concurrencyKeyLiteral reads a key as its literal string, evaluating it first if
// it was written as an expression.
//
// Through [DefaultEvaluator], so the resolution is bounded by [DefaultCostLimit]
// exactly as every other CEL evaluation here is; there is no second, unbounded
// path. The activation is inputs only — no step outputs, no vars, no run identity
// — which is the whole of what exists at submit, and is why the validator refuses
// a key that reads anything else.
func concurrencyKeyLiteral(ctx context.Context, value *Value, scope *Scope) (string, error) {
	var key string

	switch kind := value.GetKind().(type) {
	case *Value_Literal:
		s, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok {
			return "", fmt.Errorf("must be a string, got %T", kind.Literal.GetKind())
		}
		key = s.StringValue
	case *Value_Expr:
		out, err := DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, scope.Activation(ctx))
		if err != nil {
			return "", fmt.Errorf("evaluating: %w", err)
		}

		s, ok := out.Value().(string)
		if !ok {
			// A string rather than "whatever this is, stringified", because a key
			// is compared for equality against other runs' keys and a stringified
			// number, list or map is a spelling this code would be choosing on the
			// author's behalf — two spellings of one value are two permits.
			return "", fmt.Errorf("must evaluate to a string, got %s", out.Type())
		}
		key = s
	default:
		return "", fmt.Errorf("must be a string or an expression, got %T", value.GetKind())
	}

	if key == "" {
		return "", fmt.Errorf("resolved to the empty string, which names no resource")
	}

	if len(key) > MaxConcurrencyKeyLen {
		return "", fmt.Errorf("resolved to %d bytes, past the %d byte limit", len(key), MaxConcurrencyKeyLen)
	}

	return key, nil
}

// ConcurrencyOnConflictName is how an arm is written in a Flowfile, which is how a
// message about one should name it.
//
// Derived from the enum by stripping the prefix and lowering, exactly as
// [OverlapName] is, so an arm added to the schema is spelled here without anybody
// editing a list.
func ConcurrencyOnConflictName(arm Concurrency_OnConflict) string {
	if arm == Concurrency_ON_CONFLICT_UNSPECIFIED {
		return "unset"
	}

	return strings.ToLower(strings.TrimPrefix(arm.String(), "ON_CONFLICT_"))
}

// ConcurrencyOnConflictNames returns every arm an author may write, in schema
// order, for a diagnostic offering the alternatives.
func ConcurrencyOnConflictNames() []string {
	values := Concurrency_OnConflict(0).Descriptor().Values()

	names := make([]string, 0, values.Len())
	for i := range values.Len() {
		arm := Concurrency_OnConflict(values.Get(i).Number())
		if arm == Concurrency_ON_CONFLICT_UNSPECIFIED {
			continue
		}
		names = append(names, ConcurrencyOnConflictName(arm))
	}

	return names
}

// ParseConcurrencyOnConflict reads an arm as a Flowfile spells it.
func ParseConcurrencyOnConflict(name string) (Concurrency_OnConflict, bool) {
	value, ok := Concurrency_OnConflict_value["ON_CONFLICT_"+strings.ToUpper(name)]
	if !ok || Concurrency_OnConflict(value) == Concurrency_ON_CONFLICT_UNSPECIFIED {
		return Concurrency_ON_CONFLICT_UNSPECIFIED, false
	}

	return Concurrency_OnConflict(value), true
}

// CheckConcurrency reports what is wrong with a workflow's `concurrency:` block
// beyond what protovalidate's per-field rules already catch: the two trigger
// blocks whose own addressing already occupies a run's workflow id.
//
// Both are refusals rather than a precedence rule, on invariant 6's terms. A
// webhook-started run's id *is* its delivery id — that is what makes a redelivery
// join the first run instead of starting a second — and a concurrency key taking
// that address over would silently weaken dedupe to "one run per key" for an
// event stream whose whole contract is "one run per event". A schedule's fired
// execution gets an id Temporal composes with the firing time, which no caller
// can override, so a `concurrency:` block on a scheduled workflow would be a
// block that reads as enforced and enforces nothing. Refusing beats picking a
// winner, and refusing beats accepting a word that does not hold.
//
// Called by `flow validate` (through the flowfile validator, which can put a line
// and column on it) and by the server before a run or a schedule is created, so a
// hand-built specification that never went through the compiler is refused too.
func CheckConcurrency(wf *Workflow) error {
	if wf.GetConcurrency() == nil {
		return nil
	}

	if len(wf.GetTriggers().GetWebhooks()) > 0 {
		return fmt.Errorf(
			"concurrency cannot be combined with a webhook trigger: a delivery already addresses its " +
				"run by the delivery's own idempotency key, which is what makes a redelivery join the " +
				"first run rather than start a second, and a concurrency key would take that address " +
				"over. Drop one of the two")
	}

	if wf.GetTriggers().GetSchedule() != nil {
		return fmt.Errorf(
			"concurrency cannot be combined with a schedule trigger: a schedule's firings are " +
				"addressed by ids Temporal composes with the firing time, which no caller can " +
				"override, so a concurrency key would never hold between them. Exclusion between " +
				"firings is `triggers.schedule.overlap:`, which is also where queueing lives")
	}

	return nil
}
