package flowstatev1

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/cel-go/common/types/ref"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// LocalRunAddress is what a run started by the local driver answers for both
// `run.workflow_id` and `run.run_id`.
//
// A documented sentinel rather than an empty string, and rather than a synthetic
// unique id, because either of those would be a lie in one direction or the
// other. An empty string reads as "this run has no id", which sends an author
// looking for the field that failed to populate; a generated id reads as an
// address, and a local run *has no address* — there is no server in front of it
// and no Temporal behind it, so nothing can reach it by any name at all.
//
// This is the same honest answer [LocalSignalSender] gives for a wait's `sender`
// and the local driver gives for `run.local`: state outright that this is a
// rehearsal rather than leaving an author to infer it from a blank. A file that
// builds a callback URL out of `${run.workflow_id}` therefore produces
// `.../local` under `flow run local` — visibly a rehearsal, and stable, so
// `flow test` can assert on it.
const LocalRunAddress = "local"

// NewLocalRunAddress returns the address every local run reports, with no start
// recorded, which `run.started_at` renders as the Unix epoch. A caller that knows
// when its run began uses [NewLocalRunAddressAt].
//
// A constructor rather than each caller writing the pair, so "what a local run
// answers" has one definition to compare against the durable driver's — the same
// reason engine.varsScope exists.
func NewLocalRunAddress() *RunAddress {
	return &RunAddress{WorkflowId: LocalRunAddress, RunId: LocalRunAddress}
}

// NewLocalRunAddressAt returns the local run address started at the given
// instant: the run's clock at the moment it began ([ClockFromContext]), which is
// the wall clock for `flow run local` and the case's own virtual start for `flow
// test`, so a window computed from `run.started_at` is exercisable with a fixed
// one.
func NewLocalRunAddressAt(started time.Time) *RunAddress {
	address := NewLocalRunAddress()
	address.StartedAt = timestamppb.New(started)

	return address
}

// runRootValue renders a run's own address and starter identity as the map an
// expression reads under [RunRoot]: `run.workflow_id`, `run.run_id`,
// `run.identity.subject`, `run.identity.issuer`, `run.identity.namespace`,
// `run.identity.claims`, `run.identity.principal`, `run.identity.kind`, `run.local`, and `run.started_at`.
//
// The identity half is deliberately narrower than [WorkloadIdentity] itself —
// see [Scope.identity]'s doc for why `deployment` is left off — and deliberately
// shaped like [signalSenderValue]'s `sender` for the same reason both exist at
// all: they are the two places this engine hands a caller's own attestation to
// an expression, and one shape read the same way in both keeps an author from
// having to learn two renderings of one fact.
//
// identity nil renders with every field empty, which is correct for a run that
// predates this field and for a run the local driver built; local is what tells
// those two apart from a run the server genuinely attested with an anonymous
// identity (empty subject, local false) — never let the two be confused, which
// is the one rule [signalSenderValue]'s own doc states and this restates because
// nothing enforces it structurally.
//
// address nil renders both id fields empty, which is correct only for a run that
// predates the field: every driver fills it now, and [NewLocalRunAddressAt] is why
// the local one has something honest to fill it with. It is rendered rather than
// omitted so that a reference to it resolves — the same rule [InputsRoot] follows
// for an empty root, and for the same reason: a missing key describes the
// author's mistake, an unresolved reference sends them looking for a root that is
// always there.
//
// `started_at` is a timestamp, and renders as the Unix epoch where the run
// recorded none (a run that predates the field): a typed value that is plainly
// not a start, rather than a missing key that would fail an expression the
// checker had accepted. It is when the workload began, fixed for its life and
// identical on every replay, and so not a clock read; `now` stays bound only
// inside a wait. The one field a reader may expect and will not find is an
// attempt count, and [RunAddress] records why.
func runRootValue(identity *WorkloadIdentity, local bool, address *RunAddress) ref.Val {
	return TypeAdapter.NativeToValue(map[string]any{
		"identity":    IdentityShape(identity),
		"local":       local,
		"workflow_id": address.GetWorkflowId(),
		"run_id":      address.GetRunId(),
		"started_at":  address.GetStartedAt().AsTime(),
	})
}

// IdentityShape is the one rendering of a [WorkloadIdentity] an expression
// reads: `subject`, `issuer`, `namespace`, `claims` (a map, sorted by key),
// `principal`, and `kind`. Both `run.identity` ([runRootValue]) and a wait's
// `sender.identity` ([signalSenderValue]) are built from it, so the two shapes
// cannot drift; the sender drops `claims` and adds `deployment`.
//
// Claims are the run starter's own, which `run.identity.claims` already carries
// to its author; a wait's sender is a third party and does not get them (see
// [signalSenderValue]).
//
// A nil identity renders every string empty and claims empty.
func IdentityShape(identity *WorkloadIdentity) map[string]any {
	claims := make(map[string]any, len(identity.GetClaims()))
	for _, k := range slices.Sorted(maps.Keys(identity.GetClaims())) {
		claims[k] = identity.GetClaims()[k]
	}

	return map[string]any{
		"subject":   identity.GetSubject(),
		"issuer":    identity.GetIssuer(),
		"namespace": identity.GetNamespace(),
		"claims":    claims,
		"principal": Principal(identity.GetIssuer(), identity.GetSubject()),
		"kind":      PrincipalKindName(identity.GetPrincipalKind()),
	}
}

// CallerOf renders a [WorkloadIdentity] as the one [principal.Caller] a policy
// rule reads as `identity`: the egress, exec, and task-shape surfaces all bind
// this value, so they cannot disagree about who is calling. A nil identity
// renders the zero Caller ("no attested caller"), which a rule scoped to a
// tenant, kind, or action declines to match.
//
// The identity carries no granted actions yet, so Actions renders empty; the
// field is declared so a rule naming it is valid today and starts matching when
// the identity carries them.
func CallerOf(identity *WorkloadIdentity) principal.Caller {
	return principal.Caller{
		Issuer:    identity.GetIssuer(),
		Subject:   identity.GetSubject(),
		Namespace: identity.GetNamespace(),
		Kind:      PrincipalKindName(identity.GetPrincipalKind()),
		Principal: Principal(identity.GetIssuer(), identity.GetSubject()),
		Claims:    identity.GetClaims(),
	}.Normalized()
}

// PrincipalKindName is the lowercase name an expression and a trust policy
// spell a [PrincipalKind] with ("human", "workload", "agent"), and "" for
// UNSPECIFIED or a value this build does not know. Empty is the honest answer
// to "the policy assigned none": a predicate must compare against a named kind
// and cannot mistake the absence for WORKLOAD.
func PrincipalKindName(kind PrincipalKind) string {
	if kind == PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED {
		return ""
	}
	name, ok := strings.CutPrefix(kind.String(), "PRINCIPAL_KIND_")
	if !ok {
		return ""
	}

	return strings.ToLower(name)
}

// PrincipalKindNamed is the inverse of [PrincipalKindName]: the kind a trust
// policy's `principal_kind:` names, and UNSPECIFIED for "" or any other string.
func PrincipalKindNamed(name string) PrincipalKind {
	kind := PrincipalKind(PrincipalKind_value["PRINCIPAL_KIND_"+strings.ToUpper(name)])

	// Exact spelling only, as a trust policy takes it: "Human" is not "human", so a
	// test or a rehearsal cannot certify a configuration production would refuse.
	if PrincipalKindName(kind) != name {
		return PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED
	}

	return kind
}

// Principal is `issuer#subject` ([QualifiedSubject]) when both halves are
// present, and "" otherwise.
//
// The rule is decided once, here: an unauthenticated or local sender (no
// identity) and an identity missing either half give "", never a half-formed
// "#" or "issuer#". The empty value still equals itself, so a predicate that
// compares a principal must treat "" as missing (require it non-empty) rather
// than rely on the representation to keep anonymous callers apart.
//
// The join is injective because no trusted issuer contains '#' (policy
// validation refuses one in either kind), so the first '#' always ends the
// issuer and a subject may contain any further '#'.
func Principal(issuer, subject string) string {
	if issuer == "" || subject == "" {
		return ""
	}

	return QualifiedSubject(issuer, subject)
}
