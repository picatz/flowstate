package flowstatev1

import (
	"slices"
	"strings"
	"time"

	"github.com/google/cel-go/common/types/ref"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
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
// reads: `issuer`, `subject`, `namespace`, `kind`, `principal`, `claims` (any
// JSON shape) and `actions`, the keys of the [principal.Caller] every policy
// surface binds. Both `run.identity` ([runRootValue]) and a wait's
// `sender.identity` ([signalSenderValue]) are built from it, so the two shapes
// cannot drift from each other or from the typed `identity`; the sender drops
// `claims` and adds `deployment`.
//
// Claims are the run starter's own, which `run.identity.claims` already carries
// to its author; a wait's sender is a third party and does not get them (see
// [signalSenderValue]).
//
// A nil identity renders every string empty and claims and actions empty.
func IdentityShape(identity *WorkloadIdentity) map[string]any {
	return CallerOf(identity).Map()
}

// CallerOf renders a [WorkloadIdentity] as the one [principal.Caller] a policy
// rule reads as `identity`: the egress, exec, task-shape, secret and assumption
// surfaces all bind this value, so they cannot disagree about who is calling. A
// nil identity renders the zero Caller ("no attested caller"), which a rule
// scoped to a tenant, kind, or action declines to match.
func CallerOf(identity *WorkloadIdentity) principal.Caller {
	return AuthIdentity(identity).Caller()
}

// AuthIdentity reads a wire [WorkloadIdentity] as the [auth.WorkloadIdentity]
// the rest of the engine acts on. It is the one place the wire shape is read, so
// the Caller a rule sees, the identity a credential is minted for and the shape
// an expression reads cannot disagree. A nil identity is the zero identity,
// which [auth.WorkloadIdentity.Validate] rejects, so an unset identity cannot
// silently become a usable one.
//
// Claims are copied; a later change to the run state cannot change what an
// assertion will say.
func AuthIdentity(identity *WorkloadIdentity) auth.WorkloadIdentity {
	if identity == nil {
		return auth.WorkloadIdentity{}
	}

	who := identity.GetPrincipal()

	return auth.WorkloadIdentity{
		Subject:     who.GetSubject(),
		Issuer:      who.GetIssuer(),
		IssuerEntry: who.GetIssuerEntry(),
		Namespace:   who.GetNamespace(),
		Kind:        PrincipalKindName(who.GetKind()),
		Actions:     slices.Clone(who.GetActions()),
		Actors:      actorsOf(who.GetActors()),
		Deployment:  identity.GetDeployment(),
	}.WithWireClaims(who.GetClaims())
}

// ProtoPrincipal renders the caller half of an [auth.WorkloadIdentity] as the
// wire [Principal], the inverse of what [AuthIdentity] reads. Nil when the
// identity names no caller at all.
func ProtoPrincipal(identity auth.WorkloadIdentity) *Principal {
	if identity.Subject == "" && identity.Issuer == "" && identity.IssuerEntry == "" && identity.Namespace == "" &&
		identity.Kind == "" && len(identity.Claims) == 0 && len(identity.Actions) == 0 && len(identity.Actors) == 0 {
		return nil
	}

	return &Principal{
		Issuer:      identity.Issuer,
		Subject:     identity.Subject,
		Namespace:   identity.Namespace,
		Kind:        PrincipalKindNamed(identity.Kind),
		IssuerEntry: identity.IssuerEntry,
		Claims:      auth.ClaimsToStruct(identity.Claims),
		Actions:     slices.Clone(identity.Actions),
		Actors:      ProtoActors(identity.Actors),
	}
}

// ProtoActors renders an `act` chain as the wire [Actor] list, current actor
// first, and nil for an empty one.
func ProtoActors(actors []principal.Actor) []*Actor {
	if len(actors) == 0 {
		return nil
	}

	out := make([]*Actor, len(actors))
	for i, actor := range actors {
		out[i] = &Actor{Issuer: actor.Issuer, Subject: actor.Subject}
	}

	return out
}

// actorsOf reads the wire [Actor] list as the chain a rule and a credential see.
// A nil entry reads as an actor with no name, which [auth.WorkloadIdentity.Validate]
// refuses, so a malformed list cannot become a usable chain.
func actorsOf(wire []*Actor) []principal.Actor {
	if len(wire) == 0 {
		return nil
	}

	out := make([]principal.Actor, len(wire))
	for i, actor := range wire {
		out[i] = principal.Actor{Issuer: actor.GetIssuer(), Subject: actor.GetSubject()}
	}

	return out
}

// StringClaimValues is the wire form of an all-string claim set, which is what a
// test file's `sender:` and a command-line identity name: each value becomes a
// string [structpb.Value] on a [Principal].
func StringClaimValues(claims map[string]string) map[string]*structpb.Value {
	if len(claims) == 0 {
		return nil
	}

	out := make(map[string]*structpb.Value, len(claims))
	for name, value := range claims {
		out[name] = structpb.NewStringValue(value)
	}

	return out
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

// RespellPrincipalKind rewrites the `kind` of a decoded `principal:` mapping from
// the name a trust policy and every other surface use (`human`) to the schema's
// enum name, so a file read into a [Principal] spells it the one way. Only the
// exact lowercase names are rewritten ([PrincipalKindNamed]); anything else is
// left for the schema to refuse. A value that is not a mapping is left alone.
func RespellPrincipalKind(principal *structpb.Value) {
	fields := principal.GetStructValue().GetFields()

	name, ok := fields["kind"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return
	}

	if kind := PrincipalKindNamed(name.StringValue); kind != PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED {
		fields["kind"] = structpb.NewStringValue(kind.String())
	}
}
