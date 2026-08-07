package flowstatev1

import (
	"maps"
	"slices"

	"github.com/google/cel-go/common/types/ref"
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

// NewLocalRunAddress returns the address every local run reports.
//
// A constructor rather than each caller writing the pair, so "what a local run
// answers" has one definition to compare against the durable driver's — the same
// reason engine.varsScope exists.
func NewLocalRunAddress() *RunAddress {
	return &RunAddress{WorkflowId: LocalRunAddress, RunId: LocalRunAddress}
}

// runRootValue renders a run's own address and starter identity as the map an
// expression reads under [RunRoot]: `run.workflow_id`, `run.run_id`,
// `run.identity.subject`, `run.identity.issuer`, `run.identity.namespace`,
// `run.identity.claims`, and `run.local`.
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
// predates the field: every driver fills it now, and [NewLocalRunAddress] is why
// the local one has something honest to fill it with. It is rendered rather than
// omitted so that a reference to it resolves — the same rule [InputsRoot] follows
// for an empty root, and for the same reason: a missing key describes the
// author's mistake, an unresolved reference sends them looking for a root that is
// always there.
//
// The two fields under `run` that a reader may expect and will not find are a
// start time and an attempt count. [RunAddress] records why neither is here;
// the short version is that a start time is a clock read by another name, and
// `now` is bound only inside a wait precisely so a task cannot read a clock.
func runRootValue(identity *WorkloadIdentity, local bool, address *RunAddress) ref.Val {
	claims := make(map[string]any, len(identity.GetClaims()))
	for _, k := range slices.Sorted(maps.Keys(identity.GetClaims())) {
		claims[k] = identity.GetClaims()[k]
	}

	return TypeAdapter.NativeToValue(map[string]any{
		"identity": map[string]any{
			"subject":   identity.GetSubject(),
			"issuer":    identity.GetIssuer(),
			"namespace": identity.GetNamespace(),
			"claims":    claims,
		},
		"local":       local,
		"workflow_id": address.GetWorkflowId(),
		"run_id":      address.GetRunId(),
	})
}
