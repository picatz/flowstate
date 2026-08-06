package flowstatev1

import (
	"maps"
	"slices"

	"github.com/google/cel-go/common/types/ref"
)

// runRootValue renders a run's starter identity as the map an expression reads
// under [RunRoot]: `run.identity.subject`, `run.identity.issuer`,
// `run.identity.namespace`, `run.identity.claims`, and `run.local`.
//
// Deliberately narrower than [WorkloadIdentity] itself — see [Scope.identity]'s
// doc for why `deployment` is left off — and deliberately shaped like
// [signalSenderValue]'s `sender` for the same reason both exist at all: they are
// the two places this engine hands a caller's own attestation to an expression,
// and one shape read the same way in both keeps an author from having to learn
// two renderings of one fact.
//
// identity nil renders with every field empty, which is correct for a run that
// predates this field and for a run the local driver built; local is what tells
// those two apart from a run the server genuinely attested with an anonymous
// identity (empty subject, local false) — never let the two be confused, which
// is the one rule [signalSenderValue]'s own doc states and this restates because
// nothing enforces it structurally.
func runRootValue(identity *WorkloadIdentity, local bool) ref.Val {
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
		"local": local,
	})
}
