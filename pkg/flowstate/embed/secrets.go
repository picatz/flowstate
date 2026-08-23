package embed

import (
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Secrets is what an embedding program configures to let a run resolve
// ${secret(...)} references and authorize JIT credential exchange — the
// worker-side authority [v1.TaskRuntime] bundles, minus the one field
// [RunLocal] fills in itself ([auth.StepRef], which names the run and step
// currently executing and so cannot be supplied ahead of time).
//
// # The zero value, and why RunOptions leaves it nil by default
//
// [RunOptions.Secrets] is nil unless an embedder sets it, and a nil Secrets
// resolves nothing: [RunLocal] installs no worker-side authority on the
// context at all, which is exactly the "not configured" case
// [v1.ResolveSecret] and [v1.AuthorizeCredential] already refuse — the same
// posture `flow run local` gets with none of --secret-*, --auth-policy or
// --identity-broker-config on its command line (see cmd/flow/secrets.go's
// withLocalTaskRuntime). A reference in a workflow this build has no Secrets
// for is therefore always a refusal, not a resolution against whatever the
// process happens to have lying around.
//
// A non-nil *Secrets with a nil Store still resolves nothing — Store is what
// [v1.ResolveSecret] checks first — and a non-nil *Secrets with a nil Policy
// denies every reference regardless of Store, matching [auth.SecretPolicy]'s
// own zero value: "no rules configured" permits nothing. Configuring Secrets
// at all is therefore never accidentally permissive; an embedder has to name
// an actual allow rule to let anything through.
type Secrets struct {
	// Store resolves a reference's scheme to the provider that backs it. Nil
	// resolves none.
	Store *secrets.Store

	// Policy decides which workload may read which secret, evaluated before
	// Store — see [auth.SecretPolicy.Authorize]. Nil denies every reference,
	// the same as an [auth.SecretAccessPolicy] compiled with no rules.
	Policy *auth.SecretPolicy

	// Broker mints short-lived credentials for a `credential:` input's JIT
	// federation target. Nil refuses every credential target; a Secrets set
	// can configure Store and Policy for static secrets without ever setting
	// this, which is itself a supported posture — see
	// pkg/flowstate/v1/internal/conformance/authority.go's "secrets are configured,
	// federation is not" case.
	Broker *auth.Broker

	// Identity is the workload identity a run presents when resolving a
	// secret or authorizing a credential exchange — [RunLocal] validates it
	// with [auth.WorkloadIdentity.Validate] before installing it, so a
	// Secrets set with no Subject or Issuer fails the run immediately rather
	// than failing every reference in it one at a time.
	Identity auth.WorkloadIdentity
}
