package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `allow: ${...}` — the question [RehearsalSignalCases] asks, in the cases only a
// predicate can express and the fail-closed arms. Both enforcement points reach
// [v1.SignalPolicyCheck], which routes a policy to one evaluator; these cases
// are how "both drivers answer a predicate the same way" stays a checked fact.

// issuerA and issuerB mint the same local part, which is the multi-IdP fault
// `principal` exists to make one comparison.
const (
	issuerA = "https://idp-a.example.com"
	issuerB = "https://idp-b.example.com"
)

func predicate(expression string) *v1.SignalPolicy {
	return &v1.SignalPolicy{Allow: expression}
}

// signalPredicateCases are the cases only a predicate can express, and the
// fail-closed arms: every refusal here is one where the delivery would have
// been admitted had the predicate been evaluated leniently.
func signalPredicateCases() []RehearsalSignalCase {
	starter := &v1.WorkloadIdentity{
		Subject: "release-bot@example.com",
		Issuer:  "https://issuer.example.com",
		Claims:  map[string]string{"team": "release-managers"},
	}
	alice := func(issuer string) *v1.WorkloadIdentity {
		return &v1.WorkloadIdentity{Subject: "alice@example.com", Issuer: issuer}
	}
	byPrincipal := predicate(`sender.identity.principal == "` + issuerA + `#alice@example.com"`)

	usingInputs := predicate(
		`sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver` +
			` && sender.identity.claims["team"] == "release-managers"`)
	expectedApprover := map[string]*v1.Value{"expected_approver": v1.NewLiteral("sre-lead@example.com")}

	items := func(n int) map[string]*v1.Value {
		values := make([]any, n)
		for i := range values {
			values[i] = i
		}

		return map[string]*v1.Value{"items": v1.NewLiteralList(values...)}
	}
	pairwise := predicate(`inputs.items.all(a, inputs.items.all(b, a >= 0)) && sender.identity.claims["team"] == "release-managers"`)

	return []RehearsalSignalCase{
		{
			Name: "a principal from the issuer a predicate names", SignalName: "deploy-approved",
			Policy: byPrincipal, Starter: starter, Sender: alice(issuerA), Admitted: true,
			Why: "the principal is issuer#subject, so the one comparison names both halves",
		},
		{
			Name: "the same subject from another issuer", SignalName: "deploy-approved",
			Policy: byPrincipal, Starter: starter, Sender: alice(issuerB),
			Why: "two identity providers minting the same subject are two principals; a predicate " +
				"comparing subject alone would admit this one, and principal is what stops it",
		},
		{
			Name: "a claim the predicate compares", SignalName: "deploy-approved",
			Policy:  predicate(`sender.identity.claims["team"] == "release-managers"`),
			Starter: starter, Sender: approver(), Admitted: true,
			Why: "claims are bound server-side from the verified identity and are readable here, " +
				"though never in a wait's own `sender`",
		},
		{
			Name: "a sender lacking the claim the predicate reads", SignalName: "deploy-approved",
			Policy:  predicate(`sender.identity.claims["team"] == "release-managers"`),
			Starter: starter, Sender: alice(issuerA),
			Why: "a missing claim key is an evaluation error, and an error denies on both drivers; " +
				"it is never read as false-then-allow or as true",
		},
		{
			Name: "a predicate that reads the starter, with none recorded", SignalName: "deploy-approved",
			Policy:  predicate(`sender.identity.principal != run.identity.principal`),
			Starter: starter, StarterUnknown: true, Sender: approver(),
			Why: "an unknown starter leaves run unbound, so reading it errors and denies, which is " +
				"nothing a predicate could compare against",
		},
		{
			Name: "a predicate that never mentions the starter, with none recorded", SignalName: "deploy-approved",
			Policy:  byPrincipal,
			Starter: starter, StarterUnknown: true, Sender: alice(issuerA), Admitted: true,
			Why: "only a predicate that reads the starter is affected by not knowing it",
		},
		{
			Name: "a predicate comparing the sender's claim with the starter's", SignalName: "deploy-approved",
			Policy:  predicate(`sender.identity.claims["team"] == run.identity.claims["team"]`),
			Starter: starter, Sender: approver(), Admitted: true,
			Why: "run.identity carries the starter's own claims on both drivers, not only its subject",
		},
		{
			Name: "a predicate comparing the sender's claim with another team's starter", SignalName: "deploy-approved",
			Policy: predicate(`sender.identity.claims["team"] == run.identity.claims["team"]`),
			Starter: &v1.WorkloadIdentity{
				Subject: "release-bot@example.com", Issuer: "https://issuer.example.com",
				Claims: map[string]string{"team": "someone-else"},
			},
			Sender: approver(),
			Why:    "the negative direction of the case above",
		},
		{
			Name: "an input-derived predicate narrowed by a claim, for the named approver", SignalName: "deploy-approved",
			Policy: usingInputs, Starter: starter, Inputs: expectedApprover, Sender: approver(), Admitted: true,
			Why: "inputs are the run's bound arguments, recorded beside the policy at submit",
		},
		{
			Name: "an input-derived predicate, for another approver", SignalName: "deploy-approved",
			Policy: usingInputs, Starter: starter,
			Inputs: map[string]*v1.Value{"expected_approver": v1.NewLiteral("someone-else@example.com")},
			Sender: approver(),
			Why:    "the inputs decide who the predicate names",
		},
		{
			Name: "an input-derived predicate on a run that carries no inputs", SignalName: "deploy-approved",
			Policy: usingInputs, Starter: starter, Sender: approver(),
			Why: "inputs missing at delivery make the predicate error, and an error denies; the " +
				"delivery is never evaluated over a copy that silently lost them",
		},
		{
			Name: "a predicate reading only inputs", SignalName: "deploy-approved",
			Policy:  predicate(`sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver`),
			Starter: starter, Inputs: expectedApprover, Sender: approver(),
			Why: "the starter chose the inputs, so this lets them name their own approver; the " +
				"narrowing rule refuses it at validation, and a policy that reached delivery anyway " +
				"is refused by the same compile",
		},
		{
			Name: "a predicate that is not a bool", SignalName: "deploy-approved",
			Policy: predicate(`sender.identity.principal`), Starter: starter, Sender: approver(),
			Why: "only a bool can decide; a string is a refusal, never truthy",
		},
		{
			Name: "a predicate that does not parse", SignalName: "deploy-approved",
			Policy: predicate(`sender.identity.principal ==`), Starter: starter, Sender: approver(),
			Why: "a policy that cannot be compiled authorizes nobody",
		},
		{
			Name: "a predicate naming a root outside the closed scope", SignalName: "deploy-approved",
			Policy: predicate(`steps.build.ok == true`), Starter: starter, Sender: approver(),
			Why: "the scope is sender, run.identity and inputs and nothing else",
		},
		{
			Name: "a predicate within its cost bound", SignalName: "deploy-approved",
			Policy: pairwise, Starter: starter, Inputs: items(10), Sender: approver(), Admitted: true,
			Why: "the same predicate as the case below over a list small enough to afford it, so " +
				"the refusal below is about the bound",
		},
		{
			Name: "a predicate over its cost bound", SignalName: "deploy-approved",
			Policy: pairwise, Starter: starter, Inputs: items(400), Sender: approver(),
			Why: "evaluation is bounded where it is spent: an input the starter chose multiplies " +
				"the predicate's work, and exceeding the budget denies",
		},
		{
			Name: "a policy with no predicate", SignalName: "deploy-approved",
			Policy: &v1.SignalPolicy{}, Starter: starter, Sender: approver(),
			Why: "what a run frozen by a release that still recorded the retired rule list decodes " +
				"to; it authorizes nobody, and is never read as an absent policy",
		},
		{
			Name: "a predicate comparing with the starter, sender is the starter", SignalName: "deploy-approved",
			Policy:  predicate(`sender.identity.claims["team"] == "release-managers" && sender.identity.principal != run.identity.principal`),
			Starter: approver(), Sender: approver(),
			Why: "separation of duties is a comparison inside the one predicate",
		},
	}
}
