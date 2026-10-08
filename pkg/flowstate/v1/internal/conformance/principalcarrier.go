package conformance

import (
	"sync/atomic"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Cases for what a policy rule can read of the caller beyond strings: its kind
// and claims of any JSON shape, through the one [principal.Caller] every
// surface renders. A rule that read only strings would pass with the list
// dropped, so each rule here reads a list and a scalar, and a driver that lost
// the kind or the list on its route to the policy disagrees with the rule that
// needs it.
//
// The claims are not written by hand: they are what [auth.MapClaims] produces
// for a Keycloak-shaped token under [carrierEntry], so the cases prove the
// claims an issuer entry carries (a group list through `groups_claim` and
// `group_map`, a nested scalar renamed by `carry_claims`) are the ones every
// surface reads, and that a claim the token held but the entry did not carry
// is absent on all of them ([carrierUncarriedRule]).
//
// The allow:, egress and exec surfaces carry the same identity through
// [EgressIdentityCases], [ExecCases] and [TaskPolicyCases]; this file holds the
// secret and assumption surfaces, which need a fixture [Authority] rather than
// a policy on the task.

// carrierRule reads the caller's kind, a list claim and a nested object claim.
const carrierRule = `identity.kind == "agent" && "sre" in identity.claims.groups && identity.claims.slack_user == "U1"`

// carrierUncarriedRule reads a claim the token held and the entry did not
// carry, so it can never match: the caller has no such claim on any surface,
// and the rule fails to evaluate, which every surface reads as a denial.
const carrierUncarriedRule = `identity.claims.team == "platform"`

// carrierEntry is the trust policy entry the cases' callers were admitted by:
// groups read from a nested claim through a map, one nested scalar renamed, and
// nothing else carried.
func carrierEntry(groups []string) auth.TrustedIssuer {
	groupMap := make(map[string]string, len(groups))
	for _, group := range groups {
		groupMap["idp-"+group] = group
	}

	return auth.TrustedIssuer{
		Name:        "carrier-idp",
		GroupsClaim: "realm_access.roles",
		GroupMap:    groupMap,
		CarryClaims: []auth.CarryClaim{{Claim: "slack.user", As: "slack_user", Type: auth.ClaimTypeString}},
	}
}

// carrierClaims is what the entry carries of a token that also holds a `team`
// and an `email` it does not.
func carrierClaims(groups ...string) map[string]any {
	roles := make([]any, len(groups))
	for i, group := range groups {
		roles[i] = "idp-" + group
	}

	claims, err := auth.MapClaims(carrierEntry(groups), map[string]any{
		"team":         "platform",
		"email":        "reader@example.com",
		"realm_access": map[string]any{"roles": append(roles, "idp-unmapped")},
		"slack":        map[string]any{"user": "U1"},
	})
	if err != nil {
		panic(err)
	}

	return claims
}

// carrierIdentity is a caller admitted by [carrierEntry], holding the given
// groups.
func carrierIdentity(kind string, groups ...string) auth.WorkloadIdentity {
	return auth.WorkloadIdentity{
		Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "acme-tenant",
		Kind:   kind,
		Claims: carrierClaims(groups...),
	}
}

// PrincipalCarrierCases returns the secret and assumption cases. baseURL should
// come from [NewHTTPServer].
func PrincipalCarrierCases(baseURL string) []AuthorityCase {
	const bearerMaterial = "material-that-must-not-appear-in-any-rendering-carrier-bearer"
	const jitMaterial = "material-that-must-not-appear-in-any-rendering-carrier-jit"

	reflect := v1.NewExpr(`{"body": response.body, "reflected": response.headers["X-Reflected"][0]}`)
	step := func(inputs map[string]*v1.Value) *v1.Node {
		inputs["url"] = v1.NewLiteral(baseURL + "/reflect-authorization")
		inputs["outputs"] = reflect
		return &v1.Node{Id: "call", Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: inputs}}}
	}
	bearer := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}}
	redacted := func(reflected string) *v1.Workflow_StepOutputs {
		return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"call": {NamedValues: map[string]*v1.Value{
				"body":      v1.NewLiteral("echo: " + secrets.Redacted),
				"reflected": v1.NewLiteral(reflected),
			}},
		}}
	}

	return []AuthorityCase{
		{
			Name: "a secret allow rule reads the caller's kind, list claim and nested claim",
			Workflow: &v1.Workflow{
				Name:  "authority-carrier-bearer",
				Steps: []*v1.Node{step(map[string]*v1.Value{"bearer": bearer})},
			},
			ExpectedOutputs: redacted("Bearer " + secrets.Redacted),
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: bearerMaterial,
				Allow: []string{carrierRule}, Identity: carrierIdentity("agent", "dev", "sre"),
			},
			ContainmentValue: bearerMaterial,
		},
		{
			Name: "an assumption allow rule reads the caller's kind, list claim and nested claim",
			Workflow: &v1.Workflow{
				Name:  "authority-carrier-jit",
				Steps: []*v1.Node{step(map[string]*v1.Value{"credential": v1.NewLiteral("partner-api")})},
			},
			ExpectedOutputs: redacted(secrets.Redacted),
			Authority: Authority{
				Identity:   carrierIdentity("agent", "dev", "sre"),
				Federation: &Federation{Target: "partner-api", Token: jitMaterial, Allow: []string{carrierRule}},
			},
			ContainmentValue: jitMaterial,
		},
	}
}

// PrincipalCarrierDenialCases are the same rule refusing a caller that differs
// in one reading: a list without the group, and a kind that is not an agent.
// Without them the allowing cases above would also pass for a rule that said
// yes to everyone.
func PrincipalCarrierDenialCases() []AuthorityCase {
	const unreachable = "https://authority-denial.invalid/never-dialed"

	const noMatch = `allow rules: no allow rule matched`
	const uncarried = `rule error: allow rule "identity.claims.team == \"platform\"" could not be evaluated: no such key: team`

	denied := func(identity auth.WorkloadIdentity, name, allow, detail string) AuthorityCase {
		return AuthorityCase{
			Case: Case{
				Name: name,
				Workflow: &v1.Workflow{
					Name:  "authority-carrier-denied",
					Steps: []*v1.Node{bearerSecretStep("read", unreachable, "fixture-secret", "API_TOKEN")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"read": v1.FailedStepOutputs(v1.StepFailure{Kind: v1.ErrorKindPolicyDenied, Text: `task "http" failed (PolicyDenied): ` +
						`resolving bearer reference fixture-secret:API_TOKEN: ` +
						`auth: denied by secret access policy: no rule permits workload ` +
						`"flowstate:acme-tenant/_default/authority-carrier-denied/read" ` +
						`in namespace "acme-tenant" to read fixture-secret:API_TOKEN (` + detail + `)`}),
				}},
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: "must-not-resolve",
				Allow: []string{allow}, Identity: identity,
			},
		}
	}

	assumptionDenied := AuthorityCase{
		Case: Case{
			Name: "an assumption allow rule refuses a claim the entry did not carry",
			Workflow: &v1.Workflow{
				Name:  "authority-carrier-jit-uncarried",
				Steps: []*v1.Node{credentialStep("read", unreachable, "partner-api")},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"read": v1.FailedStepOutputs(v1.StepFailure{Kind: v1.ErrorKindPolicyDenied, Text: `task "http" failed (PolicyDenied): ` +
					`authorizing federation target "partner-api": auth: denied by assumption policy: ` +
					`"flowstate:acme-tenant/_default/authority-carrier-jit-uncarried/read" may not assume "partner-api" ` +
					`(` + uncarried + `)`}),
			}},
		},
		Authority: Authority{
			Identity:   carrierIdentity("agent", "sre"),
			Federation: &Federation{Target: "partner-api", Token: "must-not-mint", Allow: []string{carrierUncarriedRule}, ExchangeCalls: new(atomic.Int32)},
		},
	}

	return []AuthorityCase{
		assumptionDenied,
		denied(carrierIdentity("agent", "dev"), "a secret allow rule refuses a list claim without the group", carrierRule, noMatch),
		denied(carrierIdentity("human", "sre"), "a secret allow rule refuses another kind", carrierRule, noMatch),
		// The claim is in the token and not in the entry's carry_claims, so the
		// caller has no `team` however it reads.
		denied(carrierIdentity("agent", "sre"), "a secret allow rule refuses a claim the entry did not carry", carrierUncarriedRule, uncarried),
	}
}

// actor is one link of an RFC 8693 `act` chain on the wire.
func actor(issuer, subject string) *v1.Actor {
	return &v1.Actor{Issuer: issuer, Subject: subject}
}

// delegatedWorkloadIdentity is a caller in team-a, acting for itself or, given
// actors, through them (current actor first), for the surfaces whose cases
// carry a [v1.WorkloadIdentity].
func delegatedWorkloadIdentity(actors ...*v1.Actor) *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{Principal: &v1.Principal{
		Subject: "alice", Issuer: "https://issuer.example.com", Namespace: "team-a",
		Actors: actors,
	}}
}

// refusedClaimsRule is the rule an operator writes to exclude a class of caller
// by a claim: it admits the tenant and anyone *not* carrying `contractors`.
const refusedClaimsRule = `identity.namespace == "team-a" && !("contractors" in identity.claims)`

// refusedClaimsDenyRule is the same exclusion as a deny rule.
const refusedClaimsDenyRule = `"contractors" in identity.claims`

// refusedClaimsIdentity is a team-a agent whose `contractors` claim nests past
// [auth.MaxCarriedClaimDepth], so the identity refuses it. A rule that only
// tests that claim's absence must not read the refusal as "not a contractor";
// every surface denies instead (#2426).
func refusedClaimsIdentity() *v1.WorkloadIdentity {
	deep := any("contractor")
	for range auth.MaxCarriedClaimDepth + 1 {
		deep = []any{deep}
	}

	identity := carrierWorkloadIdentity(v1.PrincipalKind_PRINCIPAL_KIND_AGENT, "dev")
	identity.Principal.Claims["contractors"] = auth.ClaimsToStruct(map[string]any{"contractors": deep})["contractors"]

	return identity
}

// carrierWorkloadIdentity is the wire form of the same caller: a principal of
// the given kind in team-a, admitted by [carrierEntry], for the surfaces whose
// cases carry a [v1.WorkloadIdentity].
func carrierWorkloadIdentity(kind v1.PrincipalKind, groups ...string) *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{Principal: &v1.Principal{
		Subject: "spiffe://acme/agent", Issuer: "https://issuer.example.com", Namespace: "team-a",
		Kind: kind, Claims: auth.ClaimsToStruct(carrierClaims(groups...)),
	}}
}
