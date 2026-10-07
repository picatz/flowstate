package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"google.golang.org/protobuf/types/known/structpb"
)

// Cases for what a policy rule can read of the caller beyond strings: its kind
// and claims of any JSON shape, through the one [principal.Caller] every
// surface renders. A rule that read only strings would pass with the list and
// the nested object silently dropped, so each rule here reads one of each, and
// a driver that lost the kind, the list or the object on its route to the
// policy disagrees with the rule that needs it.
//
// The allow:, egress and exec surfaces carry the same identity through
// [EgressIdentityCases], [ExecCases] and [TaskPolicyCases]; this file holds the
// secret and assumption surfaces, which need a fixture [Authority] rather than
// a policy on the task.

// carrierRule reads the caller's kind, a list claim and a nested object claim.
const carrierRule = `identity.kind == "agent" && "sre" in identity.claims.groups && identity.claims.slack.user == "U1"`

// carrierIdentity is a caller whose claims are a list and a nested object,
// holding the given groups.
func carrierIdentity(kind string, groups ...any) auth.WorkloadIdentity {
	return auth.WorkloadIdentity{
		Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "acme-tenant",
		Kind: kind,
		Claims: map[string]any{
			"groups": groups,
			"slack":  map[string]any{"user": "U1"},
		},
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

	denied := func(identity auth.WorkloadIdentity, name string) AuthorityCase {
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
						`in namespace "acme-tenant" to read fixture-secret:API_TOKEN (allow rules: no allow rule matched)`}),
				}},
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: "must-not-resolve",
				Allow: []string{carrierRule}, Identity: identity,
			},
		}
	}

	return []AuthorityCase{
		denied(carrierIdentity("agent", "dev"), "a secret allow rule refuses a list claim without the group"),
		denied(carrierIdentity("human", "sre"), "a secret allow rule refuses another kind"),
	}
}

// carrierWorkloadIdentity is the wire form of the same caller: a principal of
// the given kind in team-a whose claims are a list of groups and a nested
// object, for the surfaces whose cases carry a [v1.WorkloadIdentity].
func carrierWorkloadIdentity(kind v1.PrincipalKind, groups ...string) *v1.WorkloadIdentity {
	list := make([]any, len(groups))
	for i, g := range groups {
		list[i] = g
	}
	claims, err := structpb.NewStruct(map[string]any{
		"groups": list,
		"slack":  map[string]any{"user": "U1"},
	})
	if err != nil {
		panic(err)
	}
	return &v1.WorkloadIdentity{Principal: &v1.Principal{
		Subject: "spiffe://acme/agent", Issuer: "https://issuer.example.com", Namespace: "team-a",
		Kind: kind, Claims: claims.GetFields(),
	}}
}
