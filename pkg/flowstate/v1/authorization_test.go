package flowstatev1

import "testing"

func TestAuthorizationTruthTable(t *testing.T) {
	grant := func(id string, layer PolicyLayer, effect RuleEffect) *PolicySet {
		return &PolicySet{Id: id, Revision: "7", PolicyVersion: "v1", Layer: layer,
			Kind: PolicyKind_POLICY_KIND_GRANT, Rules: []*PolicyRule{{Id: "rule", Effect: effect, Evaluation: RuleEvaluation_RULE_EVALUATION_MATCH}}}
	}
	boundary := func(id string, layer PolicyLayer, allow bool) *PolicySet {
		eval := RuleEvaluation_RULE_EVALUATION_NO_MATCH
		if allow {
			eval = RuleEvaluation_RULE_EVALUATION_MATCH
		}
		return &PolicySet{Id: id, Revision: "4", PolicyVersion: "v1", Layer: layer,
			Kind: PolicyKind_POLICY_KIND_BOUNDARY, Rules: []*PolicyRule{{Id: "limit", Effect: RuleEffect_RULE_EFFECT_ALLOW, Evaluation: eval}}}
	}
	request := func(required ...PolicyLayer) *AuthorizationRequest {
		return &AuthorizationRequest{PolicyVersion: "v1", Action: "run.start", ResourceType: "workflow", Resource: "deploy", RequiredLayers: required}
	}
	tests := []struct {
		name    string
		req     *AuthorizationRequest
		sets    []*PolicySet
		allowed bool
		reason  DecisionReason
	}{
		{"conflicting grants explicit deny wins", request(), []*PolicySet{grant("group", PolicyLayer_POLICY_LAYER_PRINCIPAL_GRANT, RuleEffect_RULE_EFFECT_ALLOW), grant("principal", PolicyLayer_POLICY_LAYER_PRINCIPAL_GRANT, RuleEffect_RULE_EFFECT_DENY)}, false, DecisionReason_DECISION_REASON_EXPLICIT_DENY},
		{"organization deny overrides resource grant", request(), []*PolicySet{grant("resource", PolicyLayer_POLICY_LAYER_RESOURCE_POLICY, RuleEffect_RULE_EFFECT_ALLOW), grant("org", PolicyLayer_POLICY_LAYER_DEPLOYMENT_GUARDRAIL, RuleEffect_RULE_EFFECT_DENY)}, false, DecisionReason_DECISION_REASON_EXPLICIT_DENY},
		{"tenant boundary intersects grant", request(), []*PolicySet{grant("principal", PolicyLayer_POLICY_LAYER_PRINCIPAL_GRANT, RuleEffect_RULE_EFFECT_ALLOW), boundary("tenant", PolicyLayer_POLICY_LAYER_TENANT_BOUNDARY, false)}, false, DecisionReason_DECISION_REASON_BOUNDARY_DENIED},
		{"resource policy grants", request(), []*PolicySet{grant("resource", PolicyLayer_POLICY_LAYER_RESOURCE_POLICY, RuleEffect_RULE_EFFECT_ALLOW)}, true, DecisionReason_DECISION_REASON_ALLOWED},
		{"delegation attenuates", request(), []*PolicySet{grant("group", PolicyLayer_POLICY_LAYER_PRINCIPAL_GRANT, RuleEffect_RULE_EFFECT_ALLOW), boundary("delegation", PolicyLayer_POLICY_LAYER_DELEGATION_SESSION_BOUNDARY, false)}, false, DecisionReason_DECISION_REASON_BOUNDARY_DENIED},
		{"missing layer", request(PolicyLayer_POLICY_LAYER_TENANT_BOUNDARY), []*PolicySet{grant("principal", PolicyLayer_POLICY_LAYER_PRINCIPAL_GRANT, RuleEffect_RULE_EFFECT_ALLOW)}, false, DecisionReason_DECISION_REASON_MISSING_REQUIRED_POLICY},
		{"boundary cannot grant", request(), []*PolicySet{boundary("tenant", PolicyLayer_POLICY_LAYER_TENANT_BOUNDARY, true)}, false, DecisionReason_DECISION_REASON_NO_EXPLICIT_ALLOW},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CombineAuthorization(tc.req, tc.sets)
			if got.GetAllowed() != tc.allowed || got.GetReason() != tc.reason {
				t.Fatalf("got allowed=%v reason=%v", got.GetAllowed(), got.GetReason())
			}
		})
	}
}

func TestAuthorizationEvaluationFailuresDeny(t *testing.T) {
	for evaluation, reason := range map[RuleEvaluation]DecisionReason{
		RuleEvaluation_RULE_EVALUATION_CEL_COMPILE_ERROR:    DecisionReason_DECISION_REASON_CEL_COMPILE_ERROR,
		RuleEvaluation_RULE_EVALUATION_CEL_EVALUATION_ERROR: DecisionReason_DECISION_REASON_CEL_EVALUATION_ERROR,
		RuleEvaluation_RULE_EVALUATION_COST_EXHAUSTED:       DecisionReason_DECISION_REASON_COST_EXHAUSTED,
		RuleEvaluation_RULE_EVALUATION_CANCELLED:            DecisionReason_DECISION_REASON_CANCELLED,
		RuleEvaluation_RULE_EVALUATION_MISSING_ATTRIBUTE:    DecisionReason_DECISION_REASON_MISSING_REQUIRED_ATTRIBUTE,
		RuleEvaluation_RULE_EVALUATION_RELATIONSHIP_FAILURE: DecisionReason_DECISION_REASON_RELATIONSHIP_RESOLUTION_FAILURE,
	} {
		t.Run(evaluation.String(), func(t *testing.T) {
			got := CombineAuthorization(&AuthorizationRequest{PolicyVersion: "v1", Action: "read", ResourceType: "secret"}, []*PolicySet{{Id: "p", PolicyVersion: "v1", Layer: PolicyLayer_POLICY_LAYER_SECRET_BOUNDARY, Kind: PolicyKind_POLICY_KIND_BOUNDARY, Rules: []*PolicyRule{{Id: "r", Evaluation: evaluation}}}})
			if got.GetReason() != reason {
				t.Fatalf("got %v", got.GetReason())
			}
		})
	}
}
