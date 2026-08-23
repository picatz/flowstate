package flowstatev1

import "slices"

const maxDecisionContributions = 64

// CombineAuthorization is the production authorization combiner and the only
// combiner used by explain/rehearsal surfaces. Authentication and construction
// of request.Principal happen before this function; it never authenticates.
//
// Applicable grants union, applicable boundaries intersect, explicit deny wins
// globally, and the final answer still requires an explicit grant allow.
func CombineAuthorization(request *AuthorizationRequest, sets []*PolicySet) *AuthorizationDecision {
	decision := &AuthorizationDecision{}
	deny := func(reason DecisionReason) *AuthorizationDecision {
		decision.Allowed = false
		decision.Reason = reason
		return decision
	}
	if request == nil || request.GetAction() == "" {
		return deny(DecisionReason_DECISION_REASON_UNKNOWN_ACTION)
	}
	if request.GetResourceType() == "" {
		return deny(DecisionReason_DECISION_REASON_UNKNOWN_RESOURCE_TYPE)
	}

	byLayer := make(map[PolicyLayer][]*PolicySet)
	for _, set := range sets {
		if set == nil {
			continue
		}
		if set.GetPolicyVersion() != request.GetPolicyVersion() {
			return deny(DecisionReason_DECISION_REASON_POLICY_VERSION_MISMATCH)
		}
		byLayer[set.GetLayer()] = append(byLayer[set.GetLayer()], set)
	}
	for _, layer := range request.GetRequiredLayers() {
		if len(byLayer[layer]) == 0 {
			return deny(DecisionReason_DECISION_REASON_MISSING_REQUIRED_POLICY)
		}
	}

	grantAllowed := false
	boundaryDenied := false
	for layer := PolicyLayer_POLICY_LAYER_DEPLOYMENT_GUARDRAIL; layer <= PolicyLayer_POLICY_LAYER_PLUGIN_BOUNDARY; layer++ {
		layerSets := byLayer[layer]
		if len(layerSets) == 0 {
			continue
		}
		outcome := &LayerOutcome{Layer: layer, Applicable: true, Allowed: false}
		layerAllowed := false
		for _, set := range layerSets {
			outcome.PolicyIds = append(outcome.PolicyIds, set.GetId())
			if outcome.Kind == PolicyKind_POLICY_KIND_UNSPECIFIED {
				outcome.Kind = set.GetKind()
			} else if outcome.Kind != set.GetKind() {
				return deny(DecisionReason_DECISION_REASON_MISSING_REQUIRED_POLICY)
			}
			for _, rule := range set.GetRules() {
				if reason := evaluationDenial(rule.GetEvaluation()); reason != DecisionReason_DECISION_REASON_UNSPECIFIED {
					return deny(reason)
				}
				if rule.GetEvaluation() != RuleEvaluation_RULE_EVALUATION_MATCH {
					continue
				}
				addContribution(decision, set, rule)
				if rule.GetEffect() == RuleEffect_RULE_EFFECT_DENY {
					decision.Layers = append(decision.Layers, outcome)
					return deny(DecisionReason_DECISION_REASON_EXPLICIT_DENY)
				}
				if rule.GetEffect() == RuleEffect_RULE_EFFECT_ALLOW {
					layerAllowed = true
				}
			}
		}
		outcome.Allowed = layerAllowed
		decision.Layers = append(decision.Layers, outcome)
		if outcome.GetKind() == PolicyKind_POLICY_KIND_GRANT {
			grantAllowed = grantAllowed || layerAllowed
		} else if outcome.GetKind() == PolicyKind_POLICY_KIND_BOUNDARY && !layerAllowed {
			boundaryDenied = true
		}
	}
	if boundaryDenied {
		return deny(DecisionReason_DECISION_REASON_BOUNDARY_DENIED)
	}
	if !grantAllowed {
		return deny(DecisionReason_DECISION_REASON_NO_EXPLICIT_ALLOW)
	}
	decision.Allowed = true
	decision.Reason = DecisionReason_DECISION_REASON_ALLOWED
	return decision
}

func addContribution(decision *AuthorizationDecision, set *PolicySet, rule *PolicyRule) {
	if len(decision.Contributions) >= maxDecisionContributions {
		return
	}
	decision.Contributions = append(decision.Contributions, &PolicyContribution{
		PolicyId: set.GetId(), Revision: set.GetRevision(), RuleId: rule.GetId(),
		Layer: set.GetLayer(), Effect: rule.GetEffect(),
	})
}

func evaluationDenial(e RuleEvaluation) DecisionReason {
	switch e {
	case RuleEvaluation_RULE_EVALUATION_CEL_COMPILE_ERROR:
		return DecisionReason_DECISION_REASON_CEL_COMPILE_ERROR
	case RuleEvaluation_RULE_EVALUATION_CEL_EVALUATION_ERROR:
		return DecisionReason_DECISION_REASON_CEL_EVALUATION_ERROR
	case RuleEvaluation_RULE_EVALUATION_COST_EXHAUSTED:
		return DecisionReason_DECISION_REASON_COST_EXHAUSTED
	case RuleEvaluation_RULE_EVALUATION_CANCELLED:
		return DecisionReason_DECISION_REASON_CANCELLED
	case RuleEvaluation_RULE_EVALUATION_MISSING_ATTRIBUTE:
		return DecisionReason_DECISION_REASON_MISSING_REQUIRED_ATTRIBUTE
	case RuleEvaluation_RULE_EVALUATION_RELATIONSHIP_FAILURE:
		return DecisionReason_DECISION_REASON_RELATIONSHIP_RESOLUTION_FAILURE
	default:
		return DecisionReason_DECISION_REASON_UNSPECIFIED
	}
}

// RequiredAuthorizationLayers returns a sorted, duplicate-free copy suitable
// for constructing a fail-closed request at a serving surface.
func RequiredAuthorizationLayers(layers ...PolicyLayer) []PolicyLayer {
	slices.Sort(layers)
	return slices.Compact(layers)
}
