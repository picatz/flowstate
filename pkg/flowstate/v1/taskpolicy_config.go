package flowstatev1

import (
	"fmt"

	"github.com/goccy/go-yaml"
)

// TaskPolicyConfig is the file form of a task-shape policy: what an operator
// writes in YAML and hands to `flow worker --task-policy`, mirroring how
// [netpolicy.Config] is the file form of an egress policy.
//
// Unlike egress policy, there is no config that "says nothing": a task-shape
// policy file with no allow and no deny rules would enforce nothing, which is
// indistinguishable from not configuring one at all and is refused by
// [TaskPolicyConfig.Policy] as a likely mistake rather than silently accepted
// as a no-op — an operator who wrote a file meant it to restrict something.
type TaskPolicyConfig struct {
	// Allow holds CEL allow rules. Configuring any turns the policy into an
	// allowlist: a dispatch must match at least one, or it is denied with
	// [TaskPolicyReasonNoAllowRule]. Attributes available to a rule: `task`
	// (the qualified task name) and `identity` (`identity.subject`,
	// `identity.issuer`, `identity.namespace`, `identity.claims`) — the run's
	// attested [WorkloadIdentity].
	Allow []string `json:"allow,omitempty" yaml:"allow,omitempty"`

	// Deny holds CEL deny rules. A matching rule denies the dispatch
	// regardless of the allow rules, and a rule that fails to evaluate denies
	// it too — deny rules run first and always win.
	Deny []string `json:"deny,omitempty" yaml:"deny,omitempty"`

	// RuleCostLimit bounds the CEL evaluation cost of a single rule. Unset
	// keeps [DefaultTaskPolicyRuleCostLimit].
	RuleCostLimit *uint64 `json:"rule_cost_limit,omitempty" yaml:"rule_cost_limit,omitempty"`
}

// ParseTaskPolicyConfig decodes a policy file from YAML or JSON, which is a
// subset of YAML. Unknown and duplicate fields are errors, so a misspelled
// key fails loudly at startup instead of silently dropping a restriction —
// the same rule [netpolicy.ParseConfig] and
// [github.com/picatz/flowstate/pkg/flowstate/v1/auth.ParsePolicy] apply to
// their own policy files, for the same reason.
//
// Parsing checks the document's shape. Whether the fields describe a usable
// policy is [TaskPolicyConfig.Policy]'s job, where every CEL rule is compiled
// and type-checked.
func ParseTaskPolicyConfig(data []byte) (TaskPolicyConfig, error) {
	var cfg TaskPolicyConfig

	if err := yaml.UnmarshalWithOptions(data, &cfg, yaml.Strict()); err != nil {
		return TaskPolicyConfig{}, fmt.Errorf("%w: %w", ErrInvalidTaskPolicy, err)
	}

	return cfg, nil
}

// Policy compiles cfg into a ready-to-evaluate [*TaskPolicy]. Every CEL rule
// is compiled and type-checked here, at configuration load — never per
// dispatch — so a mistake in a rule refuses the command that loaded it
// rather than silently governing some tasks and not others.
//
// A config with neither Allow nor Deny rules is refused: it is not a usable
// policy, and treating it as one would either enforce nothing (indistinguishable
// from the zero case, and therefore probably not what the file's author
// intended) or — worse, if ever changed to fail closed on "no rules matched"
// — deny every dispatch outright. Both are surprises a load-time refusal
// prevents.
func (cfg TaskPolicyConfig) Policy() (*TaskPolicy, error) {
	if len(cfg.Allow) == 0 && len(cfg.Deny) == 0 {
		return nil, fmt.Errorf(
			"%w: no allow or deny rules configured; a task-shape policy file with nothing "+
				"to enforce is almost certainly a mistake — remove --task-policy entirely if no "+
				"restriction is wanted, or add allow:/deny: rules",
			ErrInvalidTaskPolicy)
	}

	costLimit := DefaultTaskPolicyRuleCostLimit
	if cfg.RuleCostLimit != nil {
		costLimit = *cfg.RuleCostLimit
	}

	rules, err := compileTaskPolicyRules(cfg.Allow, cfg.Deny, costLimit)
	if err != nil {
		return nil, err
	}

	return &TaskPolicy{rules: rules}, nil
}
