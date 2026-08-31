package main

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// slackEgressPolicyEnv is an explicit host grant containing an immutable
// snapshot of the operator-owned --egress-policy. A plugin declaration is not
// authority, and the separate process is not confinement: every Slack request
// must use the client built from this policy on its actual HTTP dial path.
const slackEgressPolicyEnv = "FLOWSTATE_SLACK_EGRESS_POLICY_B64"

var egressPolicy *netpolicy.Policy

func installEgressPolicy() error {
	encoded := os.Getenv(slackEgressPolicyEnv)
	if encoded == "" {
		return nil
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("decoding Slack egress policy snapshot: %w", err)
	}
	cfg, err := netpolicy.ParseConfig(data)
	if err != nil {
		return fmt.Errorf("parsing Slack egress policy: %w", err)
	}
	egressPolicy, err = cfg.Policy()
	if err != nil {
		return fmt.Errorf("building Slack egress policy: %w", err)
	}
	return nil
}
