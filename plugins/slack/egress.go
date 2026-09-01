package main

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// slackEgressPolicyEnv is the explicit host grant every launched plugin
// receives: an immutable snapshot of the operator-owned --egress-policy. A
// plugin declaration is not authority, and the separate process is not
// confinement, so every Slack request must use the client built from this policy
// on its actual HTTP dial path.
//
// The name is the SDK's rather than Slack's own (#1332): one grant, one
// spelling, and a third-party plugin gets the same one without the host having
// heard of it.
const slackEgressPolicyEnv = sdk.EgressPolicyEnv

var egressPolicy *netpolicy.Policy

func installEgressPolicy() error {
	// Presence, not length. An operator whose --egress-policy names an empty
	// document configured a policy — the one an empty document builds, which is
	// what the worker's own built-in http task runs under — and the host sets
	// the grant to the empty string to say so. Reading it with os.Getenv made
	// that indistinguishable from no grant at all, so this plugin denied where
	// the same deployment's built-in task allowed.
	encoded, granted := os.LookupEnv(slackEgressPolicyEnv)
	if !granted {
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
