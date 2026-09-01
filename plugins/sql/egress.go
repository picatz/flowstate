package main

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// sqlEgressPolicyEnv is the one grant the host makes to every plugin it
// launches: an immutable base64 snapshot of the same --egress-policy bytes
// already parsed for built-in HTTP. It is intentionally not inherited from the
// worker's ambient environment — plugin.Config.EgressPolicy names it explicitly
// — and it is not SQL's own variable, which is why the name comes from the SDK
// rather than being spelled again here (#1332).
//
// PostgreSQL is not HTTP, so this plugin applies the policy on its own dial path
// rather than through sdk.HTTPClient; the grant it reads is the same one.
const sqlEgressPolicyEnv = sdk.EgressPolicyEnv

var egressPolicy *netpolicy.Policy

// installEgressPolicy loads the deployment policy when one was supplied. An
// absent snapshot leaves egressPolicy nil; PostgreSQL execution refuses that
// state at the task boundary, while catalog/validation-only plugin launches
// continue to work without pretending they can connect anywhere.
func installEgressPolicy() error {
	// Presence, not length. An operator whose --egress-policy names an empty
	// document configured a policy — the one an empty document builds, which is
	// what the worker's own built-in http task runs under — and the host sets
	// the grant to the empty string to say so. Reading it with os.Getenv made
	// that indistinguishable from no grant at all, so this plugin denied where
	// the same deployment's built-in task allowed.
	encoded, granted := os.LookupEnv(sqlEgressPolicyEnv)
	if !granted {
		return nil
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("decoding SQL egress policy snapshot: %w", err)
	}
	cfg, err := netpolicy.ParseConfig(data)
	if err != nil {
		return fmt.Errorf("parsing SQL egress policy: %w", err)
	}
	egressPolicy, err = cfg.Policy()
	if err != nil {
		return fmt.Errorf("building SQL egress policy: %w", err)
	}
	return nil
}
