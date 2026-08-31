package main

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// sqlEgressPolicyEnv is set by the Flowstate host to an immutable base64
// snapshot of the same --egress-policy bytes already parsed for built-in HTTP.
// It is intentionally not inherited from the worker's ambient environment:
// plugin.Config.Env names the grant explicitly.
const sqlEgressPolicyEnv = "FLOWSTATE_SQL_EGRESS_POLICY_B64"

var egressPolicy *netpolicy.Policy

// installEgressPolicy loads the deployment policy when one was supplied. An
// absent snapshot leaves egressPolicy nil; PostgreSQL execution refuses that
// state at the task boundary, while catalog/validation-only plugin launches
// continue to work without pretending they can connect anywhere.
func installEgressPolicy() error {
	encoded := os.Getenv(sqlEgressPolicyEnv)
	if encoded == "" {
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
