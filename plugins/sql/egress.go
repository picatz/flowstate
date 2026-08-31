package main

import (
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// sqlEgressPolicyEnv is set by the Flowstate host from the same
// --egress-policy file used by the built-in HTTP task. It is intentionally not
// inherited from the worker's ambient environment: plugin.Config.Env names the
// grant explicitly.
const sqlEgressPolicyEnv = "FLOWSTATE_SQL_EGRESS_POLICY"

var egressPolicy *netpolicy.Policy

// installEgressPolicy loads the deployment policy when one was supplied. An
// absent file leaves egressPolicy nil; PostgreSQL execution refuses that state
// at the task boundary, while catalog/validation-only plugin launches continue
// to work without pretending they can connect anywhere.
func installEgressPolicy() error {
	path := os.Getenv(sqlEgressPolicyEnv)
	if path == "" {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading SQL egress policy: %w", err)
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
