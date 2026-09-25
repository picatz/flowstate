package main

import (
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// trustEnv names the operator's trust policy file. It reaches this process
// through the worker's per-plugin environment:
//
//	flow worker --plugin-env jose=FLOWSTATE_JOSE_TRUST=/etc/flowstate/trust.yaml
//
// Unset means this plugin trusts nothing and verifies nothing: every call is
// refused naming the variable. There is no default trust root, because the only
// available default would be "whatever the token says about itself", which is
// the failure verification exists to prevent.
const trustEnv = "FLOWSTATE_JOSE_TRUST"

// maxTrustBytes bounds the operator's own file.
const maxTrustBytes = 1 << 20

// loadTrust reads and validates the operator's policy at startup.
//
// It is the same document `flow server --auth-policy` reads, parsed by the same
// code: a deployment that already trusts an issuer for its own API can point
// this at that file, and an operator who learns one spelling has learned both.
func loadTrust() (auth.Policy, error) {
	path := os.Getenv(trustEnv)
	if path == "" {
		return auth.Policy{}, fmt.Errorf(
			"%s is not set, so this plugin trusts no issuer and can verify nothing. An operator grants trust with "+
				"`flow worker --plugin-env jose=%s=/path/to/trust-policy.yaml`, naming the same document shape "+
				"`flow server --auth-policy` reads", trustEnv, trustEnv)
	}

	info, err := os.Stat(path)
	if err != nil {
		return auth.Policy{}, fmt.Errorf("%s (%q): %w", trustEnv, truncate(path, 256), err)
	}
	if info.IsDir() {
		return auth.Policy{}, fmt.Errorf("%s (%q) is a directory, not a trust policy", trustEnv, truncate(path, 256))
	}
	if info.Size() > maxTrustBytes {
		return auth.Policy{}, fmt.Errorf("%s (%q) is %d bytes, over the %d-byte limit this plugin reads",
			trustEnv, truncate(path, 256), info.Size(), maxTrustBytes)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return auth.Policy{}, fmt.Errorf("%s (%q): %w", trustEnv, truncate(path, 256), err)
	}

	policy, err := auth.ParsePolicy(raw)
	if err != nil {
		return auth.Policy{}, fmt.Errorf("%s (%q): %w", trustEnv, truncate(path, 256), err)
	}
	if err := policy.Validate(); err != nil {
		return auth.Policy{}, fmt.Errorf("%s (%q): %w", trustEnv, truncate(path, 256), err)
	}
	return policy, nil
}

// truncate bounds a value before it is interpolated into a message.
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}
