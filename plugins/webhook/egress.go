package main

import (
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// egressPolicy is the deployment's egress policy, granted to this process at
// launch: an immutable snapshot of the operator-owned --egress-policy, or of the
// default the worker's own built-in http task runs under when the operator
// configured none. A plugin declaration is not authority, so every delivery goes
// through the client built from this policy, on its actual HTTP dial path.
//
// Nil means the grant could not be used, and [egressRefusal] says why.
var egressPolicy *netpolicy.Policy

// egressRefusal is why there is no policy, kept so the task boundary refuses
// with the SDK's message rather than with a denial of its own invention.
var egressRefusal error

// installEgressPolicy takes the deployment's grant through [sdk.EgressPolicy].
//
// The deployment default is accepted: a webhook is an HTTPS POST to a receiver
// the author names, which is what the default policy permits (it denies
// internal address ranges and loopback), and the same posture slack takes. A
// deployment that wants deliveries confined to named receivers writes a policy,
// and this plugin obeys it. A grant this process cannot use fails closed:
// webhook.send refuses at the task boundary before it decodes inputs.
func installEgressPolicy() {
	policy, err := sdk.EgressPolicy()
	if err != nil {
		egressRefusal = err
		return
	}

	egressPolicy = policy
}
