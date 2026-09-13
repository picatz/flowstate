package main

import (
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// egressPolicy is the deployment's egress policy, granted to this process at
// launch: an immutable snapshot of the operator-owned --egress-policy, or of
// the default the worker's own built-in http task runs under when the operator
// configured none. Every registry request goes through the client built from
// it, on its actual dial path, because a plugin declaring its own destinations
// is not a boundary - the operator's policy is.
//
// Nil means the grant could not be used, and [egressRefusal] says why.
var egressPolicy *netpolicy.Policy

// egressRefusal is why there is no policy, kept so a task refuses with the
// SDK's own message - which names the environment variable and the worker that
// sets it - rather than a denial of this plugin's invention.
var egressRefusal error

// installEgressPolicy takes the deployment's grant, through [sdk.EgressPolicy]
// rather than decoding the environment here: the SDK bounds what it decodes and
// refuses by name, and a second decode path beside it is a second set of
// answers to keep correct.
//
// The deployment default is accepted rather than refused, the posture `slack`
// takes and for the same reason: a registry read is an HTTPS GET to a public
// host, which is exactly what the default policy permits. Refusing it would
// mean installing this plugin requires writing a policy file to get back what
// the worker's own http task already does. A deployment that wants registry
// reads confined writes the policy, and this plugin obeys it because every
// request goes through it.
func installEgressPolicy() {
	policy, err := sdk.EgressPolicy()
	if err != nil {
		egressRefusal = err
		return
	}

	egressPolicy = policy
}
