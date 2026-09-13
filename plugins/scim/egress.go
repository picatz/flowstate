package main

import (
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// egressPolicy is the deployment's egress policy, granted to this process at
// launch. Every request to an identity provider goes through the client built
// from it, on its real dial path: a plugin declaring its own destinations is
// not a boundary, and the identity provider is exactly the destination an
// operator most wants to name.
var egressPolicy *netpolicy.Policy

// egressRefusal is why there is no policy, kept so a task refuses with the
// SDK's message - which names the environment variable and the worker that
// sets it - rather than a denial of this plugin's invention.
var egressRefusal error

// installEgressPolicy takes the deployment's grant through [sdk.EgressPolicy],
// which bounds what it decodes and refuses by name.
//
// The deployment default is accepted rather than refused: a SCIM call is an
// HTTPS request to the organization's identity provider, which is what the
// default policy permits. An operator who wants these calls confined to one
// host writes the policy, and this plugin obeys it because every request goes
// through it.
func installEgressPolicy() {
	policy, err := sdk.EgressPolicy()
	if err != nil {
		egressRefusal = err
		return
	}

	egressPolicy = policy
}
