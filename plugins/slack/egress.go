package main

import (
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// egressPolicy is the deployment's egress policy, granted to this process at
// launch: an immutable snapshot of the operator-owned --egress-policy, or of the
// default the worker's own built-in http task runs under when the operator
// configured none. A plugin declaration is not authority, and a separate process
// is not confinement, so every Slack request goes through the client built from
// this policy, on its actual HTTP dial path.
//
// Nil means the grant could not be used, and [egressRefusal] says why.
var egressPolicy *netpolicy.Policy

// egressRefusal is why there is no policy, kept so the task boundary refuses
// with the SDK's message — which names the environment variable and the worker
// that sets it — rather than with a denial of its own invention.
var egressRefusal error

// installEgressPolicy takes the deployment's grant.
//
// It goes through [sdk.EgressPolicy] rather than reading, decoding and parsing
// the variable here (#1332): the SDK bounds the encoded and decoded lengths of
// an environment it did not build and refuses by name, and a second decode path
// beside it is a second set of answers to keep correct.
//
// The deployment default is accepted rather than refused. Slack is an HTTPS POST
// to a public host, which is exactly what the default policy permits and what
// this plugin has always been able to do on a worker with no --egress-policy;
// refusing it would mean installing this plugin requires writing a policy file
// to get back what the worker already does. That is the accepting posture
// documented on [sdk.EgressPolicyIsDeploymentDefault]; `sql` takes the other one
// for a reason that is about databases, not about grants.
//
// An unusable grant does not stop the process: discovery and validation stay
// available without network authority, and slackPost refuses at the task
// boundary before it decodes inputs or attempts a write.
func installEgressPolicy() {
	policy, err := sdk.EgressPolicy()
	if err != nil {
		egressRefusal = err
		return
	}

	egressPolicy = policy
}
