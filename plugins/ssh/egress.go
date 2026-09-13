package main

import (
	"errors"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// egressPolicy is the deployment's egress policy, or nil when this plugin
// declines to act on the one it was granted.
var egressPolicy *netpolicy.Policy

// egressRefusal is why there is no policy, so a refusal names what would grant
// it rather than being a bare denial.
var egressRefusal error

// errNoOperatorEgressPolicy is the refusal a deployment default earns here.
var errNoOperatorEgressPolicy = errors.New(
	"this worker is running under the built-in default egress policy, which decides nothing about where a command may " +
		"be executed; run the worker with --egress-policy naming a document that allows the ssh scheme and the hosts " +
		"this plugin's grants file names")

// installEgressPolicy takes the deployment's grant, or records why every call
// will be refused without one.
//
// The deployment default is refused, the posture `sql` takes and for the same
// reason, sharpened: the destination is the whole meaning of the authority this
// plugin spends. A worker's built-in default is what a deployment runs under
// when nobody has decided anything about destinations, which is not a decision
// to permit executing commands on a machine. Two independent operator
// statements are therefore required before anything runs here - a grants file
// that names the host, and an egress policy that permits reaching it - and
// neither is this plugin's to write.
//
// A refusal does not stop the process: discovery, validation and the catalog
// keep working without network authority, and ssh.run refuses at its own
// boundary before it decodes inputs.
func installEgressPolicy() {
	policy, err := sdk.EgressPolicy()
	if err != nil {
		egressRefusal = err
		return
	}

	isDefault, err := sdk.EgressPolicyIsDeploymentDefault()
	if err != nil {
		egressRefusal = err
		return
	}
	if isDefault {
		egressRefusal = errNoOperatorEgressPolicy
		return
	}

	egressPolicy = policy
}

// egressRefusalReason is why a call cannot connect.
func egressRefusalReason() error {
	if egressRefusal != nil {
		return egressRefusal
	}
	return errNoOperatorEgressPolicy
}

// classifyEgressCheck turns a policy check's error into a task failure.
//
// A denial is a decision, and no retry changes it. Anything else is not a
// decision at all and is returned as itself, so a caller's retry classification
// sees what happened rather than a permanent denial the operator never made.
func classifyEgressCheck(err error) error {
	var denied *netpolicy.DenyError
	if errors.As(err, &denied) {
		return sdk.PermissionDenied("the deployment's egress policy does not permit reaching this host over ssh")
	}
	return err
}
