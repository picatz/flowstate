package main

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// maxDaemonAddresses bounds how many addresses the daemon's name may answer
// with before this plugin refuses to authorize them one by one.
const maxDaemonAddresses = 8

// egressPolicy is the deployment's egress policy, granted to this process at
// launch. It governs the one thing this plugin does over a network: reaching a
// remote daemon named by `daemon.address`.
//
// A local `daemon.socket` is a file, not a destination, and needs no policy at
// all - which is why the policy is required at the dial rather than at startup.
var egressPolicy *netpolicy.Policy

// egressIsDeploymentDefault records that the policy above is the worker's
// built-in default rather than an operator's document.
var egressIsDeploymentDefault bool

// egressRefusal is why there is no policy, so a refusal names what would grant
// it rather than being a bare denial.
var egressRefusal error

// errNoOperatorEgressPolicy is the refusal a deployment default earns a remote
// daemon.
//
// A remote daemon is ambient authority on another host: whoever answers on that
// address decides what runs there. The worker's built-in default is what a
// deployment runs under when nobody has decided anything about destinations,
// which is not a decision to hand a machine's container runtime to a workflow.
// So a remote daemon takes two independent operator statements - a grants file
// naming the address with its mTLS material, and an egress policy permitting it
// - and neither is this plugin's to write. A local socket takes neither, being
// a file the operator already named.
var errNoOperatorEgressPolicy = errors.New(
	"this worker is running under the built-in default egress policy, which decides nothing about which container " +
		"runtime a workflow may reach; run the worker with --egress-policy naming a document that permits the daemon " +
		"address this plugin's grants file names, or grant a local daemon socket instead")

// installEgressPolicy takes the deployment's grant, recording whether it is the
// default rather than acting on it here: whether that matters depends on the
// daemon grant, which a call reads and this does not.
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

	egressPolicy = policy
	egressIsDeploymentDefault = isDefault
}

// requireOperatorEgress is the check a remote daemon passes before it is dialed.
func requireOperatorEgress() error {
	switch {
	case egressRefusal != nil:
		return sdk.PermissionDenied("%v", egressRefusal)
	case egressPolicy == nil, egressIsDeploymentDefault:
		return sdk.PermissionDenied("%v", errNoOperatorEgressPolicy)
	}
	return nil
}

// authorizedDial dials a remote daemon through the deployment's egress policy.
//
// The policy decides about the addresses the name actually answers with, at the
// dial, and the connection then goes to one of those addresses as a literal: a
// name checked once and resolved again by the dialer is a name that can answer
// differently the second time.
func authorizedDial(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, sdk.Failed("the daemon address %q is not host:port", truncate(address, 128))
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return nil, sdk.Failed("the daemon address %q names no usable port", truncate(address, 128))
	}

	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(addresses) == 0 {
		return nil, sdk.Unavailable("the daemon address could not be resolved")
	}
	if len(addresses) > maxDaemonAddresses {
		return nil, sdk.PermissionDenied(
			"the daemon address resolved to more than the %d addresses this plugin will authorize", maxDaemonAddresses)
	}

	// Every candidate is authorized and the first is what gets dialed. A name
	// answering with one permitted address and one denied address is a name
	// this plugin refuses outright rather than a coin flip.
	var chosen netip.Addr
	for _, candidate := range addresses {
		unmapped := candidate.Unmap()
		if err := egressPolicy.CheckConnection(ctx, "https", host, netip.AddrPortFrom(unmapped, uint16(portNumber))); err != nil {
			return nil, classifyEgressCheck(err)
		}
		if !chosen.IsValid() {
			chosen = unmapped
		}
	}

	var dialer net.Dialer
	return dialer.DialContext(ctx, network, net.JoinHostPort(chosen.String(), port))
}

// classifyEgressCheck turns a policy check's error into a task failure.
//
// A denial is a decision, and no retry changes it. Anything else is not a
// decision at all and is returned as itself, so a caller's retry classification
// sees what happened rather than a permanent denial the operator never made.
func classifyEgressCheck(err error) error {
	var denied *netpolicy.DenyError
	if errors.As(err, &denied) {
		return sdk.PermissionDenied("the deployment's egress policy does not permit reaching this container runtime")
	}
	return err
}
