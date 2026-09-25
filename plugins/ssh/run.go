package main

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	sshv1 "github.com/picatz/flowstate/plugins/ssh/gen/ssh/v1"
)

// maxParameters bounds how many placeholders one call may fill. A command grant
// with more than this many parameters is a command line a workflow is composing.
const maxParameters = 32

func sshRun(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if operatorGrants == nil {
		return nil, classifyGrantRefusal(grantsRefusal)
	}
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied("%v", egressRefusalReason())
	}

	var in sshv1.RunInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	if len(in.GetParameters()) > maxParameters {
		return nil, sdk.InvalidInput("a call may fill at most %d parameters", maxParameters)
	}

	// The namespace the host established for this workload, never one the
	// workload declared. An absent caller is the empty namespace, which a
	// grant naming namespaces does not match - the fail-closed direction.
	namespace := ""
	if caller, ok := sdk.CallerFromContext(ctx); ok {
		namespace = caller.Namespace
	}

	host, command, err := selectGrants(namespace, &in)
	if err != nil {
		return nil, err
	}

	commandLine, err := buildCommandLine(command, in.GetParameters())
	if err != nil {
		return nil, err
	}

	out, runErr := run(ctx, host, command, commandLine)
	if runErr != nil {
		// Only the classification travels on this path: the SDK's Execute
		// returns the error alone, so outputs encoded here would be dropped
		// before they reached the host. What the command said reaches the
		// runbook in the failure message instead, which is why run puts a
		// bounded excerpt of it there.
		return nil, runErr
	}
	if out == nil {
		return nil, sdk.Failed("the session reported neither a result nor a failure")
	}

	outputs, encodeErr := sdk.EncodeOutputs(&sshv1.RunOutputs{
		ExitCode:  out.exitCode,
		Stdout:    out.stdout,
		Stderr:    out.stderr,
		Truncated: out.truncated,
		Host:      in.GetHost(),
		Command:   in.GetCommand(),
	})
	if encodeErr != nil {
		return nil, encodeErr
	}
	return outputs, nil
}

// selectGrants resolves the two names a call carries into the operator's own
// grants, refusing anything the operator did not write.
//
// What an author learns is bounded by what their own namespace holds. A host
// this workload is not granted is not found, whether or not it exists
// elsewhere in the file; a host it does hold that does not permit the command
// says so, because both halves are then the author's own configuration to
// read. So a tenant probing for grants learns nothing it could not already
// see.
func selectGrants(namespace string, in *sshv1.RunInputs) (hostGrant, commandGrant, error) {
	host, ok := operatorGrants.Hosts[in.GetHost()]
	if !ok || !host.reachableFrom(namespace) {
		// One answer for both, deliberately: see selectRun in plugins/docker.
		// A tenant that can tell "exists but denied" from "does not exist" can
		// enumerate the operator's hosts a guess at a time, which is the
		// disclosure the reachable-names list refuses to make in bulk.
		return hostGrant{}, commandGrant{}, sdk.NotFound(
			"no host grant named %q is granted to this workload; this worker's grants file names %s",
			truncate(in.GetHost(), 64), joinNames(reachableHostNames(namespace)))
	}

	if !host.permits(in.GetCommand()) {
		return hostGrant{}, commandGrant{}, sdk.PermissionDenied(
			"the host grant %q does not permit the command %q; it permits %s",
			truncate(in.GetHost(), 64), truncate(in.GetCommand(), 64), joinNames(host.Commands))
	}

	command, ok := operatorGrants.Commands[in.GetCommand()]
	if !ok {
		// Unreachable through a validated grants file, which refuses a host
		// permitting a command that does not exist. Kept because a refusal is
		// the right answer to a state this plugin cannot otherwise describe.
		return hostGrant{}, commandGrant{}, sdk.Failed(
			"the host grant %q permits the command %q, which this worker's grants file does not define",
			truncate(in.GetHost(), 64), truncate(in.GetCommand(), 64))
	}

	return host, command, nil
}
