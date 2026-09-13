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
	if out == nil {
		return nil, runErr
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
	// A non-zero exit the grant does not count as success is a failure that
	// still carries what the command said; the host keeps the outputs on the
	// error path, so a runbook debugging one is not left guessing.
	if runErr != nil {
		return outputs, runErr
	}
	return outputs, nil
}

// selectGrants resolves the two names a call carries into the operator's own
// grants, refusing anything the operator did not write.
//
// The order matters for what an author learns: an unknown host is named as
// unknown, a known host that does not permit a command says so, and a namespace
// that may not spend the grant is refused as a permission decision rather than
// as a missing host - a tenant probing for which grants exist learns nothing
// they could not already see in their own configuration.
func selectGrants(namespace string, in *sshv1.RunInputs) (hostGrant, commandGrant, error) {
	host, ok := operatorGrants.Hosts[in.GetHost()]
	if !ok {
		return hostGrant{}, commandGrant{}, sdk.NotFound(
			"no host grant named %q; this worker's grants file names %s",
			truncate(in.GetHost(), 64), grantNames(operatorGrants.Hosts))
	}

	if !host.reachableFrom(namespace) {
		return hostGrant{}, commandGrant{}, sdk.PermissionDenied(
			"the host grant %q is not granted to this workload's namespace", truncate(in.GetHost(), 64))
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
