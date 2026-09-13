package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"maps"
	"slices"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	dockerv1 "github.com/picatz/flowstate/plugins/docker/gen/docker/v1"
)

// maxParameters bounds how many placeholders one call may fill.
const maxParameters = 32

// result is what one container did.
type result struct {
	exitCode  int32
	stdout    string
	stderr    string
	truncated bool
}

func dockerRun(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if operatorGrants == nil {
		return nil, sdk.Failed("%v", grantsRefusal)
	}

	var in dockerv1.RunInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	if len(in.GetParameters()) > maxParameters {
		return nil, sdk.InvalidInput("a call may fill at most %d parameters", maxParameters)
	}

	// The namespace the server established for this workload, never one the
	// workload declared. An absent caller is the empty namespace, which a grant
	// naming namespaces does not match.
	namespace := ""
	if caller, ok := sdk.CallerFromContext(ctx); ok {
		namespace = caller.Namespace
	}

	grant, err := selectRun(namespace, in.GetRun())
	if err != nil {
		return nil, err
	}

	argv, err := buildArgv(grant, in.GetParameters())
	if err != nil {
		return nil, err
	}

	runtime, err := newDaemon(operatorGrants.Daemon)
	if err != nil {
		return nil, err
	}
	defer runtime.close()

	out, runErr := execute(ctx, runtime, grant, in.GetRun(), argv)
	if out == nil {
		return nil, runErr
	}

	outputs, encodeErr := sdk.EncodeOutputs(&dockerv1.RunOutputs{
		ExitCode:  out.exitCode,
		Stdout:    out.stdout,
		Stderr:    out.stderr,
		Truncated: out.truncated,
		Image:     grant.Image,
		Run:       in.GetRun(),
	})
	if encodeErr != nil {
		return nil, encodeErr
	}
	// A non-zero exit the grant does not count as success is a failure that
	// still carries what the container said, so a workflow debugging one is not
	// left guessing.
	return outputs, runErr
}

// selectRun resolves the grant name a call carries.
//
// A refusal names the grants this namespace could have spent, never the ones it
// could not: the whole point of a grant naming namespaces is that another
// tenant's workflows do not learn it exists.
func selectRun(namespace, name string) (runGrant, error) {
	grant, ok := operatorGrants.Runs[name]
	if !ok {
		return runGrant{}, sdk.NotFound(
			"no run grant named %q; this worker's grants file names %s",
			truncate(name, 64), joinNames(reachableRuns(namespace)))
	}
	if !grant.reachableFrom(namespace) {
		return runGrant{}, sdk.PermissionDenied(
			"the run grant %q is not granted to this workload's namespace", truncate(name, 64))
	}
	return grant, nil
}

// reachableRuns is the sorted grant names this namespace may spend.
func reachableRuns(namespace string) []string {
	names := make([]string, 0, len(operatorGrants.Runs))
	for name, grant := range operatorGrants.Runs {
		if grant.reachableFrom(namespace) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// buildArgv fills the grant's argv with a call's parameters.
//
// There is no shell here at all - the daemon takes an argv, not a command line -
// so a value is one element of that argv and cannot become a second command.
// It is still checked against the grant's pattern first: a value that can
// choose a file to read is worth constraining even when it cannot choose a
// program to run.
func buildArgv(grant runGrant, parameters map[string]string) ([]string, error) {
	for name := range parameters {
		if _, declared := grant.Parameters[name]; !declared {
			return nil, sdk.InvalidInput(
				"this run declares no parameter %q; it takes %s",
				truncate(name, 64), joinNames(slices.Sorted(maps.Keys(grant.Parameters))))
		}
	}

	argv := make([]string, 0, len(grant.Argv))
	for _, argument := range grant.Argv {
		var refusal error

		rendered := placeholderPattern.ReplaceAllStringFunc(argument, func(match string) string {
			name := placeholderPattern.FindStringSubmatch(match)[1]

			value, supplied := parameters[name]
			if !supplied {
				if refusal == nil {
					refusal = sdk.InvalidInput("parameter %q is required by this run and was not supplied", name)
				}
				return ""
			}

			declared := grant.Parameters[name]
			limit := declared.MaxBytes
			if limit == 0 {
				limit = 256
			}
			if len(value) > limit {
				if refusal == nil {
					refusal = sdk.InvalidInput("parameter %q is %d bytes, over this run's limit of %d", name, len(value), limit)
				}
				return ""
			}
			if declared.compiled == nil || !declared.compiled.MatchString(value) {
				// The value is not echoed: it failed the operator's pattern,
				// which makes it exactly the kind of value not to write into
				// durable history.
				if refusal == nil {
					refusal = sdk.InvalidInput("parameter %q does not match the pattern this run requires of it", name)
				}
				return ""
			}
			return value
		})

		if refusal != nil {
			return nil, refusal
		}
		argv = append(argv, rendered)
	}
	return argv, nil
}

// execute creates, starts, waits for and removes one container.
//
// The removal is unconditional and happens on every path, including cancellation
// and timeout: a container left running because a workflow was cancelled is the
// failure #1348 names, and it is this function's to prevent rather than an
// operator's to notice.
func execute(ctx context.Context, runtime *daemon, grant runGrant, name string, argv []string) (*result, error) {
	config := containerConfig{
		Image:      grant.Image,
		Cmd:        argv,
		Env:        environment(grant),
		User:       grant.user(),
		WorkingDir: grant.WorkingDir,
		Labels:     containerLabels(name),
		// No TTY, so the streams stay separable; no stdin, so a container
		// waiting for input cannot hold this call open.
		Tty:         false,
		AttachStdin: false,
		OpenStdin:   false,
		HostConfig: hostConfig{
			NetworkMode:    grant.network(),
			ReadonlyRootfs: !grant.WritableRootFilesystem,
			// This plugin removes the container itself, after reading its
			// output. AutoRemove would race that read and take the exit status
			// with it.
			AutoRemove: false,
			Privileged: false,
			// Dropped wholesale rather than pruned: a grant cannot add a
			// capability back, so the set a container gets is the set every
			// container here gets.
			CapDrop:     []string{"ALL"},
			SecurityOpt: []string{"no-new-privileges:true"},
			Memory:      grant.MemoryBytes,
			NanoCpus:    grant.NanoCPUs,
			Mounts:      mounts(grant),
		},
	}
	if grant.PidsLimit > 0 {
		limit := grant.PidsLimit
		config.HostConfig.PidsLimit = &limit
	}

	timeout := grant.Timeout.duration(defaultRunTimeout)
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	container := containerName()

	id, err := runtime.create(runCtx, container, config)
	if err != nil {
		// The create may have happened even though its answer did not arrive,
		// and the identifier only ever existed in that answer. The name did
		// not - it was chosen here - so it still names whatever the daemon
		// made, and removing by it is what keeps a lost response from leaving
		// a container behind.
		if removeErr := runtime.remove(container, defaultCleanup); removeErr != nil {
			sdk.Logger(ctx).Warn("a container that may have been created could not be removed",
				"container", container, "error", removeErr)
		}
		return nil, err
	}
	// From here on there is a container, and it is this call's to clean up
	// however this call ends. The removal takes its own deadline because the
	// context that got here may already be cancelled, and a removal that fails
	// is reported rather than dropped: a container this plugin could not remove
	// is one still holding whatever the grant gave it.
	defer func() {
		if removeErr := runtime.remove(id, defaultCleanup); removeErr != nil {
			sdk.Logger(ctx).Error("a container could not be removed and may still be running",
				"container", container, "error", removeErr)
		}
	}()

	if err := runtime.start(runCtx, id); err != nil {
		return nil, err
	}

	exitCode, err := runtime.wait(runCtx, id)
	if err != nil {
		if runCtx.Err() != nil && ctx.Err() == nil {
			// This call's own timeout, not the caller's cancellation. The
			// removal happens here rather than in the deferred cleanup, because
			// whether it worked is the difference between a container that is
			// gone and one that is still running - and this is the message that
			// says which. What it had already done is not knowable either way.
			if removeErr := runtime.remove(id, defaultCleanup); removeErr != nil {
				return nil, sdk.OutcomeUnknown(
					"the container did not finish within this grant's timeout of %s and could not be removed (%s); it may still be running, and what it had already done is not known",
					timeout, truncate(removeErr.Error(), maxErrorBytes))
			}
			return nil, sdk.OutcomeUnknown(
				"the container did not finish within this grant's timeout of %s and was removed; what it had already done is not known, so it is not retried automatically",
				timeout)
		}
		return nil, err
	}

	// The output is read after the wait and before the removal: the daemon
	// keeps a stopped container's logs until it is deleted, and reading them
	// while it runs would race the exit status this result is about.
	//
	// It outlives the caller's cancellation - the container has already exited,
	// and its output is what this call is for - but not indefinitely: stripping
	// cancellation strips the deadline with it, and a daemon that accepts this
	// request and then stalls mid-stream would otherwise hold the call, and the
	// removal behind it, with nothing left to stop either.
	logCtx, cancelLogs := context.WithTimeout(context.WithoutCancel(ctx), defaultLogRead)
	defer cancelLogs()

	stdout, stderr, truncated, err := runtime.logs(logCtx, id, grant.outputLimit())
	if err != nil {
		// The container has already run. Whatever went wrong reading its output
		// - a cut stream, a stalled daemon, a refusal - a retry would run it
		// again, including everything it did through a writable mount or a
		// granted network. So this is never the retryable classification the
		// transport would otherwise suggest.
		return nil, sdk.OutcomeUnknown(
			"the container ran and exited %d, and its output could not be read (%s); it is not retried automatically because retrying would run it again",
			exitCode, truncate(err.Error(), maxErrorBytes))
	}

	out := &result{exitCode: exitCode, stdout: stdout, stderr: stderr, truncated: truncated}
	if !grant.successful(exitCode) {
		return out, sdk.Failed("the container exited %d, which this grant does not count as success: %s",
			exitCode, truncate(firstNonEmpty(stderr, stdout), 512))
	}
	return out, nil
}

// containerName is the name one call creates its container under.
//
// Unique per call and chosen before the request, which is what makes a create
// whose answer was lost reconcilable: the name is still here when the
// identifier never arrived. The prefix is what an operator looking at a host
// sees; the labels say which run grant it came from.
func containerName() string {
	var random [8]byte
	// crypto/rand.Read fills the slice or panics; it does not return an error.
	_, _ = rand.Read(random[:])
	return "flowstate-" + hex.EncodeToString(random[:])
}

// environment renders the grant's environment, sorted so two runs of one grant
// build the same container.
func environment(grant runGrant) []string {
	if len(grant.Env) == 0 {
		return nil
	}

	env := make([]string, 0, len(grant.Env))
	for _, key := range slices.Sorted(maps.Keys(grant.Env)) {
		env = append(env, key+"="+grant.Env[key])
	}
	return env
}

// mounts resolves the grant's mount names to the host paths the operator wrote.
func mounts(grant runGrant) []mountConfig {
	if len(grant.Mounts) == 0 {
		return nil
	}

	resolved := make([]mountConfig, 0, len(grant.Mounts))
	for _, name := range grant.Mounts {
		mount := operatorGrants.Mounts[name]
		resolved = append(resolved, mountConfig{
			Type:     "bind",
			Source:   mount.Source,
			Target:   mount.Target,
			ReadOnly: !mount.Writable,
		})
	}
	return resolved
}

// joinNames renders grant names for a refusal, bounded.
func joinNames(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	if len(names) > 20 {
		names = names[:20]
	}
	return truncate(strings.Join(names, ", "), 512)
}

// firstNonEmpty is the first stream that said anything.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return "no output"
}
