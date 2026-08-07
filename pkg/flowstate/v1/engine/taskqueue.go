package engine

import (
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// RunTaskQueueName is the task queue every run is submitted to when a
// deployment configures no per-tenant routing.
//
// It is also the default `flow worker --task-queue` polls, which is what makes
// a first run against `temporal server start-dev` need no configuration at all
// (invariant 8). A deployment that routes tenants to their own queues sets
// [TaskQueues.Prefix] instead, and this name then addresses nothing — see
// [TaskQueues].
//
// Deliberately free of [taskQueueSeparator]. Every composed queue name contains
// one, so no tenant's queue can ever collide with this one, whatever prefix an
// operator picks. Asserted by TestRunTaskQueueNameCannotBeComposed.
const RunTaskQueueName = "flowstate-run-task-queue"

// TaskQueues decides which Temporal task queue a run is submitted to.
//
// The zero value is the whole of today's behavior: every run, of every tenant,
// goes to [RunTaskQueueName]. That is not a compatibility shim to be removed —
// it is the single-tenant deployment, where there is nothing to route between,
// and it must stay byte-identical to what a deployment that never heard of this
// type already gets.
//
// Setting Prefix turns each tenant's runs onto a queue of its own, which is what
// makes a per-tenant worker fleet addressable: a worker started with
// `flow worker --tenant acme --task-queue-prefix <same prefix>` polls exactly
// the queue this composes for acme, and (see [TenantInterceptor]) refuses
// anything else that reaches it.
//
// # Why a composed name cannot be forged
//
// The failure to avoid is the one CLAUDE.md records for the env secrets
// provider: namespace "team" with secret "A_API_KEY" and the default tenant
// with "TEAM_A_API_KEY" both resolved one variable, because every character
// legal in a prefix was also legal in a name, so the boundary between them was
// a convention rather than a fact. No separator fixes that.
//
// This is fixed the way [auth.SubjectFor]'s `_default` is, and it is a
// structural argument rather than a careful one:
//
//   - A namespace is [auth.ValidateNamespace]'s grammar — lowercase letters,
//     digits, and a dash that is never first. It cannot contain "_".
//   - A prefix is checked against the *same* grammar by [TaskQueues.Validate],
//     which runs when configuration loads. It cannot contain "_" either.
//   - The composed name is `prefix + "_" + tenant`, and the tenant component is
//     the namespace itself, or [defaultTenantComponent] for the empty one.
//
// So the first "_" in a composed name is always the separator, at exactly
// len(prefix). If two (prefix, namespace) pairs composed the same string, the
// first "_" would sit at both len(prefix₁) and len(prefix₂), so the prefixes
// have the same length, so they are equal, so the namespaces are too. There is
// no pair of distinct inputs left to collide, and no namespace an operator can
// name that spells another tenant's queue — including the default tenant's,
// whose component starts with the one character the namespace grammar refuses.
type TaskQueues struct {
	// Prefix names the family of per-tenant queues. Empty routes every run to
	// [RunTaskQueueName], the zero-configuration path.
	//
	// It is checked against [auth.ValidateNamespace]'s grammar, because that is
	// the grammar the forgery argument above rests on — not because a prefix is
	// a namespace.
	Prefix string
}

// taskQueueSeparator joins a prefix to the tenant component.
//
// Underscore, because it is the one character [auth.ValidateNamespace] forbids
// in a namespace and [TaskQueues.Validate] forbids in a prefix. That is what
// makes the join reversible and the composition injective; see [TaskQueues].
const taskQueueSeparator = "_"

// defaultTenantComponent stands in for the empty namespace — the single-tenant
// default, and the tenant an `--insecure-no-auth` deployment's callers all
// belong to — so that a routed deployment has a queue for it too.
//
// It begins with an underscore for exactly the reason the `_default` component
// of an assertion subject does: no namespace can spell it. A tenant literally
// named "default" composes `<prefix>_default`, and the empty namespace composes
// `<prefix>__default` — two underscores, one written as the separator and one
// beginning this component — so the single-tenant default cannot be
// impersonated by naming a tenant after it. Asserted by
// TestTaskQueueNamesCannotBeForged.
const defaultTenantComponent = "_default"

// maxTaskQueuePrefixLen bounds a prefix, at the same length a namespace is
// bounded to. One grammar, one length: a prefix and a namespace are checked the
// same way, so bounding them differently would mean two numbers to keep in step.
const maxTaskQueuePrefixLen = auth.MaxNamespaceLen

// maxTaskQueueNameBytes is what Temporal accepts for a task queue name.
//
// Temporal's frontend validates a task queue name against `limit.maxIDLength`,
// whose default is 1000 bytes. Written down rather than assumed, and asserted
// against below rather than trusted, because the two numbers are owned by
// different systems and only happen to be compatible — the same discipline
// [maxFairnessKeyBytes] applies to the fairness key.
const maxTaskQueueNameBytes = 1000

// maxComposedTaskQueueLen is the longest name [TaskQueues.For] can produce: the
// longest legal prefix, the separator, and the longer of a maximal namespace and
// [defaultTenantComponent].
const maxComposedTaskQueueLen = maxTaskQueuePrefixLen + len(taskQueueSeparator) + auth.MaxNamespaceLen

// A compile-time check, so raising the namespace or prefix limit fails the build
// here rather than producing queue names Temporal quietly rejects at submit —
// which, for a queue name, would mean a run that cannot start rather than one
// that runs somewhere wrong. An array length may not be negative, so this
// compiles if and only if every composable name fits.
var _ [maxTaskQueueNameBytes - maxComposedTaskQueueLen]struct{}

// A second one, because [defaultTenantComponent] is not covered by the first:
// it is not a namespace, so nothing else bounds it.
var _ [auth.MaxNamespaceLen - len(defaultTenantComponent)]struct{}

// Enabled reports whether this deployment routes tenants to their own queues.
func (q TaskQueues) Enabled() bool { return q.Prefix != "" }

// Validate reports whether the configuration is usable.
//
// Called when configuration loads — at `flow server` and `flow worker` startup —
// rather than when a run is submitted, so a prefix that cannot compose a legal
// queue name stops the process instead of failing every submission after it.
func (q TaskQueues) Validate() error {
	if !q.Enabled() {
		return nil
	}

	if len(q.Prefix) > maxTaskQueuePrefixLen {
		return fmt.Errorf("task queue prefix is longer than %d characters", maxTaskQueuePrefixLen)
	}

	// The same grammar a namespace is admitted by, which is what the forgery
	// argument in [TaskQueues] rests on. Reused rather than restated: a prefix
	// checked by a second, similar-looking rule is a second definition of the
	// one property that matters here, and two definitions drift.
	if err := auth.ValidateNamespace(q.Prefix); err != nil {
		return fmt.Errorf("task queue prefix %q must be spelled like a namespace: %w", q.Prefix, err)
	}

	return nil
}

// For returns the task queue a run belonging to the given Flowstate namespace is
// submitted to.
//
// Unconfigured, it answers [RunTaskQueueName] for every namespace and never
// errors — including for a namespace [auth.ValidateNamespace] would refuse.
// That is deliberate and is the byte-identical default the issue asks for: a
// run whose recorded identity predates that grammar (see
// [FlowstateServer.identityFor]) starts today, and must keep starting.
//
// Configured, it fails closed. A namespace outside the grammar cannot be
// composed into a queue name whose boundary is trustworthy, and the answer to
// "which queue does this un-namespaceable tenant use" is not "the one everybody
// else uses" — that is the fallback [temporalclient.Pool.For] already refuses to
// make, for the same reason.
func (q TaskQueues) For(namespace string) (string, error) {
	if !q.Enabled() {
		return RunTaskQueueName, nil
	}

	if err := q.Validate(); err != nil {
		return "", err
	}

	if err := auth.ValidateNamespace(namespace); err != nil {
		return "", fmt.Errorf("cannot route this tenant to a task queue: %w", err)
	}

	return q.Prefix + taskQueueSeparator + tenantComponent(namespace), nil
}

// tenantComponent is the queue component naming a tenant.
func tenantComponent(namespace string) string {
	if namespace == "" {
		return defaultTenantComponent
	}

	return namespace
}
