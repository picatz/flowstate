package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
	enumspb "go.temporal.io/api/enums/v1"
)

// workerTaskQueue decides which task queue `flow worker` polls, and refuses the
// combinations that would make the refusal in [engine.TenantInterceptor] do harm
// instead of good.
//
// Three postures, and the third is the one worth stating:
//
//   - Neither --tenant nor --task-queue-prefix: today's worker, polling
//     [engine.RunTaskQueueName] or whatever --task-queue named, executing every
//     tenant's runs. Unchanged, and it has to stay unchanged — it is what a
//     first `temporal server start-dev` session gets.
//
//   - --tenant with a queue of its own: the Tier 2 worker. The queue comes from
//     --task-queue-prefix, composed exactly as the server composes it, or from
//     an explicit --task-queue for a deployment that names its queues some other
//     way.
//
//   - --tenant on the *shared* queue: refused. A tenant-restricted worker fails
//     every run that is not its own, so on the queue every tenant's runs land on
//     it would not sit quietly beside the general fleet — it would race the
//     other workers for those runs and fail the ones it won. That turns a flag
//     meant to contain a misconfiguration into an outage for everybody else, so
//     it is made unrepresentable rather than documented as a thing not to do.
//
// A prefix with no tenant is refused for the mirror-image reason: the queue name
// a prefix composes is a function of the tenant, so a worker given only half of
// the pair has been told to poll a queue nobody can name.
func workerTaskQueue(flags temporalFlags) (string, error) {
	queues := engine.TaskQueues{Prefix: flags.taskQueuePrefix}

	// When configuration loads, not when a run arrives — the same discipline
	// every other policy surface here follows.
	if err := queues.Validate(); err != nil {
		return "", err
	}

	if !flags.tenantSet {
		if queues.Enabled() {
			return "", fmt.Errorf(
				"--task-queue-prefix %q names a family of per-tenant task queues, and which one this "+
					"worker should poll is a function of the tenant: pass --tenant <namespace> "+
					"(or --tenant= for the default tenant of an untenanted deployment), or drop the "+
					"prefix to poll the single shared queue",
				flags.taskQueuePrefix)
		}

		return flags.taskQueue, nil
	}

	if err := auth.ValidateNamespace(flags.tenant); err != nil {
		return "", fmt.Errorf("--tenant is not a namespace this deployment could ever authenticate: %w", err)
	}

	// An operator who named a queue meant it: a deployment may spell its queues
	// in a way no prefix composes, and this flag is how it says so.
	if flags.taskQueueExplicit {
		return flags.taskQueue, nil
	}

	if !queues.Enabled() {
		return "", fmt.Errorf(
			"--tenant %q needs a task queue of its own: this worker refuses every run belonging to "+
				"another tenant, so on the shared queue %q — where every tenant's runs are submitted "+
				"when the server routes nothing — it would fail other tenants' work rather than leave "+
				"it to the general fleet. Pass --task-queue-prefix with the same value `flow server` "+
				"was started with, or name this fleet's queue with --task-queue",
			flags.tenant, engine.RunTaskQueueName)
	}

	return queues.For(flags.tenant)
}

// warnUnpolledTenantQueues reports every tenant this deployment can route whose
// task queue no worker is polling.
//
// This is invariant 9 reached through configuration rather than through a
// payload. A tenant mapped onto a Temporal namespace, or onto a task queue, with
// nothing polling it does not fail: its runs are accepted, start, and sit
// RUNNING forever with nothing wrong reported anywhere. The substrate's own
// answer is silence, so the answer has to be made here.
//
// A warning and not a refusal, deliberately, and this is the one place in this
// change where fail-closed is *not* the rule. Workers and servers start in
// whatever order a deployment's supervisor picks them, and a server that refused
// to start until its fleet was already polling would deadlock every deployment
// that starts the server first — including every one of the recipes in
// docs/DEPLOYMENT.md. Nor is a poller count a durable fact: it is true at this
// instant and a worker may arrive a second later. So the honest shape is a loud
// startup line naming exactly which tenant and which queue, which is the
// cheaper half the issue offers, chosen over a `flow` admin verb because it
// costs the operator nothing to have already run.
//
// Best-effort throughout: a describe that fails says so and checks the rest,
// because a diagnostic that aborts on the first tenant is a diagnostic that
// reports the least when a deployment is most broken.
func warnUnpolledTenantQueues(
	ctx context.Context, logger *slog.Logger, pool *temporalclient.Pool,
	queues engine.TaskQueues, tenants []string,
) {
	for _, tenant := range tenants {
		queue, err := queues.For(tenant)
		if err != nil {
			logger.Warn("a mapped tenant cannot be routed to a task queue; its runs will be refused at submit",
				"tenant", tenant, "error", err)
			continue
		}

		cl, err := pool.For(tenant)
		if err != nil {
			logger.Warn("a mapped tenant has no Temporal client; its runs will be refused at submit",
				"tenant", tenant, "error", err)
			continue
		}

		described, err := cl.DescribeTaskQueue(ctx, queue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if err != nil {
			logger.Warn("could not check whether a worker is polling a tenant's task queue",
				"tenant", tenant, "task_queue", queue, "error", err)
			continue
		}

		if len(described.GetPollers()) == 0 {
			logger.Warn("no worker is polling this tenant's task queue; runs submitted for it will sit "+
				"RUNNING until one starts",
				"tenant", tenant, "task_queue", queue,
				"fix", "start `flow worker --tenant "+tenant+"` against that queue")
			continue
		}

		logger.Info("tenant task queue has workers",
			"tenant", tenant, "task_queue", queue, "pollers", len(described.GetPollers()))
	}
}

// routableTenants lists the Flowstate namespaces a trust policy's tenancy
// mapping can place, in a stable order.
//
// The empty namespace is included when the mapping has a default, because that
// is the namespace an unauthenticated caller and a caller whose token names none
// both belong to — the tenant most easily forgotten when checking that every
// tenant has a fleet, since nobody wrote its name down anywhere.
func routableTenants(tenancy *auth.Tenancy) []string {
	if tenancy == nil {
		return nil
	}

	tenants := make([]string, 0, len(tenancy.Temporal)+1)
	if tenancy.Default != "" {
		tenants = append(tenants, "")
	}
	for tenant := range tenancy.Temporal {
		tenants = append(tenants, tenant)
	}

	slices.Sort(tenants)

	return tenants
}
