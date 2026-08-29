# Per-tenant worker routing

One worker fleet per tenant, each holding that tenant's own secrets, egress policy
and plugins — and refusing every run belonging to anyone else.

This is [#270](https://github.com/picatz/flowstate/issues/270), the keystone of
Tier 2 in [docs/DEPLOYMENT.md](../../../docs/DEPLOYMENT.md). It is a two-process
demo rather than a Flowfile because nothing in a workflow can express, observe, or
depend on which queue its worker polled — which is the property, not a gap.

## The claim, stated so it can be falsified

Tier 1 shares one worker between tenants, and that worker holds every tenant's
secrets in one process. Its guarantee is therefore only as good as the process
boundary it does not have: a plugin compromise on that fleet reaches everyone.

Tier 2's claim is narrower and checkable: **a compromise of this process reaches
one tenant's material.** That is what the routing buys, and everything below is
the mechanism that makes it true rather than aspirational.

## Run it

Three processes. Start the server with a prefix, which is what turns routing on:

```console
$ flow server --auth-policy examples/operations/tenant-routing/trust.yaml \
    --rpc-resource https://flowstate.example.com/rpc \
    --task-queue-prefix flowstate-run
```

Unset, the prefix routes nothing and every tenant's runs go to the single shared
queue exactly as they always have — a single-team deployment has nothing to route
between and should not have to say so.

`--rpc-resource` is unrelated to routing and is required of any server whose
trust policy names a `kind: oidc` issuer: it is the `aud` every bearer token
spent on Connect RPC must carry, and it is listed in `trust.yaml` beside each
issuer's other audiences. See
[docs/DEPLOYMENT.md](../../../docs/DEPLOYMENT.md#bearer-token-audiences-are-per-surface).

Then one fleet per tenant. Each gets its own everything:

```console
$ flow worker --tenant team-a --task-queue-prefix flowstate-run \
    --temporal-namespace temporal-team-a \
    --egress-policy /etc/flowstate/team-a/egress.yaml \
    --secret-dir /etc/flowstate/team-a/secrets \
    --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"

$ flow worker --tenant team-b --task-queue-prefix flowstate-run \
    --temporal-namespace temporal-team-b \
    --egress-policy /etc/flowstate/team-b/egress.yaml \
    --secret-dir /etc/flowstate/team-b/secrets \
    --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"
```

`--deployment-name`/`--build-id` are on both lines for a reason that has nothing to
do with tenancy — see [worker-versioning](../worker-versioning/), and note that a
worker refuses to start without them.

Note what makes those two fleets actually different: `--egress-policy` and
`--secret-dir` are per-process, so each holds one tenant's material and no other's.
That is the whole claim above, spelled as command-line arguments — the separation is
a *deployment* fact here, rather than something a policy language would need a
tenant attribute to express.

If a fleet does serve more than one tenant, the file backend has
`--secret-dir-namespaced`, which resolves below `<secret-dir>/<namespace>/` instead
of flat. Prefer it over inventing a naming convention inside one directory:
namespacing by prefix is exactly the ambiguity described [below](#why-the-queue-name-cannot-be-forged),
and a separate directory per tenant has no separator to get wrong. There are
`--secret-keychain-namespaced` and `--secret-op-namespaced` for the same reason, and
each is fail-closed per backend rather than a single global switch.

Then submit anything. There is nothing tenant-specific about the file, which is the
point — routing is a deployment fact, and the same Flowfile runs on either fleet:

```console
$ flow run examples/hello-world/workflow.yaml
```

An existing example rather than one shipped here, per
[the note in the parent README](../README.md#why-these-are-here-and-not-somewhere-else).

Watch each worker's startup line. A tenant-restricted worker says so:

> worker restricted to one tenant; runs belonging to any other will be refused

It is logged on every start rather than only when the flag is typed, because the
person reading a worker's logs a month later is usually not the one who wrote its
command line.

## The two command lines that are refused

Both halves of the pair are required, and each is refused for its own reason
rather than as a symmetry. These are the messages the binary prints.

**A tenant with no queue of its own:**

```console
$ flow worker --tenant team-a
Error: --tenant "team-a" needs a task queue of its own: this worker refuses every
run belonging to another tenant, so on the shared queue "flowstate-run-task-queue"
— where every tenant's runs are submitted when the server routes nothing — it
would fail other tenants' work rather than leave it to the general fleet. Pass
--task-queue-prefix with the same value `flow server` was started with, or name
this fleet's queue with --task-queue
```

Read that failure mode twice, because it is the one worth internalising: a flag
meant to *contain* one misconfiguration, accepted here, would have turned into an
outage for every other tenant. The restricted worker would race the general fleet
for everyone's runs and terminally fail the ones it won.

**A prefix with no tenant:**

```console
$ flow worker --task-queue-prefix flowstate-run
Error: --task-queue-prefix "flowstate-run" names a family of per-tenant task
queues, and which one this worker should poll is a function of the tenant: pass
--tenant <namespace> (or --tenant= for the default tenant of an untenanted
deployment), or drop the prefix to poll the single shared queue
```

Half the pair addresses nothing: the queue a prefix composes *is* a function of the
tenant, so a worker given only a prefix has been told to poll a queue nobody can
name.

Both are checked when configuration loads rather than when a run arrives, which is
the discipline every policy surface in this repository follows.

## Why the queue name cannot be forged

The queue is `<prefix>_<namespace>`, and the namespace comes from the
*authenticated* caller — never from the request — the same rule the tenant memo and
the fairness key already follow. So the interesting question is whether two
different `(prefix, tenant)` pairs can compose the same string, which would put one
tenant's runs on another's fleet.

They cannot, and the argument is worth reading because it is the fix for a bug this
repository actually shipped once. A namespace is `auth.ValidateNamespace`'s grammar
— lowercase letters, digits, and a dash that is never first — so it cannot contain
`_`. A prefix is checked against the same grammar. The composed name joins them with
exactly one `_`, so the first `_` is always the separator, at `len(prefix)`: two
pairs composing one string would have to be the same pair.

The default tenant's component is `_default`, which begins with the one character a
namespace may not contain. So a tenant literally named `default` gets
`<prefix>_default` and the *default* tenant gets `<prefix>__default` — different
queues. That is the same argument the `_default` component of an assertion subject
makes, and it is the fix for the ambiguity CLAUDE.md records: the env secrets
provider once resolved two different tenants' secrets to one variable, because
`prefix + NAMESPACE + "_" + name` had no unforgeable separator. No separator fixes
it when every character legal in one part is legal in the other; making one
character illegal in one part does.

`TestTaskQueueNamesCannotBeForged` asserts it over a cross product of straddling
pairs.

## The line to watch for, and why it is only a warning

A tenant mapped onto a Temporal namespace, or onto a task queue, with nothing
polling it does not fail. Its runs are accepted, start, and sit `RUNNING` forever
with nothing wrong reported anywhere — the substrate's own answer to this is
silence. So `flow server` checks at startup whether a worker is polling each
routable tenant's queue, and warns naming the tenant and the queue when none is:

> a mapped tenant cannot be routed to a task queue; its runs will be refused at submit

It warns rather than refusing for two reasons, both about honesty. A server that
would not start until its fleet was already polling deadlocks every deployment that
starts the server first, in whatever order a supervisor picks. And a poller count is
true at an instant rather than durably, so refusing on it would be enforcing
something the check cannot actually keep.

Watch for that line. It is the difference between finding a mapping gap at deploy
time and finding it when somebody asks why their run has been running since Tuesday.

The tenant easiest to miss: a mapping with a `default` also routes the *empty*
namespace — what an unauthenticated caller, and a caller whose token names no
namespace, belongs to. It has a queue like any other and appears in the mapping
under no name at all.

## What this is not

Per-**step** queue routing — "run this step on the GPU fleet" — is the same
mechanism applied at a different level, and it is not built.

And the boundary this buys is the worker's, not the substrate's. Tier 3 —
containers or microVMs per tenant, network-level enforcement, per-tenant cloud
credentials issued by the platform — is documented rather than built, deliberately:
Flowstate is designed to run *on* a substrate that provides that, not to
reimplement it.
