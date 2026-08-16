# Deployment

This is a reference for putting Flowstate somewhere real: what isolation you
actually get, what each topology looks like as commands and unit files, and
where the sharp edges are. It says what is true today, traced to the code that
makes it true — not what would be nice.

Read [Deployment portability](ARCHITECTURE.md#deployment-portability) first for
the shape of the connection layer; this document is what to do with it.

## Read this before you share a Temporal namespace

If two tenants' runs execute in the same Temporal namespace, anyone with
Temporal UI or `tctl`/`temporal` CLI access to that namespace can read **every
tenant's** workflow history: the full compiled specification, every step's
inputs and outputs, the identity claims a run carries, and its memo. That
access is Temporal's, not Flowstate's — Temporal's own visibility and
namespace permissions are what would have to gate it, and most self-hosted
clusters don't gate per-workflow.

Flowstate's own tenancy checks are real: the API refuses every cross-tenant
verb (`Get`, `List`, `Cancel`, `Terminate`, `Signal`, `Describe` — one
`ownedBy` check, all six, reported as `NotFound` rather than
`PermissionDenied` so a probe learns nothing about what exists), and a run's
namespace comes from the authenticated caller, never from the workload itself
(`docs/ARCHITECTURE.md#tenancy`). None of that reaches someone who talks to
Temporal directly instead of through the Flowstate API. **The Flowstate API's
tenancy governs the Flowstate API, and nothing downstream of it.**

This is the argument for [Tier 2](#the-four-tier-isolation-model) below: if
two tenants must not be able to read each other's history, they need separate
Temporal namespaces, not just separate rows filtered by the Flowstate server.
Everything else in this document is detail underneath that one sentence.

## The worker is the tenancy boundary

Every tenant whose runs a worker processes shares that worker's process. A
plugin binary the worker launches, or `exec:` when that lands, runs *inside*
that boundary — with the same process authority the worker itself has,
including the ability to read anything the worker's own memory holds. This
follows from how plugins are isolated at all: separate processes protect
against a crash or a runtime bug, not against code doing deliberately what its
author wrote (`docs/ARCHITECTURE.md#plugins`). A launched plugin is trusted
code, full stop; the controls after that point — `secret_inputs`, the output
scrubber — narrow what a *vetted* plugin can leak by accident, and are not
containment against one that is actively hostile.

Concretely: a plugin, or anything that achieves code execution inside a
worker process, reaches every secret material that worker holds for every
tenant it serves — not just the tenant whose run happened to launch it.

**The good news, which nobody had written down before this document:**
`pluginEnv` builds a plugin's environment from nothing, not by inheriting the
worker's own (`pkg/flowstate/v1/plugin/launch.go`, `pluginEnv`). The worker's
environment is where its own credentials live — a Temporal API key, a cloud
role, whatever the deployment set as `FLOWSTATE_SECRET_*` — and a plugin
process does not see any of it unless an operator names it explicitly in
`Config.Env`. So the blast radius above is real, but it is *not* "a plugin can
read `$FLOWSTATE_SECRET_DB_PASSWORD` off the worker's environment just by
existing" — it has to be handed a secret through the sanctioned path
(`TaskManifest.secret_inputs`, resolved worker-side and passed over the
socket) or reach it some other way. Know this before either over-trusting a
plugin ("it's sandboxed, right?") or over-building a containment layer that
duplicates a property the worker already has.

## The four-tier isolation model

Each tier is a set of claims a security reviewer can check independently.
Nothing here is aspirational — every ✅ is traced to code, and every ❌ is
something to stop assuming once you've read it.

### Tier 0 — `flow run local`

No server, no Temporal, no worker process boundary. A single command runs a
Flowfile to completion in the calling process.

- ✅ Isolation is the OS user running the command — normal filesystem and
  process permissions, nothing Flowstate-specific.
- ❌ Identity is *asserted*, not verified: `--as-namespace`, `--as-deployment`,
  and `--as-claim` on `flow run local` let you rehearse policy as any tenant
  you like, with no credential check, because that is the point of local
  rehearsal (`runLocalCmd` flags in `cmd/flow/main.go`). Every surface that
  reads an identity reads that one — the secret rules, a credential the run
  assumes, plugin tasks, `run.identity`, and the `--task-policy` and
  `--egress-policy` rules — so what you rehearse is what the worker would
  decide. What an assertion cannot do is travel: `run.local` reads true, and a
  credential minted for a local run carries a `_local` subject component no
  server-attested run can produce, so a cloud trust policy written for
  production will not match a rehearsal's.
- ❌ Never run this as a shared service. There is no authentication surface to
  turn on — it doesn't have one to withhold.

### Tier 1a — shared worker, zero configuration

One Temporal namespace, one worker fleet, one Flowstate server, `flow server`
started with an OIDC/workload-identity `--auth-policy` (or, deliberately for
development only, `--insecure-no-auth`). This is what running `flow worker`
and `flow server` with no per-tenant flags gets you.

- ✅ The Flowstate API refuses every cross-tenant verb: one `ownedBy` check
  covering `Get`/`List`/`Cancel`/`Terminate`/`Signal`/`Describe`, reported as
  `NotFound` — see [above](#read-this-before-you-share-a-temporal-namespace).
- ✅ No schema field lets a caller name a namespace, a fairness weight, or a
  sender — a run's tenancy comes only from the authenticated caller, never
  from anything a Flowfile or a request body can set.
- ✅ Secrets bind per-identity and fail closed: a deployment that registers no
  secret rules refuses every reference (`SecretAccessPolicy`, "absent means
  nothing" — `pkg/flowstate/v1/auth/secretpolicy.go`).
- ✅ Metadata endpoints (cloud instance-metadata IPs) are denied even inside an
  otherwise-allowed network — netpolicy's default posture, not a rule someone
  has to remember to add.
- ✅ Nothing to sandbox yet: the built-in task set is `log` and `http`. There
  is no `exec:`, no filesystem task — the audit that produced this document
  explicitly declines to recommend sandboxing those two, because there is
  nothing there to escape.
- ❌ **Cannot claim history privacy.** See the top of this document — this is
  the tier where it bites hardest, because there is exactly one Temporal
  namespace and it holds everyone's history.
- ❌ **Cannot claim plugin or process containment.** A launched plugin runs
  with the worker's authority; see [above](#the-worker-is-the-tenancy-boundary).
- ⚠️ **Per-tenant egress is a Tier 1b property, not a limitation of the tier.**
  One worker still runs one netpolicy configuration, but that configuration's
  CEL rules can now key on the calling tenant — see Tier 1b below. A single
  rule set that ignores identity governs every tenant's `http:` steps alike;
  writing an identity-scoped rule is what makes egress per-tenant on one worker.

### Tier 1b — shared worker, per-tenant policy rules

Same topology as 1a, plus rules that key on the calling tenant. Real today,
cheap to turn on, and undocumented until now.

- ✅ Secret rules see the workload as an object, including its namespace:
  `secret.scheme == "env" && workload.namespace == "acme"` is a rule you can
  write today (`SecretAccessPolicy.Allow` doc comment,
  `pkg/flowstate/v1/auth/secretpolicy.go`). It's wired through the same
  `--auth-policy` file passed to `flow worker` — the flag's own help text
  says plainly that its secrets rules "authorize worker-side resolution."
- ✅ Two tenants sharing one worker can therefore have secrets that resolve
  differently, or not at all, purely as a function of `workload.namespace` in
  one YAML file.
- ✅ **Task policy keyed on `identity.namespace` is now real.** The design
  record this document is written from (issue #236) described it as "once
  #228 lands" — #228 landed during the writing of this document. It is a
  *separate* mechanism from the secret rules above: `--task-policy` (or
  `$FLOWSTATE_TASK_POLICY`) on `flow worker` and `flow run local` — never on
  `flow server` or `flow validate`, for the same reason egress policy isn't:
  a deployment refusal is not a file diagnostic (`cmd/flow/taskpolicy.go`).
  A rule is CEL over `task` (the qualified task name) and `identity`
  (`identity.subject`, `.issuer`, `.namespace`, `.claims` — the run's attested
  identity), so `task == "log" && identity.namespace != "platform"` denies a
  task to every tenant but one. Fail-closed the same way secret rules are: no
  policy configured permits everything (today's default, unchanged); a
  malformed policy refuses the command to start rather than running
  unrestricted. `examples/task-shape-policy/` is the worked example — a
  Flowfile with no gate left in it at all, refused purely by worker
  configuration. Two separate files, two separate flags
  (`--auth-policy`'s `secrets:` rules and `--task-policy`'s rules), governing
  two separate decisions — don't conflate them when writing one deployment's
  configuration.
- ✅ **Egress keyed on `identity.namespace` is now real (#240).** The egress
  CEL environment carries an `identity` object — `identity.subject`, `.issuer`,
  `.namespace`, `.claims` — the same run identity the secret and task rules
  read, from the same source. So `identity.namespace == "team-a" && host ==
  "partner-a.example.com"` in one worker's `--egress-policy` file lets team-a
  reach a host that every other tenant on that worker is denied — the one
  asymmetry that previously kept egress out of this tier. It is available in
  both rule scopes, so a resolved-address rule (`... && ip == "10.0.0.5"`) can
  be tenant-scoped too. Fail-closed like the others: a run that names no
  identity (one predating identity, or a local rehearsal started without
  `--as-namespace`) presents an empty namespace and matches no tenant rule. A
  local run started *with* one presents it, so `flow run local --egress-policy
  ... --as-namespace team-a` rehearses the answer this worker would give
  team-a rather than the answer it gives a caller with no tenant at all
  (#295) — asserted, never verified, which is what Tier 0 above says about
  every `--as-*` flag. `examples/egress-policy.yaml` is the worked example. This is Tier 1b — one shared worker — not the per-tenant worker of
  Tier 2, which remains the stronger answer where history privacy is also
  required.
- ❌ Still no history privacy and no plugin/process containment — those are
  Tier 2 properties, not policy-rule properties.

### Tier 2 — per-tenant Temporal namespace + per-tenant worker

A tenant's runs execute against their own Temporal namespace, polled by a
worker fleet that serves only that tenant.

- ✅ The routing exists on the server side and is fail-closed, not
  fail-open. `auth.Tenancy` (a field of the trust policy loaded from
  `--auth-policy`) maps a Flowstate namespace onto a Temporal namespace;
  `temporalclient.Pool` dials one client per mapped namespace at server
  startup — an unreachable namespace fails the *start*, not the first
  tenant's request; and `FlowstateServer.clientFor` **refuses** a tenant the
  mapping doesn't cover, rather than silently routing it onto the default
  client (`clientFor`'s own doc comment: "a refusal is a misconfiguration
  someone fixes; a fallback is a tenancy breach nobody notices" —
  `pkg/flowstate/v1/server/server.go`). This is genuinely built, tested, and
  undocumented before this file.
- ✅ **The worker side is built.** `flow server --task-queue-prefix <prefix>`
  routes each tenant's runs to a task queue of that tenant's own, named
  `<prefix>_<namespace>`, derived from the *authenticated* tenant and never
  from the request — the same rule the tenant memo and the fairness key
  already follow. `flow worker --tenant <namespace> --task-queue-prefix
  <prefix>` starts a fleet that polls exactly that queue, and **refuses** any
  run belonging to anyone else, terminally and non-retryably, rather than
  executing it with this worker's secrets, egress policy and plugins. Both
  sides compose the name with the same function, which is the only way they
  can be relied on to agree.

  Unset, the prefix routes nothing and every tenant's runs go to
  `flowstate-run-task-queue` exactly as they always have — a single-team
  deployment has nothing to route between and should not have to say so.

  Two combinations are refused at startup rather than documented as things not
  to do. `--tenant` with no queue of its own is refused, because a
  tenant-restricted worker on the *shared* queue would race the general fleet
  for every tenant's runs and fail the ones it won — a flag meant to contain
  one misconfiguration turned into an outage for everybody else.
  `--task-queue-prefix` with no `--tenant` is refused because the queue a
  prefix composes is a function of the tenant, so half the pair addresses
  nothing.

  **Why the queue name cannot be forged across a tenant boundary.** A
  namespace is `auth.ValidateNamespace`'s grammar — lowercase letters, digits,
  and a dash that is never first — so it cannot contain `_`; a prefix is
  checked against the same grammar, so it cannot either; and the composed name
  joins them with exactly one `_`. The first `_` is therefore always the
  separator, at `len(prefix)`, so two `(prefix, namespace)` pairs that composed
  one string would have to be the same pair. The default tenant's component is
  `_default`, which begins with the one character a namespace may not contain,
  so a tenant named `default` gets `<prefix>_default` and the default tenant
  gets `<prefix>__default` — different queues. This is the same argument the
  `_default` component of an assertion subject makes, and it is the fix for the
  ambiguity that let the env secrets provider resolve two tenants' secrets to
  one variable (`CLAUDE.md`). Asserted over a cross product of straddling
  pairs by `TestTaskQueueNamesCannotBeForged`.

  What this is *not*: per-**step** queue routing ("run this step on the GPU
  fleet"), which is the same mechanism applied at a different level and is not
  built.
- ✅ What Tier 2 buys once wired to a distinct namespace per tenant: worker
  blast radius drops to one tenant (a plugin compromise on tenant A's worker
  fleet cannot reach tenant B's secrets, because they're different
  processes), history is isolated (the point at the top of this document, now
  actually addressed), and per-tenant egress becomes a *deployment* fact — run
  tenant A's worker fleet with tenant A's `--egress-policy` file — rather than
  something netpolicy's CEL would need a namespace attribute for.
- ⚠️ **Mapping completeness is a warning, not a refusal.** A tenant mapped
  onto a Temporal namespace, or onto a task queue, with nothing polling it
  does not fail: its runs are accepted, start, and sit `RUNNING` forever with
  nothing wrong reported anywhere — invariant 9's failure shape arriving
  through a configuration path. `flow server` therefore checks, at startup,
  whether a worker is polling each routable tenant's queue and logs a warning
  naming the tenant and the queue when none is. It warns rather than refusing
  because a server that would not start until its fleet was already polling
  deadlocks every deployment that starts the server first, and because a
  poller count is true at an instant rather than durably. **Watch for that
  line**; it is the difference between finding this at deploy time and finding
  it when somebody asks why their run has been running since Tuesday.

  Note the tenant that is easiest to miss: a mapping with a `default` also
  routes the *empty* namespace — what an unauthenticated caller, and a caller
  whose token names no namespace, belongs to. It has a queue like any other
  and appears in the mapping under no name at all.

**A worked Tier 2 command line, both sides:**

```console
# Server: route each tenant onto its own queue, tenants mapped onto Temporal
# namespaces by the trust policy's `tenancy:` block.
$ flow server --auth-policy /etc/flowstate/trust.yaml \
    --task-queue-prefix flowstate-run

# One fleet per tenant. Each gets that tenant's own egress policy and secrets,
# which is the whole point: a compromise of this process reaches one tenant's
# material, which is the claim Tier 1 structurally cannot make.
$ flow worker --tenant team-a --task-queue-prefix flowstate-run \
    --namespace temporal-team-a \
    --egress-policy /etc/flowstate/team-a/egress.yaml \
    --secret-dir /etc/flowstate/team-a/secrets \
    --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"

# And the default tenant of a deployment whose trust policy has a `default`:
$ flow worker --tenant= --task-queue-prefix flowstate-run ...
```

`FLOWSTATE_TASK_QUEUE_PREFIX` sets the prefix on both sides, which is the
convenient way to keep them equal — a worker that spelled it differently would
poll a queue nothing submits to, do nothing forever, and report nothing.

### Tier 3 — substrate isolation

Containers or microVMs per tenant, network-level enforcement, per-tenant cloud
credentials issued by the substrate rather than by Flowstate.

- This tier is **documented, not built**, deliberately. Flowstate is designed
  to run *on* a substrate that provides this, not to reimplement it —
  Kubernetes namespaces-with-NetworkPolicy, gVisor/Firecracker, per-tenant IAM
  roles, whatever your platform already does for isolation between untrusted
  workloads. The one exception worth taking seriously later is VISION's
  sandbox-provider plugin shape (Modal or similar, `docs/VISION.md`), which is
  a *task* a workflow calls into for untrusted work — not an orchestrator
  Flowstate would have to build and maintain.

## Deployment matrix

Every recipe below that runs `flow worker` in a production setting sets one of
`FLOWSTATE_DEPLOYMENT_NAME`/`FLOWSTATE_BUILD_ID` (or, deliberately for
non-production, `--allow-unversioned-interpreter`) — the worker refuses to
start with neither, per invariant 10 in `ARCHITECTURE.md`, and every recipe
below shows the flag because the alternative is discovering the refusal at
your first deploy rather than while reading this document.

### Local development

```console
$ temporal server start-dev
$ flow worker --allow-unversioned-interpreter
$ flow server --insecure-no-auth
```

Tier 0/1a boundary: this is the shared-server shape, but `--insecure-no-auth`
means there is no tenancy to speak of — everyone is anonymous. Fine for a
laptop; never a service that anyone but you can reach.

### Docker Compose

`examples/observability/docker-compose.yaml` is a working compose file
standing up Temporal (`start-dev`), a Flowstate server and worker, and a full
Grafana/Tempo/Loki/Prometheus/OTel-Collector stack — see
`examples/observability/README.md`. It runs `--insecure-no-auth` and
`--allow-unversioned-interpreter` deliberately, and every published port binds
`127.0.0.1` — read that README's "Insecure by design" section before adapting
it into anything that leaves your laptop; it says plainly what has to change
and why each choice was made assuming nothing else is reachable.

```console
$ docker compose -f examples/observability/docker-compose.yaml up
```

### Single VM (EC2 or similar), systemd — the best-supported production shape

No Kubernetes needed. Two systemd units on one host, or split across two hosts
for Tier 2: one worker unit per tenant's Temporal namespace.

`/etc/flowstate/worker.env`:

```env
TEMPORAL_ADDRESS=temporal.internal:7233
TEMPORAL_NAMESPACE=production
FLOWSTATE_DEPLOYMENT_NAME=flowstate
FLOWSTATE_BUILD_ID=2026.08.06-a1b2c3d
FLOWSTATE_AUTH_POLICY=/etc/flowstate/policy.yaml
FLOWSTATE_SECRET_DIR=/etc/flowstate/secrets
```

`/etc/systemd/system/flowstate-worker.service`:

```ini
[Unit]
Description=Flowstate worker
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
EnvironmentFile=/etc/flowstate/worker.env
ExecStart=/usr/local/bin/flow worker --plugin-dir /usr/local/lib/flowstate/plugins
Restart=on-failure
RestartSec=5s
DynamicUser=yes
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/flowstate

[Install]
WantedBy=multi-user.target
```

`/etc/flowstate/server.env`:

```env
FLOWSTATE_ADDRESS=127.0.0.1:9233
TEMPORAL_ADDRESS=temporal.internal:7233
TEMPORAL_NAMESPACE=production
FLOWSTATE_DEPLOYMENT_NAME=flowstate
FLOWSTATE_AUTH_POLICY=/etc/flowstate/policy.yaml
FLOWSTATE_IDENTITY_KEY=/etc/flowstate/identity-2026-07.pem
```

`/etc/systemd/system/flowstate-server.service`:

```ini
[Unit]
Description=Flowstate API server
After=network-online.target flowstate-worker.service
Wants=network-online.target

[Service]
Type=exec
EnvironmentFile=/etc/flowstate/server.env
ExecStart=/usr/local/bin/flow server
Restart=on-failure
RestartSec=5s
DynamicUser=yes
NoNewPrivileges=yes
ProtectSystem=strict

[Install]
WantedBy=multi-user.target
```

`FLOWSTATE_ADDRESS=127.0.0.1:9233` above is loopback, so `flow server` needs
no TLS configuration of its own to start — [blockers](#blockers) below is
where the choice this shape has already made gets explained. Two ways to
finish it, and this recipe assumes the first:

- **Terminate TLS in front of it** (nginx, Caddy, an ALB/NLB with a listener
  certificate), forwarding to `127.0.0.1:9233` as configured above. Nothing
  else in this file changes.
- **Terminate TLS in `flow server` itself**, skipping the reverse proxy:
  set `FLOWSTATE_ADDRESS` to the address you actually want reachable (not
  loopback) and add `FLOWSTATE_TLS_CERT_FILE`/`FLOWSTATE_TLS_KEY_FILE` to
  `server.env`, pointing at a certificate this unit can read. No further flag
  needed — a certificate configured is what [blockers](#blockers) below calls
  the ordinary way past the refusal.

To reach Tier 2 on this shape: run one `flowstate-worker` unit per tenant, each
with its own `TEMPORAL_NAMESPACE` and its own `--egress-policy` /
`--auth-policy` files, and map each tenant onto its namespace in the trust
policy the server loads (`tenancy:` under `--auth-policy`, `auth.Tenancy` /
`temporalclient.Pool`). This is the "N worker units" the audit refers to as
the least-documented production shape — it is systemd units, not Kubernetes.

### Kubernetes

The same two binaries as containers: a `Deployment` for `flow server` behind
an `Ingress`/`Service` doing TLS termination, and a `Deployment` per worker
fleet — one per tenant namespace for Tier 2, replicas for throughput within a
tenant. Plugins are Unix-socket subprocesses launched by the worker process
itself, so no extra container or sidecar is needed for them; a `--plugin-dir`
pointed at a `ConfigMap`- or `initContainer`-populated directory works as
long as that directory isn't writable by other users — group or world (see
[blockers](#blockers)).
Secrets mount as files (`--secret-dir`) or environment (`--secret-env`) the
ordinary Kubernetes way — Secret volumes or `envFrom`.

**Set `--identity` (or `FLOWSTATE_WORKER_IDENTITY`) to the pod name.** Left
unset, a worker's identity in Temporal's Event History and Task Queue poller
list is built from `--deployment-name`/`--build-id`, `--tenant` if set, and
this process's hostname — better than the SDK's own `pid@hostname` default
(every container's PID 1 is `1`), but a pod hostname is still a hash an
operator has to cross-reference against `kubectl get pods`. Wire the
downward API's pod name straight through instead:

```yaml
env:
  - name: FLOWSTATE_WORKER_IDENTITY
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

so a stuck task in Temporal's UI names the exact pod to `kubectl exec` into,
with nothing to look up.

**The pod's `FLOWSTATE_ADDRESS` needs a flag alongside it, and which one
depends on where TLS actually ends.** A pod almost always sets
`FLOWSTATE_ADDRESS=0.0.0.0:9233` (or lets the default resolve to it): the
Service that routes an Ingress's traffic to the pod addresses it by pod IP,
which a `localhost`-only listener never answers, so the container needs the
wildcard bind the same way `examples/observability/docker-compose.yaml`'s
container does. `flow server` reads that as "reaches past this machine" and
refuses to start without one of:

- **`--tls-terminated-upstream`** (or `FLOWSTATE_TLS_TERMINATED_UPSTREAM=1`),
  when the `Ingress` (or a `Service` of type `LoadBalancer` fronted by
  something doing TLS) is genuinely the TLS boundary and forwards plaintext
  to the pod on the cluster network — the ordinary shape for `nginx-ingress`
  and most managed ingress controllers by default. Say so on the container's
  `command`/`args`, not by editing the Ingress: the pod is what refuses, and
  the pod is what needs telling. This is the honest use of that flag — see
  its own help text — because the Ingress really is doing the job the flag's
  name used to claim for itself and no longer does.
- **`FLOWSTATE_TLS_CERT_FILE`/`FLOWSTATE_TLS_KEY_FILE`** (a Secret volume
  mount), when you would rather `flow server` terminate TLS itself — mutual
  TLS to the pod, or an Ingress controller that only proxies TCP — and skip
  `--tls-terminated-upstream` entirely, the same certificate-first choice the
  systemd recipe above describes.

Never set `--tls-terminated-upstream` on a `Service` of type `LoadBalancer`
or `NodePort` with no TLS-terminating `Ingress`, controller, or mesh actually
in front of it: that is exactly the plaintext-to-the-internet case the flag's
help text tells you not to use it for, and the cluster network is not a
substitute boundary the way a container's published-port binding is.

### Health checks and probes

`flow server` answers `GET`/`HEAD /healthz` with `200` and an empty body —
nothing else, deliberately: an unauthenticated endpoint that describes the
deployment (version, config, dependency state) is reconnaissance served on
request (`healthzHandler`, `cmd/flow/routing.go:118-126`). It is mounted in
two places:

- On the **public** listener, unauthenticated, always — `serverHandler`,
  `cmd/flow/routing.go:94`.
- On the **internal** listener, if `--internal-listen`
  (`FLOWSTATE_INTERNAL_ADDRESS`) names a loopback address — `internalHandler`,
  `cmd/flow/routing.go:160`. The internal listener also carries `/debug/pprof/*`
  (`cmd/flow/routing.go:167-171`), which is why it has no default and is
  refused off loopback (`checkInternalListenAddress`,
  `cmd/flow/internallistener.go:79-90`): pprof can read this process's memory
  and running goroutines, and the listener has no TLS or authentication of its
  own.

There is exactly one probe endpoint — `flow server` does not expose a
separate readiness or startup route. What makes `/healthz` usable as more than
a bare liveness check is startup ordering: `flow server` dials Temporal with
the SDK's eager `client.DialContext` (`cmd/flow/main.go:809`, wired through
`temporalclient.Dial`, `pkg/flowstate/v1/temporalclient/temporalclient.go:175-187`)
and mounts the HTTP mux — the one carrying `/healthz` — only after that dial,
and every other startup check (TLS configuration, auth policy load, plugin
catalog build), succeeds. So the first `200` from `/healthz` already implies
the initial Temporal connection worked; it does not mean the connection is
*still* good, since nothing re-checks it afterward, and it says nothing about
the pool's per-tenant Temporal clients (`temporalclient.Pool`) reconnecting
after that.

`flow worker` exposes no HTTP surface at all — no listener, no `/healthz`, no
`--internal-listen` flag — so a worker's liveness has to come from the
process itself: run it under a supervisor that treats process exit as the
signal (`systemd`'s `Restart=on-failure` in the [systemd
recipe](#single-vm-ec2-or-similar-systemd--the-best-supported-production-shape)
above, or a Kubernetes `Deployment`'s own restart-on-crash) rather than an
HTTP probe, because there is no port to point one at.

A Kubernetes `httpGet` probe is dialed by the kubelet from the node's own
network namespace, against the pod's IP — not from inside the pod's network
namespace — so it can only reach a listener bound to an address the pod IP
routes to. That is exactly what the public listener is, per the "pod's
`FLOWSTATE_ADDRESS` needs a flag alongside it" note above (`0.0.0.0:9233`,
wildcard bind). The internal listener is the opposite on purpose: refused
off loopback (`checkInternalListenAddress`,
`cmd/flow/internallistener.go:79-90`), so it never accepts a connection that
didn't originate in the same network namespace — which rules it out for a
`httpGet` probe target, not just as a matter of style. So:

```yaml
        # startupProbe: gives Temporal-dial-plus-policy-load startup room
        # before liveness/readiness get a vote, without lowering their own
        # timeouts to cover a case that only happens once per pod.
        startupProbe:
          httpGet:
            path: /healthz
            port: 9233
          failureThreshold: 30
          periodSeconds: 2
        livenessProbe:
          httpGet:
            path: /healthz
            port: 9233
          periodSeconds: 10
          failureThreshold: 3
        # readinessProbe: same route as liveness, because flow server has no
        # separate readiness check today. It answers "the process is up and
        # its one-time startup checks passed" (see above), not "Temporal is
        # reachable right now" — a mid-run Temporal outage does not flip this
        # to unready, so a readiness probe alone will not pull a pod out of
        # an Ingress/Service's rotation for that failure mode.
        readinessProbe:
          httpGet:
            path: /healthz
            port: 9233
          periodSeconds: 10
          failureThreshold: 3
```

If credential-free probe traffic on the RPC port is a concern, the internal
listener is still useful here — just not through `httpGet`. Set
`--internal-listen 127.0.0.1:9090` and point an **`exec` probe** at it
instead, since `exec` runs the command inside the container's own network
namespace, which loopback is reachable from:

```yaml
        livenessProbe:
          exec:
            command: ["/bin/sh", "-c", "wget -q -O- --timeout=2 http://127.0.0.1:9090/healthz || exit 1"]
          periodSeconds: 10
          failureThreshold: 3
```

That trades a probe dependency on the container image having `wget` (or
`curl`) for keeping probe traffic off the RPC port and away from anything
that terminates TLS in front of it; whether that trade is worth making is a
per-deployment call this document isn't going to make for you.

### Graceful shutdown

`flow worker` catches SIGINT and SIGTERM — the signal every recipe above
actually sends: `docker stop`, `systemctl stop`, and a Kubernetes pod
termination all send SIGTERM first and SIGKILL only after a grace period. On
either signal it stops polling for new work immediately, then gives in-flight
activities and workflow tasks up to `--worker-stop-timeout`
(`FLOWSTATE_WORKER_STOP_TIMEOUT`, default `2m`) to finish before exiting
regardless.

That timeout only does its job if the deployment's own grace period is at
least as long, or the platform's hard kill lands first and the wait was for
nothing:

- **systemd** — add `TimeoutStopSec=` to the `[Service]` block in the unit
  above, at least as large as `--worker-stop-timeout` (systemd's own default
  is 90s, shorter than this document's 2-minute worker default).
- **Docker / Docker Compose** — `docker stop` defaults to a 10-second grace
  period; pass `--time` (or `stop_grace_period:` in Compose) to raise it, or
  lower `--worker-stop-timeout` to fit inside the default if 10s is enough for
  your activities.
- **Kubernetes** — set the worker `Deployment`'s pod
  `terminationGracePeriodSeconds` (default 30s) to at least
  `--worker-stop-timeout`'s value; the kubelet sends SIGKILL the moment that
  elapses; it does not wait on the container.

Size `--worker-stop-timeout` to the longest activity you expect in flight, not
to the platform default — the platform default is not a fact about your
workflows, and the two are independently configured on purpose.

### Cloud Run / fly.io

These need attention before they're a good fit, not because they can't work:

- Neither `flow server` nor `flow worker` honors `$PORT` — Cloud Run and
  fly.io both expect a service to bind the port they inject via that
  variable, and neither reads it (verified: no `os.Getenv("PORT")` anywhere in
  `cmd/flow`). `FLOWSTATE_ADDRESS` is the only way to set the listen address
  today, and it's env-only — there's no `--listen` flag. An entrypoint script
  translating `$PORT` into `FLOWSTATE_ADDRESS=0.0.0.0:$PORT` is the practical
  workaround until this lands upstream.
- That `0.0.0.0` bind needs `--tls-terminated-upstream` (or
  `FLOWSTATE_TLS_TERMINATED_UPSTREAM=1`) alongside it, on both platforms, for
  the same reason the Kubernetes recipe above needs it: `flow server` reads a
  non-loopback address with no certificate configured as reaching past the
  machine and refuses to start. Both Cloud Run and fly.io terminate TLS at
  their own edge and forward plaintext to the container over their internal
  network, which is the honest case this flag exists for — it is not shipping
  plaintext anywhere the platform doesn't already own. If you point either
  platform at your container over a raw TCP proxy with no TLS termination of
  its own, that stops being true, and `FLOWSTATE_TLS_CERT_FILE`/
  `FLOWSTATE_TLS_KEY_FILE` (a certificate the container loads itself) is the
  flag you want instead.
- `--address` on `flow worker`/`flow server` means the **Temporal** address,
  not the HTTP listen address — a real foot-gun on a platform that hands you
  a `--address`-shaped port variable and expects it to mean "listen here."
- Once the port is sorted: `fly.io` works with a `dev-server`
  (`temporal server start-dev` in a sidecar/separate machine, Tier 0/1a-only,
  fine for a demo), a self-hosted Temporal cluster reached over Fly's private
  networking, or Temporal Cloud (below) — same three choices as any other
  topology, since the connection layer doesn't care what's hosting it.

### Temporal Cloud

The SDK envconfig `flow` already uses supports this today —
`pkg/flowstate/v1/temporalclient`, via `go.temporal.io/sdk/contrib/envconfig`:

```env
TEMPORAL_ADDRESS=your-namespace.a1b2c.tmprl.cloud:7233
TEMPORAL_NAMESPACE=your-namespace.a1b2c
TEMPORAL_API_KEY=tmprl_...
```

or, for mTLS instead of an API key:

```env
TEMPORAL_ADDRESS=your-namespace.a1b2c.tmprl.cloud:7233
TEMPORAL_NAMESPACE=your-namespace.a1b2c
TEMPORAL_TLS=true
TEMPORAL_TLS_CLIENT_CERT_PATH=/etc/flowstate/client.pem
TEMPORAL_TLS_CLIENT_KEY_PATH=/etc/flowstate/client.key
```

```console
$ flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"
$ flow server
```

Both pick these up from the environment with no Flowstate-specific
configuration — that's the whole point of following Temporal's own
`envconfig` convention instead of inventing one.

**Honesty check, because this is the one recipe in this document that isn't
backed by a green CI job:** this capability exists in the code and nothing
else. No test in this repository dials Temporal Cloud, no example is wired to
it, and `EnsureSearchAttributesRegistered` (used by the `--filter` search-
attribute path) has not been verified against Cloud's managed search
attributes. Treat the two env blocks above as "should work, per the SDK
contract" rather than "proven to work by something CI runs." If you exercise
this and it doesn't work as described, that's a gap in this document, not a
gap you're supposed to work around silently.

## Blockers

Read these before you assume any of the topologies above is further along
than it is.

- **`flow server` refuses to bind a non-loopback address without a TLS
  answer, and there are exactly two acceptable answers.** `--tls-cert-file`/
  `--tls-key-file` (or `FLOWSTATE_TLS_CERT_FILE`/`FLOWSTATE_TLS_KEY_FILE`)
  make it terminate TLS itself — `cmd/flow/tls.go`, `http.Server.ServeTLS`.
  `--tls-terminated-upstream` (or `FLOWSTATE_TLS_TERMINATED_UPSTREAM=1`) says
  instead that something in front of it already does — a reverse proxy, a
  Kubernetes `Ingress`, a load balancer with a TLS listener, a service mesh
  sidecar, or (the compose lab's case) a container publish binding that
  bounds reachability the same way a TLS-terminating proxy would. Loopback
  addresses (`flow server dev`, and the systemd recipe above as written) need
  neither: the bearer token authenticating every request never leaves the
  machine. Every other recipe above assumes one of the two flags is present,
  and says which. This refusal is not a "for now" gap to route around
  quietly — see `cmd/flow/tls.go`'s `refusePlaintextListener` for the code,
  and its help text (`flow server --help`) for the same choice stated at the
  command line.
- **No `$PORT` support.** Covered under [Cloud Run / fly.io](#cloud-run--flyio)
  above — an entrypoint translation is required today.
- **Plugins are Unix-domain-socket only, which makes workers POSIX.**
  `pkg/flowstate/v1/plugin` dials plugins over `AF_UNIX`
  (`internal/protocol.NetworkUnix`), with nothing conditionally compiled for
  another transport on Windows. **Windows support is for authoring, not for
  running workers.** `flow validate`, `flow fix`, `flow lsp`, and
  `flow run local` with no `--plugin-dir` work on Windows; a worker process,
  or `run local` with plugins, needs a POSIX host. State that posture to
  anyone asking "does this run on Windows" — the honest answer is "your
  editor does, your worker fleet doesn't."
- **`--plugin-dir` refuses a directory other users can write to**, group as
  well as world, and rightly:
  `plugin.doc.go` documents this refusal (a plugin directory is arbitrary
  code execution; a directory anyone can write to is arbitrary code execution
  by anyone), with `--allow-insecure-plugin-dir` as the explicit, named escape
  hatch. This bites naive container builds: a Dockerfile step that does
  `mkdir -p /plugins && chmod 777 /plugins` "to avoid permission hassles" will
  make the worker refuse to start. Set ownership correctly instead of
  widening the mode.

## Noisy neighbor

A run is scheduled under a Temporal fairness key taken from its authenticated
tenant's namespace (`FlowstateServer`'s `Priority{FairnessKey: namespace}` —
`pkg/flowstate/v1/server/server.go`), and activities inherit it from the run,
so it covers every task a run goes on to schedule and survives
Continue-As-New. That part is verified and correctly wired.

What it is **not** is a verified enforcement guarantee. Temporal marks
`Priority`/fairness as an experimental SDK feature, and whether the key
actually changes scheduling order — versus being carried and ignored — is a
property of your Temporal server version and configuration, not of anything
Flowstate controls. The honest claim is: **the key is set correctly; whether
it is enforced is a property of your Temporal deployment.** Don't take "we set
a fairness key" as "one tenant cannot crowd out another" without checking
your Temporal version's fairness support.

For the volume dimension — one tenant submitting so many runs that Temporal
itself falls over, as opposed to one tenant's runs sitting ahead of another's
in a queue — the answer is Temporal's own namespace-level rate limits, not
an app-layer limiter in front of `flow server`. A rate limiter Flowstate wrote
itself would duplicate a control the substrate already has, in front of a
substrate whose whole job is being the thing that enforces limits
correctly under load; this repo's own design bias (`CLAUDE.md`, "proto-first",
"leaning into Temporal") is to surface what Temporal does rather than
reimplement it, and this is exactly that call.

## Metrics

Telemetry is OTLP push, gated on the standard `OTEL_EXPORTER_OTLP*`
environment variables — unset means nothing is emitted at all: no exporter,
no goroutines, no network (`telemetryConfigured`, `cmd/flow/telemetry.go:98-103`).
There is no Prometheus-shaped `/metrics` scrape endpoint on either listener;
[internalHandler](#health-checks-and-probes)'s own doc comment says why —
standing one up means a second telemetry pipeline (a registry plus an
exporter) this tree does not carry today (`cmd/flow/routing.go:141-146`). If
your metrics stack expects to scrape, point an OTel Collector's OTLP receiver
at it and let the collector's own Prometheus exporter serve `/metrics` from
there; `examples/observability/docker-compose.yaml` wires exactly that
(Collector receiving OTLP, Prometheus scraping the Collector).

Every metric below is real code, cited by call site — not a proposal. Two
instrumentation sources exist, and one deliberate gap:

**RPC metrics**, from the `otelconnect` interceptor wired onto every command
that speaks Connect RPC — `flow server` (`cmd/flow/main.go:923`), `flow server
dev` (`cmd/flow/serverdev.go:724`), and every CLI/MCP client call
(`cmd/flow/client.go:270`). `otelconnect.NewInterceptor()` is called with no
options, so both its default instruments are active
(`instruments.go:44-49`, `connectrpc.com/otelconnect@v0.9.0`):

| Metric | Type | Unit | Labels | Meaning |
| --- | --- | --- | --- | --- |
| `rpc.server.duration` / `rpc.client.duration` | histogram | ms | `rpc.system`, `rpc.service`, `rpc.method`, `rpc.connect.error_code` or `rpc.grpc.status_code`, `net.peer.name`, `net.peer.port` | Wall time per RPC, server- or client-side depending which end recorded it |
| `rpc.server.request.size` / `rpc.client.request.size` | histogram | bytes | same as above | Uncompressed request message size |
| `rpc.server.response.size` / `rpc.client.response.size` | histogram | bytes | same as above | Uncompressed response message size |
| `rpc.server.requests_per_rpc` / `rpc.client.requests_per_rpc` | histogram | 1 | same as above | Messages received per RPC (1 for every non-streaming call) |
| `rpc.server.responses_per_rpc` / `rpc.client.responses_per_rpc` | histogram | 1 | same as above | Messages sent per RPC |

(`rpc.service`/`rpc.method` come from the Connect procedure path;
`net.peer.*` from the connection's remote address — `attributes.go:48-83`,
same module.)

**Plugin metrics**, from every plugin process a worker launches
(`pkg/flowstate/v1/plugin/telemetry.go:42-47`):

| Metric | Type | Unit | Labels | Meaning |
| --- | --- | --- | --- | --- |
| `flowstate.plugin.operation.duration` | histogram | s | `flowstate.plugin.name`, `flowstate.plugin.operation`, `flowstate.task.name` (when the operation is task-scoped), `flowstate.plugin.outcome` | Duration of one host-to-plugin operation (`start`, `health`, `execute`) |
| `flowstate.plugin.calls` | counter | — | same as above | One increment per operation, same attribute set as the duration it accompanies |
| `flowstate.plugin.health.checks` | counter | — | `flowstate.plugin.name`, `flowstate.plugin.health.status` (`serving`, `not serving`, `unreachable`, or `unknown` — `plugin.go:87-99`) | One increment per health poll result (`plugin.go:353`, `plugin.go:385`) |
| `flowstate.plugin.restarts` | counter | — | none | One increment per relaunch actually attempted, after the restart budget and backoff both let it through (`plugin.go:732`) |
| `flowstate.plugin.launch.failures` | counter | — | none | One increment per failed plugin launch (`launch.go:93`) |
| `flowstate.plugin.protocol.errors` | counter | — | none | One increment when a launch fails specifically on handshake — `ErrHandshake` or `ErrHandshakeTimeout` (`launch.go:96`) |

The last three carry no labels at the call site — a launch failure or a
protocol error happens before a plugin identity is necessarily known, and
`restarts` is recorded without one either, so none of the three can be
filtered by plugin name today; only the span each operation opens carries
`flowstate.plugin.name` regardless of outcome. `flowstate.plugin.name` and
`flowstate.task.name` are `ClassConfiguration` labels (bounded by which
plugins/tasks a deployment installs, not by a caller) and every label passes
through `pkg/flowstate/v1/metricschema` before reaching an instrument, which
drops an unrecognized key and caps a runaway value's cardinality behind an
`OverflowValue` sentinel rather than losing the measurement — see that
package's doc comment for the full policy.

**Temporal SDK metrics** are also live once telemetry is on: `initTelemetry`
wires a `client.MetricsHandler` (`opentelemetry.NewMetricsHandler`, meter name
`temporal-sdk`) into both the server's and the worker's Temporal client
options, which the SDK had never had a handler for before this landed
(`cmd/flow/telemetry.go:35-41`, `:290-292`). That handler emits the Go SDK's
own instrument set — task-queue backlog, poller counts, workflow-task
latency, activity failures among them — under names and label conventions
Flowstate does not define and this document is not going to restate, since
they belong to `go.temporal.io/sdk` and drift with it rather than with this
codebase; see the [Temporal SDK metrics
reference](https://docs.temporal.io/references/sdk-metrics) for the current
list. Verified here only as "on and reachable," not enumerated.

**No metric exists yet for a run's own lifecycle** — started, completed,
failed, a step's retry count — searched for and not found anywhere under
`pkg/flowstate/v1` or `cmd/flow` outside the two tables above. An operator
wanting "runs per tenant per hour" or "step failure rate" today has to derive
it from spans (`engine.startTaskSpan` and neighbors) or from `flow list`
against Temporal visibility, not from a counter. That is a real gap, not a
documentation one — file it separately rather than treat this catalog as
covering it.

## Worker versioning, every time

Every recipe in this document that starts a production worker sets
`FLOWSTATE_DEPLOYMENT_NAME`/`FLOWSTATE_BUILD_ID` or
`--allow-unversioned-interpreter` explicitly, because the worker **refuses to
start** with neither — see
[Versioning: pinned within a run, upgraded between runs](ARCHITECTURE.md#versioning-pinned-within-a-run-upgraded-between-runs).
A deployment that discovers this at its first production rollout, rather than
while reading a deployment guide, has already had a worse morning than
necessary.
