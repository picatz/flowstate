# The observability lab

**None of this is required.** Flowstate's default is stderr logs and no exporter:
unset `OTEL_EXPORTER_OTLP_ENDPOINT` and there is no collector, no goroutine, no
network, no propagator — a first run needs nothing but the binary. This directory
is the rung above that default, for when somebody wants to *see* the thing:
`docker compose up`, one environment variable, and a run turns into a trace that
starts at the person who typed `flow run`, threads through the server, into the
workflow, and out across its steps — with the same run visible in the Temporal UI
and the same numbers on a Grafana dashboard.

Take the rungs in order. Point `OTEL_EXPORTER_OTLP_ENDPOINT` at a collector you
already run and you have most of the value with none of this directory. The lab
exists so the claim can be checked rather than asserted.

## What comes up

| Service | Image | What it is | URL (host) |
| --- | --- | --- | --- |
| Grafana | `grafana/grafana:12.3.0` | The one window onto the other three | <http://127.0.0.1:3000> |
| Tempo | `grafana/tempo:2.9.0` | Traces | <http://127.0.0.1:3200> (API) |
| Loki | `grafana/loki:3.6.0` | Logs | <http://127.0.0.1:3100> (API) |
| Prometheus | `prom/prometheus:v3.7.0` | Metrics | <http://127.0.0.1:9090> |
| OTel Collector | `otel/opentelemetry-collector-contrib:0.140.0` | The vendor-neutral fan-in for all three signals | <http://127.0.0.1:4318> (OTLP/HTTP) |
| ClickHouse | `clickhouse/clickhouse-server:25.8.3.66` | Columnar logs and traces, beside LGTM | <http://127.0.0.1:8123> (query API) |
| Temporal | `temporalio/admin-tools:1.29.0-tctl-1.18.6-cli-1.5.0` | `temporal server start-dev` — frontend and UI | <http://127.0.0.1:8233> (UI) |
| Flowstate server | built from this repo | The control plane `flow run` talks to | <http://127.0.0.1:9233> |
| Flowstate worker | the same image | Where workflows and steps actually execute | — |

Nine long-running services, seven of them things you did not write. Two decisions are worth
knowing before you read the compose file:

**Temporal is `start-dev`, not `auto-setup`.** The `admin-tools` image carries the
`temporal` CLI, so one container gives the frontend, the UI, and a SQLite
database — the same cluster every Flowstate test and every developer's laptop
already targets. The `auto-setup` image would mean a separate database, a schema
migration on first boot, and a separate UI container: three more moving parts, and
a cluster that is not the one anybody develops against.

**Prometheus pulls; nothing pushes to it.** The collector exposes what it has
received at `:8889` and Prometheus scrapes it. A scrape target that is briefly
unreachable is a gap in a graph; a push pipeline that is briefly unreachable is
backpressure into the process being observed.

## One instrumentation, two storage examples

Flowstate does not contain a ClickHouse, Loki, Tempo, Prometheus, or Grafana
client. It emits OTLP. The collector fans the same logs to Loki and ClickHouse,
and the same traces to Tempo and ClickHouse; metrics remain in Prometheus because
its pull model and PromQL are the clearest operational example. Remove either
exporter and **nothing in the application changes**. This is the practical
meaning of avoiding backend lock-in, rather than a claim that every backend has
identical storage or query semantics.

ClickHouse earns its place here for high-volume, high-cardinality investigation:
column pruning, compression, and explicit TTLs make cost visible. The demo keeps
only 24 hours and batches before inserting. A production deployment should use a
database and user dedicated to telemetry, a non-empty secret delivered outside
compose, TLS, replicated tables, storage policies, quotas, and separate databases
or row policies per trust boundary. A `tenant.id` column is useful for filtering;
it is **not isolation** if a caller can issue arbitrary SQL. Flowstate's strongest
tenant boundary remains separate Temporal namespaces and worker credentials, and
the observability store must be partitioned to match that boundary.

In Grafana Explore, choose **ClickHouse** and use its Logs or Traces query builder.
The contrib collector creates and owns `otel_logs` and `otel_traces`; the fixed
datasource uid and explicit defaults keep provisioning reproducible. Tempo/Loki
remain the dashboard defaults so this addition proves portability without
silently changing the existing walkthrough.

### Signal taxonomy (and the honest profiles boundary)

| Signal | Semantic role | Demo destinations | Cardinality rule |
| --- | --- | --- | --- |
| Metrics | Alert and aggregate | Prometheus | Never put run, workflow, trace, span, user, or tenant IDs in metric attributes. |
| Logs | Explain discrete decisions | Loki + ClickHouse | IDs are structured fields; retention and tenant access are enforced at storage. |
| Traces | Follow one request/run/step | Tempo + ClickHouse | Run and workflow IDs belong here; sample deliberately at sustained volume. |
| Profiles | Explain CPU/allocation cost | Not emitted yet | Profiles can contain code and values; do not pretend a continuously profiling SDK is OTLP portability. |

OpenTelemetry Profiles is not treated as production-ready by this example yet.
Adding a vendor-specific profiling agent just to fill a Grafana panel would
contradict the backend-neutral claim and expand the data-exfiltration surface.
When the Go OTel SDK and collector provide a stable profiles pipeline, it should
join the same receiver with an explicit collection interval, tenant attributes,
redaction review, and bounded retention. Until then, use a separately governed
Pyroscope deployment if continuous profiling is worth that operational choice,
or capture bounded Go profiles during an incident. Empty architecture boxes are
better than a misleading green demo.

### Docker-backed contract test

The opt-in Go integration test uses the Moby client directly (no shelling out to
`docker`) to pull and start the pinned ClickHouse image, publish a random
loopback port, create a production-shaped tenant/event table, and prove both
trace correlation and tenant-scoped queries. It also proves an unscoped query
can see both tenants: a regression guard against documenting a filter as a
security boundary.

```console
$ GOMEMLIMIT=1GiB go test -tags=integration -timeout 120s ./examples/observability
```

The test skips only when no Docker-compatible daemon is reachable. Image pull is
bounded by the command timeout; containers are labeled and always removed by
test cleanup.

## Insecure by design

Every one of these is a deliberate choice that is correct *only* because nothing
here is reachable from another machine. If you change any published port from
`127.0.0.1` to `0.0.0.0`, you have removed the argument that makes the rest of
this list acceptable, and you own what follows.

- **Every published port binds `127.0.0.1`.** This is the load-bearing one.
- **The Flowstate server runs `--insecure-no-auth`.** Every caller is anonymous
  and can start workflows. The server logs a warning saying exactly that on every
  start, which you will see in the lab's own log panel — that is a nice thing to
  look at once.
- **The Flowstate server also runs `--tls-terminated-upstream`.** `flow
  server` otherwise refuses to bind a non-loopback address (like the 0.0.0.0
  this container needs — see the compose file's comment on
  `FLOWSTATE_ADDRESS`) without a TLS certificate. Nothing in this stack
  terminates TLS, so the flag's name is not describing what usually earns it
  — what makes it safe *here* is the same fact `--insecure-no-auth` above
  relies on: the published port is loopback-only, so nothing outside this
  machine can complete a connection to reach it, plaintext or not. Neither
  flag is exercised by `docker compose config -q`, which only parses YAML —
  this whole deployment stopped serving anything the moment the refusal
  landed, and the only way to have caught that was to run it.
- **Grafana is anonymous, with the Admin role, and the login form is disabled.**
  No password is set, because the alternative is a password living in a compose
  file in a public repository, which is worse than no password on a port only this
  machine can reach.
- **The worker runs `--allow-unversioned-interpreter`.** See below; it is the one
  insecure-by-design choice that is also a *correctness* choice, so it gets its
  own section.
- **Loki and Tempo are single-tenant, unauthenticated** (`auth_enabled: false`, no
  `X-Scope-OrgID`). Flowstate has a tenant boundary of its own, and mapping it
  onto Loki/Tempo tenants is the interesting exercise this lab deliberately does
  not attempt.
- **Everything inside the compose network is plaintext**, including OTLP to Tempo
  (`tls: insecure: true`).
- **No credentials appear anywhere in this directory**, real or fake. There is
  nothing here to leak and nothing to rotate. Check that this stays true.
- **The Temporal database and the three time-series stores are unbounded**, in named volumes,
  with no rotation. `docker compose down -v` is the cleanup, and you will want it.

## Why the worker is unversioned, honestly

`flow worker` refuses to start without a Worker Deployment version unless you say
`--allow-unversioned-interpreter`, and the refusal is not bureaucratic. The worker
evaluates CEL *in workflow code* — step conditions, a loop's `items:`, a step's
`vars:`, most task inputs — so the binary decides what a run in flight computes.
Replace an unversioned worker and you change the meaning of workloads that are
already running.

Accepting that here is honest because a lab has no workloads worth protecting and
you will rebuild this image constantly. It would be dishonest anywhere else. The
alternative is two flags, and the lab works exactly as well with them:

```yaml
    command: ["worker", "--address=temporal:7233", "--deployment-name=flowstate", "--build-id=lab-1", "--verbose"]
```

Bump `--build-id` whenever you rebuild, and watch runs stay pinned to the version
they started on until continue-as-new. That is worth doing once, in a lab, before
it matters.

## How logs get to Loki

Flowstate speaks OTLP for logs, so they get there the same way traces and metrics
do: the process exports them to the collector, and the collector forwards them to
Loki. There is nothing in this directory carrying them.

That is a recent change and it deleted a whole rung of scaffolding. The three
options a lab has when a binary writes logs and does not send them are the Loki
Docker driver plugin (a machine-wide `docker plugin install`), the collector's
`filelog` receiver over `/var/lib/docker/containers` (a host mount, root, and an
assumption about the log driver), and a shared volume the services redirect
stderr into and the collector tails. This lab picked the third, and the third is
now gone: no volume, no `>> /var/log/flowstate/…`, no `filelog` receivers, and no
`entrypoint: ["/bin/sh", "-c"]` wrapper that existed only so a redirection could
be spelled.

Two things came back with it.

**`docker compose logs` works again.** The stated cost of the old design was that
`docker compose logs flowstate-server` showed nothing, because stderr was a file.
stderr is stderr:

```console
$ docker compose -f examples/observability/docker-compose.yaml logs -f flowstate-worker
```

**Nothing is parsed at query time.** The old pipeline shipped slog's logfmt as
opaque text and left Loki to parse it with `| logfmt`. An OTLP record arrives with
its attributes already structured, so the dashboard's query is a bare stream
selector.

### What Loki no longer sees, said plainly

The exporter carries what Flowstate writes through `slog`, which is the server's
and worker's own commentary and every `log:` step. It does not carry two things
that used to reach Loki because they landed on the same stderr:

- **The Temporal SDK's own logger.** The Temporal client is not given a `slog`
  logger, so its lines — poller errors, task failures the SDK reports itself — go
  to stderr and stop there.
- **Four `log.Printf` warnings** in `cmd/flow`, about telemetry that could not
  start and about talking to a server over plain HTTP.

Both are in `docker compose logs`. Wiring the Temporal SDK's logger into `slog` is
a real improvement and a separate change; until then this is the boundary, and it
is better stated here than discovered by someone grepping Loki for a line they
watched scroll past.

## The walkthrough

Everything below is meant to be pasted. It runs from the repository root.

### 1. Bring the lab up

```console
$ docker compose -f examples/observability/docker-compose.yaml up -d --build
```

First run builds the Flowstate image from this repository's `go.mod` (one build,
two services) and pulls six images. Wait for Temporal to report healthy — the
Flowstate services depend on it and will not start before it does:

```console
$ docker compose -f examples/observability/docker-compose.yaml ps
```

### 2. Run a workflow from your host, against the lab

```console
$ OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
    go run ./cmd/flow run examples/observability/workflow.yaml --address 127.0.0.1:9233
```

Three things about that command line:

- **`OTEL_EXPORTER_OTLP_ENDPOINT` is what makes the trace start at you.** Without
  it the client is silent by design, and the trace starts at the server instead —
  which is a strictly less interesting picture and the whole reason the client
  half of the propagator exists.
- **`--address 127.0.0.1:9233` is already the default** (`localhost:9233`), so you
  can leave it off. It is spelled out because the variable that would set it,
  `FLOWSTATE_ADDRESS`, means *where to listen* to `flow server` and *where to
  connect* to every client verb — the compose file sets it to `0.0.0.0:9233`
  inside the server's container for exactly that reason, and setting it in your
  own shell would point your client at a socket rather than at the published port.
- **`go run ./cmd/flow`** so you are exercising your working tree. A built `flow`
  on your `PATH` works identically.

It prints the run's identity before it starts following it:

```
started flowstate-workflow-8b1f… ; come back to it with `flow watch flowstate-workflow-8b1f…`
```

Copy that workflow id. It is the string that joins the two worlds.

### 3. Find the trace in Grafana

Open <http://127.0.0.1:3000/explore>, choose the **Tempo** datasource, and search:

```
{resource.service.name="flowstate"}
```

`flowstate` is the CLI — the service name compiled into the binary, which the
compose file deliberately does *not* override on your host. One trace, and it
should contain three services:

| Service | Spans you will see |
| --- | --- |
| `flowstate` | the client's RPC span for `flowstate.v1.WorkflowService/Run`, and `StartWorkflow:Run` |
| `flowstate-server` | the server side of the same RPC, as a child |
| `flowstate-worker` | `RunWorkflow:Run`, then `RunActivity:TaskAuthorized` once per step |

That is the claim the lab exists to check: one trace id from the person to the
step. If the worker's spans are a separate trace, the Temporal tracing
interceptor is not installed on both the client and the worker — it has to be
both, and only one of the two is a silent failure.

To go the other way — from a workflow id you already have to its trace — Temporal
tags every span it creates, so this is a TraceQL query:

```
{span.temporalWorkflowID="flowstate-workflow-8b1f…"}
```

### 4. Find the same run in the Temporal UI

<http://127.0.0.1:8233/namespaces/default/workflows/flowstate-workflow-8b1f…>

Same workflow id, no translation. You are looking at the durable history — every
step as an activity, the `sleep: 2s` as a real timer, the fan-out as three
scheduled activities — beside a trace of the same run in Grafana. `RunWorkflow:Run`
in Tempo and `Run` in the Temporal UI are the same execution seen from two sides.

### 5. Look at the dashboard

<http://127.0.0.1:3000/d/flowstate-lab>

Provisioned from `grafana/dashboards/flowstate.json`, and built only from metrics
Flowstate already emits — nothing was added to the engine to make a panel work:

- **Runs started** and **RPC latency p95**, from otelconnect's instruments on the
  control plane (`rpc.server.duration`, recorded in milliseconds, which is why the
  Prometheus series is `rpc_server_duration_milliseconds_*`).
- **Run duration end to end** and **step execution latency**, from the Temporal
  SDK's own histograms (`temporal_workflow_endtoend_latency`,
  `temporal_activity_execution_latency`, both in seconds). A Flowstate step that
  does work is a Temporal activity, so activity latency *is* step latency.
- **Workflows completed / failed.** These are up-down counters in the SDK, so the
  Prometheus exporter writes them as gauges: no `_total` suffix, and `rate()` is
  not meaningful on them. The value is cumulative since the worker started.
- **Everything the SDK is actually emitting**, a table of the raw `temporal_*` and
  `rpc_*` series. Metric names pass through three components that may each append
  a unit or a suffix, so when a panel is empty this is how you tell "not measured"
  from "spelled differently". The same list is at
  <http://127.0.0.1:8889/metrics>.
- **Logs**, from Loki, as OTLP records rather than parsed text.

Log-to-trace correlation is by trace id, in both directions, and it is worth
knowing exactly how far it reaches.

A `log:` step's record **carries the trace and span ids of the step it ran in**.
The task emits through its context, and on the worker that context is the
activity's — which holds the span Temporal's tracing interceptor opened. So a line
in the log panel has a **TraceID** field that opens the trace, and a span in Tempo
links to the lines that run produced. That is the join the lab could not make
before, when the honest answer was "the same service, around the same moment" and
the workflow id was the only real key.

The server's and worker's **own** lines carry no trace id, and cannot: a worker
saying it is starting up is not inside anybody's request. Find those by service
and time. A `log:` step in `flow run local` carries none either — the local driver
makes no RPC and opens no span, so there is no trace for the line to belong to,
though the record is still exported.

The workflow id still works as a key and is still the string that joins Grafana to
the Temporal UI. It is no longer the only one.

### 6. Tear it down

```console
$ docker compose -f examples/observability/docker-compose.yaml down -v
```

`-v` matters: the Temporal database and three time-series stores are in named
volumes and none of them are bounded. (The log-file volume is gone — logs go over
OTLP now, so there is no file to grow.)

## What CI checks, and what it does not

CI validates that the compose file parses and resolves:

```console
$ docker compose -f examples/observability/docker-compose.yaml config -q
```

That is a real check — it catches a malformed YAML anchor, an unknown key, a
service referring to a volume that does not exist — and it needs no Docker daemon
and pulls no images. It is not a smoke test. Standing the full stack up in CI
would mean pulling seven images and waiting on a Temporal cluster for a signal
about eight upstream projects rather than about this repository.

**The full-stack smoke is a manual step**, and it is the walkthrough above. Run it
when you change anything in this directory. The two failure modes it catches that
`config -q` cannot are an image tag that no longer resolves, and a backend config
file that a new version of its own service has stopped accepting.

The image tags here are pinned deliberately — a lab that drifts underneath you is
worse than no lab — but a pin is a claim about a registry, and the check that
matters is the walkthrough. If a tag has moved on, change the one line and say so
in the table above. For a lab you intend to keep, pin by digest instead
(`image: grafana/tempo@sha256:…`), which is the same argument
`.github/workflows/ci.yml` makes about pinning actions to a commit.
