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
| OTel Collector | `otel/opentelemetry-collector-contrib:0.140.0` | The fan-in for all three signals | <http://127.0.0.1:4318> (OTLP/HTTP) |
| Temporal | `temporalio/admin-tools:1.29.0-tctl-1.18.6-cli-1.5.0` | `temporal server start-dev` — frontend and UI | <http://127.0.0.1:8233> (UI) |
| Flowstate server | built from this repo | The control plane `flow run` talks to | <http://127.0.0.1:9233> |
| Flowstate worker | the same image | Where workflows and steps actually execute | — |

Eight services, seven of them things you did not write. Two decisions are worth
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
- **The Temporal database and the log files are unbounded**, in a named volume,
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
    command:
      - exec flow worker --address=temporal:7233 --deployment-name=flowstate --build-id=lab-1 --verbose >> /var/log/flowstate/worker.log 2>&1
```

Bump `--build-id` whenever you rebuild, and watch runs stay pinned to the version
they started on until continue-as-new. That is worth doing once, in a lab, before
it matters.

## How logs get to Loki

Flowstate writes structured logs to stderr. It does not speak OTLP for logs yet —
that is the next rung in [VISION.md](../../docs/VISION.md) — so something has to
carry stderr into the collector. Three options, and the lab picks the third:

1. **The Loki Docker driver plugin.** Requires `docker plugin install` on the
   host: a machine-wide mutation, a prerequisite outside the compose file, and a
   failure mode where `docker compose up` breaks for every service because a
   plugin is missing.
2. **The collector's `filelog` receiver over `/var/lib/docker/containers`.**
   Requires mounting a host path, running the collector as root to read it, and
   assuming the `json-file` log driver. Three assumptions about the host.
3. **A shared volume the two Flowstate services write to and the collector reads
   read-only.** No host mount, no Docker socket, no root, no plugin, and it
   behaves identically on Docker Engine, Docker Desktop and Podman.

The cost of (3) is stated plainly: stderr is redirected into a file, so
`docker compose logs flowstate-server` shows nothing. Read the logs the way the
lab intends — in Grafana — or directly:

```console
$ docker compose exec flowstate-worker tail -f /var/log/flowstate/worker.log
```

The redirection uses `exec` so that `flow` is pid 1 and receives the `SIGTERM`
from `docker compose down`. A pipeline into `tee` would keep both outputs but
leave the *shell* as pid 1, and a shell does not forward signals — the process
would be killed at the grace period instead of flushing its last spans, which are
precisely the spans somebody is looking at a trace to find.

Nothing parses the lines at ingest. They are slog's logfmt and Loki parses logfmt
at query time (`| logfmt`), so a parser in the collector would be an untested
moving part in the ingest path buying a conversion that is free later. When
Flowstate grows an OTLP log exporter, this whole rung — the volume, the
redirection, the two `filelog` receivers — deletes itself.

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
- **Logs**, from Loki, parsed with `| logfmt` at query time.

Log-to-trace correlation is honest about its limits: the Tempo datasource links a
span to the logs of the same service around the same moment, and *not* by trace
id, because Flowstate's stderr lines do not carry one yet. Correlate on the
workflow id, which both the logs and the Temporal UI use, until the OTLP log
exporter lands.

### 6. Tear it down

```console
$ docker compose -f examples/observability/docker-compose.yaml down -v
```

`-v` matters: the Temporal database, the log files, and three time-series stores
are in named volumes and none of them are bounded.

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
