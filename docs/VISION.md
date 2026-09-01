# Where this is going

Directions decided or seriously entertained, recorded so they survive the people
and sessions that had them. Distinct from [DSL.md](DSL.md), which records language
decisions with their reasoning, and from [ARCHITECTURE.md](ARCHITECTURE.md), which
describes what is built: this file holds intent that is *not built yet* and should
shape work done in the meantime. When an item lands, its entry moves into the
document that owns the shipped thing, and a decision it forced gets recorded where
decisions live.

The standing rule from DSL.md applies here doubly: everything below is a claim
about the future, which is the least reliable kind. Nothing in this file justifies
building a capability that is not reachable from a Flowfile, and nothing in it may
be cited as though it were shipped.

## What the platform is for

Not a CI system — the engine targets anything that has to finish correctly
despite crashes, network failures, and long waits. The workload shapes to design
toward, beyond the obvious pipelines: security orchestration and response,
agentic investigations, business processes with humans in the loop, chat-driven
operations (Slack/Discord bots as both trigger and approval surface), and
LLM-driven workloads — including the platform powering an agentic system itself,
with an LLM provider or a coding agent (Claude Code, Codex) as a step a durable
workflow drives, retries, and gates. The engine's durable waits, approval
signals, and policy surfaces are precisely the parts an agentic system lacks;
the bet is that a policy-governed durable substrate is what makes agents safe to
operate.

## The plugin ecosystem

The built-in registry stays small — the admission test in DSL.md holds — so the
breadth lives in plugins, spelled `<plugin>.<task>:`. Six ship in-tree and so are
no longer listed here — `codex`, `git`, `github`, `slack`, `sql`, `vcs`; the
README's *Extend* row and [PLUGINS.md](PLUGINS.md) are the record. Still wanted,
each landing with a worked example verified in CI:

- **discord** — post, as `slack.post` already does; and for both channels the
  signal bridge (#96), so an approval gate is answered from the channel where the
  question was asked.
- **grpc** — the generic caller for the unary row of the interaction-shape table;
  the streaming rows stay refused until they have an execution model.
- **vault / openbao** — the canonical *secrets* plugin example (the in-tree
  `secrets/vault` package is the in-process ancestor).
- **1password** — exists in-tree as a provider; needs local verification against
  the real agent before it is claimed.
- **docker, or a sandbox-provider plugin** (Modal or similar) — a place to run
  untrusted work that is not the worker's own host; pairs with `exec:`'s policy.
- **github-actions** — bidirectional integration, deliberately weird: GHA as a
  trigger source and as a target, so each system can gate the other. The in-tree
  `github` plugin covers issues and pull requests; the Actions half is what remains.
- **llm** — a provider-agnostic completion/tool-call task, which is what the
  agentic workloads above stand on.

## Remote plugin distribution (not now, but do not foreclose it)

Today a plugin is a local executable on an explicit search path, and that stays
the default posture forever. The direction to keep open: fetchable plugins with
local caching, modeled on the Go module system's git-backed design — version
pinning, content addressing, and eventually a sumdb-like transparency log — or
built on OCI artifacts instead, which brings digests, registries, and signing
(sigstore) for free. Leaning OCI. Explicitly **not** go-getter. A deployment
would choose its posture: local-binaries-only, or a configured allowlist of
fetchable plugins. The dotted task namespace, discovery-by-explicit-path, and
the handshake's verification are the hooks this builds on; the `plugins:` header
reserved for Phase 3 is the natural home for version minimums.

## Remote plugins as services, and MCP in both directions

A plugin is Connect RPC over a Unix socket, which means nothing about it is
inherently local: the same three services over TLS to a remote host is a
*hosted* plugin, authenticated with OAuth2/OIDC (3-legged where a human's
consent is in the loop) instead of the local handshake token. That is the same
shape as MCP servers, and MCP should be first-class in both directions. The
serving direction landed — `flow mcp` over stdio and the authorized HTTP subset of
`flow mcp serve`, with the tool roster derived from the service descriptor (see
[CLI.md](CLI.md) and the generated-ecosystem note below) — so what this file still
holds is the other direction: consuming MCP services as capability providers (#108).

## Policy on plugins, and identity all the way down

The identity that authenticates a run should flow coherently through everything
the run touches, with delegation done to the RFC (8693 `act` chains — the auth
package already implements the exchange), so a downstream system sees both what
is acting and on whose behalf. On top of that identity, an admin applies CEL
policy *to plugins*: which namespaces may call a plugin's tasks, what inputs may
contain, redactions and transformations on what a plugin sees — the same
one-policy-language argument netpolicy and the secret policy already make,
extended to the third-party boundary. Open question, deliberately unanswered:
do plugins get OIDC identities of their own, a scoped subset of the worker's, or
none — and what does a *remote* plugin present back to us?

## Webhooks, in both directions

Workloads live among systems that speak webhooks, and both directions belong
here. Inbound: Slack, Discord, Jira, a GitHub App, or an arbitrary system posts
an event; signature verification is a first-class necessity (per-provider HMAC
or asymmetric schemes, timestamp and replay windows), and a verified event
feeds the trigger mechanism and bridges into signals — a webhook answering a
`wait_for_signal:` gate is the human-in-the-loop story meeting the systems the
humans already use. Outbound: a workflow sends a webhook with the secret store
doing the signing and auth, scoped by least privilege without making the easy
case hard. The plugin protocol grows a webhook *capability* beside secrets and
tasks, so an integration plugin can receive (verify, normalize, route) and
send for its own provider; where verification helpers live — CEL function,
task, or the capability contract itself — is decided by what keeps the DSL
ergonomic and the trust boundary clean. All of it coherent with the identity
model, observable, and fail-closed: an unverifiable event is not an event.

## Security posture

The gating concern for all of the above, not a follow-up. Sandboxing for plugin
and `exec:` workloads beyond process isolation — filesystem, network, and
syscall confinement, possibly via the sandbox-provider plugin shape. Secret
material zeroed where the runtime allows it — verify what the Go runtime actually
ships before relying on it; the closure-holding design exists because strings
cannot be wiped. Everything fail-closed, every parser bounded, per CLAUDE.md.

JIT access used to be listed here and is not any more, per this file's own rule:
short-lived, audience-scoped credentials via workload identity federation landed,
with the broker in the worker where the credential is used. See the outbound half
of [ARCHITECTURE.md](ARCHITECTURE.md#identity-in-both-directions) and
`examples/http-federated/`.

## The observability lab, and Flowstate as an OTLP citizen

The telemetry hooks landed for the server and worker (metrics and traces behind
OTEL_EXPORTER_OTLP_*, structured logs), and client-to-server trace propagation
followed: client commands initialize the tracer and the W3C propagator when the
same variables opt in, flush before exiting, and a trace now begins at the person
running `flow run` rather than at the server. The direction from here is to make Flowstate a
first-class citizen of a modern OpenTelemetry stack, and to prove it end to end.

All three signals over OTLP used to be listed here and is not any more, per this
file's own rule: logs ship through the OTLP log exporter alongside metrics and
traces, so a collector is the single ingress for everything and the lab no longer
tails a file into Loki. An `otelslog` bridge sits *beside* the stderr handler
rather than replacing it — adding a collector gains a destination and does not
cost the terminal — and OTEL_EXPORTER_OTLP_LOGS_ENDPOINT now counts as telemetry
being configured, which it deliberately did not while nothing exported logs.

What that bought, and its edge: a `log:` step on the worker emits with the
activity's context, so its record carries the trace and span ids of the step's own
span and a log line is one click from its trace in both directions. The same is
true under `flow run local` since #523's gap 3 gave that driver the identical
task span. Lines emitted outside a span — the server's and worker's own start-up
commentary — are exported and searchable but carry no trace id. The remaining rung is the Temporal SDK's own
logger, which is not a `slog` logger and so still reaches stderr only.

What is left:

- **Correlated with Temporal.** Temporal emits its own traces and metrics;
  the goal is that a span from `flow run` threads through the server, into the
  workflow and its activities, and is visible both in Grafana Tempo and clickable
  from the Temporal UI — one trace id, both worlds. Follow Temporal's own OTel
  guidance (the interceptor and tracer contract) so the correlation is theirs to
  keep working, not ours to reinvent.
- **A docker-compose lab** under `examples/` (or `deploy/`) that stands up the
  full modern Grafana stack — Grafana, Tempo (traces), Loki (logs), Prometheus or
  Mimir (metrics), the OpenTelemetry Collector as the fan-in — alongside a
  Temporal dev server and a Flowstate worker and server, so `docker compose up`
  gives a working lab that shows metrics, logs, and traces correlated by trace id,
  with a provisioned dashboard. Research the current recommended stack before
  building; the OTel Collector config is where the cohesion lives. Largely landed
  in `examples/observability/`, and now genuinely correlated by trace id rather
  than by workflow id; what is left is the manual smoke being manual.
- **Plugins for the stack itself**, deliberately and slightly cursed: a Temporal
  plugin, a Grafana plugin, a Loki plugin, so a Flowstate workflow can query and
  act on the very systems observing it. The circularity is the point when threaded
  carefully — a workflow that reads its own traces from Tempo, or files a Grafana
  annotation, is powerful precisely because it closes the loop. It composes with
  the plugin ecosystem and must stay optional.
- **Progressive adoption.** None of this is required to start: stderr logs and no
  exporter is the zero-config default. An operator adds the collector endpoint,
  then the lab, then the plugins, one rung at a time — and the examples prove each
  rung works rather than asserting it.

## The generated ecosystem

The generated ecosystem used to be listed here and is not any more, per this
file's own rule: reference docs generated from the descriptors, the registry
and the cobra tree landed under `docs/reference/`, pinned in CI with
`git diff --exit-code`; `flow mcp` derives its tools from the service schema
with no hand-kept list (a test holds the two together in both directions); and
the catalog RPC is the one answer every consumer reads. See
[CLI.md](CLI.md) for the surface and `cmd/flow/internal/docsgen` for the mechanism.
One piece of guidance survives because it is not yet needed rather than done:
custom protoc options are available if plain leading comments stop being enough
— but check first whether SourceCodeInfo survives to where the generator runs,
because it does not survive into the compiled-in descriptors.
