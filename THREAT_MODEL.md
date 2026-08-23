# Threat Model

Read this before you copy anything in `examples/` into production.

This document states what Flowstate defends against today, what it does not, and
which gaps are specified but unbuilt. Every claim about present behavior names the
file and line that makes it true. Every claim about future behavior names an issue
and says plainly that it has not landed. Where a control is real but narrower than
its name suggests, that is said here rather than left for someone to discover.

Two companion documents carry part of this load and are not repeated:
`docs/DEPLOYMENT.md` holds the four-tier isolation model and the per-tier checklist
of what each topology actually provides, and `docs/ARCHITECTURE.md` holds the
invariants a change is measured against. This document is the composed adversary
analysis that neither one is.

## 1. The system and its trust boundaries

Flowstate compiles a YAML-plus-CEL Flowfile into a typed Protobuf specification and
executes it, either in the calling process (`flow run local`) or durably on Temporal
through a control plane (`flow server`) and a worker fleet (`flow worker`). Tasks
are the only thing that touches the outside world; today the built-in set is `log`
and `http` (`pkg/flowstate/v1/eval_task_library.go:35`,
`pkg/flowstate/v1/eval_task_http_def.go:66`), and everything else
is a plugin, which is a separate process speaking Connect RPC over a Unix socket.
Policy surfaces (authentication, egress, secret access, task shape, signal
authorization) are CEL over deployment-supplied configuration files, evaluated
outside the workload.

The boundaries, in the order an attacker meets them:

- **Author to deployment.** A Flowfile says what should happen. Where it may reach,
  as whom, and with what is deployment configuration the file cannot name.
- **Caller to server.** A caller presents a bearer token; the server decides whether
  it may start or address a run, and which tenant it is.
- **Server to worker.** The server records ownership and routes; the worker executes
  and holds the material.
- **Worker to plugin.** A separate process, launched by the worker, reached over a
  socket only the worker can open.
- **Plugin (and task) to network.** Outbound requests from a task, governed by
  egress policy.
- **Tenant to tenant.** Namespaces scope runs, secrets, egress rules, credential
  assumption, and scheduling.
- **Run to history.** What a run writes into Temporal history is durable and read by
  anyone with substrate access.
- **Editor and agent tooling to workspace.** `flow lsp`, `flow validate`, `flow fix`
  and `flow mcp` run on an author's machine against files an author may not have
  written.

## 2. Assets

1. **Workflow history.** Durable, and broadly readable by anyone with Temporal
   access to the namespace: the full compiled specification, every step's inputs and
   outputs, identity claims, and memos (`docs/DEPLOYMENT.md:11-33`).
2. **Secrets.** Values resolved worker-side, inside the activity that needs them,
   never carried in the spec or in history (invariant 7,
   `docs/ARCHITECTURE.md:154-157`).
3. **Identity keys and issuer material.** The private key Flowstate signs workload
   assertions with, and the trust policy naming which issuers it believes.
4. **Deployment policy files.** The crown jewel. `--auth-policy`, `--egress-policy`,
   `--task-policy`. Whoever can write these decides who is authenticated, which
   tenant they are, which secrets resolve, which tasks dispatch, and where a run may
   connect. Compromise here subsumes every other control in this document.
5. **Plugin binaries.** Launched code running with the worker's authority.
6. **Examples and walkthroughs.** Files people copy. `examples/observability/`
   deliberately runs `--insecure-no-auth` and `--allow-unversioned-interpreter`
   (`docs/DEPLOYMENT.md:313-319`), which is correct for a demo and wrong everywhere
   else.

## 3. Adversaries and their reach

**A malicious workflow author inside a tenant.** Can write any legal Flowfile and
submit it as themselves. Cannot choose their own tenant: namespace comes from the
authenticated caller and is recorded at submit
(`pkg/flowstate/v1/server/server.go:234`, `:710`). Reaches whatever the deployment's
egress, secret, and task-shape policies permit that tenant, and nothing else. Can
consume budget: a legal Flowfile may `sleep: 24h`, loop to its declared
`max_iterations`, or fetch large bodies.

**A co-tenant.** Cannot address another tenant's runs through the API: one `ownedBy`
check covers every addressed verb and answers `NotFound` rather than
`PermissionDenied`, so a probe learns nothing (`pkg/flowstate/v1/server/lifecycle.go:46`,
`:85`, `:104`). Can read another tenant's history if both execute in the same
Temporal namespace and they have substrate access, because the Flowstate API's
tenancy governs the Flowstate API and nothing downstream of it.

**A network attacker on egress paths.** Sees and can answer requests a task makes.
Bounded by categorical address denial, per-hop redirect re-checks with the https to
http downgrade refused, and a response body cap
(`pkg/flowstate/v1/netpolicy/netpolicy.go:9-20`, `:499`,
`pkg/flowstate/v1/netpolicy/options.go:35`, `:38`). The TLS floor (1.2 or better,
verified certificates) applies only to https requests, and the default scheme set
allows plain `http` too (`pkg/flowstate/v1/netpolicy/options.go:110`), so a workflow
that asks for an `http://` URL gets traffic an on-path attacker can read and modify.
A deployment that wants the TLS guarantee on every request restricts `schemes` to
`https` in its egress policy. Either way the attacker can return hostile content,
which becomes step output, which becomes history and possibly an agent's context.

**A compromised or malicious plugin.** Holds the worker's process authority. This is
not contained and is not claimed to be: separate processes buy protection against a
crash or a runtime bug, not against code doing deliberately what its author wrote
(`docs/ARCHITECTURE.md:470-481`, `docs/DEPLOYMENT.md:35-51`). It reaches every
tenant that worker serves. It does not inherit the worker's environment: plugin
environments are built from nothing (`pkg/flowstate/v1/plugin/launch.go:500`).

**A caller with a stolen token.** Acts fully as that principal until the token
expires. Bounded by audience (`pkg/flowstate/v1/auth/policy.go:88`), by optional
`MaxTokenAge` on the issuer entry (`:156`), and by clock skew and token size limits
in the verifier (`pkg/flowstate/v1/auth/verifier.go:55`, `:59`). No revocation list
exists in this tree.

**An agent driving authoring or execution surfaces with injected instructions.** See
section 5. Reaches whatever `flow mcp`'s process configuration permits, which is
decided at process start-up and never per call (`cmd/flow/mcp.go:647-694`).

**An insider with history read access.** Reads everything in asset 1 for every
tenant in that namespace. Nothing in Flowstate mitigates this today; the encryption
seam that would is specified in #353 A.1 and #113, not landed
(`docs/ARCHITECTURE.md:663-678`, #271 tracks the gap).

## 4. Per boundary: enforcement, limits, planned hardening

### Author to deployment

**Today.** A Flowfile cannot name its tenant or a fairness weight, and it cannot
forge a sender: the identity a signal arrives with is attested by the server from
the authenticated caller, never claimed by the file or the payload. What the file
*does* control is the gate itself: top-level `signals:` is author-written policy,
an author may weaken or delete it, and a signal name with no policy admits any
authenticated caller in the tenant. An approval gate is therefore author-declared
and deployment-attested, not author-proof; the author-proof layer is deployment
policy of the #187 shape, which can require that a workflow of a given shape carry
the gate. Egress, secret access, and task shape are configured on the worker and on
the server, never in the file. `flow validate` reports properties of the file and stays silent
about deployment decisions, deliberately, so a diagnostic never asserts a rule the
author's machine may not share (`docs/DEPLOYMENT.md:134-152`,
`pkg/flowstate/v1/eval_task_http_check.go:17-25`).

**Limits.** Task-shape policy is opt-in: a nil policy permits every task
(`pkg/flowstate/v1/taskpolicy.go:133-146`). This is the documented zero case, not
fail-closed, and it differs on purpose from secret rules, whose absence permits
nothing (`pkg/flowstate/v1/auth/secretpolicy.go:50-60`). A deployment that has not
written a task policy has not restricted task shape at all.

**Planned.** Isolation tiers with per-task minimums, #341 E, not landed. Plan
capability and approval bound to a plan digest, #341 D, not landed.

### Caller to server

**Today.** Bearer token verified against a trust policy that is data, not code
(`pkg/flowstate/v1/auth/policy.go:24`). An empty policy trusts nobody; a missing
verifier rejects everything (`pkg/flowstate/v1/auth/connect.go:170-177`). Only
asymmetric algorithms are verifiable, so no HMAC token can be checked against an
issuer's public key (`pkg/flowstate/v1/auth/policy.go:192-231`). A verified caller
whose namespace claim is missing or fails the namespace grammar is rejected, never
admitted to a default tenant (`:119-142`, `:345`). Unauthenticated error text never
describes the trust policy (`pkg/flowstate/v1/auth/connect.go:113-120`). Submitted
specifications are size-bounded at submit (`pkg/flowstate/v1/size.go:39`, `:103`),
and `List` is bounded by executions read and by requests made
(`pkg/flowstate/v1/server/list.go:51`, `:63`).

**Limits.** `flow server` speaks cleartext HTTP only: there is no TLS flag and no
`ListenAndServeTLS` call, so the bearer token is on the wire in the clear unless TLS
is terminated in front of it (`docs/DEPLOYMENT.md:490-496`,
`cmd/flow/main.go:719`). The CLI refuses to send a token over plaintext to anything
but this machine (`cmd/flow/credentials.go:63`), which protects the client, not the
server's own posture. `--insecure-no-auth` admits everyone as anonymous and is a
development posture (read at `cmd/flow/main.go:148`, resolved to
`auth.InsecureAnonymousVerifier` at `cmd/flow/main.go:767`;
`pkg/flowstate/v1/auth/connect.go:142-160`, `docs/DEPLOYMENT.md:306-311`).

**Planned.** OAuth 2.1 alignment for the remote MCP surface and webhook ingress as
attested signals, #337, not landed.

### Webhook sender to server

**Today.** Mounted only when a deployment passes `--webhook`, at
`POST /webhooks/<workflow>/<trigger>` (`cmd/flow/routing.go`,
`pkg/flowstate/v1/server/webhook.go`). A sender authenticates by signing the raw
body under the key the trigger's `verify:` names, resolved through the deployment's
secret providers when the flag is read rather than per request; a scheme this build
cannot check, or a key this deployment cannot resolve, stops the server. Comparison
is constant-time (`hmac.Equal`) and the key is revealed only into an HMAC. The body
is capped by `http.MaxBytesReader` as the first statement of the handler, so no path
below it can read past `v1.MaxWebhookPayloadBytes`; deliveries in flight are bounded
and shed with a 503 past the bound; `with:` evaluation is bounded by the CEL cost
limit; candidate signatures per header are bounded. Every refusal decided before a
delivery is known genuine — unknown workflow, unknown trigger, bad signature — is one
status and one sentence, with an HMAC spent on the unrouted path so the timings
match. A run's id is a digest over tenant, workflow, trigger and idempotency key, so
a redelivery joins rather than duplicating and a key cannot address another tenant's
run or be read back out of the id.

**Limits.** The route is cleartext like the rest of the server, so a signature and
body are on the wire in the clear unless TLS is terminated in front. Verification
proves possession of a shared key, not the sender's identity: anyone holding the key
can deliver. `--secret-require-namespace` is incompatible with the receiver, which
resolves in the deployment's own tenant.

**Planned.** Per-trigger rate limits, enable and disable per deployment beyond the
flag, and stored deliveries for replay, #490, not landed.

### Server to worker

**Today.** Tenant is recorded in the run's memo at submit and authorized against on
every later request (`pkg/flowstate/v1/server/server.go:234`, `:710`,
`pkg/flowstate/v1/server/lifecycle.go:85`). With `--task-queue-prefix` and
`--tenant`, each tenant's runs go to a queue derived from the authenticated tenant,
and a worker refuses a run belonging to anyone else; the composed queue name cannot
be forged across a tenant boundary because the separator is the one character the
namespace grammar forbids (`docs/DEPLOYMENT.md:186-223`). Signal authorization is
per name, checked against the run's declared policy and its recorded starter, and
fails closed on an unreadable memo or a missing starter where the policy demands the
comparison (`pkg/flowstate/v1/server/lifecycle.go:140-234`,
`pkg/flowstate/v1/signalpolicy.go:332`).

**Limits.** Mapping completeness is a warning, not a refusal: a tenant routed to a
queue nothing polls gets runs that sit RUNNING with nothing wrong reported
(`docs/DEPLOYMENT.md:235-251`). Fairness keys are set correctly; whether they are
enforced is a property of the Temporal deployment (`docs/DEPLOYMENT.md:517-532`).

**Planned.** Pending-wait reporting so a surface can show what a run is blocked on,
#347, not landed. Gate prompts and starter exposure so an approver sees what and
whose before clicking, #348, not landed.

### Worker to plugin

**Today.** `--plugin-dir` names an explicit local path an operator controls; a
relative search path, or one writable by any user other than its owner (group
or world), is refused, with
`--allow-insecure-plugin-dir` as the named escape hatch
(`pkg/flowstate/v1/plugin/doc.go:50-52`, `docs/DEPLOYMENT.md:508-515`). Plugin
environments are built from nothing rather than inherited
(`pkg/flowstate/v1/plugin/launch.go:500`). Secret inputs a plugin task consumes are
resolved host-side, before `Execute`, and only for inputs the `TaskManifest` named;
an unnamed input is refused (`docs/ARCHITECTURE.md:432-448`). Responses from a
plugin are byte-bounded at the RoundTripper, below the RPC library, so no error path
the library treats specially can miss the cap
(`pkg/flowstate/v1/plugin/transport.go:124`, `CLAUDE.md`). The distribution digest a
run is pinned to is taken from the same open descriptor the process is executed
through, on Linux via `/proc/self/fd`, so a binary *replaced* between the hash and
the exec — written beside and renamed over, which is how software on disk ordinarily
replaces itself — cannot make the recorded provenance describe bytes that never ran
(`pkg/flowstate/v1/plugin/image.go`). The descriptor pins the inode rather than its
contents, so a writer who modifies that inode *in place* in the same window can still
part the digest from what runs; this admits no new principal, because both the search
path and each binary in it are refused when they are writable by group or world
(`discover.go:75,111`), which leaves only the owner — the party who already chooses
what this worker executes. Sealing a private copy would close it and is not done: the
image is hashed as a stream precisely so that one very large plugin file cannot stop a
worker from starting, and copying it into memory to seal it reintroduces that.

**Limits.** Pinning the digest to the executed image is a Linux guarantee, and
not one it makes about every plugin. A platform with no way to execute an
already-open descriptor falls back to executing the path, which leaves the window
this closes; so does any image the kernel runs through an *interpreter* rather
than directly, because the kernel starts that interpreter and hands it the path to
reopen after the descriptor naming it is gone — and the bytes that then run are
the interpreter's, which nothing here hashes. `#!` is the familiar case and not
the only one: a `binfmt_misc` registration without the open-binary (`O`) flag
behaves identically for whatever format it claims, and the format it most often
claims is *foreign-architecture ELF*, which is how a multi-arch image runs
anything at all. Which images may be pinned is therefore an allowlist rather than
a list of known-bad markers a host can add to at any time, and it asks the two
questions the kernel asks: *would `binfmt_elf` claim this image* — every header
field whose failure returns `-ENOEXEC` and hands the file on to the next
registered format, not merely a class/byte-order/machine triple that a malformed
image also carries (#732, #741) — and *is `binfmt_elf` the format that is asked*,
which is read from the `binfmt_misc` registry, because a registration is offered
every image before the native loader is
(`pkg/flowstate/v1/plugin/execformat_linux.go`, `binfmtmisc_linux.go`). Anything
unproven, unparseable or unreadable takes the by-path fallback. The residual case
is a registry that cannot be seen: a container that does not mount
`binfmt_misc` is still subject to the host's registrations, and an absent registry
is not proof of an empty one. Every case launches, and
every case says which guarantee it is giving, and why, in a log line at every launch
(`pkg/flowstate/v1/plugin/image.go`, `image_linux.go`, `image_other.go`). An
operator who needs the strong guarantee ships a native binary. A launched
plugin is trusted code with the worker's authority. The output
scrubber matches known plaintext and is defeated by any deliberate transform:
base64, hex, a hash, splitting across two fields. It is a containment tier for
accidents and is explicitly not containment against an adversarial plugin
(`docs/ARCHITECTURE.md:450-481`, `pkg/flowstate/v1/secrets/scrub.go:56`).

**Planned.** Vetting or signing what runs before a binary is trusted with a socket,
#146, not landed. Isolation tiers with plugins declaring the tier they require and a
tier that cannot enforce the declared policy refusing to dispatch, #341 E, not
landed.

### Plugin and task to network

**Today.** The default egress policy denies loopback, private, link-local,
multicast, unique-local, CGNAT, unspecified, broadcast, reserved and
IPv4-translation ranges plus cloud metadata endpoints; disables proxies; requires
TLS 1.2 with verified certificates; bounds request, dial, handshake and header
phases; caps the body; and re-checks policy at every redirect hop, refusing an https
to http downgrade (`pkg/flowstate/v1/netpolicy/netpolicy.go:9-20`, `:155`, `:499`).
Address checks run in the dialer against the address actually dialed, so a
DNS-rebinding answer gains nothing (`:26-31`, `:441`). CEL rules are compiled and
type-checked when configuration loads, deny beats allow, and a rule that errors
denies (`:91-111`). Rules may key on the run's identity, including namespace, so one
worker can serve two tenants with different reach
(`pkg/flowstate/v1/eval_task_http_run.go:316`, `docs/DEPLOYMENT.md:153-166`).

**Limits.** This is enforced *inside* the Go `http` task
(`pkg/flowstate/v1/eval_task_http_def.go:30`, `:66`). That is honest exactly while
every task is our code, which is #341 invariant 1 stated as a limit: the moment a
task is a container running arbitrary code, in-process enforcement is theater,
because the code opens its own sockets. A plugin task's network access is the
worker's to govern by whatever the operating system provides, not by netpolicy. With
a proxy configured the dialer never sees the target, so the check weakens to a
pre-resolution one (`pkg/flowstate/v1/netpolicy/netpolicy.go:33-37`), which is why
proxies are off unless named.

**Planned.** Boundary enforcement compiling the same policy file to a network
namespace plus enforcing proxy, with a tier that cannot enforce refusing to
dispatch, #341 E, not landed.

### Tenant to tenant

**Today.** Namespace comes from the authenticated caller and is unforgeable by the
workload (`docs/ARCHITECTURE.md:502-513`). Secret providers are namespaced
explicitly and fail closed per backend: the env provider refuses a namespace it has
no configured prefix for, and prefixes are checked disjoint at construction rather
than derived, because every character legal in a prefix is legal in a name
(`pkg/flowstate/v1/secrets/env.go:56-89`, `:194-212`, `:213`). Secret rules and task
rules and egress rules all read the same identity object.

**Limits.** On one Temporal namespace, history is shared. On one worker, material is
shared: anything achieving code execution in a worker reaches every secret that
worker holds for every tenant it serves (`docs/DEPLOYMENT.md:48-51`). Per-tenant
history and per-tenant blast radius are Tier 2 properties requiring separate
namespaces and separate fleets, not policy-rule properties.

**Planned.** Per-tenant crypto-shredding keys, #353 A.2, not landed.

### Run to history

**Today.** Secrets stay references through compilation, submission and workflow-side
resolution; only the activity resolves the value (invariant 7). The `Secret` type
marshals redacted, refuses to deserialize, and redacts when formatted
(`docs/ARCHITECTURE.md:340-349`). Heartbeat phases are a closed vocabulary with no
constructor because heartbeat details are written into history
(`docs/ARCHITECTURE.md:235`). Containment is tested across `%v`, `%+v`, `%#v` and
`%s`, on the value, in a struct, and in a slice (`CLAUDE.md`).

**Limits.** Everything that legitimately goes into history goes in unsealed. History
confidentiality today is whatever the cluster's database and filesystem encryption
provide; every `DataConverter` in the tree is the default one and no seam exists for
an operator to supply a codec (`docs/ARCHITECTURE.md:663-678`). Nothing is
forgettable: there is no erasure path.

**Planned.** The codec slot wired into both drivers' client construction identically,
null codec by default, with failure-path encoding enabled whenever a codec is
configured, #353 A.1 (design record #113, gap #271), not landed. `flow shred` and
crypto-shredding, #353 A.2, not landed.

### Editor and agent tooling to workspace

**Today.** Validation performs no I/O on the keystroke path; `CheckURL` resolves
hosts only where a proxy is configured, which is why it is kept out of the validator
(`CLAUDE.md`, the diagnostics rule; `pkg/flowstate/v1/netpolicy/netpolicy.go:547`,
`pkg/flowstate/v1/eval_task_http_check.go:17-25`). `flow lsp` answers `unknown task` for
plugin tasks unless a person passes `--plugin-dir` on the command line their editor
starts the server with, because executing plugin binaries to check a file is not
something an editor or a cloned repository may decide
(`docs/ARCHITECTURE.md:406-425`). `flow fix` must know what the grammar binds; two
corruption classes are documented in `CLAUDE.md` and are the reason the rewriter
takes scope from where the engine evaluates a thing.

**Limits.** Opening a repository runs the language server against files the author
did not write. The bound is that nothing is executed and nothing is fetched, not
that the content is trusted.

## 5. Prompt injection through agent surfaces

Compose the pieces rather than treating this as a separate topic.

**What exists today.** `flow mcp` projects every WorkflowService RPC as a tool by
walking the service descriptor (`cmd/flow/mcp.go:32-48`), serves three embedded docs
resources whose content is the repository's own, compiled in through
`cmd/flow/internal/reference` (`cmd/flow/mcpresources.go:40-48`,
`cmd/flow/internal/reference/reference.go:38`), and offers `flowstate_run_local`,
which executes a model-composed Flowfile in this process.

**Where untrusted content enters.** Not from the docs resources, which are embedded
repo content. It enters as data a run fetches and returns: an `http:` step's response
body becomes a step output, which becomes part of the tool result the model reads.
A `log:` step's text is collected and returned as data rather than written to stdout,
because stdout is the MCP transport (`cmd/flow/mcp.go:815-821`). So the injection
path is content in, tool result out, model acts.

**Which bounds apply today.** Every reach-deciding knob is process configuration
taken at start-up, and a client speaking stdio never chooses any of it
(`cmd/flow/mcp.go:647-694`). Egress for `flowstate_run_local` defaults to *deny
everything*, stricter than the CLI's own default, on the argument that the caller is
not a person (`cmd/flow/mcp.go:709-744`). The call is wall-clock bounded, default two
minutes, because a workflow is untrusted input and `sleep: 24h` is legal
(`:684`). Arguments are refused on unknown fields (`:764`). Sensitive declarations are
honored in the tool result, because a leaked credential in a transcript is a leaked
credential (`:832-838`). Secret access, task shape and identity rehearsal are the
same policy files the worker loads (`:658-676`).

**The honest gaps.** The stdio posture is one credential and one configuration for the
whole process: the model does not present an identity, so nothing here distinguishes
a call the human intended from a call injected content induced. #350 states the same
property for the approval card from the other direction: the fragment carries no
authority, every card action is a tool call the server authorizes on its own terms,
and the attested-approver property is gated on the remote MCP surface (#337) and is
not claimed by v1. Both are the same fact: today's agent surface authenticates the
process, not the request.

**The future loop.** #341 invariant 2 names two agent postures. In the *transparent
loop*, every tool call is a step dispatch, so task-shape policy, egress, budgets and
brokered credentials all apply per call, and the trajectory is the workflow history.
In the *opaque delegate*, a whole agent runs as one sandboxed task and its trajectory
is internal. Neither the `llm` task nor the `mcp:` tool source exists in this tree
(#341 C, not landed; the built-in task set is `log` and `http`,
`pkg/flowstate/v1/eval_task_library.go:35`,
`pkg/flowstate/v1/eval_task_http_def.go:66`). Until #341 C and E land, the
transparent loop's per-call policy spine is a design claim and the opaque delegate
has no sandbox tier to run in. Batch approval bound to a plan digest, which is what
saves a human from per-call fatigue, is #341 D, not landed.

## 6. The governance recursion

Every control in this document is decided by a file: `--auth-policy`,
`--egress-policy`, `--task-policy`, the plugin directory's contents. Whoever can
change one of those changes who is authenticated, which tenant they are, what
resolves, what dispatches and where it may connect. Editing a policy file is
therefore a higher-privilege operation than anything the policy governs, and today
that operation is governed entirely by whatever protects the filesystem and the
deployment pipeline, which is outside Flowstate. The self-hosting answer #353 C
proposes is that policy changes ship as Flowstate runs: the change is a workflow
with an approval gate whose approver is attested, whose plan is a digest the apply
step refuses to diverge from, and whose verdict, allow as well as deny, is recorded
as evidence carrying the rule and the evaluated fact values (#353 principle 2 and
workstream D, #341 D and H). None of that is landed. Stated plainly: today, policy
change is unaudited by Flowstate.

## 7. The issuer as a single point of failure

**What compromise yields.** The issuer's private key signs the workload assertions
Flowstate presents outbound. Anyone holding it can mint an assertion for any
subject, namespace, workflow, run and step this deployment could, and present it to
every relying party that trusts the published key set. That is the outbound half of
the entire federation story in one file.

**What bounds it today.** Assertions are short-lived by default and cannot be
configured long: `DefaultAssertionLifetime` is five minutes and
`MaxAssertionLifetime` is one hour, so an assertion cannot become a standing grant
(`pkg/flowstate/v1/auth/issuer.go:29-36`). Claims the issuer sets itself are reserved
and cannot be carried from a submitting token, so a caller cannot choose the subject
of the assertion minted for it (`:53-59`). A key a deployment no longer signs with
stays published only for `DefaultKeyRetention`, twenty four hours (`:41-45`), whether
it was retired in process by `Issuer.Rotate` or named as a verify-only key at
start-up (`auth.WithVerifyOnlyKey`), which is the form an operator can actually
reach — see the runbook below. Which workload may assume which
target is CEL, evaluated under a cost limit
(`pkg/flowstate/v1/auth/assume.go:11-15`). Key custody is a PKCS#8 PEM file that
`flow keys` writes at mode 0600 and, everywhere except Windows, verifies after
writing (`cmd/flow/keys.go:228-280`). Two qualifications: on Windows the permission
check is deliberately skipped, because POSIX bits are synthesized there and access
is governed by filesystem ACL defaults the check cannot see; and on a filesystem
that reports a wider mode, the command errors without deleting the already-written
key, so the residual risk in both cases is a key on disk under whatever protection
the filesystem gave it, which custody procedures have to cover rather than assume
away. Inbound, the mirrored bound is audience plus optional `MaxTokenAge`
(`pkg/flowstate/v1/auth/policy.go:88`, `:156`).

**Rotating the issuer key.** Rotation is the whole response to a suspected key
compromise here, so it has to be a procedure someone can rehearse rather than a
method in the library. `Issuer.Rotate` rotates in process and no deployment calls
it; what an operator performs is a restart, and until #891 a restart published the
new key and nothing else — every assertion the previous process signed, still valid
for the rest of its five minutes, stopped verifying at any relying party that
refetched the key set. `--identity-key` therefore repeats, and the order is the
rule: **the first occurrence signs, and every later one is published for
verification only**, without its private half being retained. Nothing is derived
from the file name or its modification time.

```sh
# 1. Generate the new key. Naming the file names the published key id.
flow keys generate --out /etc/flowstate/keys/2026-09.pem

# 2. Restart with both, newest first. The process signs with 2026-09 and keeps
#    publishing 2026-08, so assertions the previous process signed keep verifying.
flow server --auth-policy /etc/flowstate/auth.yaml \
  --identity-key /etc/flowstate/keys/2026-09.pem \
  --identity-key /etc/flowstate/keys/2026-08.pem

# 3. After the retention window (federation.key_retention, default 24h, which has
#    to outlast both the old assertions and every relying party's cached key set),
#    restart with the new key alone and delete the old one.
flow server --auth-policy /etc/flowstate/auth.yaml \
  --identity-key /etc/flowstate/keys/2026-09.pem
```

The start-up line names what was actually published (`signing_key` and
`verify_only_keys`), so step 2 is verifiable rather than assumed. A key that cannot
be read or parsed, and two keys publishing one key id, refuse start-up rather than
being skipped: a key silently left out is a rotation the operator believes is
covered and is not. A verify-only entry may be the old private key file already
mounted, or just its public half as a PKIX PEM (`openssl pkey -in 2026-08.pem
-pubout`), which is the narrower custody choice.

Rotating is not revoking. Publishing the outgoing key for its retention is
precisely what keeps rotation from rejecting valid assertions, so it does nothing
about a key believed compromised. `Issuer.RevokeKey` (#897) withdraws a published
key immediately, and is — like `Rotate` — a library verb no deployment calls today;
the restart-shaped equivalent is to not name the suspect key at step 2 at all,
accepting that everything it signed stops verifying as each relying party's cached
key set expires.

**What is not bounded.** There is no revocation of an already-minted assertion inside
its lifetime, no threshold or HSM custody, and no separation between the codec keys
#353 A.1 would introduce and the signing keys here. #353's anti-goals state that
codec keys, signing keys and issuer keys are one custody design or the design is
wrong. The broader identity program is #337; none of its four directions beyond what
is already in `pkg/flowstate/v1/auth/` has landed.

## 8. Non-goals and honest gaps

**Non-goals.**

- Flowstate does not reimplement substrate isolation. Tier 3 is documented, not built,
  deliberately (`docs/DEPLOYMENT.md:278-290`).
- Flowstate is not an identity provider beyond its workload-identity broker (#337).
- No multi-region machinery; residency is declaration plus validation (#353 A.4).
- No app-layer rate limiter in front of `flow server`; the volume dimension is
  Temporal's namespace limits (`docs/DEPLOYMENT.md:534-542`).
- The scrubber will not be hardened toward adversarial transforms; doing so would
  spend effort on a tier it was never meant to occupy
  (`docs/ARCHITECTURE.md:460-466`).

**Honest gaps, all present-tense.**

1. No history confidentiality. No codec seam exists (#113, #271; #353 A.1 specifies
   it).
2. No erasure. Nothing forgets (#353 A.2).
3. `flow server` is cleartext HTTP; TLS must be terminated in front of it
   (`docs/DEPLOYMENT.md:490-496`).
4. Egress enforcement is in-process and therefore honest only while every task is our
   code (#341 invariant 1).
5. A launched plugin is trusted code with the worker's authority
   (`docs/ARCHITECTURE.md:470-481`).
6. Task-shape policy's zero case permits everything
   (`pkg/flowstate/v1/taskpolicy.go:133-146`).
7. Policy denials are recorded; allows are not, so a transcript cannot answer "why was
   this permitted" (#353 principle 2, workstream D).
8. The stdio agent surface authenticates the process, not the request (#350, #337).
9. No token revocation, inbound or outbound, within a credential's lifetime.
10. Windows is an authoring platform, not a worker platform; plugins are AF_UNIX only
    (`docs/DEPLOYMENT.md:499-507`).
11. Fairness keys are set; enforcement is a property of your Temporal deployment
    (`docs/DEPLOYMENT.md:525-532`).
12. Mapping completeness is a startup warning, not a refusal
    (`docs/DEPLOYMENT.md:235-246`).
