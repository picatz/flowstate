# Handoff

Where the work stands and what to pick up next. Delete this file once it is stale
— it describes a moment, not the design. [ARCHITECTURE.md](./ARCHITECTURE.md) is
the durable document.

Branch: `harden-and-expand-engine`, pushed. Verified from a fresh clone of the
pushed branch, not from the working tree: it builds clean, vets clean, and all ten
packages pass.

Start here: `go build ./... && GOMEMLIMIT=1GiB go test -timeout 180s ./pkg/flowstate/...`
Bounds are not optional — see CLAUDE.md for why.

Verify from a clean clone before believing a green working tree. Several times today
an editor reported a package broken that built fine, and once reported one fine that
was broken; both were snapshots taken mid-edit while another agent was writing. The
clone is the only view that is not a snapshot of someone's unsaved work.

## Resolved late in the day

**The Flowfile parser now bounds alias expansion.** It previously bounded only how
far an alias *chain* was followed, not how many nodes an alias *expanded to*, and
expansion is what explodes:

```yaml
a: &a ["x","x","x","x","x","x","x","x","x","x"]
b: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a,*a]
c: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b,*b]
```

Ten per level, ten levels, 10^9 nodes, every chain one hop long. A `flowfile.test`
process reached 23 GB resident and 32 GB of swap on this and had to be killed.

`maxNodes` and `maxDepth` in `flowfile/parse.go` now bound it, reported as an
ordinary diagnostic with a position:

```
7:22: steps[0].task.inputs.m5[5][6][6][6][8]: holds more than 100000 values once
      aliases are expanded, which is more than a Flowfile is meant to hold
```

Verified against the document above under a 512 MiB cap: rejected immediately.

Still worth doing tomorrow: confirm the four parser tests that were failing
(`TestParseReportsPositions`, `TestParsePositionPaths`, `TestParseExpressionContexts`,
`TestParseAnchorsAndMerge`) now pass, with a bounded run —
`GOMEMLIMIT=512MiB go test -timeout 30s ./pkg/flowstate/v1/flowfile/`. Fuzzing that
package is reasonable again now that the bound exists, but run it with
`-parallel 1`, a small `-fuzztime`, and `GOMEMLIMIT` set.

## Also unfinished

**`pkg/flowstate/v1/secrets` landed and is green.** It was the last red package and
is now committed: references, a provider registry dispatching by scheme, env and file
in-process, Vault, and the value hygiene described below. Local providers for the
macOS keychain and 1Password were in progress at the end of the day and may or may not
have landed with it — check whether `keychain.go` and `onepassword.go` are tracked.

Both shell out, which is right for those two: `op` and `security` are CLI-first and
neither has a stable library. Two things that shape must get right, because neither
fails loudly. The subprocess needs a bounded timeout, since a keychain prompt waits
for a human who is not there and an activity that hangs holds its slot until the
schedule-to-close timeout fires. And its stderr must never reach a log line: `op` puts
the item reference in its error text, and `security` echoes the account name.

**`pkg/flowstate/v1/sensitive` does not exist yet.** It is the agreed home for one
secret-bearing value type shared by `secrets` and `auth`, which today have two with
different guarantees. It has to be a leaf package depending only on the standard
library: `auth` will need `secrets` (an OAuth exchanger reading a stored client
secret from a file), so the type cannot live in either. Material belongs in a
closure, because `fmt` cannot call a method on a value it reaches through an
unexported field and so prints the fields instead — that is how a private signing
key was found printable through `%+v` on an enclosing struct.

## The plugin ecosystem, now started

`proto/flowstate/v1/plugin.proto` defines the protocol and is generated. Agent
`plugin-host` is building `pkg/flowstate/v1/plugin/**`: discovery of
`flowstate-plugin-<name>` executables, a handshake over a Unix socket, supervision,
Connect clients, adapters onto the secrets and task registries, and an SDK plus a
worked example plugin.

The design decisions, so they are not relitigated: separate processes because that
is where isolation actually exists; Connect RPC and Protobuf so the protocol is a
schema rather than a Go interface and a plugin can be written in any language;
capability-based rather than typed, because the useful integrations are rarely
single-purpose and one binary should be able to serve secrets and tasks both. The
handshake follows the lessons go-plugin learned — magic cookie, version
negotiation, one handshake line on stdout then stderr only, bounded handshake
timeout, socket at mode 0600, a per-launch shared secret, and lifecycle in both
directions — without using go-plugin itself, since our protocol is proto-first.

A plugin ships protobuf descriptors for its task inputs and outputs, so a plugin
task is indistinguishable from a built-in one to validation, editor completion, and
generated documentation. That is the registry invariant applied across a process
boundary.

What a plugin cannot do is escape policy: it resolves only permitted schemes,
receives the tenant rather than choosing it, and its network access remains the
worker's to govern.

Left to decide: whether Vault stays in-process, becomes a plugin, or both. Env and
file should stay in-process — a map lookup and a syscall do not justify a process
boundary.

## Not started, roughly in order of value

1. **`${secret('scheme:name')}` does not compile.** It passes `flow validate` and
   then fails at run time with `no such overload`, because CEL's parser accepts a
   call to an unknown function and nothing rejects it earlier. The README carries a
   note saying so; remove the note when this lands. Assigned to `flowfile-parser`:
   compile the marker to a `SecretRef` at compile time, refuse it anywhere but as a
   whole input value, and explain why in the message. It cannot be a runtime CEL
   function — resolving in workflow code would put the value somewhere history can
   reach.

2. **Wire secrets into a task end to end.** The schema, the resolver, and the
   providers exist; nothing connects them. A string input should accept a
   `SecretRef`, resolved inside the activity. The DSL needs syntax for it —
   `${secret('env:API_KEY')}` compiling through `ParseRef`, so a malformed
   reference is a compile error. Scrub the response body before evaluating an
   `outputs` expression, or a server echoing a token back lands it in step outputs,
   and step outputs go to history.

3. **Mount the JWKS and discovery handlers**, both **outside** the authn
   middleware — a relying party fetches them before it has any credential, so
   putting them behind authentication is how a working federation setup silently
   stops verifying. Worth a test asserting it.

       mux.Handle(auth.DiscoveryPath, broker.Issuer().Handler())
       mux.Handle(broker.Issuer().JWKSPath(), broker.Issuer().Handler())

   Also needs `--identity-key` / `FLOWSTATE_IDENTITY_KEY`, a PEM path, decoded with
   `x509.ParsePKCS8PrivateKey` into `auth.NewSigningKey(keyID, parsed)`. The key id
   is published in the JWKS and named in every assertion, so a date like `2026-07`
   makes rotation self-documenting. `policy.Federation.Broker(key)` gives the broker.

4. **Wire the Temporal namespace mapping.** `auth.Tenancy` is done and needs exactly
   two calls from `temporalclient`: `TemporalNamespaces()` at startup, to dial one
   client per distinct namespace so per-run selection is a map lookup rather than a
   connection attempt; and `TemporalNamespace(identity.Namespace)` per run. The bool
   matters — **false with no error means the deployment maps nothing, so use the
   namespace the process was configured with**, which is the zero-configuration
   path. An error means namespaces are mapped but this one has neither an entry nor
   a default, which is fail-closed on purpose. A nil `*Tenancy` is valid and returns
   false everywhere, so it can be called unconditionally.

5. **Call `SecretPolicy.Authorize` from the secrets store.** It is implemented and
   tested but nothing invokes it, so secret access is currently unauthorized.

6. **Durable waiting.** `sleep:` and `wait_until:` as durable timers, and
   `wait_for_signal:` with `flow signal` for human approval gates. This is the
   capability that most distinguishes the engine from a task runner, and the frame
   stack now makes it tractable.

7. **Sub-workflows.** A `workflow:` step as a Temporal child workflow, which also
   relieves payload pressure on fan-out by giving each one its own history.

8. **Lifecycle RPCs.** List, cancel, terminate, signal, query — thin wrappers over
   what Temporal already provides. The service is still only `Run` and `Get`.

## What is done and verified

- Zero reachable vulnerabilities, including standard library, via a pinned toolchain.
- Bounded CEL evaluation: the verified attack expression now fails in ~60 ms instead
  of running seconds past its deadline.
- Governed egress, verified by hand: a workflow targeting a loopback metadata
  endpoint is denied with a reason, and reaching localhost needs an explicit opt-in.
- Fail-closed authentication; the server refuses to start unconfigured.
- Real specification validation, replacing 3,408 generated lines that enforced
  nothing.
- Correct retry classification, so a non-idempotent request is not repeated because
  its failure was deterministic.
- Per-step timeout, retry, and `continue_on_error`; conditionals; `for_each` with
  bounded concurrency; `parallel` branches; frame-stack Continue-As-New.
- Execution parity: the same cases run against both drivers and agree.
- A language server providing diagnostics, hover, completion, go-to-definition, and
  document symbols, verified over the real protocol rather than only in tests.
- All ten examples validate and run.

## Traps that will otherwise be rediscovered painfully

**`picatz/jose` behaves in four surprising ways**, which is why the auth package does
some things the long way. Simplifying to `jwt.ParseAndVerify` plus `jwk.FetchSet`
looks obvious and would silently break Kubernetes tokens:

- `jwt.Verify`'s audience check rejects array audiences — its type switch handles
  only `string` and `[]string`, while JSON yields `[]any` — so it errors on every
  multi-audience token, including every Kubernetes projected service-account token.
  Claim validation is therefore hand-rolled and jose is used only for signatures.
- It cannot verify ES384, and cannot sign ES512. Hence ES384's absence from
  `DefaultAlgorithms`. `TestOIDCVerifierVerifiesEveryAdvertisedAlgorithm` mints and
  verifies end to end for every advertised algorithm and keeps that list honest.
- `jwk.Validate` rejects `kty: OKP`, so `jwk.FetchSet` cannot load an Ed25519 key
  set at all.
- `jwk.FetchSet` reads the response body unbounded.

**`auth` imports no other Flowstate package, deliberately.** `pkg/flowstate/v1` must
be able to import it for the http task, so the dependency points one way and
crossings happen as interfaces (`IdentitySource`, `SecretReference`) or plain Go
values. This one self-reports if broken, since the build fails.

**Do not format a `secrets.Ref` with `%v`.** The generated message has its own
`String()` emitting protobuf text format, so a log line would read
`scheme:"env" name:"API_KEY"`. Use `secrets.RefString`.

## Two bugs worth remembering

Both were found by writing something ordinary, not by looking for them.

Restoring the `http` task's documented `body` output revealed that **output shaping
never worked under Temporal at all** — the engine pre-resolved every expression in
workflow code, but `outputs` references response variables that do not exist yet.
The registry now declares which inputs a task evaluates itself.

Writing a plain `printf` example revealed that a list mixing a step reference with
a literal was rejected, because the literal and expression paths had diverged. And
input resolution mutated the task in place, which would have made a loop's second
iteration read the first iteration's values.

A third, found by a teammate's own test and worth the same attention: secret
authorization initially authorized the *zero* identity, because a workload with no
namespace resolved to a `default` placeholder tenant and matched rules written for
it. A run that reached a worker without the identity it was submitted with would
have been authorized as that tenant.
