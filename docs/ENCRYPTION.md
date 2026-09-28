# Payload encryption

Flowstate can encrypt everything a run writes to durable history, under keys
your deployment holds and the Temporal cluster never sees. This page is the
contract: what is protected, from whom, where plaintext still exists, how to
turn it on, how to operate keys, and what to do when something goes wrong.

> [!IMPORTANT]
> Encryption is **off** unless you configure a payload keyring. A server or
> worker without one logs `payload encryption is off` at startup and writes
> step outputs, signals and failure messages to history in plaintext. Set
> `--require-payload-encryption` in production so a missing keyring stops the
> process instead.

## What is protected, and from whom

Four kinds of data move through a run, and they are handled differently.

| Kind | Example | What Flowstate does |
| --- | --- | --- |
| Credentials | an API token a task sends | Kept out of history entirely. A workflow carries a secret *reference*; only the activity that uses it resolves the value, worker-side. Encryption is a second layer, not the mechanism. See [ARCHITECTURE.md](ARCHITECTURE.md), invariant 7. |
| Sensitive application data | a customer record a step returned | Legitimately part of the run, so it is in history. With a keyring, it is sealed there. `sensitive: true` on an input or output is display etiquette only: it hides a value from `flow get`, `flow watch` and similar, and does not keep it out of history. |
| References and handles | `secret("env:TOKEN")`, a run id | Names, not material. Stored like any other value, so a reference name that is itself revealing is sealed only if a keyring is configured. |
| Operational metadata | workflow type, task queue, timestamps, search attributes | Never encrypted: Temporal needs it to schedule and index. See [What stays visible](#what-stays-visible). |

Encryption protects history **at rest and in the cluster**. It is for the
people and systems that can read Temporal's store but should not read your
workloads' data:

| Party | Without a keyring | With a keyring |
| --- | --- | --- |
| Temporal operator, database admin, backup reader, Temporal Web user | Reads every payload | Reads ciphertext, sizes and metadata |
| Another tenant in a separate Temporal namespace | Isolated by Temporal's namespace ACLs | Also isolated cryptographically: different keys, namespace authenticated into every payload |
| Another tenant in a **shared** Temporal namespace | Reads what the namespace permits | Same key, so encryption does not separate them; separate them with namespaces (see [DEPLOYMENT.md](DEPLOYMENT.md)) |
| A Flowstate worker or server with the keyring | Plaintext | Plaintext: it has to compute on the data |
| A caller of `flow codec serve` | n/a | Plaintext of its own tenant's namespace, only with the `payload.decode` action |
| Code running inside a worker (a task, a plugin) | Plaintext of what it is given | Unchanged. Encryption is not a sandbox |

Plaintext necessarily exists in the memory of every Flowstate process holding
the keys, in anything those processes log (a `log:` step prints what its
author told it to print; worker error logs can quote an input), and on the
screen of whoever decodes history through the codec server. Encryption does not
change any of those.

## What is encrypted

Every payload the Temporal Go SDK passes through the client's data converter,
as verified against the pinned SDK (`go.temporal.io/sdk` v1.48.0) and against
raw history in `pkg/flowstate/v1/server/encryption_e2e_test.go`:

- workflow start input (the run's specification and inputs) and result
- activity arguments and results, including local activities
- signals, including signal-with-start
- queries and updates, arguments and results
- memos: tenant, starter, workflow name, signal policy, request ids
- heartbeat details
- the state carried across Continue-As-New
- schedule actions and schedule memos
- failure messages and stack traces, at every level of a cause chain, and
  failure details (the failure converter's `EncodeCommonAttributes` is forced
  on whenever a codec is configured)

Each payload is sealed with AES-256-GCM under a content key derived by
HKDF-SHA256 from the namespace's current key and a fresh 256-bit salt. The key
id and the Temporal namespace are authenticated, so a payload cannot be edited,
relabelled to another key, or moved to another namespace without failing to
decrypt. The whole payload is sealed, metadata included, so even the message
type is hidden. The construction and its argument are in
[`pkg/flowstate/v1/payloadcodec/envelope`](../pkg/flowstate/v1/payloadcodec/envelope/envelope.go).

## What stays visible

| Visible | Why |
| --- | --- |
| Search attributes: `FlowstateNamespace` (the tenant), `FlowstateWorkflowName`, `TemporalChangeVersion` | The cluster indexes them, so the SDK always writes them with the default converter. Nothing payload-derived is ever projected into one. The tenant name is therefore visible even though its memo copy is sealed. |
| Headers (trace context) | Context propagators encode their own headers; trace ids only. |
| Workflow and activity types, workflow ids, run ids, task queues, timestamps, event types and order | Temporal's own scheduling data. A Flowstate workflow id can embed a digest of a request, never its content. |
| Each payload's size, plus a constant | Ciphertext length follows plaintext length. |
| Each payload's key id and envelope version | Needed to choose a key before decrypting. Key ids are chosen by you; do not put anything secret in one. |
| Messages the Temporal server writes itself, such as a timeout | They carry no workload data. |

## Turning it on

### 1. Make a key

```sh
flow codec keygen --out /etc/flowstate/payload-keys/default-2026-09.key
```

The key is 32 random bytes, base64 on one line, written at mode 0600. Nothing
of it is printed. `keygen` refuses to overwrite a file.

Keep a backup of every key somewhere at least as protected as the key itself.
**A lost key is lost history**: nothing can decrypt what it sealed.

### 2. Write a keyring

A keyring names each Temporal namespace's keys by location, never by value, so
it can live in configuration management:

```yaml
# /etc/flowstate/payload-keyring.yaml
namespaces:
  default:
    current: default-2026-09
    keys:
      - id: default-2026-09
        file: payload-keys/default-2026-09.key   # relative to this file
```

It is a `flowstate.v1.PayloadKeyring` message, defined with its validation
rules in [`proto/flowstate/v1/payload_encryption.proto`](../proto/flowstate/v1/payload_encryption.proto).
Each key is held in a `file` (owner-only permissions, or it is refused) or an
`env` variable. A key id names one key in one namespace across the whole
keyring. Quote an id that is all digits, or YAML reads it as a number.

### 3. Point every process at it

```sh
export FLOWSTATE_PAYLOAD_KEYRING=/etc/flowstate/payload-keyring.yaml
export FLOWSTATE_REQUIRE_PAYLOAD_ENCRYPTION=true

flow server --auth-policy /etc/flowstate/auth.yaml --rpc-resource https://flowstate.example.com/rpc
flow worker
```

`flow server`, `flow worker` and `flow codec` take the same values as
`--payload-keyring` and `--require-payload-encryption`. `flow server dev` and
`flow run local` read the environment variables. Every client is built with
its own namespace's codec. A process that dials a namespace the keyring does
not cover refuses to start, instead of writing that namespace in plaintext.

### 4. Check it

```sh
flow codec status
```

```text
payload encryption: on (required)

namespace default
  default-2026-09          58c8b4949fd07dd6  current
```

The second column is a one-way fingerprint of the key. Run `status` on every
machine: two processes whose fingerprints for one id differ hold different
keys, and each will refuse the other's payloads. `-o json` writes a
`flowstate.v1.PayloadEncryptionStatus`.

## Journeys

### Local development

`flow run local` has no durable history, so nothing is encrypted. It still
loads and validates the keyring from `FLOWSTATE_PAYLOAD_KEYRING` exactly as a
worker would. A keyring that would stop a worker, such as one with a
world-readable key file, stops the rehearsal too.

### An encrypted run you can inspect

```sh
flow codec keygen --out default.key
cat > keyring.yaml <<'EOF'
namespaces:
  default:
    current: dev-1
    keys: [{id: dev-1, file: default.key}]
EOF

FLOWSTATE_PAYLOAD_KEYRING=$PWD/keyring.yaml flow server dev
flow run examples/parameterized-deploy/workflow.yaml --input-file examples/parameterized-deploy/inputs.json
```

Look at the stored history with Temporal's own CLI, which has no key:

```sh
temporal workflow show -w <workflow id> -o json | grep -c '<a value from your inputs>'   # 0
```

Every payload's `encoding` reads `binary/flowstate-envelope-v1`. `flow get`
still shows the result, because the server holds the keyring.

### Reading history in Temporal Web or the CLI

`flow codec serve` is the codec endpoint Temporal's tools call. For local use:

```sh
flow codec serve --insecure-no-auth --payload-keyring keyring.yaml
temporal workflow show -w <workflow id> --codec-endpoint http://127.0.0.1:8089
```

`--insecure-no-auth` serves anyone who can reach the listener, so it is refused
on any address but loopback.

In production, callers authenticate against your trust policy and need the
decode action explicitly:

```yaml
# auth.yaml (excerpt)
issuers:
  - name: sso
    issuer: https://sso.example.com
    audiences: [flowstate-codec]
    namespace_claim: team
    actions: [workload.read, payload.decode]
tenancy:
  temporal:
    team-a: tenant-a
    team-b: tenant-b
```

```sh
flow codec serve --listen 0.0.0.0:8089 \
  --auth-policy /etc/flowstate/auth.yaml \
  --payload-keyring /etc/flowstate/payload-keyring.yaml \
  --cors-origin https://temporal.example.com \
  --tls-cert-file codec.crt --tls-key-file codec.key
```

In Temporal Web, set the Codec Server endpoint to the server's URL and turn on
passing the user's access token. With the CLI, pass `--codec-endpoint` and
`--codec-auth "Bearer <token>"`.

The server's rules:

- **Explicit action.** The caller's policy entry must list `payload.decode`
  (or `payload.encode` to encrypt what someone types into the UI). An entry
  that lists no actions is *not* granted it. This differs from the RPC
  actions, where no list means unrestricted.
- **Own namespace only.** `X-Namespace` must be the Temporal namespace the
  caller's own tenant maps to. A caller who names another is refused.
- **No shared namespaces.** If more than one tenant maps to the namespace, or
  it is the tenancy default, a decode cannot tell whose payload it was given.
  It is refused unless you start the server with `--allow-shared-namespaces`,
  accepting that any authorized tenant there reads all of them.
- **Bounded before keys are used.** Bodies are capped at 4 MiB, requests at
  256 payloads, and each caller at 600 requests a minute.
- **Nothing kept or echoed.** Responses are `Cache-Control: no-store`. Errors
  never contain a payload, a key, or which of wrong key, wrong namespace or
  tampering made a payload undecodable. CORS origins are compared exactly.
- **Audited.** Every decision about an authenticated caller, allow or deny,
  is an audit record with `httpEndpoint: /decode`, the caller, and their
  tenant, and no payload. With `--audit-required`, a decision that cannot be
  recorded is answered 503 and not acted on. A request the trust policy
  refuses before it reaches the codec is logged, not audited, as on
  `flow server`, so an unauthenticated caller cannot write the trail at will.

The protocol names a namespace and nothing else, so no codec server can
authorize per run. A person who may decode a namespace may decode every run in
it. If that is too broad, give tenants their own namespaces.

### Two tenants

Map each tenant to its own Temporal namespace, and give each namespace its own
keys:

```yaml
namespaces:
  tenant-a:
    current: a-2026-09
    keys: [{id: a-2026-09, file: keys/a-2026-09.key}]
  tenant-b:
    current: b-2026-09
    keys: [{id: b-2026-09, file: keys/b-2026-09.key}]
```

A worker serving only tenant A can be given a keyring with only
`tenant-a`, so it holds no key that reads tenant B. `flow server` serves every
tenant and holds every key. Both can use identical secret names and step ids;
the keys are what differ. `pkg/flowstate/v1/codecserver/codecserver_test.go`
exercises a forged namespace header and ciphertext moved between tenants.

### Rotating a key

Rotation is two rollouts, so no process ever meets a payload sealed under a key
it has not been given:

1. **Distribute.** Add the new key to the namespace's `keys`, leaving
   `current` alone. Roll every server, worker and codec server. Check
   `flow codec status` everywhere.
2. **Switch.** Set `current` to the new id. Roll again. New payloads are sealed
   under it; old ones still decrypt under the old key.

Keep the old key in the keyring for as long as any history sealed under it
must stay readable. That is at least the namespace's retention period after
the switch, plus the life of any run still in flight or in a backup you might
restore. `TestRotationKeepsAnInFlightRunReadable` rotates keys under a run
parked at an approval and finishes it on the new fleet.

### Emergency and loss

| Situation | What happens | What to do |
| --- | --- | --- |
| A key file is unreadable or has loose permissions | The process refuses to start and names the key | Fix the file; nothing was written with a partial keyring |
| A key is missing from one worker's keyring | That worker fails every workflow task that reads a payload under it. The run stays RUNNING and retries on other workers | Distribute the key; compare `flow codec status` fingerprints |
| The same id holds different material on two machines | Each refuses the other's payloads as failing authentication | Treat as a misconfiguration; restore the right key from backup |
| A key is suspected compromised | Everything sealed under it is readable by whoever has it | Rotate immediately (both steps), then re-evaluate how long the old key must stay |
| A key is lost with no backup | History sealed under it is unrecoverable; runs that need it cannot progress | Terminate those runs; there is no recovery |
| The keyring is gone and encryption is required | Nothing starts | Restore the keyring. Never turn encryption off to "get running": new plaintext would sit beside history no one can read |

Removing a key on purpose makes everything sealed under it unreadable. That is
crypto-erasure, and it is all-or-nothing per key. It is not deletion of one
person's data, and it does nothing about copies that were decrypted and stored
elsewhere, such as logs, exports, or the result a client already fetched.

### Turning encryption on for existing history

History written before a keyring existed is plaintext, and a namespace that
requires encryption refuses to read it. While old runs are still in flight,
let that one namespace read them:

```yaml
namespaces:
  default:
    current: default-2026-09
    accept_unencrypted: true   # remove once no pre-encryption history is needed
    keys: [{id: default-2026-09, file: default-2026-09.key}]
```

Everything written is still sealed. Remove the setting once the namespace's
retention has passed. While it is set, anything able to write a plaintext
payload into that namespace's history is read back as though it were
protected, which is exactly what the default refusal prevents.

> [!WARNING]
> `flow server` reads its own memos (a run's tenant, starter, labels) through
> one reader for every namespace, and that reader accepts unencrypted payloads
> only if **every** namespace in the keyring does. In a keyring with several
> namespaces, set `accept_unencrypted` on all of them for the migration, or the
> server will treat pre-encryption runs in the migrating namespace as not found
> even though workers can still finish them. Per-namespace server reads are
> [#2163](https://github.com/picatz/flowstate/issues/2163).

## Embedding

A Go program running its own worker uses the same pieces:

```go
keyring, err := envelope.LoadFile("/etc/flowstate/payload-keyring.yaml")
if err != nil {
	return err
}
codecs := keyring.PayloadCodecConfig()

// One client per namespace, each with that namespace's codec.
temporal, namespace, err := temporalclient.DialWithNamespace(ctx, temporalclient.Config{Codec: codecs})
if err != nil {
	return err
}
mine, err := codecs.ForNamespace(namespace)
if err != nil {
	return err
}

w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{})
engine.Register(w, engine.TaskRuntimeConfig{}.WithDataConverter(mine.DataConverter()))
```

The interpreter's converter is bound to the worker it is registered on, so
several workers with different keyrings can run in one process.

> [!CAUTION]
> Pass the converter to `engine.Register` (or `embed.RunDurable`) whenever the
> client has a codec. Without it, the interpreter falls back to the SDK's
> default converter for everything it writes from workflow code, and the first
> activity's arguments reach history unencrypted before the strict activity
> side refuses them. `flow worker` and `flow server dev` always pass it; an
> embedding program has to. Removing this requirement is
> [#2164](https://github.com/picatz/flowstate/issues/2164).

## Guarantees and limits, by boundary

| Data | Boundary | Enforced by | Proved by | Limitation |
| --- | --- | --- | --- | --- |
| Payloads | Worker/server → Temporal history | Envelope codec on every client (`temporalclient.Config.Options`) | `TestEncryptedHistoryHoldsNoPlaintext`, `TestContinueAsNewCarriesOnlyCiphertext` (real Temporal, raw history) | Sizes, types, ids, timing visible |
| Failure messages and stacks | Worker → history | Failure converter forced to encode | Same tests: every failure message reads `Encoded failure` | Server-generated failures (timeouts) are plaintext, carrying no workload data |
| Memos | Server → history | Client data converter; server reads through the keyring reader | Same tests, plus `TestCodecMemosAreReadThroughTheConfiguredConverter` | The server's reader holds every namespace's keys, so it cannot detect a memo moved between two of its namespaces, and it accepts unencrypted memos only if every namespace does |
| Workflow-side writes in an embedding program | Interpreter → history | The worker's converter, passed with `TaskRuntimeConfig.WithDataConverter` | `TestCodecCoversInputsSignalsAndOutputs`, `TestSignalsAreLostWhenTheInterpreterBypassesTheCodec` | An embedder who omits it writes the first activity's arguments in plaintext |
| Plaintext payloads in history | History → any reader | Decode refuses unencrypted payloads unless `accept_unencrypted` | `TestUnencryptedPayloadsAreRefusedUnlessAccepted`, `TestAMarkedPayloadNeverDowngrades` | Migration setting reopens it for one namespace |
| Tampered or spliced payloads | History → worker | AES-GCM with key id and namespace authenticated | `TestTamperingIsRefused`, `TestCrossNamespaceSpliceFailsEvenUnderTheSameKey`, fuzzing | Not bound to a run: a history writer can move or replay a payload within one namespace |
| Keys | Disk/env → process | Owner-only files, bounded reads, closure-held material | `TestKeyringRefusesKeysItCannotRead`, `TestKeyMaterialDoesNotFormat` | A compromised process holding keys reads everything they seal |
| Decoded history | Codec server → a person | Trust policy, explicit action, own-namespace, no shared namespaces, bounds, audit | `codecserver_test.go`, `TestCodecServeAuthenticatesWithTheTrustPolicy` | Per-namespace, never per-run; the person sees plaintext |
| Search attributes, headers | Worker/server → history | Nothing: never encrypted | `walkHistoryEvent` excludes them explicitly | Tenant and workflow name visible |
| Worker and server logs | Process → log sink | Not encrypted | — | A `log:` step and some error logs print plaintext; treat logs as sensitive |

## See also

- [THREAT_MODEL.md](../THREAT_MODEL.md), "Run to history", for how this fits
  the rest of the boundaries.
- [DEPLOYMENT.md](DEPLOYMENT.md) for giving tenants their own namespaces.
- [reference/cli.md](reference/cli.md) for every `flow codec` flag.
