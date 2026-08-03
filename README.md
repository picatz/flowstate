# Flowstate

**Flowstate** is a durable, policy-governed workload engine. You declare a workload in
[YAML] with [CEL] expressions; Flowstate compiles it into a typed [protobuf][protobufs]
specification and executes it on [Temporal]'s [durable execution] engine.

It is not a CI system. CI is one workload shape among many — the engine targets anything
that has to finish correctly despite crashes, network failures, and long waits: data
pipelines, provisioning and orchestration, operational runbooks, agentic pipelines with
approval gates, and scheduled maintenance work.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the layer model, the invariants
the implementation holds to, and how Temporal's primitives map onto the DSL — and
[docs/VISION.md](docs/VISION.md) for where the platform is going and what should
shape work in the meantime.

[CEL]: https://cel.dev/
[YAML]: https://yaml.org/
[DSL]: https://en.wikipedia.org/wiki/Domain-specific_language
[protobufs]: https://protobuf.dev/
[Temporal]: https://temporal.io/
[durable execution]: https://docs.temporal.io/temporal#durable-execution

## Overview

```mermaid
flowchart LR
  %% User actor
  User((User))

  %% User Local Environment
  subgraph LocalEnv [Local Environment]
    direction LR
    Flowfile["<code>Flowfile</code><br/>(<strong>DSL</strong>: YAML + CEL)"]
    CLI["Flowstate CLI<br><code>$ flow run</code>"]
    Compiler["Flowfile Compiler<br/>(YAML+CEL → Protobuf)"]
    ProtobufSpec["Protobuf Workflow<br/>Specification"]
    
    %% Local component relationships
    Flowfile --> CLI
    CLI --> Compiler
    Compiler --> ProtobufSpec
  end

  %% Server/API Environment
  subgraph ServerEnv [Flowstate API Server]
    direction LR
    APIServer["HTTP/gRPC API Server<br/>(WorkflowService)"]
    TemporalClient["Temporal Client"]
    
    %% API component relationships
    APIServer --> TemporalClient
  end

  %% Temporal Cloud Environment
  subgraph TemporalEnv [Temporal Cloud or OSS]
    direction LR
    TemporalServer["<strong>Temporal Server</strong><br/>(Durable Execution Engine)"]
  end
  
  %% Worker Environment (Separate Cloud/GCP)
  subgraph WorkerEnv [Flowstate Temporal Worker Environment]
    direction LR
    Worker["Flowstate Worker<br/><code>$ flow worker</code>"]
    
    subgraph TaskExecution [Task Execution]
      TaskLib["<strong>Go Task Library</strong><br/>(log, http, etc.)<br/><i>Tasks implemented in Go</i>"]
      CELEngine["CEL Engine<br/><i>Only for expression evaluation<br/>in workflow input/outputs</i>"]
    end
    
    %% Worker component relationships
    Worker --> TaskLib
    Worker --> CELEngine
    TaskLib -.-> CELEngine
  end
  
  %% Cross-environment relationships
  User --> Flowfile
  User --> CLI
  CLI -- "Run Workflow<br/>(Send Protobuf)" --> APIServer
  TemporalClient -- "Start Workflow<br/>(ExecuteWorkflow)" --> TemporalServer
  TemporalServer -- "Schedule Tasks" --> Worker
  Worker -- "Complete Tasks" --> TemporalServer
  
  %% Result flow
  TemporalServer -- "Workflow Results" --> TemporalClient
  APIServer -- "Status/Results" --> CLI
  CLI -- "Show Results" --> User
  
  %% Styling
  classDef userStyle fill:#f9f,stroke:#333,stroke-width:2px;
  class User userStyle;
  
  classDef localStyle fill:#d4f4fa,stroke:#333,stroke-width:1px;
  class LocalEnv localStyle;
  
  classDef serverStyle fill:#d8fad4,stroke:#333,stroke-width:1px;
  class ServerEnv serverStyle;
  
  classDef temporalStyle fill:#faecd4,stroke:#333,stroke-width:1px;
  class TemporalEnv temporalStyle;
  
  classDef workerStyle fill:#fad4d8,stroke:#333,stroke-width:1px;
  class WorkerEnv workerStyle;
  
  classDef taskExecStyle fill:#f8e0e4,stroke:#333,stroke-width:1px;
  class TaskExecution taskExecStyle;
```

## Flowfile Structure

A `Flowfile` is a YAML file that defines a series of steps to be executed in order. A step has a unique `id` and then names the work it does directly: the task's own name is the key, and that task's inputs are the value beneath it. Inputs can be static values or dynamic expressions using CEL syntax.

A value, though, never comes from a task. Tasks do things — emit a line somebody reads,
make a request — and what a file computes comes from expressions and from `vars:`. That
split is deliberate and it is what the rest of this section is arranged around.

### Example

```yaml
edition: v2026.2
name: multi-step-hello-world
vars:
  service: billing
steps:
  - id: hello
    log:
      message: ${'checking ' + vars.service}
  - id: probe
    http:
      url: https://example.com/healthz
  - id: report
    log:
      message: ${'%s returned %d'.format([vars.service, steps.probe.status_code])}
```

Six things in it are worth reading in order:
1. `edition:` names the grammar the file is written in. It is required, and it is what lets
   a spelling be retired without a file written last month quietly changing meaning.
2. `vars:` names a value. `service` is declared once and read anywhere as
   `${vars.service}` — there is no task that exists to hand a string to the next step,
   because computing a value is not something the outside world needs to be asked for.
3. The first step has an `id` of `hello` and uses the built-in `log` task, whose `message`
   input is what a person reading the run will see.
4. `${...}` marks an expression. Without the fence the value is the text exactly as
   written, which is what lets an input hold the literal string `vars.service`.
5. The `probe` step uses the built-in `http` task, which *does* produce outputs — a
   response is something the world handed back rather than a value the file computed.
6. The last step reads one of them. A step's output is reached through `steps`: the step's
   `id` (`probe`) selects the step, and the output name (`status_code`) selects the value.
   Inputs and outputs are strongly typed, and which ones exist depends on the task being
   used (see below).

### Referring to a step's outputs

A step is named through a root — `${steps.<id>.<output>}` — while a name bound *where the
expression is written* stays bare: a loop's `as:` binding, and `now` inside `wait_until:`.

Rooted or bare follows from *who chose the name*. An author's own names — a loop's
binding, a step's `vars:` — stay bare. Names the system injects get a root: a step's
outputs under `steps.`, the workflow's vars under `vars.`, a signal's payload under
`payload.`, and the http response under `response.`. That rule is what lets each of
those sets grow without the next name capturing a binding somebody already had.

Two namespaces rather than one, because a single flat one cannot tell a bare `${name}`
inside a loop from a reference to a step called `name`. The language used to forbid the
overlap instead: an iterator could not take a step's id, no step could be called `now`,
and every CEL reserved word was refused as a step id, since a bare reference to one does
not parse. Rooting makes the collision unrepresentable, so those refusals went with
it — an iterator may share a step's id, a step may be called `now`, and of the
twenty-one reserved words only `true`, `false`, `null` and `in` are still refused,
because CEL's lexer rejects those before a field selection is reached at all.

A file written the older, bare way is rewritten by `flow fix`, and until it is run
`flow validate` names the step and says which command to run rather than reporting an
unknown name.

### Naming a value: `vars:`

`vars:` names values, and it is written at two positions that mean deliberately
different things.

At the top of a file it declares what is *ambient* — in scope everywhere — so it is read
through a root, `${vars.<name>}`:

```yaml
edition: v2026.2
name: ambient-vars
vars:
  region: eu-west-1
  targets: [alpha, beta, gamma]
  banner: ${'deploying to ' + 'eu-west-1'}

steps:
  - id: announce
    log:
      message: ${vars.banner}
```

On a step it declares what is *lexical* — in scope inside that step and nowhere else —
so it stays bare, exactly like the name a loop binds:

```yaml
edition: v2026.2
name: lexical-vars
vars:
  region: eu-west-1

steps:
  - id: describe
    vars:
      subject: ${'release for ' + vars.region}
    log:
      message: ${subject}
```

Same word, same syntax; how the name is spelled follows from its standing, which is the
rule the previous section describes. On a `for_each` or a `parallel:`, a step's vars
reach the whole body — so a loop can name what it iterates and what it decorates in one
place.

Five rules, all reported by `flow validate` rather than discovered at run time:

- **The `${...}` fence is still required** for an expression. Without it the value is the
  text as written, which is what lets a var hold the literal string `steps.greet.result`.
- **A workflow-level var may reference nothing.** It is evaluated once, before the first
  step, so there is no step to read and no clock to ask; it has literals, operators and
  the profile's functions. A step's var is under no such restriction: it may read a step
  that has already run and any `${vars.<name>}`.
- **A var may not read its siblings**, at either position. `vars:` is a mapping, and a
  mapping has no order, so "the one above" is not something the file can mean.
- **A step's own vars are not in scope for its `if:`.** The condition decides whether the
  step runs at all, so at the moment it is asked the step's bindings do not exist yet.
  Write the condition against `${vars.<name>}` or an earlier step, or lift the binding to
  the workflow.
- **A name already bound is refused, not shadowed.** A step's var may not take the name
  of an enclosing loop's binding or an enclosing step's var, and neither may be `now`.
  Two bindings of one bare name eleven lines apart is how `${body}` comes to mean two
  things; renaming costs a moment, once.

A name declared by one step is not in scope for the next. If two steps need the same
value, declare it at the top of the file — that is what the ambient position is for.

See [examples/workflow-vars](examples/workflow-vars) and
[examples/step-vars](examples/step-vars).

### What a run takes, and what it answers: `inputs:` and `outputs:`

A `vars:` block names what the *file* decided. `inputs:` names what the *caller* decides,
and `outputs:` names what the run reports back:

```yaml
edition: v2026.2
name: deploy

inputs:
  service:
    type: string
    required: true
    description: which service to deploy
  region:
    type: string
    default: eu-west-1
  replicas:
    type: int
    default: 2

outputs:
  placed:
    value: ${inputs.service + ' in ' + inputs.region}
    description: what this run deployed and where

steps:
  - id: plan
    log:
      message: ${'planning ' + inputs.service}
      fields:
        replicas: ${string(inputs.replicas)}
```

An input is declared with a `type:` — `string`, `int`, `float`, `bool`, `list` or
`struct` — and either a `default:` or `required: true`. It is read as `${inputs.<name>}`,
rooted for the reason `vars.` and `steps.` are: a root cannot collide with a step id, so
no precedence rule has to exist for anyone to read the file. Inputs are in scope wherever
an expression is — a step's `if:`, its `vars:`, a task's inputs — except in the
workflow-level `vars:` block, which is evaluated once before the run's arguments are in
scope. Writing one there is refused by `flow validate` with a diagnostic naming the var,
its position, and where to write the reference instead — not left to fail at run time.

Arguments are values, never expressions: `${...}` in an argument is refused rather than
evaluated, because an expression is something a reviewed file says and an argument is
data a caller sends. Everything else is refused while the caller is still there to be
told — an undeclared name, a missing required input, a value of the wrong type — by one
function both drivers call, so a rehearsal refuses exactly what production refuses.

Supply them on the command line, where the declaration decides how each word is read:

```console
$ flow run local examples/parameterized-deploy/workflow.yaml --input service=checkout --input replicas=3
$ flow run local examples/parameterized-deploy/workflow.yaml --input-file examples/parameterized-deploy/inputs.json
```

`--input name=value` is repeatable; a `list` or `struct` is written as JSON
(`--input targets='["alpha","beta"]'`). `--input-file` takes a JSON object keyed by input
name, which is the form for arguments that outgrew a command line, and a `--input` flag
wins over the file it is given beside.

`outputs:` names expressions evaluated once, after every step has finished, against the
run's whole scope — `${steps.<id>.<output>}`, `${vars.<name>}` and `${inputs.<name>}`.
They are the run's *answer*, as against the transcript of what each step produced, and
they come back in the `runOutputs` field of the same document either driver writes:

```console
$ flow run local examples/computed-outputs/workflow.yaml -o json | jq .runOutputs
```

An output that cannot be computed fails the run: an output is the answer somebody asked
for, so a run that cannot produce its answer has not succeeded.

See [examples/parameterized-deploy](examples/parameterized-deploy) and
[examples/computed-outputs](examples/computed-outputs).

### Saying why a step is there

A step can carry `description:` — prose the mechanics under it cannot supply, written
directly under `id:`:

```yaml
edition: v2026.2
name: describing

steps:
  - id: roster
    description: >-
      The roster service is the source of truth for who is on call. A stale answer
      here pages the wrong person, so this deliberately does not retry into a cache.
    http:
      url: https://example.com/roster

  - id: settle
    description: Give the read replica time to catch up with the write we just read past.
    sleep: 2s
```

It is a property of the *step* rather than of the task it runs, which is why a `sleep`
or a `for_each` can carry one — often the steps most in need of explaining, since what a
wait is waiting for appears nowhere else in the file. It also has to live there: the keys
under `http:` are that task's inputs, so a `description` written among them would be
asking for an input by that name.

See [examples/edition-and-descriptions](examples/edition-and-descriptions).

## Controlling how a step runs

A step can declare when it runs and how it behaves when it fails. One policy does not
fit every step: reading a config file and provisioning a cluster differ by orders of
magnitude in how long they take and how safe they are to repeat.

```yaml
edition: v2026.2
name: step-policy

steps:
  - id: check
    http:
      url: https://example.com/readyz

  - id: deploy
    if: ${steps.check.status_code == 200}   # only runs when this is true
    timeout: 30s                     # bounds one attempt
    retry:
      attempts: 3                    # total attempts, so 1 disables retrying
      interval: 1s                   # delay before the second attempt
      backoff: 2.0                   # multiplier applied after each attempt
      max_interval: 10s              # ceiling on the delay
    http:
      method: POST
      url: https://example.com/deployments

  - id: notify
    continue_on_error: true          # a failure here does not end the run
    http:
      method: POST
      url: https://example.com/notify

  - id: register
    http:
      method: POST
      url: https://example.com/registry
      outputs: '${ {"id": response.json.id} }'
    undo:                            # how to take this step back
      http:
        method: DELETE
        url: ${"https://example.com/registry/" + steps.register.id}
```

Behavior worth knowing:

- A step whose `if` is false is **skipped and produces no outputs**, so a later step
  referencing it will not resolve. That is deliberate — the value genuinely does not exist,
  and inventing an empty one would hide the mistake of depending on a step that did not run.
- Only failures that could plausibly succeed on another attempt are retried. A step that
  failed because its inputs were invalid, or because a policy denied it, fails once no
  matter what `retry` says. Retrying an operation known to be unrepeatable is worse than
  failing.
- `continue_on_error` records the failure as `${steps.<id>.error}` rather than discarding it,
  so a later step can branch on whether it worked.
- `undo` is saga compensation, and runs when a *later* step fails and the run cannot
  continue — never when the step it is written on fails, which is what `retry` and
  `continue_on_error` are for. A step registers its compensation by succeeding, so a step
  that was skipped or that failed compensates nothing; the registered ones run in reverse
  order, because steps depend forwards. Its inputs are resolved the moment the step
  succeeds, which is why `${steps.register.id}` — a step naming itself — means something
  here and nowhere else. The run still reports FAILED afterwards; what it undid is in the
  failure. `flow cancel` triggers the same compensations, in the same order, under a
  bounded budget — that run ends CANCELED, and `flow terminate` compensates nothing at
  all, which is the difference between the two verbs. See `examples/saga-provisioning/`.
- These behave identically under `flow run local` and durable execution. Local retries are
  in-process and therefore not durable — a crash loses them — but the observable outcome
  matches, which is what makes a local run worth trusting.

See [examples/conditional-and-retry](examples/conditional-and-retry).

## Workloads that are not a straight line

A step can repeat over a list, or split into branches that run at the same time.

```yaml
edition: v2026.2
name: fan-out

vars:
  targets: [alpha, beta]   # a list is a list; nothing needs to compute one

steps:
  # Repeat the body once per item. Inside it, the current item is bound to the
  # iterator's name; body steps can reference each other within an iteration.
  - id: process
    for_each:
      items: ${vars.targets}
      as: name          # defaults to `item`; bare, so it may share a step's id
      max_parallel: 3         # omit or 1 to run one at a time
      steps:
        - id: label
          http:
            method: POST
            # the iterator, bare: a binding, not a step
            url: ${'https://example.com/process/' + name}

  # Independent work with no reason to be sequential.
  - id: checks
    parallel:
      - steps:
          - id: check_config
            http: { url: https://example.com/config }
      - steps:
          - id: check_quota
            http: { url: https://example.com/quota }

  - id: summary
    log:
      message: >-
        ${'config %d / quota %d / processed %d'.format([
          steps.check_config.status_code,
          steps.check_quota.status_code,
          size(steps.process.results)])}
```

The scoping rules are worth knowing, because they are what keep results independent of
timing:

- **A loop reports its iterations through its own `results` output** — a list with one
  element per iteration, each a map of body step id to that step's outputs. Body outputs do
  not leak into the enclosing scope, because with more than one iteration they would
  overwrite each other and a later step would read whichever iteration happened to finish
  last. So `${steps.process.results}` is available afterwards; `${steps.label.status_code}`
  is not.
- **The iterator is a local binding, not a step**, which is why it is written bare and why
  it may be named after a step in the same file. `${name}` inside the body is the current
  item; `${steps.name.status_code}` would be a step called `name`, if there were one.
- **Parallel branch outputs do merge** once the block completes, so
  `${steps.check_config.status_code}` works afterwards. Branches must not reference each
  other, since there is no ordering between them — `flow validate` reports it if they do.
- Iterations and branches each start from the outputs that existed before the block, so
  neither can observe the other's work. That is what makes concurrent and sequential
  execution produce the same result.
- `max_parallel` bounds concurrency under durable execution. Local runs execute
  sequentially so their output is deterministic and comparable, which is the whole point of
  running locally.

See [examples/fan-out-and-parallel](examples/fan-out-and-parallel).

## Task Outputs and Data Flow

### Parse JSON without a JSON task

You can keep HTTP simple — returning `status_code`, `headers`, `body` as this step's
outputs — and pull a field out of the body in an expression, so there is no HTTP+JSON
task and no step that exists only to hold a computation. `json_parse(string)` is
available with nothing to enable:

```yaml
edition: v2026.2
name: json-via-cel
steps:
  - id: resp
    http:
      method: GET
      url: https://httpbin.org/json
  - id: report
    vars:
      title: ${json_parse(steps.resp.body)['slideshow']['title']}
    log:
      message: ${'fetched ' + title}
```

The parsing happens where the value is used, in the engine, at no cost in activities or
history. There is no step in the middle, because there is nothing for one to do: a step is
an effect, and selecting a field out of a string is not one.

Where to put the expression is the only real choice, and the two positions read
differently. A `vars:` binding on the step names the value, which is worth doing when it
is long or read twice — `title` above is read once and named anyway, because
`${'fetched ' + json_parse(steps.resp.body)['slideshow']['title']}` says the same thing
and is harder to see the shape of. Written inline it is one expression and no binding.
If more than one step needs it, it belongs in the workflow's own `vars:` instead.

A step is `steps.<id>.<output>` here as everywhere, and `flow validate` reference-checks
these expressions like any other — which the older `cel` step's `expr` input, evaluated by
the task against a scope the validator could not see, was never able to offer.

### Shape HTTP outputs to fit limits

To keep workflow payloads small, the `http` task supports an optional `outputs` input (a map literal or CEL map) which defines exactly what the step should return. When `outputs` is present, only those named values are returned instead of the default `status_code/body/headers`.

Example:

```yaml
edition: v2026.2
name: output-shaping
steps:
  - id: web
    http:
      method: GET
      url: https://httpbin.org/json
      # ${...} marks a CEL expression, as everywhere else in a Flowfile.
      # Quote it so YAML does not read the colons inside as mapping syntax.
      outputs: "${ {'status': response.status_code, 'title': json_parse(response.body)['slideshow']['title']} }"
```

Unlike other inputs, `outputs` is evaluated by the `http` task after the response
arrives, so its expression sees the response — reached through `response.*`, because
those names are the *system's* rather than yours. `steps.` is reachable alongside it, so
a shaping expression may combine the response with an earlier step's output.

Two spellings that look alike and are not: `response.body` is this response, read here;
`steps.web.body` is this step's output, read later.

Available in `expect:` and `outputs:`:
- `response.status_code` (int)
- `response.body` (string)
- `response.headers` (map[string]list[string])
- `response.json`, when `parse_json: true`

`flow fix` rewrites the older bare spelling. For a name in one of these inputs that is
*not* the response's, it still declines and says so — there it may be a step, and only
the author knows.

Tip: prefer returning only the fields a later step actually needs. Carrying less keeps a
workload comfortably inside Temporal's default payload limits; genuinely large data is
better handled with a payload codec that offloads the blob and carries a reference
(the claim-check pattern) than by moving more bytes through history.

### Governed network access

A workflow chooses the URL the `http` task fetches, which means it asks the worker to make
a request on the author's behalf — including to addresses only the worker can reach, like
internal services and cloud instance metadata endpoints. Requests therefore go through an
egress policy that, by default:

- allows `http` and `https` to public addresses only, denying loopback, private,
  link-local, multicast, unique-local, and carrier-grade NAT ranges, plus the well-known
  cloud metadata endpoints;
- checks the address actually being connected to, after resolution, so a name that resolves
  to a public address once and an internal one later gains nothing;
- re-applies the policy to every redirect hop, so a public URL cannot bounce into an
  internal target;
- bounds the request with timeouts and caps the response body.

Operators can express additional rules as CEL — the same language workflows are written
in — matching on `url`, `scheme`, `host`, `port`, `method`, `path`, and the resolved `ip`.
Rules are compiled and type-checked when the policy is built, so a malformed rule is a
startup error rather than a surprise mid-run. Denials are reported distinctly from network
failures and are never retried, since a denied request would only be denied again.

To develop against a service on `localhost`, set `FLOWSTATE_ALLOW_LOOPBACK_EGRESS=true`.
It is an explicit opt-in because the same permission is what would let a workflow reach a
worker's own internal endpoints in production.

The full policy — scheme and port allowlists, CIDR ranges, the CEL rules above, redirect
and body-size bounds, the TLS floor — is configured as a file, with durations written as
`30s` and sizes as `1MiB` or `10MB`, the way they are said:

```console
$ flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)" \
    --egress-policy egress-policy.yaml
```

`flow run local` takes the same flag, so a rehearsal is governed by the same rules a
worker would apply — see [examples/egress-policy.yaml](examples/egress-policy.yaml) for a
worked policy. A file replaces the default policy entirely rather than layering on top of
it, and `FLOWSTATE_ALLOW_LOOPBACK_EGRESS` is ignored while one is in force: a policy
assembled from two places is a policy nobody can read in either. Every field a file can
set can only tighten or relocate a bound, never remove one — an explicitly empty scheme
list, a zero body cap, or a zero timeout all refuse to load rather than building a policy
that allows everything or nothing by accident.

## Secrets

A secret never appears in a workflow. A *reference* to one does:

```yaml
steps:
  - id: notify
    http:
      method: POST
      url: https://api.example.com/events
      bearer: ${secret('vault:prod/api#token')}
```

`bearer:` is the input built to consume a reference whole. The worker resolves it
inside the activity that makes the request, sets `Authorization: Bearer <value>`,
and the value exists for that call and nowhere else — see
[examples/http-secret](examples/http-secret) for a worked file.

A reference may also sit inside a list or a mapping written in an input the task
turns into request bytes itself — the http task's `headers:`, `form:` and `json:`:

```yaml
      headers:
        Accept: application/json
        Authorization: ${secret('env:API_TOKEN')}
```

The mapping compiles to a structure whose entries are values, so the entry is
still a reference in the specification and in the activity payload, and the worker
resolves it as it sets the header. What decides whether a position accepts one is
that single question — does the *task* apply this input's entries itself, inside
the activity — because an input the workflow resolves is one whose resolved value
travels into history. Everywhere else is refused, and each refusal is deliberate
rather than pending:

- **`query:`**, although it is the same kind of map as `form:`. A query string is
  written to access logs, browser history, and a `Referer` header on redirect — a
  secret there is a secret published.
- **the raw string `body:`**, which is a string field: a reference is not a string,
  and there is nothing between it and the wire to resolve one.
- **an expression sharing the list or mapping** with the reference. The entries of
  a structure holding a reference travel as they were written, and an expression
  among them would have to be evaluated by the workflow to get there. For an
  Authorization header this costs nothing: `bearer:` takes the credential and
  leaves the rest of `headers:` free to compute.
- **read by an expression** anywhere — an `if:`, a loop's `items:`, a `vars:`, an
  `outputs:` declaration — because computing with it in workflow code would put
  the result in history.

```
a secret reference cannot be read in an expression; pass it to a task input that
accepts one (vault:prod/api#token)
```

A reference is `scheme:name`. The scheme selects which backend resolves it, and the
name means whatever that backend means by it — an environment variable, a path under
a mounted directory, a vault path.

The reference is all that exists in the compiled workflow, in the request that
submits it, and in the durable history Temporal keeps.

A resolved value is also scrubbed out of what the step reports. A server that
reflects the `Authorization` header back in its response body or headers would
otherwise turn a credential into a step output, which is durable — so the response
is scrubbed before the body is parsed, before `expect:` is evaluated, and before
`outputs:` shapes anything.

Which backend resolves a scheme is the deployment's choice, and the same workflow
runs unchanged against any of them. That is the point of referencing rather than
embedding: a laptop can resolve `db:password` from the OS keychain while production
resolves it from a vault, and the Flowfile does not know or care.

A deployment registers only the schemes it permits. An unregistered scheme is
refused rather than guessed at, and a deployment that registers nothing refuses every
reference — the right configuration for one that should not handle secrets at all.

## Plugins

The built-in tasks and secret backends will not cover everything, so the engine is
extensible by code it does not ship. A plugin is a separate executable named
`flowstate-plugin-<name>` that speaks Connect RPC over a Unix socket.

Separate, because a plugin runs inside a worker that holds credentials and can reach
internal networks: in the same process, a panic takes the worker down and a bug reads
whatever the worker can. The protocol is defined in the schema rather than as a Go
interface, so a plugin can be written in any language with Connect or gRPC support.

A plugin advertises *capabilities* rather than being of a kind, so one binary can
resolve secrets and provide tasks both — which is what the useful integrations
actually look like. A plugin-provided task ships its own protobuf descriptors, which
is what lets validation check its inputs by name and by type without this build ever
having compiled its schema.

Point a worker at a directory and its plugins' tasks become step keys:

```console
$ flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)" \
    --plugin-dir /usr/local/lib/flowstate/plugins
Loaded plugin example 0.1.0 from /usr/local/lib/flowstate/plugins/flowstate-plugin-example (tasks: example.greet)
```

```yaml
steps:
  - id: greet
    example.greet:
      name: ${vars.who}
```

`flow plugins --plugin-dir <dir>` reports what a worker started that way would load,
by launching each plugin and asking it — which is the only way to know, since nothing
is read from a binary to discover what it does. `--output json` carries the same
answer as a document.

A plugin's secret schemes are registered alongside its tasks — `flow worker`
resolves `${secret('<scheme>:<name>')}` through a plugin the same way it resolves
one through the built-in `env` or `file` provider, once the deployment's secret
policy permits the scheme.

One thing is deliberately not connected yet, and is called out rather than
quietly implied: **`flow validate` and the editor do not see plugin tasks**. They
build their registry from the built-ins alone, so a plugin's task reads as
`unknown task` there while running correctly on the worker. Closing that means
executing plugin binaries to check a file, which is not something an editor should
do on a keystroke.

A plugin extends what the engine can do, not what it is allowed to do: it resolves
only permitted schemes, receives the tenant a workload belongs to rather than
choosing one, and its network access remains governed by the worker.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design and the handshake.

## Configuration

Flowstate's own settings. [docs/reference/envvars.md](docs/reference/envvars.md) is the
generated version of this table — every variable the code reads, with where it is read,
held to the tree by a test that fails on a read this list does not carry. The table below
is the tour; that one is the enumeration.

| Variable | Default | Purpose |
|---|---|---|
| `FLOWSTATE_ADDRESS` | `localhost:9233` | Address the API server listens on, and that `flow run` connects to |
| `TEMPORAL_TASK_QUEUE` | `flowstate-run-task-queue` | Task queue workers serve and workflows are routed to |
| `FLOWSTATE_ALLOW_LOOPBACK_EGRESS` | unset | Permit the `http` task to reach loopback addresses (ignored when `--egress-policy` is set) |
| `FLOWSTATE_EGRESS_POLICY` | unset | Path to an egress policy file (see `--egress-policy` above) |
| `FLOWSTATE_TOKEN_FILE` | unset | File holding the bearer token `flow` authenticates with, re-read per request |
| `FLOWSTATE_TOKEN` | unset | Bearer token, used when no token file is set |
| `FLOWSTATE_DEPLOYMENT_NAME` | unset | Worker Deployment this worker belongs to (see below); a worker refuses to start without both halves of a version unless `--allow-unversioned-interpreter` accepts the risk |
| `FLOWSTATE_BUILD_ID` | unset | Version identifier for this worker's binary, unique per build; required with the deployment name, same refusal |
| `FLOWSTATE_VERBOSE_LOGGING` | `false` | Verbose logging |
| `FLOWSTATE_AUTH_POLICY` | unset | Default for `--auth-policy`: on `flow server` the trust policy naming which issuers and claims to accept; on `flow worker` and `flow run local` the same file's secrets rules, authorizing worker-side resolution |
| `FLOWSTATE_IDENTITY_KEY` | unset | Default for `--identity-key`: the PKCS#8 PEM key Flowstate signs its own short-lived assertions with, required when the trust policy configures federation |
| `FLOWSTATE_SECRET_ENV_ALLOW` | unset | Default for `--secret-env`: comma-separated names this process may resolve as `env:` secrets, whose values come from `FLOWSTATE_SECRET_<NAME>` |
| `FLOWSTATE_SECRET_DIR` | unset | Default for `--secret-dir`: the directory `file:` secrets are read from |
| `FLOWSTATE_PLUGIN_DIR` | unset | Default for `--plugin-dir`: directories to discover plugins in, separated the way `$PATH` is — the form an image bakes in rather than repeating on every command line |
| `FLOWSTATE_MAX_STEPS_PER_RUN` | unset | Server-side ceiling on the steps one run may submit; an unparseable or non-positive value is ignored rather than lowering the bound |
| `FLOWSTATE_INSECURE_PLAINTEXT_TOKEN` | `false` | Set to `true` to permit sending a bearer token over plain HTTP to somewhere that is not loopback. It is a refusal by default, because a token on the wire in the clear belongs to whatever is between here and there |
| `FLOWSTATE_SYMBOLS` | unset | Override symbol selection (`unicode`/`ascii`) when terminal detection guesses wrong |
| `FLOWSTATE_BACKGROUND` | unset | Declare the terminal background (`dark`/`light`) instead of querying for it — also the way out of the four-second wait on a terminal that never answers the query |

And the standard OpenTelemetry variables, which are read by the exporters
themselves rather than re-spelled here:

| Variable | Default | Purpose |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Turns telemetry on and says where it goes. Unset means no exporter, no goroutines, no network |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | unset | The same, for a deployment sending metrics somewhere different; either variable being set enables telemetry |
| other `OTEL_EXPORTER_OTLP_*` | — | Headers, protocol, timeouts: read by the OTLP exporters directly, so anything else OTLP-speaking is configured the same way |

### Authenticating

`flow` presents a bearer token when one is configured, and is anonymous when one is
not — which is what a development server started with `--insecure-no-auth` expects.

```console
$ flow run workflow.yaml --token-file /var/run/secrets/tokens/flowstate
$ FLOWSTATE_TOKEN="$(gcloud auth print-identity-token)" flow list
```

The file form is the one to reach for, because it is the shape federated identity
actually arrives in: Kubernetes projects a service account token to a path and rotates
the file underneath you. It is re-read on every request for that reason, so a token that
rotates mid-command keeps working.

There is deliberately no flag that takes the token itself. A credential in `argv` is a
credential in `ps` and in shell history.

**A credential is not sent over plain HTTP to anywhere but this machine.** A bearer token
is a bearer token — whoever holds it is you — so `flow` refuses rather than warns, and
tells you to use an `https://` address. If something else is providing the encryption (a
sidecar terminating TLS, say), `FLOWSTATE_INSECURE_PLAINTEXT_TOKEN=true` overrides it.

### How much a workload may carry

A run carries its specification and its step outputs together, across every
continue-as-new, and Temporal will not store a payload past its blob limit. So there are
two limits, and both report rather than truncate:

- A **specification** is refused past 1 MiB, by `flow validate` and again at submit. Note
  that this is not the size of the file: expressions compile to syntax trees, and a
  Flowfile of ordinary expressions has been measured expanding more than fivefold.
- A **run** fails, with a reason, if what it must carry forward would not fit.

The second one exists because the alternative is silence. Temporal's refusal fails the
workflow task rather than the run, and a failed workflow task is retried — so a workload
that outgrows the limit reports RUNNING, climbs an attempt count nobody is watching, and
never finishes. A run that cannot continue is failed here instead, which is the difference
between something you can find and something you cannot.

If you hit either, the fix is nearly always the same: a step producing something large
should write it somewhere and pass a reference, because every output a later step can
still reach is carried across every suspension.

### Deploying without disturbing running workloads

A worker runs an *interpreter*. There is one workflow type, and every workload anybody has
written is a value it executes — so shipping a change to the engine changes the behaviour of
every run in flight at once, including one that is a month old and half finished. Temporal
replays a run's history through whatever code the worker is running now, which is what makes
that a correctness question rather than a stylistic one.

Setting both halves of a version turns that off:

```console
$ flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"
```

A run is then **pinned** to the interpreter it started on and takes the **current** version
at its next Continue-As-New. Deploying stops reaching work already underway; long workloads
still migrate forward on their own, so an old version drains rather than being held open by
whatever is still running on it. Point traffic at a new build the usual way — with `temporal
worker deployment set-current-version`, or the equivalent in your rollout tooling.

Unversioned is the default and is fine for development, where "whatever is deployed runs
whatever is in flight" is what you want. The worker says which of the two it is at startup.

### Connecting to Temporal

Connection settings follow Temporal's own environment configuration rather than a scheme
invented here, so the standard variables behave as they do across the Temporal ecosystem:
`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, and the `TEMPORAL_TLS_*`
family for TLS and mTLS.

Flowstate also reads the same TOML configuration file the `temporal` CLI uses
(`TEMPORAL_CONFIG_FILE`, defaulting to the conventional location), so a profile already
configured for the CLI works without being restated. Profiles are how one installation
addresses several environments:

```console
$ TEMPORAL_PROFILE=staging flow worker \
    --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"
```

`--address`, `--namespace`, and `--profile` on `flow worker` and `flow server` override
whatever that configuration resolves to.

With no configuration at all, Flowstate connects to a local development server — what
`temporal server start-dev` provides. Self-hosted is the default and needs no setup;
Temporal Cloud is reached by configuring an API key or mTLS, and nothing in the defaults
assumes a hosted service.



Each step in a `Flowfile` has named inputs, and a step that brings something back has named outputs, which later steps reference in CEL expressions as `${steps.<id>.<output>}` — the step `id` selects the step and the output name selects the value. Which outputs a step has is determined by the task being used, and they can be of various types (e.g., `string`, `int`, `map`, etc.). A task is free to have none: outputs are what a step learned from the world outside, so a task that only acts on it has nothing to report. Values a file computes for itself are named with `vars:` instead.

### Saying what happened: `log:`

`log:` emits a message for a person to read. It is the one task whose purpose is to be
*seen* rather than to produce a value, and where the message goes depends on who is
watching — `flow run local` renders it to your terminal, and on a worker it reaches the
same logs everything else there does, tagged with the run that emitted it.

```yaml
edition: v2026.2
name: canary-watch

vars:
  service: billing

steps:
  - id: canary
    log:
      level: warn
      message: canary is taking 10% of traffic; watch the error rate
      fields:
        service: ${vars.service}
        stage: canary
```

`level:` is one of `info`, `warn` or `error`, defaulting to `info` — written the way you
would say it rather than the way a protocol stores it, and `flow validate` names the
three if you write a fourth. There is no `debug`, because what an operator wants
filtered is a property of a deployment rather than something a workflow author can know;
and no `fatal`, because emitting a line does not stop a run, so a level claiming
otherwise would be a promise the engine cannot keep.

`fields:` carry structure for a sink that has somewhere to put it, and are ignored by
one that does not. They are string-valued: everything this reaches renders to text in
the end, so compose the string in the expression where you can see what it will say.

Log lines go to stderr, so `flow run local ... | jq` still reads a single JSON document
from stdout.

See [examples/logging](examples/logging).

### Available Tasks

| Task Name | Inputs | Outputs |
|-----------|--------|---------|
| `log`     | `message`, `level`, `fields` | *(none)* |
| `http`    | `url`, `method`, `headers`, `body`, `query`, `form`, `json`, `parse_json`, `outputs`, `expect`, `retry_on_unknown_outcome`, `bearer`, `credential` | `status_code`, `headers`, `body`, `json` |

`log` has no outputs, which is the design rather than a gap: a log line is an effect on a
reader, not a value for a later step. Naming one — `${steps.announce.result}` — would give
a step two reasons to exist, so the file could no longer say which one was meant. To carry
a value, name it with `vars:`.

Names only, deliberately: types and constraints belong to the schema, and repeating them
here is how the two disagree. `flow tasks` prints the same catalog with every type, which
required input is which, and the CEL libraries every expression reaches — derived from the
registry, so it cannot drift from what the engine will execute.

[docs/reference/tasks.md](docs/reference/tasks.md) is that same catalog written down: every
input with its type, whether it is required, and whether the task evaluates it itself, plus
every CEL function an expression may call. It is generated from the registry and pinned in
CI, so it is the one to read when the answer has to be complete.

This table is checked against that registry by a test, because it *had* drifted: it was
missing six of `http`'s inputs and one of its outputs, while the sentence beneath it
claimed a listing derived from the registry cannot drift. A hand-written table is a second
source of truth by construction, so the only thing that keeps one honest is something that
fails when it is wrong.

`flow tasks --output json` emits the same catalog as one document, for a script or an
agent that has to address a value rather than recognise one:

```console
$ flow tasks --output json | jq '.tasks[] | select(.name == "http") | .inputs[] | select(.required)'
{
  "name": "url",
  "type": "string",
  "required": true,
  "deferred": false
}
```

Steps chain through their outputs. For example, the following `Flowfile` makes an HTTP `GET` request to `https://microsoft.com`, and then logs the status code of the response:

```yaml
edition: v2026.2
name: chained
steps:
  - id: web
    http:
      method: GET
      url: https://microsoft.com
  - id: report
    log:
      message: ${string(steps.web.status_code)}
```

> [!TIP]
> Use `${...}` for expressions, like referencing a previous step's output as `${steps.<id>.<output>}`.
> Every expression in a workflow — a task input, a `vars:` value, `if:`, `items:`, `wait_until:` — is evaluated by the engine against one vocabulary, and reaches the same CEL extension libraries: `bindings`, `comprehensions`, `encoders`, `json`, `lists`, `math`, `optional`, `protos`, `regex`, `sets`, `strings`. There is nothing to enable; `flow tasks` prints the same set, and every function in it — `sortBy`, `math.greatest`, `json.encode`, `upperAscii` — since a library name says what is switched on and nothing about what it contains. `flow tasks --output json` carries the same names for a consumer that is not a person.

### Waiting

A step can wait instead of doing work. Waiting is a step kind rather than a task,
because a wait is the engine's business: nothing runs, no worker slot is held, and a
workload parked for a month costs the same as one parked for a second.

```yaml
edition: v2026.2
name: waiting
steps:
  # A duration.
  - id: settle
    sleep: 2s

  # A moment. `now` is bound here and nowhere else — it is the replay-safe clock,
  # so the same expression yields the same instant on every replay of the run.
  - id: hold
    wait_until: ${now + days(1)}

  # A signal from outside, with a deadline.
  - id: approval
    wait_for_signal:
      name: approval
      timeout: 72h

  - id: act
    if: ${!steps.approval.timed_out && has(steps.approval.payload.approver)}
    log:
      message: ${'approved by ' + steps.approval.payload.approver}
```

`wait_for_signal:` is how a human reaches a workload:

```console
$ flow signal <id> approval --data '{"approver": "someone@example.com"}'
```

The step's outputs say what happened. `timed_out` is always present, so a workload
branches on the deadline passing rather than failing on it, and `payload` carries
whatever the sender sent — which may be nothing, since `--data` is optional. That is why
the step above tests `has(...)` as well as `timed_out`: a signal sent with no payload
leaves `payload` an empty map, and reaching into it for a key nobody sent fails the step.
Guarding both is the difference between a workload that handles a bare approval and one
that only handles the approval you had in mind.

A signal that arrives before the step is reached is not lost — a declared channel is
drained before the run suspends, so approving in advance works.

Note the `message:` above is one expression rather than text with a `${...}` in it. The
DSL has no string interpolation, and writing `approved by ${...}` is refused rather than
silently shipped as literal text. That is not a hypothetical: the first draft of this
section had exactly that mistake, and `flow validate` caught it.

See [examples/approval-gate](examples/approval-gate) and
[examples/wait-until-a-moment](examples/wait-until-a-moment).

## Workloads that run on a cadence

A workflow can say when it is meant to run:

```yaml
triggers:
  schedule:
    cron: "0 7 * * MON-FRI"      # or a list of expressions
    time_zone: Europe/Dublin     # empty means UTC
    jitter: 5m                   # spread firings, so a fleet is not a herd
    overlap: skip                # if the last one is still going
```

or, for an interval rather than a calendar:

```yaml
triggers:
  schedule:
    every: 15m
```

**The block declares; it does not create.** `flow run` and `flow run local` ignore it
entirely, so a scheduled workflow is still an ordinary file you can run once, now.
Creating the schedule is a separate act, typed by a person:

```console
$ flow schedule create examples/scheduled-report/workflow.yaml
NAME       scheduled-report
WORKFLOW   scheduled-report
STATE      live
CADENCE    0 7 * * MON-FRI, in Europe/Dublin, jittered by up to 5m0s, on overlap skip
RUNS TAKEN 0
NEXT       2026-08-04T06:00:00Z
           2026-08-05T06:00:00Z
```

That is not ceremony. A file that begins running on its own the moment it merges is a
surprise, and its first firing is indistinguishable from somebody having meant it. The
next firing times come back unasked for, because a cadence meaning something other than
what was intended is almost always visible there and almost never visible in the
expression that produced it — and `--paused` lets you read them before anything fires.

Arguments come from the command that creates the schedule, through the same flags
`flow run` takes, and are bound and type-checked **once, at creation** rather than at
each firing — a refusal at 03:00 in a worker's log, for a mistake made at a keyboard a
week earlier, helps nobody. One file can therefore serve several cadences:

```console
$ flow schedule create report.yaml --name report-eu --input region=eu-west-1
$ flow schedule create report.yaml --name report-us --input region=us-east-1
$ flow schedule list
NAME       STATE  NEXT                  NOTE
report-eu  live   2026-08-04T06:00:00Z
report-us  live   2026-08-04T06:00:00Z
```

The rest of the verbs are what an incident wants. `flow schedule pause <name> --note
"upstream is down, INC-4471"` stops the firings and leaves the arrangement in place;
`flow schedule resume` starts them again; `flow schedule trigger` fires one now, which
is how a schedule is tested — it exercises the arguments the schedule stored and the
tenant it records on the runs it starts, none of which running the workflow by hand
would prove. A run a schedule started is an ordinary run: `flow get`, `flow watch`,
`flow list` and `flow cancel` all address it.

Schedules are Temporal Schedules underneath, so a firing that comes due while the
cluster is unavailable is caught up rather than lost — which is the whole difference
from a cron entry calling `flow run`. A schedule belongs to your tenant and is named
within it, so two teams may both have a `nightly-report` without either learning of the
other, and every firing acts as the identity that created the schedule, frozen at that
moment.

See [examples/scheduled-report](examples/scheduled-report).

## Getting Started

### Authoring

Check a workflow without running it. This matters for a workload engine: steps have side
effects, so executing a file to find a typo means causing part of it.

```console
$ flow validate examples/hello-world/workflow.yaml
examples/hello-world/workflow.yaml: ok
```

Problems are reported with the position, the step, and what to do about them:

```console
$ flow validate broken.yaml
broken.yaml:4:5: step "web": unknown task "htpp"; available tasks are http, log
broken.yaml:8:16: step "out" input "message": references step "later", which runs later; steps can only reference steps defined before them
```

`flow run` and `flow run local` apply the same checks before executing anything, so a
mistake is reported rather than partially performed.

A file that passes has a second question worth asking, and `flow compile` is where it is
asked. `flow validate` answers whether a file is correct; this answers what a correct
file *becomes* — the workflow specification, the same `Workflow` message `flow run`
submits to a server, written to standard output as protojson:

```console
$ flow compile examples/hello-world/workflow.yaml | jq '.steps[0].task.name'
"log"
```

It executes nothing and contacts nothing: the compiler that answers here is the same one
behind the `Compile` RPC and the `flowstate_compile` MCP tool, running in this process.
That makes it the thing to read when a step's expressions, defaults or retry policy are
not what you expected — the specification says what would have run, without running it.
It takes no `--input`, deliberately: a workflow's `inputs:` are bound when a run is
submitted, so the specification is the same document whatever it is later run with. A
file with problems is refused, with its diagnostics on standard error and nothing on
standard output, so `flow compile x.yaml | jq` never sees a diagnostic and a broken file
never produces a half-answer.

When a spelling in the language is replaced, it is replaced rather than deprecated, and
the migration is a command:

```console
$ flow fix workflow.yaml
workflow.yaml:10: step references rooted under `steps`
workflow.yaml:4: `task:` naming "http" rewritten to `http:`
workflow.yaml:1: `edition: v2026.2` added, which is now required
```

That is three retired spellings in one pass: a step that named its task through
`task:`/`name:`, a reference that named a step bare, and a file with no `edition:` line.
A bare reference is what `flow validate` reports as `` `greet` is a step, and a step is
named `steps.greet` now``, which names the command rather than leaving an author to guess
what an unknown name means.

What `flow fix` will not do is guess at intent, and the tasks retired at this edition —
`echo`, `printf` and `cel` — are where that shows. A retired step whose result a later
step reads has one honest answer, so the rewriter takes it: the step becomes a `vars:`
binding and the references to it are rewritten. A retired step whose result *nothing*
reads does not. It may have meant "show a human this line", which is `log:`, or it may
have been computing a value nobody wanted, in which case it can simply go — and no tool
can tell those apart from the file. Those are reported with their position and left
alone, which is the same contract that covers flow style and YAML aliases:

```console
$ flow fix workflow.yaml
workflow.yaml:8:5: `printf:` is retired and nothing reads `steps.b.result`, so this cannot
tell what the step was for: a line for a person to see is `log:`, and a step that produced
a value nobody uses can simply go. Only you know which, so this leaves it alone
```

It rewrites only the lines it must and copies the rest through byte for byte, so
comments and formatting survive and a file with nothing to change comes back identical —
which is what makes pointing it at a whole directory safe. A shape it cannot rewrite
without guessing, such as a task written in flow style or standing behind a YAML alias,
is reported with its position and left alone rather than mangled. `flow fix --check`
writes nothing and exits non-zero if there is work, which is the form CI runs.

Every file names the grammar it is written in with a top-level `edition:`:

```yaml
edition: v2026.2
name: deploy
steps:
  - id: start
    log:
      message: rolling the service
```

What it buys is a *refusal*: a build that does not have that grammar says so instead of
reading the file as something else, which is what makes retiring a spelling safe to do at
all. It used to be optional, and that turned out to be the one thing it could not afford
to be — "absent means current" is not a default but a promise to reinterpret, so a file
written before a sweep would silently change meaning rather than be refused. `flow fix`
writes the line, below any header comment, so the ceremony is not yours.

It is not a compatibility switch. A build compiles one grammar; declaring an older
edition is a file `flow fix` can bring forward, and declaring a newer one means upgrading
`flow` rather than editing the file.

List what workflows can use:

```console
$ flow tasks
```

Flowstate also ships a language server (`flow lsp`) providing diagnostics, hover, and
completion for Flowfiles — see [docs/EDITORS.md](docs/EDITORS.md) for editor setup.

### Running

Start a local Temporal development server:

```console
$ temporal server start-dev
...
```

Start a Temporal worker for Flowstate. The flag accepts running the expression
interpreter unversioned, which is fine against a dev server that outlives nothing;
a production worker passes `--deployment-name` and `--build-id` instead:

```console
$ go run ./cmd/flow worker --allow-unversioned-interpreter
...
```

Start the Flowstate API server:

```console
$ go run ./cmd/flow server
...
```

Run a `Flowfile` locally (without Temporal):

```console
$ go run ./cmd/flow run local ./examples/hello-world-multi-step/workflow.yaml
INFO hello world
INFO HELLO WORLD
{"stepValues":{"greet":{"namedValues":{}},"shout":{"namedValues":{}}}}
```

Run a `Flowfile` using Temporal via the Flowstate API server:

```console
$ go run ./cmd/flow run ./examples/hello-world-multi-step/workflow.yaml
{"stepValues":{"greet":{"namedValues":{}},"shout":{"namedValues":{}}}}
```

Byte for byte the same answer, which is the point: the two drivers write it through one
renderer, so a `jq` expression written against a rehearsal works against production.

Both steps in that example are `log:` steps, so both appear in `stepValues` with an empty
`namedValues` — the run is reporting that the steps completed and produced no values,
which is what a log step is. Emitted rather than omitted, because a reader who finds the
key missing cannot tell "no values" from "a field this build does not have" without going
to the schema. The lines a person reads went to stderr, which is why piping the stdout of
either command into `jq` still sees one JSON document.

`--output json` asks for the run itself rather than only its answer — the status, the
outputs or the failure, and when it began and ended — as the schema's own `GetResponse`,
which is again the same document from either driver:

```console
$ go run ./cmd/flow run local ./examples/hello-world-multi-step/workflow.yaml -o json \
    | jq '{status, steps: (.outputs.stepValues | keys)}'
{
  "status": "STATUS_COMPLETED",
  "steps": ["greet", "shout"]
}
```

A local run's `workflowId` and `runId` come back empty, and that is the honest answer
rather than an omission: a local run is a process, so there is no id to watch it by. That
is the whole of what the local driver cannot give you.

## CLI

`flow` is the command line interface. You write a workflow as a Flowfile, check
it, and then either run it on your own machine or hand it to a server that
executes it durably through Temporal.

Run `flow <command> --help` for the full flags of any of these, or read
[docs/reference/cli.md](docs/reference/cli.md), which is every command and flag generated
from the binary's own command tree — including which environment variable each flag
default comes from.

| Command | What it does |
| --- | --- |
| `flow validate <file...>` | Check Flowfiles without executing them. Reports the line and column of each problem. `--output json` or `jsonl` carries the diagnostics as data. |
| `flow compile <file>` | Print the workflow specification a Flowfile compiles to, executing nothing and contacting no server. Where `flow validate` answers whether a file is correct, this answers what it becomes: the same `Workflow` message `flow run` submits, as protojson on stdout. A file with problems is refused with its diagnostics on stderr and nothing on stdout. |
| `flow fix <path...>` | Rewrite Flowfiles from a retired spelling into the current one, preserving comments and formatting. `--check` reports and writes nothing, exiting non-zero if there is work. |
| `flow fmt <path...>` | Rewrite Flowfiles into the form `flowfile.Marshal` writes. Unlike `flow fix`, this does not preserve comments, blank lines, key order, or quote style — it renders from the parsed workflow, not the source text. `--check` reports and writes nothing; `--stdout` writes one file's result to standard output. A file that does not parse is left untouched. |
| `flow run <file>` | Submit a workflow to a server, which runs it durably, and follow the run until it finishes. Arguments for a workflow's `inputs:` come from `--input name=value` (repeatable) or `--input-file inputs.json`. |
| `flow run local <file>` | Run a workflow in this process, with no server and no Temporal. Takes the same `--input`/`--input-file` as `flow run`, and answers signal gates from `--signal name=json`. `--output json` or `jsonl` carries the same document `flow run` writes. |
| `flow get <id>` | Report what a run is doing, and its outputs if it finished. Status and the run's declared `outputs:` on stderr, the outputs document on stdout, so `flow get id \| jq` sees only the data. |
| `flow watch <id>` | Follow a run until it finishes: a live view on a terminal, one line per change without one. Exits with the run's outcome. |
| `flow list` | List your runs. |
| `flow signal <id> <name>` | Deliver a signal to a run that is waiting, which is how a human approval reaches a workload. |
| `flow cancel <id>` | Ask a run to stop, letting it clean up. |
| `flow terminate <id>` | Stop a run immediately, running none of its cleanup. |
| `flow schedule` | Create and manage schedules that run workflows on a cadence. A Flowfile's `triggers:` block declares the cadence; these verbs are what act on it. |
| `flow schedule create <file>` | Create a schedule from a Flowfile's `triggers:` block. The specification, the cadence and the arguments are all checked here, while somebody is present to be told. Takes the same `--input`/`--input-file` as `flow run`; `--name` gives one workflow more than one cadence, and `--paused` creates it without letting it fire. |
| `flow schedule list` | List your schedules, with whether each is live and when it next fires. |
| `flow schedule describe <name>` | Show one schedule: its cadence as the file declared it, the arguments every firing runs with, when it next fires, and what it has run lately. |
| `flow schedule delete <name>` | Delete a schedule. Future firings stop; runs it already started keep going. |
| `flow schedule pause <name>` | Stop a schedule firing without deleting it, recording a `--note` saying why. |
| `flow schedule resume <name>` | Let a paused schedule fire again. Firings missed while it was paused are not made up. |
| `flow schedule trigger <name>` | Fire a schedule now rather than waiting for its cadence, which is how a schedule is tested. Fires even a paused one. |
| `flow tasks` | List the tasks a workflow may use, and the libraries every expression reaches. |
| `flow plugins` | List the plugins on a search path and the tasks each one adds, by launching them and asking. |
| `flow worker` | Start a Temporal worker, which is what actually executes steps. |
| `flow server` | Start the Flowstate API server that accepts workflows. |
| `flow lsp` | Serve the Flowfile language server over stdin and stdout, for editor diagnostics. |
| `flow mcp` | Serve the control plane to an AI agent over stdin and stdout: one tool per RPC with schemas derived from the API's own, plus `flowstate_run_local` to rehearse a Flowfile in-process, and read-only resources carrying the DSL reference, the task catalog and the examples. What a local run may reach is decided by this process's flags — with none, egress is denied. See [docs/CLI.md](docs/CLI.md#flow-mcp-the-same-surface-for-an-agent) for client configuration. |
| `flow keys` | Generate and inspect signing keys for workload identity. |
| `flow keys generate` | Generate a signing key, write it PKCS#8-PEM at file mode 0600, and print its public JWK. Refuses to overwrite an existing key. |
| `flow keys public` | Print the public JWK for an existing signing key, without touching the private half. |
| `flow jwt` | Sign and inspect JSON Web Tokens for admin debugging. |
| `flow jwt sign` | Sign a debugging JWT with a key from `flow keys generate`. Lifetime is capped at one hour. |
| `flow jwt inspect <token>` | Print a JWT's header and claims. Verifies the signature only when `--key` is given. |

`run`, `get`, `watch`, `list`, `signal`, `cancel` and `terminate` talk to a server,
and take `--address` (or `FLOWSTATE_ADDRESS`) to say which one. `run local` does
not: it contacts nothing.

### Following a run

`flow get` answers once. `flow watch` keeps answering until the run stops, which is
the difference between asking what a workload is doing and watching it do it —
`flow run` follows the same way, because it is the same code.

Following adapts to where its output goes, and the two shapes are the same
information rather than two features:

- **On a terminal** it draws a live view that updates in place: the status, and how
  long it has been watched — the moving number being what distinguishes a run that is
  working from a watch that has frozen. When the run ends, the steps that produced
  outputs are listed as a summary.
- **Without one** — a pipe, a redirect, a CI job — it prints one line per *change*.
  Not per poll: a run that sits on one step for four minutes says nothing for four
  minutes, rather than repeating itself 240 times.

It is step-by-step progress. The server answers a running execution with where it has
got to — which step, and which activities are retrying, with the attempt count and the
last failure — and both `flow get` and `flow watch` render it, through the same helpers
so the two surfaces cannot drift apart.

Underneath that, every task activity heartbeats while it works, carrying the phase it
has reached: `requesting` while a request is out, `reading the response` once a peer
has answered. That is the difference between "this step has been running for four
minutes" and knowing which end of it is slow — and it is the same mechanism that lets
a `flow cancel` reach a step that is mid-request, since Temporal delivers a
cancellation to a running activity in the reply to a heartbeat.

The live view is drawn on **stderr**, and the outputs go to stdout exactly as
`flow get` writes them. So one invocation does both:

```console
$ flow watch flowstate-workflow-3f7c | jq .stepValues
```

`--output jsonl` turns it into an event stream — one document per change, the
server's own schema, readable as it arrives, which is the shape a script or an agent
wants:

```console
$ flow watch flowstate-workflow-3f7c -o jsonl \
    | jq -c '{status, done: (.outputs.stepValues // {} | keys | length)}'
```

`--output json` writes the final state as one document instead. `--plain` asks for
the line-per-change shape on a terminal, for a scrollable transcript or a screen
reader.

The exit code is the run's: zero when it completed, non-zero when it failed, was
canceled, terminated, or timed out. So `flow watch id >/dev/null && ./promote.sh`
behaves the way a shell reader expects. Stopping watching — `q`, `esc`, `ctrl+c` —
does not stop the run, and exits zero, because an interrupted watch is not a failed
workload.

A brief outage is survived rather than fatal: a watch lasts as long as the run, and
over an hour a server restart is close to certain. It says so on screen while the
server is quiet, and gives up after 30 seconds of it.

### Stopping a run

`cancel` and `terminate` are different asks, and the difference decides whether
the workload's cleanup happens.

**`flow cancel`** is cooperative. The run is asked to stop and gets to finish
responding, so a workload that has to release a lock, roll back a partial
deployment, or tell somebody it gave up still does. The cost is that a run wedged
on something that never returns may not stop at all.

**`flow terminate`** is not. The execution stops where it is, no further step
runs, and nothing the workload would have done on the way out is done. Its
`--reason` is recorded on the run, and it is the only account of the decision
anyone will find, because a terminated run does not get to explain itself.

Prefer cancel. Reach for terminate when a run must stop now, or when cancelling
did not stop it — and understand that terminating something holding a resource is
a decision to leak that resource on purpose.

### Listing runs

`flow list` returns one page and stops. A page can come back short, or even
empty, while runs of yours remain: the tenant a run belongs to is recorded as a
Temporal memo, which cannot be queried, so the server reads a bounded number of
executions per request and keeps the ones that are yours. In a busy namespace a
scan can spend its whole budget among other tenants' runs.

So an empty page is not the end of the listing — only an empty page token is.
`flow list` says on stderr when more remain, and `flow list --all` keeps asking
until they do not.
