# Flowstate documentation

Flowstate runs workloads that have to finish correctly despite crashes, network
failures, and long waits. You describe one in a YAML Flowfile with CEL
expressions; Flowstate checks it, compiles it to a typed specification, and runs
it on your machine or durably on Temporal, under policy about who may start it,
answer it, and reach what.

Start with the first section, then follow the one that matches what you are
doing. [The repository README](../README.md) is the one-page overview.

## Start here

| Document | Read it to |
| --- | --- |
| [Get started](GETTING_STARTED.md) | Write a workflow, test it, step through it, and run it locally and then durably, in about twenty minutes. |
| [How Flowstate works](CONCEPTS.md) | Understand the building blocks and how they fit together, with orientation if you know GitHub Actions or Temporal. |
| [Why Flowstate, and when not](COMPARISON.md) | Compare it with the Temporal SDK, Argo, Step Functions, CI systems, and durable functions, including the workloads it does not fit. |
| [Use cases](USE_CASES.md) | See four production-shaped workflows (settlement, access review, incident response, onboarding) and what each does not do. |
| [Examples](../examples/README.md) | Find a tested workflow for each feature, arranged as learning journeys. |

## Write workflows

| Document | Read it to |
| --- | --- |
| [The Flowfile language](LANGUAGE.md) | Look up any construct: values, expressions, tasks, control flow, waits, retries, compensation, triggers, policy, and limits. |
| [Style](STYLE.md) | Choose among legal spellings: the one canonical form for each construct, and the rule that decides it. |
| [Task reference](reference/tasks.md) | *Generated.* Every built-in task's inputs, outputs, and bounds. |
| [CEL reference](reference/cel.md) | *Generated.* Every function and macro an expression may call. |
| [Diagnostics reference](reference/diagnostics.md) | *Generated.* The stable `code` of every `flow validate` diagnostic. |

## Test, debug, and edit

| Document | Read it to |
| --- | --- |
| [Testing workflows](TESTING.md) | Write `*.test.yaml` cases with stubbed tasks, scripted signals, and a virtual clock. |
| [Debugging a workflow](DEBUGGING.md) | Hold a run at any step and inspect it, locally, in an editor, or as an agent; and pause a durable run. |
| [Editor setup](EDITORS.md) | Get diagnostics, completion, and hover from `flow lsp` in VS Code, Neovim, Helix, Zed, or Emacs. |
| [Using Flowstate from an agent](MCP.md) | Connect Claude Code, Codex, or another MCP client to `flow mcp`, and learn the authoring loop it supports. |
| [MCP tool reference](reference/mcp.md) | *Generated.* Every tool `flow mcp` serves. |

## Run and operate

| Document | Read it to |
| --- | --- |
| [Deployment](DEPLOYMENT.md) | Run a server and workers for a team: isolation tiers, topologies, health, metrics, audit, capacity, and upgrades. Read the first section before sharing a Temporal namespace. |
| [Payload encryption](ENCRYPTION.md) | Encrypt run history under keys the deployment holds: what it protects and from whom, what stays visible, the codec server for Temporal's UI and CLI, and key rotation and loss. |
| [Secrets and credentials](SECRETS.md) | Configure secret providers and access policy, and mint short-lived credentials instead of storing long-lived ones. |
| [Workload identity federation](WORKLOAD_IDENTITY_FEDERATION.md) | Understand the metadata documents Flowstate publishes as an issuer, and what each cloud's relying party requires. |
| [MCP over HTTP](MCP_AUTHORIZATION.md) | Authorize agents that reach `flow mcp serve` over HTTP. |
| [Authorization freshness](AUTHORIZATION_FRESHNESS.md) | The design for ordering policy changes across a fleet. Mostly not yet implemented; the page says which parts exist. |
| [Command reference](reference/cli.md) | *Generated.* Every command and flag. |
| [Environment variables](reference/envvars.md) | *Generated.* Every environment variable the binary reads. |
| [Task policy reference](reference/task-policy.md) | *Generated.* The fields of a `--task-policy` file. |

Also in the repository root: the [threat model](../THREAT_MODEL.md), with each
trust boundary, what enforces it, and the known gaps; and the
[security policy](../SECURITY.md) for reporting a vulnerability.

## Build on Flowstate

| Document | Read it to |
| --- | --- |
| [The control-plane API](API.md) | Call the ConnectRPC API from curl, Go, or another language, and see which surfaces are stable. |
| [Embedding](EMBEDDING.md) | Compile and run workflows inside a Go program with `pkg/flowstate/embed`, and register Go functions as tasks. |
| [Writing a plugin](PLUGINS.md) | Add tasks or secret providers as a separate executable, from an empty directory to a task a worker runs. |
| [First-party plugins](../plugins/) | See what each in-tree plugin provides and bounds: Docker, Git, GitHub, JOSE, OCI, OIDC, SCIM, Slack, SQL, SSH, VCS, and Codex. |

## Design and direction

| Document | Read it to |
| --- | --- |
| [Architecture](ARCHITECTURE.md) | Learn the layers, the invariants every change is checked against, and the tenancy, secret, and plugin models. |
| [Language design decisions](DSL.md) | Understand why each construct is shaped as it is, and what was refused. A record of decisions, not a tutorial. |
| [The command line contract](CLI.md) | See the rules every `flow` command follows: streams, colour, errors, and exit statuses. |
| [CLI design language](CLI_DESIGN.md) | The concrete tokens, symbols, and views a new command is checked against. |
| [Vision](VISION.md) | Directions the project intends to take. Nothing there is shipped. |

## Contributing

| Document | Read it to |
| --- | --- |
| [Contributing](../CONTRIBUTING.md) | Propose and land a change. |
| [CI](CI.md) | Understand what the verification tiers run and how the gate decides. |
| [Agent configuration](agents/README.md) | See how Claude Code, Codex, and Amp are configured to work on this repository. |
| [plans/](plans/) | Internal: agent-orchestration process and past plans. Not product documentation. |

### Writing documentation here

- **Generated pages are never edited by hand.** Everything under `reference/`
  carries a banner naming its source. Change the source, then regenerate:

  ```console
  $ go run ./cmd/flow docs generate          # docs/reference/
  $ go generate ./cmd/flow/internal/reference # the copies of LANGUAGE.md, DSL.md, and the examples compiled into flow
  ```

  Editing [LANGUAGE.md](LANGUAGE.md), [DSL.md](DSL.md), or an example's
  `workflow.yaml` needs the second command: `flow mcp` serves compiled-in
  copies, and a test fails when they drift.
- **Complete Flowfiles in pages are tested.** A fenced `yaml` block that starts
  with `edition:` in a page listed in `shownDocs`
  (`pkg/flowstate/v1/flowfile/shown_test.go`) must compile, lint clean, and be
  in `flow fmt` form; a test file must load. A block preceded by
  `<!-- mirrors: path -->` must match that file exactly. Fence a sketch of
  something that does not exist yet as `yaml (proposed)`.
- **Diagrams are Mermaid,** used where a shape would otherwise take a paragraph,
  with the essential meaning also in the text.
- **Callouts are rare.** `[!WARNING]` for a risk to isolation, a secret, or
  data; `[!IMPORTANT]` for a prerequisite; `[!TIP]` for a real shortcut;
  `[!NOTE]` for context the reader should not skim past.
- **This index lists every page.** `TestTheDocsIndexListsEveryDocument` fails
  when a page under `docs/` is added or removed without an entry here.
