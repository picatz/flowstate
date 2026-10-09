# Flowstate for Claude Code

A Claude Code plugin that makes Flowfiles easy to write, test, and debug from
an agent session. It builds on the surfaces `flow` already has instead of
adding new ones.

```
/plugin install flowstate --marketplace picatz/flowstate
```

It needs `flow` on `PATH` (`go install github.com/picatz/flowstate/cmd/flow@latest`).

| Part | What it does |
| --- | --- |
| `.mcp.json` | Runs `flow mcp`: validate, compile, task catalog, local run, test, and debug tools, plus the language guide and examples as resources. |
| `skills/` | `flowfile-author`, `flowfile-test`, and `flowfile-debug` teach the loop: read the guide, validate, test, step through. |
| `hooks/` | A mod. After Claude edits a Flowfile it runs `flow validate`, tells the model what is wrong, shows a status-line count, and `/flowstate` opens a pane with the newest runs (from `flow list`, only when a server answers; with none it says so and stays local) and the Flowfiles touched this session. When the session's directory holds a Flowfile, or a prompt names one, it adds a short context block: the task names (at most 40, from `flow tasks -o json`) and the file's `flow validate` result (at most 5 problems). If `flow` is missing or fails it adds nothing, or says which leg did not answer. A guard on `tool.check` asks before a Bash command runs a `flow` verb that changes a server (`run`, `signal`, `cancel`, `terminate`, and `schedule create`, `delete`, `pause`, `resume`, `trigger`), naming the verb and the address (`--address`, else `FLOWSTATE_ADDRESS`, else `localhost:9233`); a command it cannot parse that names such a verb asks too, and local verbs (`validate`, `fmt`, `lint`, `test`, `run local`, `tasks`, `compile`, `timeline`, `list`, `graph`) never do. It also refuses an Edit, Write, or MultiEdit that puts an apparent secret literal in a Flowfile (a token shape such as `ghp_`, `xoxb-`, `AKIA`, `sk-`, a PEM private key, or a `password`, `secret`, `token`, or `api_key` key with a plain string) and points to `${secret('scheme:name')}`. |
| `agents/` | `flowfile-engineer` takes an intent to a validated, tested, locally run Flowfile and reports each leg as passed, failed, or not run. |
| `commands/` | `/flowstate:new <description>` hands a description to that agent, scaffolding with `flow init` when the directory has no Flowfile. |

The plugin does not register `flow lsp`: Claude Code picks a language server by the file's last extension only, so a `*.flow.yaml` entry never matches and `.yaml` would attach it to every YAML file. Wire the language server into your editor with [docs/EDITORS.md](../../docs/EDITORS.md); the validation hook and MCP tools cover Flowfiles in the plugin.

Check the plugin with `claude plugin validate editors/claude-code` and run the
mod's tests with `claude plugin test editors/claude-code`. Try it without
installing: `claude --plugin-dir editors/claude-code`.

## Options

These are settings under the plugin's `/plugin` config screen.

| Option | Default | Effect |
| --- | --- | --- |
| `flowBinary` | `flow` | The executable the mod runs; set it when `flow` is not on `PATH`. |
| `validateOnEdit` | `true` | Turn the after-edit validation off. |
| `guardServerActions` | `true` | Turn off the confirmation before a server-changing `flow` verb. The secret refusal has no option. |

## Types come from the schema

The mod reads `flow validate -o jsonl`, which is the schema's
`flowstate.v1.DiagnosticReport`. Its TypeScript declarations,
`types/flowstate.d.ts`, are generated from `proto/` by
`cmd/protoc-gen-flowstate-ts` in the same `buf generate` run as the Go types, with
the schema's comments as TSDoc, and CI fails if they drift. To declare another
answer for a new feature, add a `message=` option for it in `buf.gen.yaml` and run
`go tool -modfile=tools/external/go.mod buf generate`.
