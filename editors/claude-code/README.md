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
| `hooks/` | A mod. After Claude edits a Flowfile it runs `flow validate`, tells the model what is wrong, shows a status-line count, and `/flowstate` opens a pane with the newest runs (from `flow list`, only when a server answers; with none it says so and stays local) and the Flowfiles touched this session. |
| `agents/` | `flowfile-engineer` takes an intent to a validated, tested, locally run Flowfile and reports each leg as passed, failed, or not run. |
| `commands/` | `/flowstate:new <description>` hands a description to that agent, scaffolding with `flow init` when the directory has no Flowfile. |

The plugin does not register `flow lsp`: Claude Code picks a language server by the file's last extension only, so a `*.flow.yaml` entry never matches and `.yaml` would attach it to every YAML file. Wire the language server into your editor with [docs/EDITORS.md](../../docs/EDITORS.md); the validation hook and MCP tools cover Flowfiles in the plugin.

Check the plugin with `claude plugin validate editors/claude-code` and run the
mod's tests with `claude plugin test editors/claude-code`. Try it without
installing: `claude --plugin-dir editors/claude-code`.

## Options

Both are settings under the plugin's `/plugin` config screen.

| Option | Default | Effect |
| --- | --- | --- |
| `flowBinary` | `flow` | The executable the mod runs; set it when `flow` is not on `PATH`. |
| `validateOnEdit` | `true` | Turn the after-edit validation off. |

## Types come from the schema

The mod reads `flow validate -o jsonl`, which is the schema's
`flowstate.v1.DiagnosticReport`. Its TypeScript declarations,
`types/flowstate.d.ts`, are generated from `proto/` by
`cmd/protoc-gen-flowstate-ts` in the same `buf generate` run as the Go types, with
the schema's comments as TSDoc, and CI fails if they drift. To declare another
answer for a new feature, add a `message=` option for it in `buf.gen.yaml` and run
`go tool -modfile=tools/external/go.mod buf generate`.
