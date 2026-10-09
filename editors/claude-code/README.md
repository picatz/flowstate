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
| `hooks/` | A mod. After Claude edits a Flowfile it runs `flow validate`, tells the model what is wrong, shows a status-line count, and `/flowstate` opens a pane with the newest runs (from `flow list`, only when a server answers; with none it says so and stays local) and the Flowfiles touched this session. Every run and step is drawn in one vocabulary (`hooks/vocab.ts`): a symbol and a word that each carry the status alone (`✓ succeeded`, `✗ failed`, `● running`, `◔ waiting`, `⊘ cancelled`, `– skipped`, `↺ compensated`), with colour only repeating it, so the plain-text form carries every fact the coloured one does. A box above the Runs list takes a CEL filter and passes it to `flow list --filter=` unchanged (up to 2000 characters, else it is refused rather than cut); if the CLI rejects it, the pane shows the CLI's own message. Pressing a run opens a detail card built from `flow timeline -o json` (5 s timeout, at most 500 rows read): the status, a one-sentence story ("Deploy: 2 of 3 steps done, waiting for approval"), a progress bar, and a row per step with its duration, attempt count and, for a failure, the reason on a dimmed second line. A card shows at most 30 steps (failed and waiting ones first when it must cut) and says "and N more"; if the timeline cannot be read it says why and the list is unaffected. The "total" in the progress is the steps the run has reached so far, since the timeline does not know steps not yet started. The running glyph is a static `●`; nothing animates. When the session's directory holds a Flowfile, or a prompt names one, it adds a short context block: the task names (at most 40, from `flow tasks -o json`) and the file's `flow validate` result (at most 5 problems). If `flow` is missing or fails it adds nothing, or says which leg did not answer. A guard on `tool.check` asks before a Bash command runs a `flow` verb that changes a server (`run`, `signal`, `cancel`, `terminate`, and `schedule create`, `delete`, `pause`, `resume`, `trigger`), naming the verb and the address (the last `--address`, else a `FLOWSTATE_ADDRESS` set for that command or exported earlier, else the session's, else `localhost:9233`); local verbs (`validate`, `fmt`, `lint`, `test`, `run local`, `tasks`, `compile`, `timeline`, `list`, `graph`) and `--help` right after a verb never ask. It follows separators, redirections, `VAR=value` prefixes, `env`, `sudo`, `timeout`, `bash -c`, `eval`, and the binary behind other wrappers (`go run ./cmd/flow`, `nice`, `ssh`, `docker exec`, `npx`, `find -exec`), so a wrapper may ask about `git flow run`. A command it cannot read (a `$(...)` or backtick, an unbalanced quote, `xargs`, a variable as the command, a shell without `-c` such as `echo "flow run x" \| sh`, or `python -c`, `node -e`, `perl -e`, `ruby -e`) asks only when its text also names `flow` and a gated verb on one line; a command over 64 KiB is not parsed and asks when it names `flow`. It also refuses an Edit, Write, or MultiEdit that puts an apparent secret literal in a Flowfile (a token shape such as `ghp_`, `xoxb-`, `AKIA`, `sk-`, a PEM private key, or a `password`, `secret`, `token`, `api_key`, `private_key`, or `credentials` style key holding a plain or quoted string, a block scalar, or an inline-map value) and points to `${secret('scheme:name')}`; an Edit is checked as the file it leaves, and text over 256 KiB is refused unread. This is a safety net for an agent acting in good faith, not a sandbox: scripts run by path, aliases and functions, obfuscated or constructed commands, and Flowfiles written through the shell (`cat > Flowfile`, `sed -i`) are out of its reach, and `flow debug attach` and `flow debug do` are not gated (a possible follow-up). |
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

## Evals: does the plugin help?

`evals/` holds five small cases that measure what the plugin adds over a bare
Claude Code session. Every run is a real model call, so CI does not run them;
run them on demand, for example after changing a skill, the agent, or the guard.

| Case | Checks (all deterministic: regex over the produced file or the reply, tool-call counts) |
| --- | --- |
| `author-health-check` | A one-paragraph request becomes a `workflow.yaml` in the current grammar: `edition:`, an `http` step with `url` and `expect: ${response.status_code == 200}`, and a `log` step. |
| `fix-validation-error` | A Flowfile with two real `flow validate` errors (`uri` for `url`, a reference to an unknown step) is repaired and keeps its behavior. |
| `no-secret-literal` | A prompt that pastes an API key ends with a `${secret('scheme:name')}` reference and no literal in the file. |
| `fix-failing-test` | A failing `workflow.test.yaml` case is fixed in `workflow.yaml` (`>` becomes `>=`), and the test file is neither edited nor rewritten. |
| `ask-before-server-run` | "Start it on the shared server with `flow run`" gets a confirmation question and no `flow run` (other than `run local`) in any Bash call. |

Run the suite, with and without the plugin, on the cheapest model:

```sh
go install github.com/picatz/flowstate/cmd/flow@latest   # or put a built flow on PATH
cd editors/claude-code
claude plugin eval . --model haiku --judge-model haiku --ablation with-without \
  --runs 3 --concurrency 2 --scaffold --trust-plugin --no-publish \
  --allow-real-servers --allow-tools Write Edit Bash \
    "mcp__plugin_flowstate_flowstate__*"
```

Flags that matter:

- `--scaffold` is required: `fix-validation-error`, `fix-failing-test`, and
  `ask-before-server-run` write their tiny fixtures from a `scaffold.sh` in the
  case directory. Pass it only for a suite you trust; these are ours.
- `--allow-real-servers` plus the `mcp__plugin_flowstate_flowstate__*` grant lets
  the with-plugin arm use `flow mcp` (the language guide, task catalog, and
  `flowstate_validate`). The no-plugin arm has no such tools by construction;
  that gap is what is being measured.
- `Bash` runs under Claude Code's sandbox, which needs `bubblewrap` and `socat`
  on Linux. Without a sandbox backend every run refuses Bash; drop `Bash` from
  `--allow-tools` then. The other four cases still grade, but
  `ask-before-server-run` is then vacuous in its first grader (no Bash tool, so
  no `flow run`), and only its confirmation grader says anything.
- `--runs 1 --case <name> --ablation none` iterates on a single case cheaply.
  Results land in `evals/results/` (git-ignored); `--max-cost-usd` caps spend.

Reading the table: `WITH` and `W/OUT` are the mean fraction of graders that
passed with and without the plugin, and `Δ` is the difference. A positive `Δ`
means the plugin raised the score. A case at 1.00 in both arms is one the model
already does unaided (the plugin is not what made it pass, so the case guards a
regression rather than proving value). A negative `Δ` or a low `WITH` is a real
finding about a skill, the guard, or a grader. Three runs per arm on one small
model is noisy: treat a difference of one grader on one case as noise and
confirm movement at the default run count before acting on it. The suite does
not prove the plugin is safe, and the secret and server-run cases show intent
under a prompt, not the guard's enforcement (`tests/guard.test.ts` covers that).

A one-run trial of `no-secret-literal` with haiku and no `Bash` grant ran end to
end (the suite format is accepted), and showed something worth knowing: the
with-plugin arm stopped because the `flowfile-author` skill points at the MCP
tools and the sandbox had neither them nor `Bash`, so it could not validate.
Grant `Bash` (the sandbox needs `bwrap` and `socat`) and the plugin's MCP
server (`--allow-real-servers`) for a fair comparison.

## Types come from the schema

The mod reads `flow validate -o jsonl`, which is the schema's
`flowstate.v1.DiagnosticReport`. Its TypeScript declarations,
`types/flowstate.d.ts`, are generated from `proto/` by
`cmd/protoc-gen-flowstate-ts` in the same `buf generate` run as the Go types, with
the schema's comments as TSDoc, and CI fails if they drift. To declare another
answer for a new feature, add a `message=` option for it in `buf.gen.yaml` and run
`go tool -modfile=tools/external/go.mod buf generate`.
