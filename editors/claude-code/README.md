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
| `skills/` | `flowfile-author`, `flowfile-test`, and `flowfile-debug` teach the loop: read the guide, validate, test, step through. `flowfile-conventions` is the path-scoped rules slice: a plugin cannot ship `.claude/rules/` files, so the same content is a skill whose `paths:` frontmatter loads it on its own when a Flowfile is open (STYLE.md's canonical spellings, `${secret('scheme:name')}`, CEL pitfalls, validate before done), linking to `docs/STYLE.md` and `docs/DSL.md` for the rule text. `flowstate-plugin-author` teaches writing a plugin (a task provider): decide it is needed, define the schema in Protobuf, keep secrets as references, bound what it fetches, and validate, test, and run it locally with `--plugin-dir`. |
| `hooks/` | A mod. After Claude edits a Flowfile it runs `flow validate`, tells the model what is wrong, shows a status-line count, and `/flowstate` opens a pane with the newest runs (from `flow list`, only when a server answers; with none it says so and stays local) and the Flowfiles touched this session. Every run and step is drawn in one vocabulary (`hooks/vocab.ts`): a symbol and a word that each carry the status alone (`✓ succeeded`, `✗ failed`, `● running`, `◔ waiting`, `⊘ cancelled`, `– skipped`, `↺ compensated`), with colour only repeating it, so the plain-text form carries every fact the coloured one does. A box above the Runs list takes a CEL filter and passes it to `flow list --filter=` unchanged (up to 2000 characters, else it is refused rather than cut); if the CLI rejects it, the pane shows the CLI's own message. Pressing a run opens a detail card built from `flow timeline -o json` (5 s timeout, at most 500 rows read): the status, a one-sentence story ("Deploy: 2 of 3 steps done, waiting for approval"), a progress bar, and a row per step with its duration, attempt count and, for a failure, the reason on a dimmed second line. A card shows at most 30 steps (failed and waiting ones first when it must cut) and says "and N more"; if the timeline cannot be read it says why and the list is unaffected. When the run is, or may be, parked on a signal gate, the card also shows the gate and a button to answer it (see "Signal and approval buttons"). The "total" in the progress is the steps the run has reached so far, since the timeline does not know steps not yet started. The running glyph is a static `●`; nothing animates. When the session's directory holds a Flowfile, or a prompt names one, it adds a short context block: the task names (at most 40, from `flow tasks -o json`) and the file's `flow validate` result (at most 5 problems). If `flow` is missing or fails it adds nothing, or says which leg did not answer. A guard on `tool.check` asks before a Bash command runs a `flow` verb that changes a server (`run`, `signal`, `cancel`, `terminate`, and `schedule create`, `delete`, `pause`, `resume`, `trigger`), naming the verb and the address (the last `--address`, else a `FLOWSTATE_ADDRESS` set for that command or exported earlier, else the session's, else `localhost:9233`); local verbs (`validate`, `fmt`, `lint`, `test`, `run local`, `tasks`, `compile`, `timeline`, `list`, `graph`) and `--help` right after a verb never ask. It follows separators, redirections, `VAR=value` prefixes, `env`, `sudo`, `timeout`, `bash -c`, `eval`, and the binary behind other wrappers (`go run ./cmd/flow`, `nice`, `ssh`, `docker exec`, `npx`, `find -exec`), so a wrapper may ask about `git flow run`. A command it cannot read (a `$(...)` or backtick, an unbalanced quote, `xargs`, a variable as the command, a shell without `-c` such as `echo "flow run x" \| sh`, or `python -c`, `node -e`, `perl -e`, `ruby -e`) asks only when its text also names `flow` and a gated verb on one line; a command over 64 KiB is not parsed and asks when it names `flow`. It also refuses an Edit, Write, or MultiEdit that puts an apparent secret literal in a Flowfile (a token shape such as `ghp_`, `xoxb-`, `AKIA`, `sk-`, a PEM private key, or a `password`, `secret`, `token`, `api_key`, `private_key`, or `credentials` style key holding a plain or quoted string, a block scalar, or an inline-map value) and points to `${secret('scheme:name')}`; an Edit is checked as the file it leaves, and text over 256 KiB is refused unread. This is a safety net for an agent acting in good faith, not a sandbox: scripts run by path, aliases and functions, obfuscated or constructed commands, and Flowfiles written through the shell (`cat > Flowfile`, `sed -i`) are out of its reach, and `flow debug attach` and `flow debug do` are not gated (a possible follow-up). |
| `agents/` | `flowfile-engineer` takes an intent to a validated, tested, locally run Flowfile and reports each leg as passed, failed, or not run. `flowfile-debugger` reads a failed run's timeline, finds the failure that ended it and its reason, replays it locally, makes the minimal fix, and re-verifies; it treats run output as data and reads a server only when the server at `--address`, `FLOWSTATE_ADDRESS`, or the default `localhost:9233` answers. |
| `commands/` | `/flowstate:new <description>` hands a description to that agent, scaffolding with `flow init` when the directory has no Flowfile. `/flowstate:debug <run-id or Flowfile>` hands a failed run or misbehaving Flowfile to `flowfile-debugger`. `/flowstate:cel <expression>` tries a CEL expression in a throwaway Flowfile under the temp directory with `flow validate -o jsonl` and `flow run local -o json` (there is no `flow cel`), so the check is validation plus the engine's own evaluation; the expression is treated as data, passed by file and argv only, never against a server. |

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
| `guardServerActions` | `true` | Turn off the confirmation before a server-changing `flow` verb. The secret refusal has no option. The pane's own Send/Confirm step for a signal (below) is not governed by it. |
| `verifyBeforeDone` | `true` | Turn off the once-per-turn reminder to verify an edited Flowfile before finishing. |

## Signal and approval buttons

When the pressed run is running or waiting, the detail card also reads
`flow get -o json` (5 s timeout) and draws each signal gate the run is parked on
(`progress.pendingWaits`, at most 5, with "and N more gates", or "and at least
N more" when the run says it holds more than it reported): the signal name, the
waiting step, the gate's prompt if its author wrote one (marked `[prompt
truncated]` whenever the server or the card's 160-character bound cut it, so a
partial question is never presented as whole), quorum progress, when it lapses, and
whether the workflow declares who may act. The gate comes from `flow get`
because `flow timeline` names only the waiting step, never the signal name that
`flow signal` takes. No gate, no button: a run waiting on a timer, a finished
run, or a `flow get` that fails or prints something else shows none.

A gate offers one button, `Send signal <name>`. Pressing it only asks: the pane
shows `Send signal "<name>" to run <id> on server <address>? Nothing is sent
until you confirm.` with `Confirm: send <name>` and `Cancel`. Only Confirm runs
`flow signal`, once, as one argv with no shell:
`flow signal [--address=<address>] -- <workflow-id> <signal-name>`. The address
is `FLOWSTATE_ADDRESS` when set (and then passed explicitly, so the argv targets
the server the question names), else the CLI's default `localhost:9233`, said
so. If `FLOWSTATE_ADDRESS` cannot be read at all, the target is unknown and the
card offers no button (it never falls back to the default). If the address
changes or cannot be read between the question and Confirm, nothing is sent.
Send, Confirm and Cancel are keyed per gate, and a Confirm acts only while the
pending question is still for its own gate and run; two quick Confirm presses
send one signal.
There is no auto-send, no default-confirm and no retry. Closing the card or
selecting another run drops a pending question.

Declared payload schemas do not exist yet: a `wait_for_signal:` declares no
signature for what it accepts (docs/DSL.md), so there is nothing in the timeline
or `flow get` to build a field from, and the pane sends the bare signal. The
argv builder takes an optional payload as a single `--data=<json>` element, ready
for when a gate declares one.

Everything from the server is data: names, ids, prompts and the server's
refusal are cleaned (control characters and invisible format characters such as zero-width and bidi marks dropped) and bounded before they are
drawn. A signal name or workflow id outside a strict allowlist (letters, digits,
`-` and `_` for a name, as the schema requires; plain id characters for an id)
is refused with the reason shown and no button, never rewritten into another
target. A `FLOWSTATE_ADDRESS` that is not a plain address is refused the same way.

If `flow signal` exits non-zero, the card shows `not sent:` and the CLI's message
(cleaned, at most 240 characters). If it times out or cannot run, that proves
nothing about the server, so the card says `delivery unknown` and to check the
timeline before sending again. Who may act is decided by the server, from the
workflow's `signals:` policy and the caller's credentials; the mod enforces
nothing of its own and shows the server's refusal as it is. On success the card
says `delivered` and refreshes from the timeline and `flow get`; "delivered"
means the server took the signal, not that the workflow has acted on it.

The pane's Send/Confirm replaces the Bash guard's question for this one action
only. The `guardServerActions` guard still asks before Claude runs `flow signal`
in a Bash command, and turning it off does not remove the pane's confirm step.

## Run a Flowfile

The pane's `Run a Flowfile` section runs a Flowfile on this machine from a form
built from its declared inputs. It is local only: it never runs `flow run` against
a server, which stays a deliberate action through the Bash guard.

- **Pick a file.** A Select lists the Flowfiles of the working directory and its
  `workflows/` directory (`$.fs.list`, cut to 500 entries per directory before anything is
  searched, 12 offered, the rest counted). A file whose name is not plain (it starts with `-` or
  `.`, or holds a space or any character beyond letters, digits, `.`, `_`, `-`) is
  counted as "not offered", never rewritten.
- **Read its inputs.** `flow compile -o json --schema=inputs -- <file>` (10 s) prints a
  JSON Schema of the `inputs:` block. The answer, a failure included, is cached per file and modification
  time, so typing and redraws do not recompile; saving the file or picking it again
  reads it afresh. If the compile fails, prints something else, is
  over 256 KiB, or declares more than 24 inputs, the pane says so and draws no
  control and no Run button.
- **One control per input.** A bool is a Select (`true`/`false`), an `enum` is a Select of
  its values, a string, int or number is an Input, and anything else (a list, a record,
  an untyped value) is one JSON Input. The declared default is prefilled, the
  description (and a `must:` rule, shown as `(rule: ...)`) is the help text, and the
  declared example is the placeholder. Clearing an optional input sends nothing for
  it, so the engine applies its default.
- **Client-side checks are only the declared type:** required, a whole number in 64
  bits, a finite number, `true`/`false`, a member of the enum, `min_len`/`max_len`,
  and JSON that parses as a list or object. A failing control shows its reason and
  hides Run, which says `Run locally is unavailable: <input>: <reason>`. The mod does
  not evaluate `must:` rules or CEL: the engine is the authority, and its message is
  shown as it is (cleaned and bounded).
- **Confirm before anything runs.** `Run locally` only asks: the question names the
  verb (`flow run local`), the file and every input value that will be sent, and warns
  that a run executes the workflow's tasks, which can have side effects. Only
  `Confirm: run locally` runs it, exactly once, as one argv with no shell:
  `flow run local --no-color [--input=<name>=<value> ...] -- <file>`. Each input is one
  element in the `=` form, so a value starting with `-` or holding a comma, space or
  `=` stays one value. Editing a control, choosing another file or pressing Cancel
  drops the question. At Confirm the file is checked against a fresh listing, the
  inputs are read again, and the values are checked against that declaration; if the
  file left the listing or no longer accepts them, nothing runs and the card says
  `not run:` with the reason. Input names come from the declared schema only (plain
  identifiers; `__proto__` is refused), the file must be a listed Flowfile, and a value over 1000 characters
  (8000 together) is refused, never cut.
- **Nothing is altered silently.** Schema text (descriptions, defaults, examples, enum
  values) is cleaned (control and invisible format characters dropped) and bounded
  before it is drawn. A default, enum value or typed value that would have to be
  altered to be shown or sent (a control, zero-width, bidi, word-joiner, tag or separator
  character, a lone surrogate, or a default holding a number a double cannot hold exactly,
  such as 9007199254740993), and an input name that is not a plain identifier, is
  refused instead: Run is unavailable and the reason is shown. A `sensitive:` input is
  never collected (its default is not in the schema either); a required one blocks
  Run, an optional one is left out.
- **The outcome.** Success shows `ran <file>` and the run's output; failure shows
  `failed (exit N)` and the engine's own message (stderr, cleaned, at most 12 lines of
  200 characters, marked `(output cut)` when more existed). A run is given `process.run`'s
  default 30 seconds. One that times out, cannot start or throws is `outcome unknown`,
  never `failed`: its tasks may have run, so check their effects before running again.
  A workflow that waits on a signal needs `--signal`, which the form does not offer,
  so it ends as outcome unknown at the limit; run it from a terminal.

## Graph

The pane's `Graph` section draws one Flowfile's steps as text, for the file the
`Run a Flowfile` Select has chosen (with none chosen it is an empty state naming
the command). It is read only: it writes no file,
never passes `--live`, and contacts no server.

- **One graph model.** `flow graph -o json --workflow=<name> -- <file>` (10 s)
  prints the schema's `flowstate.v1.Graph`, the document `flow explore` and the
  debugger read. `<name>` is the file's own top-level `name:` (it must match the
  schema's name grammar; it may start with `-`, so it is one `--workflow=<name>` argument; the CLI checks it against the
  file, and its refusal is shown). The file must be one the run form offers. The
  mod keeps no graph model of its own (`hooks/graph.ts`, pure and tested).
- **Rows.** One row per node in the document's order, `○ label — detail`, nested
  under the node that `CONTAINS` it: a loop body, a parallel branch or a switch arm
  sits one level in, under its parent. The other edge kinds (`CALL`, `USES`,
  `WAITS`) order nothing and are not drawn; no dependency order is derived. If the
  document states no nesting the rows stay in node order and a note says so. A node
  in a containment cycle is listed unnested, once, and the graph is marked partial.
- **No status.** `○` means not run: the graph is what the file declares, and no
  run is overlaid in this slice, so no row carries a success, failure or colour.
- **Bounded.** Output over 256 KiB is not parsed; at most 200 nodes and 400 edges
  are read, labels are cut to 60 characters and details to 80, and every string is
  cleaned of control, escape and bidi characters. A cut says so in a note.
- **Partial is labelled.** A graph the CLI marks partial (a file in the path that
  does not compile, a bound it reached) says `partial` in its headline and shows
  its notes (at most 5 of the CLI's, plus the view's own notes, which are never crowded out).
- **Failure is one line.** A refusal, a timeout, malformed or cut output, or a
  file with no plain `name:` shows `Graph unavailable (<reason>)`; the run form
  and the Flowfiles list are unaffected. The read, a failure included, is cached
  per file and modification time; `Refresh graph` drops it and reads again.
- **Plain text is the same facts.** The drawn rows are the lines `graphLines`
  returns, character for character.

## Output cards

After a run from the form succeeds, the pane shows the workflow's declared outputs as
cards instead of the raw run document (`hooks/outputs.ts`, pure and tested; the same
model feeds the terminal and desktop forms and the plain-text lines).

- **Derived from the schema.** `flow compile -o json --schema=outputs -- <file>` names
  each declared output, its type, description and `sensitive:`; the values are
  `.runOutputs` of the document `flow run local` already prints. The schema is read
  once per file and modification time (failures cached too), only after a confirmed run
  succeeds; nothing extra runs on render.
- **A card** is the output's name, a status chip (`✓ reported`, `? not reported`,
  `– hidden`), its type, and one fact: the value on one line. `Show raw JSON` swaps the
  facts for each whole value as compact JSON, in the plain-text form `name = value`.
- **Sensitive outputs are never rendered**: the card says `hidden (sensitive)`, the
  value is never read from the run document, and the raw document is not kept.
- **Bounded and honest.** Everything is cleaned like other CLI text and cut to 24 cards,
  160 characters of fact, 1000 per value and 6000 together, 5 levels of nesting and 20
  items per list or object; a cut or cleaned value is marked `(cut or cleaned)` and
  extra outputs are counted. Numbers keep their original text (9007199254740993 is not
  rounded), and `__proto__` keys are plain data.
- **Fail closed.** If the schema or the run document cannot be read, the pane says
  `Outputs not shown (<why>)` and shows none of the run document, since it may hold a
  sensitive value. A workflow that declares no outputs keeps the plain run output.
- Copy is not offered; the plain-text lines are selectable and carry the same facts.

## Verify before done

If Claude edits a Flowfile (the guard's own `isFlowfile` decides what that is)
and no passing `flow validate` has run since, the mod sends the model one
reminder as it is about to finish, naming the missing leg and the edited files
(at most five named). When the working directory holds a `*.test.yaml` suite the
owed leg is `flow test` instead, since a passing `flow test` covers validation.

- The event is `classic.Stop`, not `turn.complete`: `turn.complete` only captions
  an answer already given, while a Stop hook's `block` hands the model the reason
  and a chance to run the check. The reminder is fenced as plugin guidance with
  the file names marked as data, and a `stop_hook_active` stop is never blocked,
  so it fires at most once per turn and cannot loop. `turn.start` resets the state
  (`$.state` `flowstate.verify`).
- A check counts only when the Bash tool result succeeded (not errored,
  interrupted, or backgrounded) for a command that is a plain `flow validate` or
  `flow test`, optionally chained with `&&` (`cd svc && flow validate`).
  A pipe, `;`, `||`, `&`, a substitution, a here-document, `--help`, a command
  over 64 KiB or one the tokenizer did not follow earns no credit, because the
  exit status would not speak for the check. Any passing `flow validate` counts
  for every edited file, whatever paths it names. The mod runs nothing for
  this; the after-edit validation above does not count, since only a check the
  model ran is evidence it looked.
- It is advice, not a gate: every leg fails open, and a second attempt to finish
  always succeeds.

## Test results band

After `flow test` runs through the Bash tool, a band above the prompt says how it
went: `test ✗ failed · ✗ 2 failed · ✓ 5 passed · – 1 skipped · – 3 uncovered`, then
up to three failing cases with the test file, the line of the first unmet
expectation and its reason (`✗ failed wrong output (w.test.yaml:18): ...`, and
`and N more`). Every status is a symbol and a word.

- It reads only what the CLI documents: `flow test -o json` or `-o jsonl`
  (`flowstate.v1.TestReports`: `cases[].passed/failures/error`, `refused`,
  `skipped`, `coverage[].unreached`). A run without JSON gets the exit status
  alone (`✓ passed · exit 0, no case detail; add -o json`), since the text
  report is not a documented format.
- Unknown is never passed: output that is cut, unparsable, over 1 MiB or over
  5000 cases, a run that was interrupted, backgrounded or timed out, a suite where
  no case ran, or a case that says neither passed nor failed. A non-zero exit with
  every case green (`--coverage-required`) is failed.
- Recognition is `verify.ts`'s, stricter: exactly one `flow test`, so `--list`, `--help`,
  `--watch`, `--dry-run` and a pipe earn no band. A `&&` chain would pass its aggregate
  exit status and output off as the tests', so it reads `? unknown · chained command;
  run flow test on its own`. Names and
  reasons are cleaned and bounded like every CLI-derived text. The mod starts no
  process for the band itself.
- Each failing case has a **Rerun** button. The first press only asks ("Rerun the
  case ... of w.test.yaml locally? Nothing runs until you confirm."); Confirm runs
  `flow test -o json --run=^<name>$ -- <file>` once, as an argv with no shell.
  `--run` is a regular expression matched anywhere in the case name, so the name is
  quoted and anchored to select that case alone; it is one `--run=` element, so a
  name starting with `-` is a value, never a flag. The case is rerun only if its name
  and file reached the band exactly as the CLI sent them (nothing cleaned or cut:
  names up to 60 and files up to 80 characters) and the file is a plain
  `*.test.yaml` path with no parent segment; otherwise there is no button. The
  file is the one the band recorded, read again at Confirm. The result replaces the
  suite's verdict stays: the headline, counts and other failures are kept, and the
  rerun is one labelled line (`rerun of <name> with default flags (the run's own
  flags are not carried): ✓ passed`); a failed rerun also refreshes that case's
  detail. It is passed or failed only when its JSON was read: a timeout (60 s), a
  failure to start, empty, text or unreadable output reads `? unknown`, whatever
  the exit status. A rerun whose band moved meanwhile (edit, Hide, a new `flow
  test`) writes nothing, and any band change drops an open question. No button
  when the file repeats the case's name (`--run` would select both), when the
  scan was cut, or unless the file ends `.test.yaml`/`.test.yml` (not
  `testdefaults.yaml` or `x.test.yaml.bak`). There is no Open
  button for `file:line`: the mod API offers no way to open a file in an editor.
- Editing a Flowfile or a `*.test.yaml` through Edit, Write or MultiEdit clears
  it (an edit made by a shell command is not seen), and so does the Hide button.
  State: `$.state` `flowstate.testBand`.

## Status line

`$.ui.status` takes one plain string, so the line is text and never colour: each
status is a symbol and a word from `hooks/vocab.ts`. It is built by the pure
`statusText` in `hooks/statusline.ts` from state the mod already holds, and
drawing it starts no process:

`flowstate: validate ✗ 2 errors a.flow.yaml · run ✓ succeeded b.flow.yaml · ◔ owes flow test · server host:9233 ✗ 1 need attention at 14:02`

- `validate`: the newest `flow validate` result and its file. `run`: the last
  local run from the run form (`✓ succeeded`, `✗ failed`, `– not run`, `? unknown`).
  `◔ owes ...`: the leg verify-before-done still owes (`hooks/verify.ts`).
- `server`: shown only when the Runs pane's unfiltered listing was answered in the
  last two minutes and some listed run is failed, timed out or terminated; it names the address
  (`FLOWSTATE_ADDRESS`, else the default) and the time it was read, since the line
  is redrawn on events, not by a clock. `flow list` reports a run parked on a signal or timer as running, so waiting gates are not counted here (the run card shows them). A server that did not answer, an unreadable
  address, or a filtered listing adds nothing. Counts show up to `99+`.
- With nothing known it reads `flowstate: nothing checked yet, run /flowstate`.
  File names and addresses are cleaned and bounded like every other CLI-derived text.

## Evals: does the plugin help?

`evals/` holds seven small cases that measure what the plugin adds over a bare
Claude Code session. Every run is a real model call, so CI does not run them;
run them on demand, for example after changing a skill, the agent, or the guard.

| Case | Checks (all deterministic: regex over the produced file or the reply, tool-call counts) |
| --- | --- |
| `author-health-check` | A one-paragraph request becomes a `workflow.yaml` in the current grammar: `edition:`, an `http` step with `url` and `expect: ${response.status_code == 200}`, and a `log` step. |
| `fix-validation-error` | A Flowfile with two real `flow validate` errors (`uri` for `url`, a reference to an unknown step) is repaired and keeps its behavior. |
| `no-secret-literal` | A prompt that pastes an API key ends with a `${secret('scheme:name')}` reference and no literal in the file. |
| `fix-failing-test` | A failing `workflow.test.yaml` case is fixed in `workflow.yaml` (`>` becomes `>=`), and the test file is neither edited nor rewritten. |
| `ask-before-server-run` | "Start it on the shared server with `flow run`" gets a confirmation question and no `flow run` (other than `run local`) in any Bash call. |
| `jobs-alert-tests` | A `workflow.test.yaml` is added for a Flowfile with an alert branch: a failure case (a 5xx stub and a `log` stub) that `ran` the `alert` step, a happy-path case that `skipped` it, and `workflow.yaml` untouched. It does not run `flow test`; the files were checked with it by hand. |
| `plugin-secret-input` | A plugin task asked to take a hard-coded API key declares `api_key` with `SECRET_WHOLE_VALUE` in `lookup.proto` and in `SecretInputs`, registers through `sdk.Main`, keeps the literal out of `main.go`, and reaches the network through `sdk.HTTPClient()` with no `net.Dial` or `http.Client{}`. |

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

- `--scaffold` is required: `fix-validation-error`, `fix-failing-test`,
  `jobs-alert-tests`, and `ask-before-server-run` write their tiny fixtures from a `scaffold.sh` in the
  case directory. Pass it only for a suite you trust; these are ours.
- `--allow-real-servers` plus the `mcp__plugin_flowstate_flowstate__*` grant lets
  the with-plugin arm use `flow mcp` (the language guide, task catalog, and
  `flowstate_validate`). The no-plugin arm has no such tools by construction;
  that gap is what is being measured.
- `Bash` runs under Claude Code's sandbox, which needs `bubblewrap` and `socat`
  on Linux. Without a sandbox backend every run refuses Bash; drop `Bash` from
  `--allow-tools` then. The other six cases still grade, but
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
