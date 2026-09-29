# Debugging a workflow

A failing test tells you *that* something is wrong. This tells you *why*.

`flow test` answers with a verdict — `expected step "discount" to have run` — and
that is the right answer for a suite. It is the wrong answer for the next five
minutes, when what you need is the value the condition actually saw. The
debugger holds the run at a step boundary so you can ask.

One debugger, several ways in. Every front speaks the same model — the typed
contract in `proto/flowstate/v1/debug.proto` — so a habit learned at one carries
to the others:

| You are | Reach for | What it drives |
| --- | --- | --- |
| a person, debugging a test case | `flow test --debug --run '<case>' <file>` | a prompt, over a stubbed run |
| a person, debugging a real local run | `flow run local --debug <workflow>` | the same prompt, over a real run |
| a person, debugging a durable run | `flow debug attach <workflow-id>` | the same commands, over a run on a worker |
| an editor | [`flow dap`](EDITORS.md#stepping-a-run-flow-dap), launch or attach | a local run, or a durable one |
| an agent | the `flowstate_debug` tool, or the `flowstate_debug_session_*` tools | a scripted session, or one retained across calls |
| a Go program | [`embed.Debug`](EMBEDDING.md#debugging-an-embedded-run) | a local run, in process |

To try it, step through a loop yourself, or replay a recorded session over the
same file:

```console
$ flow run local --debug examples/loop-accumulate/workflow.yaml
$ flow debug replay examples/loop-accumulate/debug.script examples/loop-accumulate/workflow.yaml
```

[examples/debugging](../examples/debugging) walks one small workflow — a loop, a
parallel block and a call — through every front, local and durable.

`flow test --debug` steps through exactly one case in one test file; `--run`
selects it when the file holds more than one.

Local and durable sessions answer the same questions, but a durable run can be
held at fewer places, for a bounded time, by a caller its workflow names. The
architecture's [driver parity boundary](ARCHITECTURE.md#execution-model) names
what a local run proves and what still needs a durable one;
[Debugging a durable run](#debugging-a-durable-run) says what differs.

## One model: sites, occurrences, addresses

A **site** is a place in the compiled program: the workflow that declares a step
and the chain of step ids from that workflow's top level down to it. A step id is
unique within a visibility domain rather than within a file — two sibling loops
may each have a body step called `page` — so the chain, not the last id, is what
names one site.

An **occurrence** is one arrival at a site. The same site reached in the third
iteration of a loop, in the second branch of a `parallel:`, or through a second
`call:` is a different occurrence. Its **address** is the text form, and it is
what the debugger prints and what you type:

| Address | Means |
| --- | --- |
| `pages[2]/page` | the step `page` in iteration 2 of the loop `pages` |
| `checks#1/check_quota` | the step in branch 1 of the parallel `checks` |
| `route?0/chosen` | the step in case 0 of the switch `route` |
| `fan_out(child)/greet` | the step `greet` in the workflow `child` that the call `fan_out` ran |

Indexes count from zero. A breakpoint or `until` target is the same grammar with
every qualifier optional, matched against the *end* of an address: `page` arms
every `page`, a caller's and a callee's alike; `pages/page` arms the `page` in
`pages`; `pages[2]/page` arms that one iteration's. A target never guesses
between two sites — anything longer than a bare id narrows it.

Each stop is a **snapshot**: the run's state (running, pause requested, held,
completed, failed, expired, detached), why it is held (entry, step, breakpoint,
pause, failure, until, or autopsy for a finished test case), the occurrence, its frames innermost first — the step, then each
iteration, branch, arm or call around it — and the session's breakpoints with
their hit counts. A snapshot carries a **revision** that increases on every
change and is never reused, so a question asked about one stop is refused, not
answered, once the run has moved on. A snapshot also carries the backend's
**capabilities**; a front advertises only what they say and refuses the rest by
name.

Every front names a stop by its address, so a stop inside a loop, branch or call
says which iteration, branch or arrival it is: the console prompt prints `break
at orders[1]/charge (task "log")`. The prompt's `backtrace` and the structured
fronts — `flow debug attach` and `do`, the MCP session tools, and Go — list the
same frames, numbered the same way, and `flow dap` shows them as its call
stack:

```text
held at orders[1]/charge (task "log") — breakpoint orders/charge, revision 6
  #1 batch.charge (task "log")
  #2 batch.orders [iteration 1]
```

## The commands

The vocabulary is the one a debugger has had since `dbx`, which is the point —
nothing here is worth learning twice. `help` lists it.

| Command | What it does |
| --- | --- |
| `step`, `s` | run to the next step anywhere, including inside this one. An empty line does the same at the prompt. |
| `next`, `n` | run this step whole, including a loop, parallel, switch or call; stop at the next step at this level or above |
| `finish`, `out` | run until the loop, parallel, switch or call around this step is left, stopping at the next step outside it — not at the next iteration or branch, which `next` reaches |
| `continue`, `c` | run to the next breakpoint, or to the end |
| `until <step>`, `u` | run to that step without stopping in between; a run that completes without reaching it says so, local or durable |
| `until <step> if <expr>` | run to that step, stopping only where the expression holds |
| `break <step>`, `b` | stop there whenever it is reached |
| `break <step> if <expr>` | stop there only when the expression holds |
| `break <step> hit <count>` | stop there from the given arrival on; `hit == 3`, `hit > 3`, `hit % 5` and the rest filter by count |
| `log <step> <message>` | record the message at every arrival without stopping; each `{expr}` is CEL |
| `catch none\|uncaught\|all` | stop where a step fails: never, when its failure will propagate, or always |
| `delete <step>`, `d` | remove that breakpoint |
| `breakpoints` | list them |
| `backtrace`, `bt` | list this step and each iteration, branch, arm and call around it |
| `inspect <expr>`, `p` | evaluate a CEL expression against the held run |
| `complete <partial-command>` | list what could be written at the end of that text |
| `scope` | list what the run can name right now |
| `info` | describe the step it is stopped at |
| `detach` | clear every breakpoint and let the run finish unattended |
| `quit`, `q` | end the run here (which fails the case — see below) |

A `<step>` is a bare id or an address. The structured fronts — `flow debug
attach` and `do`, the MCP session tools, and `embed`'s `Driver` — read the same
lines, less the prompt's own `complete` and `quit`, plus four: `status` prints
the current snapshot, `pause` holds a running run at its next boundary (a run
that completes before reaching one says so), `expand <expr>` pages a map's or
list's children, and `clear` removes every breakpoint, whoever set it. One
prompt form they do not take is `until <step> if <expr>`: a typed resume
names a step and nothing more, so the condition is refused rather than dropped.
`break <step> if <expr>` and `continue` say the same thing there.

A condition is the step's own `if:`, evaluated where the breakpoint is: the
same function, the same scope, and the same refusal of anything that is not a
boolean. So inside a `for_each` the loop's binding is in scope —
`break charge if item.amount > 500` stops at the one iteration you care about
instead of all ten thousand — and a condition cannot mean something different
here than it would written on the step.

It is compiled when you type it, not at each arrival: a malformed expression is
refused there and then, with nothing set, and so is one that cannot type-check
or is not a boolean. A breakpoint accepted broken looks armed and never fires,
which is a failure with no symptom.

A condition that reads a name nothing can bind where the breakpoint fires is
refused too. A condition reads what the step's `if:` reads: `steps`, `inputs`,
`vars`, `run` and `trigger`, and the bare names the loops and steps around it
bind (a `for_each`'s `as:`, a `loop:`'s state, an enclosing step's `vars:`). It
can read those before the run reaches them, so a condition on a loop's binding,
typed at the first step before the loop has run, is armed. A name bound nowhere,
or bound only inside a loop the step is not in, is refused on every front, local
and durable, and a misspelling gets a near name suggested. Against
`examples/debugging`, whose `orders` loop binds `amount`:

```text
debug> break receipt if amount > 500
break receipt: `amount` is bound only inside the loops and steps that declare it, and this breakpoint fires outside them; a condition reads what the step's `if:` reads: `steps`, `vars`, `inputs`, `run`, `trigger`
debug> break charge if amont > 500
break charge: `amont` is not bound where this breakpoint fires; a condition reads what the step's `if:` reads: `steps`, `vars`, `inputs`, `run`, `trigger`, and here `amount`; did you mean `amount`?
```

A program too large to list every step of (past 65,536 sites) cannot say where
a step past the cut fires. Its condition is armed unchecked, and the breakpoint
says so.

A condition that cannot be *evaluated* at some arrival does not hold the run
there, and says so once. That case is ordinary rather than exceptional: a step
id is unique within a visibility domain rather than within a file, so two
sibling loops may each have a body step called `page`, and a condition written
about one of them cannot be answered in the other. Not holding the run is what
keeps `break page if total == 3` from parking you in the loop you were not
debugging — and the notice is what keeps a condition that can never be answered
from looking like one whose answer was always no.

A hit count counts the arrivals whose condition held, so
`break charge hit 2 if amount > 500` stops at the second large order, not the
second order. A bare number means from that arrival on; `==`, `>`, `>=`, `<`,
`<=` and `%` (every Nth) say otherwise.

A logpoint never stops the run. Its message is recorded as an observation at
each arrival, with each `{expr}` replaced by that expression's rendered value,
through the session's redaction:

```text
debug> log orders/charge charging {amount} of {size(inputs.amounts)}
logpoint at orders/charge
debug> continue
log orders[0]/charge: charging 120 of 3
log orders[1]/charge: charging 900 of 3
```

A step whose `if:` is false is never a boundary, so a breakpoint on it does
not stop. The session says why instead, on every front and on both drivers, by
quoting the condition that decided:

```text
discount skipped: `if: steps.price.value > 5000` was false
```

The quote comes from the evaluation that made the decision, and nothing is
evaluated again to produce it; `inspect` at the next stop is how you find which
operand made it false. An `if:` that raises an error instead of answering fails
its step, and the step list and observations show that step as failed rather
than as one the run never reached.

`catch uncaught` stops at a step whose failure its own step does not tolerate
with `continue_on_error:`, after the failure is recorded and before it
propagates; `catch all` stops at tolerated failures too. A container that
tolerates the failure further out is not consulted, so such a stop may precede a
run that goes on.

Over MCP this is the difference between reachable and not. A script is bounded
at a hundred commands, so `break charge if item.id == "x"` then `continue` is
two commands where stepping to the five-thousandth iteration is impossible.

`complete` is the tab key made into a command, and it exists because a terminal
has a key for this and nothing else does. Without it the completion below is
reachable only by a person with a keyboard, while a scripted session — the
`flowstate_debug` tool's whole shape — could not ask at all. It answers like
`inspect`: a question about where the run is standing, which does not move it.

```text
debug> complete inspect steps.
build   a step that has run
test    a step that has run
```

## The prompt

At a terminal, `debug>` is a real prompt rather than a reader: **tab completes**,
the editing keys work (ctrl-a, ctrl-e, ctrl-w, ctrl-u, ctrl-k, the arrows), and
up and down walk the commands you have already typed in this session.

Tab completes over the *paused run's own scope*, which is the point:

```text
debug> inspect <TAB>
steps.        step outputs
vars.         workflow variables
inputs.       run inputs
…             the profile's functions
debug> inspect steps.<TAB>
build   a step that has run
price   a step that has run
debug> inspect steps.price.<TAB>
value   an output this step produced
```

An editor can only offer what a task *declares*. A paused run knows which steps
have actually produced outputs and what those outputs are actually called — so
`steps.price.<TAB>` after a shaping expression offers the names the run
produced, not the ones the task's schema names. The rules for where each name
may be written are the language server's own, shared, so `steps.<id>.<output>`
means one thing in both places.

Tab also completes the commands, and the step ids `break`, `until` and `delete`
take — `break` over every step the workflow declares, including the ones inside
a `for_each` body, because a breakpoint is for somewhere the run has not been.

**A completion is a name and never a value.** No preview, no type, no length:
a debugger's printing is behind the same redaction as everything else here (see
*Sensitive values* below), and a popup that showed you what a name held would be
a second door around it. Where a case's redaction would withhold a *name*, the
offer is dropped rather than shown redacted.

**ctrl-C ends the run**, exactly as `quit` does — a run abandoned at a
breakpoint did not pass. **ctrl-D** leaves the debugger and lets the run finish
unattended, which the session says out loud when it happens.

None of this applies when stdin is not a terminal. `flow test --debug <
script.txt` and the `flowstate_debug` tool read the same commands the same way
they always did; the line editor is attached only where somebody is actually
typing.

## What `inspect` answers

`inspect` is the reason to stop at all. It is the *engine's own* evaluator over
the run's own activation, so it can name exactly what the file could name at
that point — `steps.<id>.<output>`, `inputs`, `vars`, a loop's binding, `now`
inside a wait — and it is cost-bounded ([`DefaultCostLimit`]) like every
expression in the file. It cannot resolve a secret: `secret(...)` is compiled
into a reference when a workflow is built and is never a function anything
calls, so there is nothing there to call.

## The autopsy

A case that fails is held open once more *after* the verdict, with the failures
printed and the finished run still questionable. This is where most debugging
actually happens: you do not know which step to break on until you know which
expectation broke.

```text
autopsy: the case failed 1 expectation(s); the run is over, but its scope is still here
  expect.ran: expected step "discount" to have run, but it produced no recorded outputs
(`inspect` questions the finished run; `quit` or `continue` leaves — the verdict is already in)
debug> inspect steps.price.value
4000
debug> inspect steps.price.value > 5000
false
```

At the autopsy the bindings a failing `expect.check:` was judged under are in
scope too — the file's `vars`, and a `run` root carrying `failed` and `error` —
so a claim that failed can be taken apart with the same names it was written
with. `scope` lists them, and `complete` answers here as well:

```text
debug> complete inspect run.
error    bound for this autopsy
failed   bound for this autopsy
```

which matters more here than anywhere else, since these are the only bindings
a check was ever judged under and the only place they can still be read.

**The verdict is already in, and nothing here can change it.** The autopsy runs
after the expectations are judged, so a debugged run cannot be argued into
passing. `quit` is the one exception in the other direction: abandoning a run is
a verdict, and a case whose run was abandoned did not pass. A command with
nothing left to act on — `break`, `backtrace`, `info` — says so rather than
reading as a typo.

## Driving it as an agent

MCP has no console, so the session takes a **script** instead — which works
because the session reads its commands as a stream, the same property that makes
a session replayable.

```json
{
  "workflow": "edition: v2026.3\nname: checkout\n...",
  "tests": "tests:\n  - name: a big cart gets the discount\n    ...",
  "commands": ["step", "step", "inspect steps.price.value", "inspect steps.price.value > 5000", "continue"]
}
```

The answer carries three things: the `session` transcript (each fragment with
the `tone` a terminal would have coloured it — `break`, `warning`, `danger`), the
`script` the session accepted, and the `report` — the ordinary `flow test`
verdict, because a debugged run is the run.

```text
[break  ] break at price (value)
[info   ]   price -> value: 4000
[info   ]   discount skipped: `if: steps.price.value > 5000` was false
[break  ] break at charge (task "log")
[info   ]   charge completed
[break  ] autopsy: the case failed 1 expectation(s); the run is over, but its scope is still here
[danger ]   expect.ran: expected step "discount" to have run, but it produced no recorded outputs
[info   ] 4000
[info   ] false
```

Five commands, and the bug is in hand: the cart total is 40, the price step
multiplies by 100, and the threshold is 5000.

Two properties worth knowing before you write a script.

**A script that runs out is not a hang.** The session resumes and the run
finishes, saying so (`no more commands — continuing to the end of the run`).
That is what makes a scripted session safe on a surface with no console — and it
means a script of pure `inspect` commands is a legitimate thing to send.

**`script` is the input to the next call.** Re-send it with more commands
appended and you get the same session, further along. There is no session handle
to keep alive, and nothing to leak if you never call again.

The tool debugs a *test case* — stubs, no egress, no secret resolved, a virtual
clock — which is why it needs no operator opt-in. Debugging a real, unstubbed
local run is `flow run local --debug`, at a terminal, under that command's own
egress policy.

### A session that outlives the call

A script is right when you know the questions in advance. When the next command
depends on the last answer, `flow mcp` over stdio keeps a session open across
calls instead:

| Tool | What it does |
| --- | --- |
| `flowstate_debug_session_start` | start a session over one test case — the same stubbed run `flowstate_debug` uses — held at its first step |
| `flowstate_debug_session_attach` | attach to a durable run on the configured server; `session_id` rejoins one |
| `flowstate_debug_session_command` | run one command line and answer with the typed result: the receipt, the next stop's snapshot, or an inspection |
| `flowstate_debug_session_observe` | read the snapshot and the transcript since the last observe; `after_revision` and `wait_seconds` wait for the next stop |
| `flowstate_debug_session_end` | end it: a durable run is detached and continues (`keep` leaves its session attached), a test case finishes and its report is returned |

```json
{"name": "flowstate_debug_session_command",
 "arguments": {"session_id": "5bd7…", "command": "break receipt if size(steps.flagged.value) > 0",
               "expected_revision": 2, "request_id": "agent-7"}}
```

A session is leased: every call renews it, one idle for ten minutes is ended,
and none lives longer than an hour — a sweeper ends it whether or not anyone
calls again. One `flow mcp` process holds at most eight, and at most one of
them over a test case: while it is open, a second `start`, `flowstate_test`
and `flowstate_debug` are refused, naming it, because its case holds the
process's task registry until it ends. Attaching with the `session_id` of a
session this process already holds answers that session rather than a second
one, but only on the same run: a different `workflow_id`, or a different pinned
`run_id`, is refused. A session that is still ending is waited for, up to ten
seconds, by a rejoin of it and by a new `start`, `flowstate_test` or
`flowstate_debug` while it is a test case, and after that the call is refused
as still ending. A case that fails before it can hold — a stub or an expectation naming no
step — says why in the snapshot's message, as well as in the report `end`
returns.
`expected_revision` refuses a command meant for a stop the run has already left,
rather than applying it to the next one. A movement or an inspection carries it
to the run, which judges it in the same step as the command; a pause or a
breakpoint change, which no stop binds, is checked just before it is sent. A command carrying a `request_id` is
answered from memory when retried, so a lost response never moves a run twice,
a start carrying one never starts a second run, and an attach carrying one
answers with the session it attached, whose id the lost response carried,
rather than attaching again beside it. The key names one call: reused for
another workflow, it is refused.

`start` takes the workflow and tests as text, like `flowstate_debug`, so a
`call:` to a relative path has no directory to resolve against and the case
fails to compile; attach to a durable run of it instead, or use
`flow test --debug`. The session
tools are absent from `flow mcp serve`: a session held open for minutes would
hold the lock every other HTTP caller's stubbed run waits on.

## Replay, and what a session records

Every accepted command is recorded, and `Session.Script()` hands the list back.
Two consequences:

- A session is reproducible. The script that found a bug is the script that
  demonstrates it, and it goes in the issue.
- Mistyped commands are not recorded. `setp` is answered (`unknown command
  "setp"`) and left out, so a replayed script re-runs the questions rather than
  the typing.

## Sensitive values

A debugger *is* a reveal: the session narrates each step's values as it goes, and
`inspect` reaches whatever is in scope. So the same rule the renderers follow
applies here rather than a second, weaker one.

- `flow run local --debug` **refuses** a workflow whose declarations would make
  the final render withhold its transcript, naming `--reveal-sensitive`. Say the
  reveal out loud, or do not attach a debugger — there is no third answer where
  the debugger quietly shows what the renderer would have hidden.
- `flow dap` makes the same refusal before starting the local run. An editor can
  state the deliberate reveal as `"revealSensitive": true` in its launch
  configuration, or whoever starts the adapter can pass `--reveal-sensitive`.
- `embed.Debug` makes it too, before anything runs: a workflow that declares
  sensitive values, or whose declarations cannot be read, is refused unless the
  embedding program sets `DebugOptions.RevealSensitive`.
- Under `flow test --debug` and `flowstate_debug`, the case's own redaction
  posture applies to **everything the session prints** — each step's account as
  it arrives, every `inspect` answer, and the autopsy's failures — so a
  declared-`sensitive:` input or a case secret renders `[redacted]` there
  exactly as it does in the transcript beside it. Evaluation still sees the
  real value, and a claim comparing against one still holds; only the printing
  withholds. This is a transcript control, not a boundary against the person
  at the prompt. Whoever runs `flow test --debug` or `flowstate_debug` supplied
  the case's fixtures, so `inspect inputs.token == "guess"` answers truthfully
  and a breakpoint condition can name a sensitive value. Redaction keeps the
  value out of the transcript; it does not stop the session's owner from asking
  about it. The one exception is the autopsy's own bindings: `flow test` binds
  the file's `vars` and `run.error` there already redacted, so
  `inspect vars.token == "the real value"` answers false at the autopsy even
  where the same check was true, while `inputs` and `steps` still compare
  against real values. The autopsy prints a note saying which is which.
- An input a **called** workflow declares `sensitive:` is withheld too, on both
  drivers, although the case's posture never saw that declaration. It is
  withheld at a hold inside that callee, and in whatever it calls with the
  value. It is also withheld from each step's account and from a failure that
  quotes it. That covers the failing step, every call the failure passes
  through, the failed run's final message, and the call's own account of the
  outputs the callee hands back.
- A value that crosses back into the caller's scope stays withheld there. That
  covers a returned output and a tolerated call's recorded error, whether they
  are read at a later hold, in a later step's account, or inside another callee
  the caller passes them to under a plain name. It also covers an output the
  callee declares `sensitive:`, whatever it was computed from, and a
  compensation's failure in the run's final message that quotes the inputs it
  was registered with. On the durable driver, a run declaring `debug:` withholds
  these values from the failure it records, a failed compensation's text
  included, since that failure is printed by a reader that knows only the
  root's declarations. They are withheld at the source, so revealing sensitive
  values when reading that run does not bring them back. On the durable
  driver, what a call handed back is kept in the run's memory and never
  written to its history. So after Continue-As-New, a run whose calls handed
  back anything withheld withholds everything a session is shown, rather than
  less than it did before the seam.
- The case's own transcript and report withhold these values everywhere, with
  or without a debugger, a `--seeds` divergence report included. They are
  rendered after the run, from everything its steps withheld.
- A durable inspection renders a run's declared-`sensitive:` inputs as
  `[redacted]`, and the same holds: a predicate over one answers truthfully.
  That is why evaluating anything against a durable run needs its own action,
  `workload.debug_inspect`, rather than riding on permission to hold the run.

## Reading a durable run

For a run executing on a worker somewhere else, two verbs answer two
questions.

`flow get <id>` answers what a run **is** doing: its status and timing, where
it has reached, the steps Temporal is retrying right now and why the last
attempt failed, the gates it is parked on, and — for a run shaped as an entity,
which never finishes and therefore never has outputs — a bounded snapshot of
the state it is carrying.

`flow timeline <id>` answers what it **did**, which is the question left when a
run has already finished and there is no present to report:

```text
TIME      WHAT     STEP                        DETAIL
10:14:02  step     `request`
10:14:02  done     `request`
10:14:02  waiting  `approval` · wait timeout
10:16:31  signal   deploy-approved
10:16:31  step     `deploy`
10:16:32  done     `deploy`
10:16:32  ended
```

It starts nothing, signals nothing and changes nothing, which is what makes it
the one verb about a live workload that an agent can be pointed at unattended —
`flowstate_get_timeline` over MCP is the same answer.

A step that retried appears once per attempt as a `failed` row, which is what
makes a stuck run legible: the same step failing five times the same way is a
different fact from five steps failing once.

```text
TIME      WHAT     STEP        DETAIL
10:14:02  step     `charge`    attempt 1
10:14:04  failed   `charge`    attempt 1: connection refused
10:14:09  failed   `charge`    attempt 2: connection refused
10:14:24  done     `charge`    attempt 3
```

An attempt that has failed and is *waiting out its retry backoff* is the one
thing this does not show. History holds nothing about it yet — the fact lives in
the worker's pending state until the next attempt starts — so `flow get` is the
verb that answers it, which is the same split as everywhere else: `get` for now,
`timeline` for what happened. The row appears here as soon as the next attempt
begins.

Every failed attempt gets a row with its attempt number, including attempts
Temporal records only as detail on the next attempt's start, so filtering the
timeline for failures finds all of them. Failure text is decoded through the
deployment's own payload converter, so a deployment that encrypts payloads still
sees the real message rather than `Encoded failure`.

A failure's message is bounded, and says so when it was cut. A task fails with
whatever string it likes, and a run started by an outside party is not ours to
assume anything about; the answer as a whole stops against a byte budget too,
because a per-message cap times an entry ceiling is still several megabytes.

`truncated` says the account is not the whole of a segment — never something to
infer from a short answer. Continue it with `--run-id` and `--after-event-id`,
which the command prints for you:

```text
this is not the whole of this run's account — continue with --run-id 0198f1e2-… --after-event-id 4821
```

Both flags, and a cursor without a run id is refused rather than guessed at.
Event ids restart at 1 in every segment, so a cursor counts within one and means
nothing until that one is named — and an unnamed run resolves to *whichever is
latest*, which is a different segment the moment the workload continues as new
between two calls. Applied there, the old cursor would skip the new segment's
beginning or mix two segments into one account, with nothing in the answer
saying so.

Raising `--max-entries` is not the way past a truncation either: the ceiling is
a ceiling, and one segment can legitimately hold several times the largest
answer the server returns. Each read walks the run's history from the start,
which is what lets a resumed page still name its steps: a label is written onto
a step's *scheduling* and nowhere else, so a reader that began in the middle
would have rows it could not name.

A run that continued as new has an account per segment, and the chain is
walkable in both directions — `nextRunId`, `previousRunId`, and `firstRunId`
for where the workload began. Both directions matter because omitting a run id
reads the *latest* segment, whose successor is by definition empty: forward
links alone would leave a caller holding only a workflow id unable to reach any
earlier segment at all.

What it never reads is an activity's payload. That is the resolved task, and
decoding it to label a row would put an author's inputs on the read path where
the caller is whoever asked. A step is named by its label or not at all. The one
payload-shaped thing reported is a failure's outermost *message*, exactly as
`flow get` already reports the last failure of a retrying step — never the
chain, because Temporal's failure converter writes every level of an unwrapped
error into what it persists.

Underneath both, the run's own history names its steps. Every command the
interpreter writes carries a one-line summary, so a run is legible in the two
tools an operator already has — Temporal Web, and `temporal workflow show`:

| Summary | The command it labels |
| --- | --- |
| `` `build` `` | the activity that step's task runs in |
| `` `pages` > `page` `` | the same, for a step inside a `loop:`, `parallel:` or `call:` |
| `` `build` · undo `` | the compensation that undoes it |
| `` `nap` · sleep `` | the durable timer a `sleep:` parks on |
| `` `gate` · wait timeout `` | the timer bounding a `wait_for_signal:` |
| `run vars` | the run's own top-level `vars:`, evaluated once |
| `` `fan_out` · call vars `` | a callee's `vars:`, named by the step that called it |
| `plugin admission` | the check that the worker has the plugins the run pins |

The position and not only the id, because an id is unique within a *visibility
domain* rather than within a file: two sibling `loop:` blocks may each declare a
body step called `page`, legally, since body outputs do not escape. A very deep
position is elided from the outside in and says so with a leading `…`, keeping
the step that actually ran.

This matters because one interpreter runs every workflow, so the *activity* is
always typed `Task` or `TaskInScope` — without the summary, a hundred-step run
renders as a hundred identical rows and the only thing telling them apart is
inside each activity's input payload, which is the last place a reader should
be looking. Those payloads hold resolved task inputs; a label is a separate,
deliberately tiny field carrying step ids and nothing else.

One command is labelled by id alone: a compensation. It is dispatched from the
run-level undo stack, whose entries record a step id and no position, so two
sibling loops each undoing a body step of the same name still read alike.

On a deployment running a payload codec these are encrypted with everything
else and read back through its codec server, exactly as the workflow-level
summary beside them is.

## Debugging a durable run

A durable run can be held at a step boundary, stepped, given breakpoints, and
questioned with CEL — by a caller its workflow names, for a bounded time. A hold
gives you time: to look at the systems the run touches before its next step
acts, to read what it has computed so far, or to stop it advancing while you
decide what to do.

### Who may: `debug:` and two actions

The workflow has to allow it. `debug:` has the grammar of one `signals:` entry,
and **without it, nobody may debug the run**, including the person who started
it:

```yaml
debug:
  allow:
    - claims:
        team: sre
```

The server then asks for one of two authorization actions, and a token that
carries an action list must name the one the call needs:

| Action | Covers |
| --- | --- |
| `workload.debug` | `DebugAttach`, `DebugGet`, `DebugResume`, `DebugSetBreakpoints`, and a raw `Signal` on the reserved `flowstate_debug` channel |
| `workload.debug_inspect` | `DebugInspect`, any breakpoint set carrying a condition or a log message, and reading those expressions back |

Inspection is its own action because it is a disclosure: an expression can test
any value in the held scope, whatever its rendering hides. A breakpoint
condition is the same disclosure one bit at a time — whether the run stopped
answers `inputs.token == "guess"` — so setting one needs the inspect action too,
and so does reading one back: a caller without it sees such a breakpoint with
no definition, and a client that would resend the set refuses rather than drop
it.
Only the session's holder may resume, set breakpoints on, or inspect it; a
second caller's attach is refused rather than queued. Every decision, allowed
or denied, is audited with the session, the request id and the operation, and
an inspection or a conditional set is recorded by the digest of its
expressions, never their text.

The reserved channel is closed to other doors: `SignalWithStart` refuses a
`flowstate_` name outright, and a raw `Signal` onto `flowstate_debug` needs
`workload.debug` beside `workload.signal`, plus `workload.debug_inspect` when it
carries a condition or a log message.

### Attach, read, drive

```console
$ flow debug attach <workflow-id>
attached to <workflow-id> — session 5ae9… (pending: delivered; the run applies commands at its next step boundary, and has not reached one yet. …)
held at orders (for_each) — pause, revision 2
  #1 debugging.orders (for_each)
  session 5ae9…, lease until 2026-09-28T00:54:12Z
debug> break receipt if size(steps.flagged.value) > 0
breakpoint at receipt if size(steps.flagged.value) > 0
debug> continue
held at receipt (call "receipt") — breakpoint receipt, revision 4
  #1 debugging.receipt (call "receipt")
debug> step
held at receipt(receipt)/compose (value) — step, revision 6
  #1 receipt.compose (value)
  #2 debugging.receipt (call "receipt")
debug> inspect inputs.count
3
debug> detach
applied
```

(The lease line after each stop is left out here.)

`flow debug attach` reads commands from the terminal or `--script`, and prints
each answer as text; with `-o jsonl` each answer is a line of the schema's JSON,
and with `-o json` they are one array, written when the session ends; with
either, the prompt goes to stderr. At a terminal a line that fails prints why
and the prompt returns. A script's later lines assume its earlier ones did
what they said, so with `--script` such a line fails the attach and releases
the run: one that errors, one the run refuses, such as a stale movement, or a
`break` or `log` the run takes but will not arm, such as a condition that does
not compile. A set still pending has no verdict yet, so it does not fail the
script. `flow debug do` fails on the same lines. It renews the
session's lease while it runs. `detach`, `quit`, or the end of input releases
the run; `disconnect` leaves the session attached, and prints how to rejoin it
with `--session` before the lease lapses. `--program <file>` names the Flowfile
the run was started from so frames can show lines; it is used only when it
compiles to the program the run executes, the plugin and task pins the
deployment wrote on admission aside, and otherwise the attach says the file
does not match and lines are not shown. A program compiled from a file records
the digest of the file's bytes, so a file whose lines moved, even without
changing a step, names a different program and its lines are not used. A run
submitted without a file, through the API, records no digest and shows no
lines. Where a deployment runs its own copy of a workflow in place of the one
submitted, the run executes that copy, so `--program` must name the deployed
file.

Two more verbs work without holding anything open:

```console
$ flow debug get <workflow-id>                          # the snapshot; changes nothing
$ flow debug do <workflow-id> --session 5ae9… next      # one command in a session left attached
$ flow debug do <workflow-id> --session 5ae9… inspect steps.orders.results.size() -o json
```

Each `flow debug do` is a fresh client, and a breakpoint set is replaced
whole, so before a line changes the set it adopts every breakpoint the run
reports from the definition reported with it: `break` and `delete` add to or
take from what an earlier call or an editor set, `delete log <step>` removes a
logpoint, and `clear` removes the lot.

The same five RPCs are on the [API](API.md) and are MCP tools of their own
(`flowstate_debug_attach` and its neighbours); `flow dap`'s attach and the
`flowstate_debug_session_attach` tool drive them for you.

### Holding a durable run

**Where it holds.** A durable run holds only at a step boundary where it has
one position: a step at the run's own top level, or at the top level of a
workflow a `call:` reached. Inside a `for_each` or `loop:` body, a `parallel:`
branch, or a `switch:` arm the run can be at several places at once and a hold
names one, so those bodies run as a unit. `step` from a `call:` step enters the
callee; `step` from a loop runs the whole loop. A breakpoint on a step inside
such a body is reported not armed, saying to break at the enclosing step instead,
and an `until` whose step is inside one, or that names no step at all, is
refused and the run stays held, rather than released to the end. Past
`MaxDebugStaticSites` (65,536 step sites) the run cannot list every site, so it
asks the program as written instead: a breakpoint or `until` on a step the
program declares where a run holds — at its top level or a callee's — is
armed or applied, even when that step lies beyond the cut, and one on a step it
declares only inside such a body, or never declares, is refused as it would be
below the cap. The local driver runs those bodies one step at a time and stops
everywhere.

**What a hold does not stop.** A hold parks workflow code before a step starts.
Work already dispatched — an activity, an HTTP call, a timer, a called
workflow's own step — keeps going, and so does time: a `wait_for_signal:`
timeout and the run's execution timeout are measured on the clock, not in steps.
An attach asked while a step runs, or while the run sleeps or waits, takes effect
at the next boundary; until then its receipt is pending.

**The hold is a lease.** Each attach or renewal buys `--lease` (default 2
minutes, at most 10), and the whole session ends at most 10 minutes after it
was first granted, however often it is renewed. A lease nobody renews lapses,
the session ends as `expired`, and the run resumes on its own, so an abandoned
debugger cannot park a production run. `flow debug attach`, `flow dap` and the
MCP sessions renew while they are connected.

**It lives in the run.** The session — its id, holder, lease, breakpoints, hit
counts, revision and recent receipts — is decided by workflow code from recorded
signals, so a server restart, a second server, or a worker killed mid-session
changes nothing: the next worker replays the history and the run is still held
where it was. It crosses Continue-As-New with the run, and each occurrence says
which segment it ran in. `--run-id` pins the chain by its first run id; unset,
a session follows the current one.

| | Local | Durable |
| --- | --- | --- |
| Where it stops | every step boundary, including loop bodies, parallel branches and switch arms | top-level steps of the run and of each called workflow |
| `step`, `next`, `finish`, `until`, `pause` | yes | yes, at those boundaries |
| Conditional and hit-count breakpoints | yes | yes; a condition needs `workload.debug_inspect` |
| Logpoints (`log`) | yes | taken with the set, but reported not armed |
| Failure stops (`catch`) | yes | refused as unsupported |
| A breakpoint or `until` inside a loop body, branch or arm | yes | the breakpoint is not armed and the `until` is refused |
| Source-line breakpoints | when a source map is known | resolved by the client to a step, only through a source map that matches the run's program; `flow dap`'s attach has none |
| `inspect`, `expand`, `scope` | yes | yes, while held, needing `workload.debug_inspect` |
| Task notes (`NoteTask`) | yes | no |
| Lease, holder, audit | none: it is your process | yes |
| Ending the run | `quit` | never: `detach` and `quit` release it |

### Retry-safe commands and receipts

Every command carries a request id, and the run answers each with a receipt:

| Status | Means |
| --- | --- |
| `applied` | the run acted on it; the receipt names the first revision that shows it |
| `pending` | delivered, not yet applied — the run has not reached a boundary. Poll `DebugGet`, or retry with the same request id |
| `duplicate` | that request id was already applied; nothing moved twice |
| `stale` | the command named a revision the session has left |
| `conflict` | another caller holds the session, or the session id is not the one the run holds |
| `refused` | not valid now, such as stepping a run that is not held |
| `unsupported` | this backend does not do it — a failure stop, durably. A logpoint is not refused this way: the set carrying it is applied and the logpoint is reported not armed |
| `incompatible` | the run's interpreter predates the protocol the command needs; nothing was sent it would misread |
| `ended` | the session or the run is over |

Delivery is not application, which is why `pending` exists: a command travels
as a signal and takes effect when the run next reaches a boundary. A resume can
name the revision it was decided against, so a `next` meant for one stop is
refused as stale rather than applied to the stop after it. The run keeps its
most recent receipts — up to 64, each message cut to 1 KiB — in its own state,
so a retry after a lost response is answered from the run rather than applied
again, even across a worker restart.

### The signal it travels on

Every command is a typed ask on the reserved `flowstate_debug` channel, so it is
ordered in history with everything else the run hears, and `flow timeline`
shows each one and each lease:

```text
10:53:09Z  waiting  debug lease c9a7… held by https://flowstate.local/dev#sre@example.com expires …
10:53:09Z  signal   flowstate_debug
```

The earlier untyped hold still works for a run whose workflow declares `debug:`:
`flow signal <workflow-id> flowstate_debug --data '{"verb": "pause", "lease": "5m"}'`
holds at the next boundary and `{"verb": "resume"}` releases it, with the same
lease and holder rules, and with `workload.debug` required as above. While a
typed session is attached, its commands own the hold and an untyped resume is
ignored.

A local run needs none of this: `flow run local --debug` holds at every step
with no lease and no policy.

## Debugging an embedded run

`embed.Debug` runs a workflow under the same local session, in the calling
process, and hands back a value whose methods are the contract's: `Snapshot`,
`WaitSnapshot`, `Resume`, `ReplaceBreakpoints`, `Inspect`, and `Driver()` for
the command lines above. A custom task can report its own progress to whoever is
watching with `v1.NoteTask`. [Embedding](EMBEDDING.md#debugging-an-embedded-run)
has the example.

## What it does not do yet

- Go backwards. Historical or reverse debugging is not implemented: every front
  reports `reverse` as unsupported, and a rerun is not history.
- Stop a durable run where a step fails, or record a logpoint durably. A
  failure stop is refused as unsupported: the durable driver has no place to
  hold after a failure is recorded. A logpoint is taken with the breakpoint set
  but reported not armed, since its expressions would be a second, unaudited
  inspection channel; a scripted `flow debug attach` fails on it.
- Hold inside a step. Every driver holds between steps only: never inside a
  task, an HTTP call or a called activity, and never partway through a `sleep:`
  or a `wait_for_signal:`. A durable run holds at fewer boundaries still — see
  [Holding a durable run](#holding-a-durable-run).
- Change a value. The debugger reads a run; it cannot edit it.
- Share a session. A local session has one driver: one terminal, one DAP client,
  or one MCP session. A durable session has one holder.

[`DefaultCostLimit`]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1#DefaultCostLimit
