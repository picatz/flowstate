# Debugging a workflow

A failing test tells you *that* something is wrong. This tells you *why*.

`flow test` answers with a verdict — `expected step "discount" to have run` — and
that is the right answer for a suite. It is the wrong answer for the next five
minutes, when what you need is the value the condition actually saw. The
debugger holds the run at each step so you can ask.

One session core, three ways in. They are the same object with different front
doors, so a habit learned at one carries to the others:

| You are | Reach for | What it is |
| --- | --- | --- |
| a person, debugging a test case | `flow test --debug --run '<case>' <dir>` | a prompt, at the step boundary |
| a person, debugging a real local run | `flow run local --debug <workflow>` | the same prompt, over a real run |
| an agent | the `flowstate_debug` MCP tool | the same session, driven by a script |

## The commands

The vocabulary is the one a debugger has had since `dbx`, which is the point —
nothing here is worth learning twice.

| Command | What it does |
| --- | --- |
| `step`, `s` | run this step, stop at the next. An empty line does the same. |
| `continue`, `c` | run to the next breakpoint, or to the end |
| `until <step-id>`, `u` | run to that step without stopping in between |
| `break <step-id>`, `b` | stop there whenever it is reached |
| `break <step-id> if <expr>` | stop there only when the expression holds |
| `delete <step-id>`, `d` | remove that breakpoint |
| `breakpoints` | list them |
| `inspect <expr>`, `p` | evaluate a CEL expression against the paused run |
| `complete <partial-command>` | list what could be written at the end of that text |
| `scope` | list what the run can name right now |
| `info` | describe the step it is stopped at |
| `quit`, `q` | end the run here (which fails the case — see below) |

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

A condition that cannot be *evaluated* at some arrival does not hold the run
there, and says so once. That case is ordinary rather than exceptional: a step
id is unique within a visibility domain rather than within a file, so two
sibling loops may each have a body step called `page`, and a condition written
about one of them cannot be answered in the other. Not holding the run is what
keeps `break page if total == 3` from parking you in the loop you were not
debugging — and the notice is what keeps a condition that can never be answered
from looking like one whose answer was always no.

Over MCP this is the difference between reachable and not. A script is bounded
at a hundred commands, so `break charge if item.id == "x"` then `continue` is
two commands where stepping to the five-thousandth iteration is impossible.

`complete` is the tab key made into a command, and it exists because a terminal
has a key for this and nothing else does. Without it the completion below is
reachable only by a person with a keyboard, while a scripted session — the
`flowstate_debug` tool's whole shape — could not ask at all. It answers like
`inspect`: a question about where the run is standing, which does not move it.

    (flow) complete inspect steps.
    build   a step that has run
    test    a step that has run

## The prompt

At a terminal, `debug>` is a real prompt rather than a reader: **tab completes**,
the editing keys work (ctrl-a, ctrl-e, ctrl-w, ctrl-u, ctrl-k, the arrows), and
up and down walk the commands you have already typed in this session.

Tab completes over the *paused run's own scope*, which is the point:

```
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

```
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

```
debug> complete inspect run.
error    bound for this autopsy
failed   bound for this autopsy
```

which matters more here than anywhere else, since these are the only bindings
a check was ever judged under and the only place they can still be read.

**The verdict is already in, and nothing here can change it.** The autopsy runs
after the expectations are judged, so a debugged run cannot be argued into
passing. `quit` is the one exception in the other direction: abandoning a run is
a verdict, and a case whose run was abandoned did not pass.

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

```
[break  ] break at price (value)
[info   ]   price -> value: 4000
[info   ]   discount skipped (`if:` was false)
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
- Under `flow test --debug` and `flowstate_debug`, the case's own redaction
  posture applies to **everything the session prints** — each step's account as
  it arrives, every `inspect` answer, and the autopsy's failures — so a
  declared-`sensitive:` input or a case secret renders `[redacted]` there
  exactly as it does in the transcript beside it. Evaluation still sees the
  real value, and a claim comparing against one still holds; only the printing
  withholds.

## Reading a durable run

The debugger is a local-driver instrument, so the question for a run already
executing on a worker somewhere else is what it can *tell* you rather than
where you can stop it.

Two verbs, answering two questions.

`flow get <id>` answers what a run **is** doing: its status and timing, where
it has reached, the steps Temporal is retrying right now and why the last
attempt failed, the gates it is parked on, and — for a run shaped as an entity,
which never finishes and therefore never has outputs — a bounded snapshot of
the state it is carrying.

`flow timeline <id>` answers what it **did**, which is the question left when a
run has already finished and there is no present to report:

```
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

A step that retried appears once per attempt, with the attempt number and the
sentence the previous one failed with, which is what makes a stuck run legible:
the same step failing five times the same way is a different fact from five
steps failing once. The row saying how a step *ended* carries that attempt
number too, which Temporal does not record on it — a terminal event references
the scheduling and the start and names no attempt, so the account carries it
forward from the start it belongs to.

`truncated` says the account is not the whole of a segment — never something to
infer from a short answer. Continue it with `--after-event-id` set to the last
row's event id. Raising `--max-entries` is not the way past it: the ceiling is
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

## What it does not do yet

Pausing a durable run. Reading one is the section above; *stopping* one is a
different problem — it needs a wire protocol, a lease so an abandoned session
cannot park a production run forever, and a policy for who may attach — and it
is [#928](https://github.com/picatz/flowstate/issues/928)'s slice 2. Today the
debugger is a local-driver instrument, which is where authoring happens.

DAP, so that an editor's own debug UI drives this, is the front after MCP.

[`DefaultCostLimit`]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1#DefaultCostLimit
