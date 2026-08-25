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
| `delete <step-id>`, `d` | remove that breakpoint |
| `breakpoints` | list them |
| `inspect <expr>`, `p` | evaluate a CEL expression against the paused run |
| `scope` | list what the run can name right now |
| `info` | describe the step it is stopped at |
| `quit`, `q` | end the run here (which fails the case — see below) |

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
with. `scope` lists them.

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
- At the autopsy, the case's own redaction posture applies to what prints: a
  file var holding a secret's plaintext renders `[redacted]`, exactly as a
  failing check's witnesses render it. Evaluation still sees the real value —
  only the printing withholds.

## What it does not do yet

Durable runs. Pausing a run executing on a worker somewhere else is a different
problem — it needs a wire protocol, a lease so an abandoned session cannot park
a production run forever, and a policy for who may attach — and it is
[#928](https://github.com/picatz/flowstate/issues/928)'s slice 2. Today the
debugger is a local-driver instrument, which is where authoring happens.

DAP, so that an editor's own debug UI drives this, is the front after MCP.

[`DefaultCostLimit`]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1#DefaultCostLimit
