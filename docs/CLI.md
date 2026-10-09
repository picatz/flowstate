# The Flowstate command line

This is the contract the `flow` binary holds itself to: how it serves two
audiences at once (a person reading a terminal and a program reading a pipe),
what goes to stdout and what to stderr, how colour and symbols degrade, how
errors are worded, and what exit statuses mean. It is written for people adding
or reviewing a command, and for anyone scripting against `flow` who wants to
know which behaviours they can rely on.

For what the commands are, see the generated [command reference](reference/cli.md),
built from the same command tree the binary uses, with every flag, default, and
environment variable. For using them, start with [Get started](GETTING_STARTED.md);
[Testing workflows](TESTING.md), [Debugging](DEBUGGING.md), and
[Using Flowstate from an agent](MCP.md) cover `flow test`, `flow debug`, and
`flow mcp`. Configuring secrets on a worker or a local run is in
[Secrets and credentials](SECRETS.md). [CLI_DESIGN.md](CLI_DESIGN.md) holds the
concrete design tokens, symbols, and views this page's rules are applied with.

## The two audiences

**stdout carries the answer. stderr carries the account of it.**

`flow get x | jq` must receive a workload's outputs and nothing else. `flow list |
awk '{print $1}'` must receive rows. Everything else a run produces — what it is
doing, that it succeeded, that more remains, that a page failed — belongs on
stderr, where a person watching a terminal still sees it and a pipe does not.

The rule generalizes past the obvious cases, and the awkward ones are where it
earns its keep:

- A **diagnostic is an answer**, not commentary. `flow validate` exists to report
  problems, so its diagnostics are its output. An editor and a `make` wrapper both
  consume them, which is why they keep the `path:line:col: message` shape that
  every tool already parses.
- A **confirmation is an account**, not an answer. "asked X to stop" tells a person
  something happened; no pipeline wants it, so it does not go where a pipeline
  reads.
- **The same fact must not be written twice.** A result printed both to a log line
  and to stdout is a result a pipe reads once and a person reads twice, and the two
  copies drift.

When a command has no answer, stdout stays empty. An empty stdout is a meaningful
value: it is what "no runs" looks like to a program. A table header written when
there are no rows is worse than nothing, because it is indistinguishable from a
listing that succeeded and found none.

## Colour is a capability, never a preference

Styling is decided once, per stream, from what that stream can actually do:
whether it is a terminal, what colour depth it supports, and what the environment
has asked for. Nothing else in the CLI decides for itself.

The consequences are non-negotiable, because each of them is a way people get
burned:

- **A pipe gets no escape sequences.** Not fewer, none. Detection is per stream, so
  `flow get x | jq` may be plain while the status line on stderr is styled, in the
  same invocation.
- **`NO_COLOR` is honoured**, and so is `CLICOLOR_FORCE` for the person who wants
  colour through a pager. A CI job with no TTY gets plain text without being asked.
- **`TERM=dumb` means dumb.** No cursor movement, no repainting, no spinner.
- **Every style must survive removal.** Meaning is carried by the words and the
  layout; colour and weight only make the meaning faster to find. A line that reads
  correctly in black and white is the only kind allowed, because that is the line a
  log file, a screen reader, and a colour-blind reader receive.

## The palette works in both directions

A terminal's background is the user's choice, and a palette that assumes one is a
palette that is unreadable for half its audience. Colours are therefore declared as
a pair — one value for a light background, one for dark — and resolved once at
startup.

Two rules keep the pair honest. Both members must clear a contrast floor against
their own background, which rules out the mid-tone greys that look fine to whoever
picked them and vanish for everyone else. And a colour never carries meaning
alone: status is a word first and a colour second, so the palette is an
accelerator rather than the channel.

Where the depth is lower than the palette assumes, colour degrades to the nearest
of what is available and then to weight — bold and dim — and then to nothing. Each
step down loses emphasis and no information.

Which half of each pair to use is a question asked *of the terminal*, in the one
place the CLI does that: an OSC 11 query written out, and a reply read back. Two
things follow, and both are load-bearing.

It is asked at most once per process, and only when the answer can change a byte.
Below ANSI both halves resolve to the same styles, so a `NO_COLOR` reader and a
`TERM=dumb` terminal are never asked — which matters because those are among the
terminals least likely to reply.

And a terminal that replies to *nothing* is waited on for two seconds per file,
which is four seconds of a command printing nothing before it behaves normally.
That reads as a hung network or a wedged server, the two places somebody would look
and neither of them it.

That is narrower than it sounds, and worth stating precisely because the obvious
guess is wrong. The query asks two things at once — the background colour, and the
primary device attributes every terminal answers — so a terminal that simply does
not implement background reporting still ends the wait immediately. Measured
against a pty answering only the second: 0.02s. The four seconds belong to a pty
answering neither, which is automation holding a tty rather than a terminal
somebody is sitting at.

`FLOWSTATE_BACKGROUND=dark` or `=light` settles it without asking, for exactly that
case: 4.02s to 0.02s. Anything else in that variable — including empty — is ignored
rather than guessed at, since a variable somebody exported and left blank is not an
assertion about their terminal.

## Symbols, not emoji

The CLI uses no emoji. They render at inconsistent widths, break column alignment,
are read aloud unpredictably, and carry tone into places that should be reporting
facts.

What it does use is a small set of restrained typographic marks, and each one has a
plain ASCII fallback selected by the same capability detection that decides colour.
A symbol is decoration for a label, never a replacement: a status is `RUNNING`, and
the mark beside it helps the eye find the row.

`FLOWSTATE_SYMBOLS=unicode` or `=ascii` overrides that detection, on the same
principle as `FLOWSTATE_BACKGROUND`: the derivation can be wrong about a terminal,
and the person sitting at it is the only one who can see that.

## One vocabulary, everywhere

The words a person meets in `flow --help` are the words they write in a Flowfile,
the words the RPC uses, and the words the documentation uses. A concept with two
names is a concept the reader has to translate, and every translation is a place to
be wrong.

Two distinctions matter enough to state:

- A **workload** is the thing someone defines; a **run** is one execution of it. A
  workload is addressed by its workflow id; one attempt at it has a run id. They are
  different identifiers with different lifetimes, and `--run-id` exists precisely
  because approving a deploy means approving the workload, not one attempt.
- A **namespace** is a Flowstate tenant. Where a deployment also maps onto Temporal
  namespaces, the text says *Temporal namespace* in full, every time, because the
  two are different boundaries and the reader cannot tell from context.

Where a flag name must be spelled the same on two commands but means two different
things, that is a defect, not a convention.

## Errors say what to do next

A refusal names what was refused, why, and what would work instead. Three habits
carry most of that:

- **Name the thing and the verb.** "refused while listing runs" beats "permission
  denied", because the reader knows which of the six things they just ran was
  refused.
- **Do not narrow a cause you do not know.** A run that cannot be addressed may not
  exist, may belong to another tenant, or may have aged out of retention. Saying
  "check the id" when it might be any of the three sends people hunting for the
  wrong mistake.
- **Advice a reader can paste beats advice they must interpret.** Where a fix is a
  command, the message contains that command.

The exit status is part of the message. A run that finished as a failure exits
non-zero, so `flow get x && deploy` behaves the way the shell reader expects — the
query succeeded, and what it reports is a failure.

## A command is the act; a file is only a declaration

`flow schedule create` exists as a separate verb rather than as something `flow run`
notices, and the reason generalises beyond schedules.

A Flowfile may declare that a workload is meant to run every weekday at 07:00. That
declaration is reviewed with the steps it belongs to, which is the whole argument for
writing it in the file. It is also, on its own, inert: `flow run` does not create a
schedule and `flow run local` ignores the block, so nothing in this tool turns merging
a file into work being done. Somebody types the verb.

The cost of the alternative is not that a schedule appears — it is that its *first
firing looks like somebody meant it*. An unexpected run at 07:00 on a Tuesday is
indistinguishable from an intended one until somebody goes looking for who asked, which
is the kind of ambiguity an operator pays for at the worst moment. A verb somebody typed
leaves an answer to that question.

Two habits follow for anything else that reaches this shape:

- **Refuse everything refusable at the moment a person is present.** `flow schedule
  create` checks the specification, the cadence and the arguments there, rather than
  letting a firing discover them. A refusal at 03:00 in a worker's log, about a mistake
  made at a keyboard a week earlier, is a refusal nobody reads.
- **Answer with what makes the mistake visible.** `create` prints the next firing times
  without being asked, because a cadence that means something other than what was
  intended is almost always obvious there and almost never obvious in the expression
  that produced it. `--paused` exists so that answer arrives before anything fires.

A scheduled Flowfile that names plugin tasks is checked against a saved catalog:

```console
$ flow schedule create workflow.yaml --plugin-catalog plugins.lock.json
```

Scheduling executes no plugin in the CLI process, so it does not take
`--plugin-dir`; the catalog is the reviewable descriptor source, while the server
and workers remain responsible for resolving and executing the deployment's
plugins. Offline compilation accepts either posture: `flow compile --plugin-dir`
for a developer with the binaries, or `flow compile --plugin-catalog` for CI and
review without executing plugin code.

## Interactive surfaces are optional, never required

Anything the CLI can do interactively it can also do non-interactively, because the
same task is done from a laptop and from a CI job. A terminal UI is an alternative
presentation of a capability that already exists as plain output and flags — never
the only way to reach one.

Whether a terminal may be *detected* depends on what the surface changes:

- A surface that changes **what a command does** — a picker that chooses the
  argument, a form that supplies input, a prompt that decides — is entered
  deliberately, by a flag or a subcommand. Detection there means the same invocation
  does two different things depending on where it ran, which is the defect the
  `--output` flag exists to avoid.
- A surface that changes only **how the same information is presented** may follow
  the terminal, on three conditions. The non-terminal shape has to be the same
  command carrying the same information; a flag has to be able to ask for the plain
  shape *on* a terminal, because a person reading with a screen reader or capturing
  under `script(1)` must not be trapped by having a TTY; and an explicitly requested
  `--output` format must win, since a document was asked for and a terminal was not.

`flow watch` is the second kind, and the way it splits the streams is what makes the
two shapes compose rather than compete: the live view is drawn on stderr and the
outputs go to stdout, so one invocation shows progress on the terminal and pipes its
answer to `jq`.

The debugger's full-screen view is the third. `flow debug attach`, `flow run local
--debug` and `flow test --debug` open it by default where stdin and stdout are both
terminals of at least 60 columns by 12 rows, because it is the same session with the
same commands and answers drawn as panes. It is never used under `--script`, with a
piped stdin or stdout, with a machine `--output`, in a CI environment (`CI` set) or
with `TERM=dumb`; there the line editor and the script front print the bytes they
always did, and say nothing about the screen. `--tui=false` asks for the line editor
on a terminal, and `--tui` spelled out where there is no terminal is declined with one
sentence on stderr instead of being ignored. See [Debugging](DEBUGGING.md#the-default-is-a-full-screen-debugger).

Animation follows from the same reasoning. It exists to say "still working" during a
wait whose length is not known, it stops the moment there is something to report,
and it never appears where output is not a terminal. Nothing that is only decorative
is worth a repaint — which is why a live view moves a number that answers a question
(how long has this been going) rather than a spinner that answers none.

Two rules about what such a view may claim, both of them the no-silent-caps rule
from `CLAUDE.md` applied to a screen. A list cut to fit says how many it cut and how
many there are, because a window that looks like a whole list is one a reader counts
wrongly. And a view that stopped being able to reach the server says so, rather than
becoming a still screen that cannot be told apart from a wedged one.

## The machine surface

Everything above serves a person. For a program, the contract is:

- **The server verbs are projections of RPCs.** `run`, `get`, `list`,
  `signal`, `cancel`, `terminate`, `timeline`, and the `schedule` verbs are thin
  clients of the [control-plane API](API.md); a capability a script needs from a
  server is an RPC, not a CLI feature. Local verbs (`validate`, `compile`,
  `fmt`, `fix`, `lint`, `test`, `run local`) call the same library code the API
  does, in process.
- **`--output json` is a document, and it is stable per verb.** Verbs that
  answer with a run (`run`, `run local`, `get`, `watch`) write the run document:
  a step's outputs at `.steps.<id>.<output>` and declared outputs at
  `.runOutputs.<name>`, as plain JSON values. `--raw` writes the schema's own
  protojson instead, for a consumer generated from `flowstate.v1`. `validate`,
  `compile`, `test`, and the listing verbs write their response or report
  message as protojson. `lint` and `server dev` write their own documented
  shapes. `--output jsonl` is the same document on one line, or one line per
  change for `watch`.
- **Mutations answer with one envelope.** `cancel`, `terminate`, `signal`, and
  the schedule mutations have empty RPC responses, so each writes a
  `flowstate.v1.MutationResult`: `verb`, `workflowId`, `runId`, `scheduleName`,
  `signalName`, and `result`. It carries only what this process knows: what it
  asked, and that the server accepted it. `delivered` means the server accepted
  a signal, not that a waiting step consumed it; read the run back with `get` to
  learn that. The meaning of each `result` value is in every mutation verb's
  `--help`.
- **Exit status has three values.** `0`: the command succeeded and the answer is
  not a refusal. `1`: the command worked and the answer is a refusal or a
  finding: diagnostics found, a check failed, or a run finished as a failure. `2`:
  the invocation itself was wrong: an unknown flag or command, or the wrong
  number of arguments. A program branches on these, never on prose.
- **Reads are pure.** `validate`, every `--check`, and every read have no side
  effects, so a program or an agent can loop on them unattended. Mutations run
  when invoked; there is no confirmation prompt, so a script that should not
  mutate should not call a mutating verb.

## `flow mcp`: the same surface, for an agent

`flow mcp` serves the same RPCs, plus local validation, testing, debugging, and
rehearsal, to an AI agent over the Model Context Protocol. How to configure a
client, what the server exposes, and the loop it is shaped around are in
[Using Flowstate from an agent](MCP.md).

## `flow test`: the local driver only, on purpose

`flow test` runs a workflow's `*.test.yaml` cases through the local driver, with
stubbed tasks, scripted signals, and a virtual clock, so the edit-run-read loop
takes well under a second and needs nothing provisioned. Durable behavior
(history, recovery, Continue-As-New) is what `flow run` against a dev server is
for. The test file format and the command are documented in
[Testing workflows](TESTING.md).

## What this means for a change

A change to this surface is finished when:

- Data goes to stdout, everything else to stderr, and neither is written twice.
- The output is correct with `NO_COLOR=1`, correct through a pipe, correct on a
  dumb terminal, and correct in a CI log — each verified, not assumed.
- Every added string uses the vocabulary above, and no string contains an emoji.
- A test asserts the *record* rather than that a value appeared somewhere: rows are
  checked as rows, in order, on the line they belong to. `CLAUDE.md` has the longer
  version of why.
