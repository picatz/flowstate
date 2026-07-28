# The Flowstate command line

This is the contract the `flow` binary holds itself to, and the reasoning behind
each rule. It exists because a command line is a product surface with two distinct
audiences — a person reading a terminal and a program reading a pipe — and almost
every defect in this area comes from serving one of them in a way that breaks the
other.

[docs/ARCHITECTURE.md](ARCHITECTURE.md) describes what the system is.
[CLAUDE.md](../CLAUDE.md) describes how to change it. This describes what a person
meets.

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

## Symbols, not emoji

The CLI uses no emoji. They render at inconsistent widths, break column alignment,
are read aloud unpredictably, and carry tone into places that should be reporting
facts.

What it does use is a small set of restrained typographic marks, and each one has a
plain ASCII fallback selected by the same capability detection that decides colour.
A symbol is decoration for a label, never a replacement: a status is `RUNNING`, and
the mark beside it helps the eye find the row.

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

## Interactive surfaces are optional, never required

Anything the CLI can do interactively it can also do non-interactively, because the
same task is done from a laptop and from a CI job. A terminal UI is an alternative
presentation of a capability that already exists as plain output and flags, and it
is entered deliberately, not by detecting that someone happens to have a TTY.

Animation follows from the same reasoning. It exists to say "still working" during a
wait whose length is not known, it stops the moment there is something to report,
and it never appears where output is not a terminal. Nothing that is only decorative
is worth a repaint.

## What this means for a change

A change to this surface is finished when:

- Data goes to stdout, everything else to stderr, and neither is written twice.
- The output is correct with `NO_COLOR=1`, correct through a pipe, correct on a
  dumb terminal, and correct in a CI log — each verified, not assumed.
- Every added string uses the vocabulary above, and no string contains an emoji.
- A test asserts the *record* rather than that a value appeared somewhere: rows are
  checked as rows, in order, on the line they belong to. `CLAUDE.md` has the longer
  version of why.
