# An approval that chases, escalates, and eventually decides without you

An approval gate that only waits is a request that can go missing. Somebody is asked
once; if they are on leave, in a meeting, or simply never see the message, the request
sits open until a person happens to notice it — and nothing in the system knows the
difference between "still being considered" and "forgotten".

This example is the shape a real approval chain has instead. It asks, waits a period,
asks again, and after a couple of unanswered periods it asks somebody else — the backup
approver. If the whole budget of asks runs out with nobody deciding, the request is
rejected on purpose, by the workflow, and the run says so. Nothing is left open and
nothing is left to a person remembering.

`examples/approval-gate` is the single ask this is built on, and its `signals:` block is
what decides who may answer. `examples/renewal-reminder` is the cadence — a `loop:`
around a bounded `wait_for_signal:` — and `examples/expense-approval` is escalation
written as two gates in sequence, each person asked once. This file is what those three
compose to.

## The durability property

Every period this run spends waiting costs nothing and holds nothing. The wait is state
on the execution substrate, not a worker parked in a loop, so a chase that takes four
days survives every deployment of the worker fleet that happens in those four days, and
an answer arriving on the fourth day resolves it exactly as fast as one arriving in the
first minute.

The comparison worth making is against the obvious alternative. A cron job chasing
approvals has to persist, itself, which request it is chasing, how many times it has
asked, who it asked last, whether it has escalated yet, and what to do when the budget
runs out — and re-derive all of it after every restart, from a table it also has to
keep correct. Here there is nothing to re-derive: the loop's carried state *is* how many
asks have gone out, and the run *is* the request.

## The cadence is the gate's own timeout

There is no scheduler in this file and no reminder table. The cadence is one line:

```yaml
- id: gate
  wait_for_signal:
    name: approval-decision
    timeout: 1h
```

An iteration asks, then waits `1h` for an answer. If the answer comes, the chase is
over. If the deadline passes, the wait reports `timed_out`, the loop goes round, and the
next ask goes out. So the period between reminders is not configured anywhere — it *is*
the gate's timeout, and there is no second copy of it to drift.

That is also what makes the cadence testable rather than merely described.
`workflow.test.yaml` runs the identical file on `flow test`'s virtual clock, where a
signal can be scripted for a named moment:

```yaml
signals:
  - name: approval-decision
    at: 2h30m
...
    outputs:
      reminders_sent: 2
```

`2h30m` against a `1h` cadence is inside the third ask, so exactly two reminders have
gone out — and the case asserts `2`, not "at least one". Change the timeout to `2h` and
that number becomes 1 and the case fails, which is the whole point: the number is a
function of the cadence, so the cadence is what the test is really pinning.

## `max_iterations:` is not the reminder budget

This is the one thing to take away from the file, because the grammar cannot yet say it
and the wrong reading is the natural one.

A chasing loop has two ceilings that look like the same number:

- **`vars.ask_budget`** — how many times anybody is asked before the request is decided
  without them. This is a policy: the business has decided that four unanswered asks
  means no.
- **`max_iterations:`** — the engine's *per-segment* ceiling, the number of iterations
  one Continue-As-New segment runs before the run suspends and resumes carrying its
  state (see "carried state" in `docs/DSL.md`). This is a bound on a mechanism.

They differ in what happens when they are reached. Reaching `max_iterations:` **fails
the run** — deliberately, because a loop whose stop condition never held has not decided
anything, and reporting that as a completed run is how a request quietly stops being
chased with nobody deciding it should. Reaching `vars.ask_budget` **completes the run**
with `decision: auto_rejected`, which is a decision the file made on purpose and an
answer a caller can act on.

So the budget lives in the loop's `until:`, which is the only place a loop can end
without failing:

```yaml
until: ${steps.gate.verdict != "undecided" || asks + 1 >= vars.ask_budget}
```

and `max_iterations:` stays a ceiling well above it that this file never reaches. Collapse
the two and it goes wrong in whichever direction you collapsed them: lower
`max_iterations:` to "the budget" and the auto-reject the file promises becomes a failed
run, and raise `vars.ask_budget` past `max_iterations:` and it becomes a failed run at a
number nobody wrote down.

`examples/renewal-reminder` is the file where the other reading is correct — a customer
who never renews genuinely has no outcome the workflow may invent, so its budget *is*
`max_iterations:` and running it out *is* a failure. The two files disagree because they
are answering different questions, which is worth seeing side by side.

## The decision is attested, and escalation does not widen who may make it

Who may answer this run is `signals:`, checked by `FlowstateServer.Signal` against the
sender the server authenticated — before Temporal ever sees the delivery. A caller who
fails it is refused synchronously and the workflow is never told anything was sent.

```yaml
signals:
  approval-decision:
    allow:
      - subject: ${"https://issuer.example.com#" + inputs.primary_approver}
        claims:
          team: release-managers
      - subject: ${"https://issuer.example.com#" + inputs.backup_approver}
        claims:
          team: release-managers
    distinct_from_starter: true
```

Two rules, matched as alternatives — and that is the whole of the escalation's security
story. **Escalation changes who is asked; it cannot change who is allowed.** The backup
approver can answer at any point in the run, including before the escalation ever
happens, because this file's author wrote a rule naming them. There is no moment where
the policy is looser than it was at submit: `signals:` is read by the server from the
run's own memo, frozen when the run started, never re-read from a running workflow's own
reasoning. A chase that escalated by *widening a policy* would be a workflow that can
grant itself authority, which is exactly the thing this shape must not be.

Within each rule the two constraints are ANDed. The `claims:` half is literal — this
file's author wrote `team: release-managers` and no caller's input can touch it. The
`subject:` half is interpolated from an input, so a caller can narrow which release
manager this particular run accepts, and cannot invent an approver outside the team the
file already named. `flow validate` refuses an interpolated subject with no literal
constraint beside it, for that reason.

`workflow.test.yaml` writes the negative direction rather than only the positive one: a
sender who satisfies the claim exactly, is distinct from the starter, and is simply not
either approver this run named is refused — so the chase runs to its budget and
auto-rejects, which is the same answer nobody answering at all produces.

And what the run reports as `approver_subject` is `sender.identity.subject`, never a
field from the payload. A payload is evidence; a sender is identity.

## Two commands

Locally, with nobody answering — four asks a period apart, the last two to the backup,
then the auto-reject. Note that `flow run local` reads the wall clock, so this is a real
four hours; the file's subject is a long duration, and the test file is the better way
to watch it:

```console
$ flow run local examples/approval-escalation/workflow.yaml \
    --input-file examples/approval-escalation/inputs.json
```

```console
$ flow test examples/approval-escalation/
```

Answer it instead, standing in for the approver `signals:` names — an assertion rather
than an attestation, which the gate's own `sender.local` says out loud, but checked by
the same function the server checks it with:

```console
$ flow run local examples/approval-escalation/workflow.yaml \
    --input-file examples/approval-escalation/inputs.json \
    --signal approval-decision='{"approved": true}' \
    --signal-as-subject sre-lead@example.com \
    --signal-as-issuer https://issuer.example.com \
    --signal-as-claim team=release-managers
```

And durably, where the chase spans real days across as many worker deployments as it
takes (needs a Temporal dev server, `flow worker` and `flow server` — see the main
README's Quickstart):

```console
$ flow run examples/approval-escalation/workflow.yaml \
    --input-file examples/approval-escalation/inputs.json
```

```console
$ flow signal <workflow-id> approval-decision --data '{"approved": true}'
```

## The interesting lines

- **The loop asks before it waits.** `renewal-reminder` waits first and does the
  periodic work on the lapse; here an iteration asks and then waits a period for the
  answer. That is what makes the first ask an ordinary iteration rather than a step
  outside the loop that would have to be written twice, and it makes the count honest:
  one iteration is one ask, so `results.size()` is how many times somebody was asked and
  every ask after the first is a reminder.
- **`escalation` is a real step behind `if: ${asks == vars.asks_before_escalation}`.**
  It runs on the one iteration that crosses the threshold, so a person reading the run's
  log sees the moment the request changed hands rather than inferring it from a
  recipient that quietly differs.
- **`undecided` covers both a lapse and an answer that decided nothing.** A signal
  carrying no `approved` field is an acknowledgement, not a decision, so the chase
  continues — one comparison in `until:` for both, since the request still needs an
  answer either way. The cost, which the file states rather than hides: that answer
  resolves the wait immediately, so the next ask goes out at once instead of a period
  later, and enough acknowledgements spend the budget. That direction fails closed, which
  is the side to be wrong on.
- **`reminders_sent` and `escalated` are derived from the loop's own record.** Two
  `value:` steps — `asks_made` and `reminders` — name what `steps.chase.results` says
  once, and every reader reads those, rather than a second counter carried alongside
  `asks` that could disagree with the record, or the same `results.size()` arithmetic
  restated at five sites (which is what `flow lint` reports as R5/repeated-expression).
- **`outputs.decision` cannot disagree with the branch that ran.** `settle` dispatches on
  the same `steps.outcome.value` this output reports, so the report is not derived from
  the same facts as the branch — it *is* the branch. The three outcomes are assembled by
  two one-question `value:` steps rather than a nested ternary, which is `docs/STYLE.md`
  R5 and what `flow lint` reports.
