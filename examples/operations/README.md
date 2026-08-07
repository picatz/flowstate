# Operations

Two capabilities in this repository are shipped, tested, and documented, and cannot
be demonstrated by a Flowfile — because neither one is a property of a workflow.
They are properties of the *processes* a deployment runs:

| Walkthrough | Shows |
| --- | --- |
| [tenant-routing](tenant-routing/) | `flow server --task-queue-prefix` and `flow worker --tenant`: one worker fleet per tenant, each with that tenant's own secrets and egress policy, and the two half-configured command lines that are refused at startup |
| [worker-versioning](worker-versioning/) | `flow worker --deployment-name --build-id`: a run pinned to the interpreter it started on, upgraded at Continue-As-New, and the refusal when given half the pair |

## Why these are here, and not somewhere else

**Not under `examples/plugins/`.** That directory tree exists for one mechanical
reason, stated in its own READMEs: a file naming a plugin's task must sit outside
the `examples/*/workflow.yaml` glob, because with no plugin loaded the correct
answer for such a file is a diagnostic. Neither walkthrough here names a plugin
task, and filing them there would tell a reader they need a plugin built before
they can follow along. They do not.

**Not as `examples/<name>/workflow.yaml` either**, which is the more interesting
half. Every directory matching that glob is enumerated by CI: it is run on both
drivers, checked by `flow fix --check`, and required by
`TestEveryExampleHasATestFile` to carry a `workflow.test.yaml` asserting what it
teaches. Neither of these has anything for such a file to assert — a workflow
cannot observe which task queue its worker polled or which build id that worker
declared, and a `workflow.test.yaml` claiming otherwise would be asserting the
harness rather than the capability.

So each walkthrough runs an **existing** example instead, and says which one. That
is deliberate: shipping a new, otherwise-unused `workflow.yaml` alongside a README
would put a Flowfile in the tree that nothing in CI checks, which is a small copy
of exactly the gap [#272](https://github.com/picatz/flowstate/issues/272) found and
this directory exists to close. The one file with no coverage should not be added
by the change that closes the coverage gap.

**One directory rather than two.** Both are the same kind of thing — a claim about
process topology that a reader confirms by starting two processes and watching what
they refuse — and both are read by the same person on the same afternoon, the one
standing up a deployment. `docs/DEPLOYMENT.md` is where the tiers and the matrix
live; these are the two command-line walkthroughs that document links out to.

## The bar these are held to

The same one every example is held to, adapted to what can be checked here. Every
command below is one you can paste. Every refusal quoted is the message the binary
actually prints, taken from the code that prints it rather than paraphrased — the
refusals are the interesting part, because a misconfiguration that is *accepted* is
the failure shape both of these exist to prevent.
