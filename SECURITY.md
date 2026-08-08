# Security Policy

Flowstate runs other people's workloads under other people's policies, so a
security report here is a report about a boundary somebody is depending on.
This document says how to make one and what happens next.

## Reporting a vulnerability

Report privately through GitHub's private vulnerability reporting on this
repository (Security tab, "Report a vulnerability"). That path keeps the
report, the discussion, and any fix coordinated in one place, visible to the
maintainer and to you, and to nobody else until an advisory is published.

Please do not open a public issue for anything you believe is a
vulnerability, and do not include working exploits against deployments you do
not own.

A good report names the boundary it crosses. The ones this project promises,
and therefore most wants to hear about breaking:

- A tenant reaching another tenant's runs, secrets, signals, or schedules.
- A secret's material appearing anywhere in workflow history, logs, errors,
  memos, or telemetry, in any formatting shape.
- An unauthenticated or under-authorized caller reaching an RPC, a signal
  delivery satisfying a `signals:` policy it should not, or
  `distinct_from_starter` being satisfiable by the starter.
- Egress policy bypass: a task reaching a destination the deployment's policy
  denies, including via redirects, DNS tricks, or a plugin.
- A parser, evaluator, or reader made to consume unbounded memory, time, or
  requests from input an outside party chooses.
- `flow fix` corrupting a valid file, since the whole promise of the command
  is that it is safe to run on anything.
- Plugin sandbox assumptions: a plugin exceeding what its manifest and the
  operator's base configuration grant it.

Reports about the documented development postures are appreciated but are not
vulnerabilities: `--insecure-no-auth`, `flow run local`'s rehearsal
identities, and anything the docs already mark "read this before copying it
into production" behave as documented.

## What to expect

- Acknowledgment within 3 business days.
- An assessment (accepted, needs more information, or declined with reasons)
  within 14 days.
- A fix or documented mitigation for accepted reports as fast as severity
  warrants, with a published advisory and credit to the reporter unless you
  ask otherwise.

This is a maintained open-source project, not a staffed security team; those
targets are honest rather than aspirational, and complex reports may take
longer with communication rather than silence.

## Safe harbor

Good-faith research against your own deployments of Flowstate is welcome.
Testing against deployments you do not own or operate is out of scope and is
not authorized by this policy. Good faith means: no data exfiltration beyond
the minimum proof, no degradation of other people's service, no social
engineering, and private disclosure with reasonable time to fix. Within those
bounds, this project will not pursue or support action against you for the
research itself.

## Supported versions

Fixes land on `main`. Until versioned releases carry their own support
statement, `main` is the supported version and advisories will say which
commits are affected.
