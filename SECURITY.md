# Security Policy

Flowstate runs other people's workloads under other people's policies, so a
security report here is a report about a boundary somebody is depending on.
This document says how to make one and what happens next.

[THREAT_MODEL.md](THREAT_MODEL.md) is the design document behind this policy:
what each boundary below is meant to hold against, and — in its "Non-goals and
honest gaps" section — what it deliberately does not cover yet. Check that
section before reporting: a gap named there is a documented, accepted
limitation rather than a new finding. A report showing one of those gaps is
worse in practice than the model assumed is still very much wanted.

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
- Prompt-injection-driven agent misuse: the stdio agent surface (`flow mcp`)
  authenticates the process it is talking to, not each individual request, so
  a report showing how untrusted content reaching an agent can make it act
  outside what the operator who launched that process intended is in scope.
- Issuer-key compromise or workload-assertion forgery: anything that lets a
  caller mint or accept a workload identity assertion the issuer never
  signed, or that widens what an assertion can claim beyond what
  `THREAT_MODEL.md` §7 ("The issuer as a single point of failure") says is
  bounded.

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
