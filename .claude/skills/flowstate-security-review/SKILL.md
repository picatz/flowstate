---
name: flowstate-security-review
description: Review a Flowstate change or suspected vulnerability across trust boundaries.
---

# Flowstate security review

Read `THREAT_MODEL.md`, `SECURITY.md`, and the changed trust boundary as needed.
Scanner output or another model's assertion begins as an observation, not a
confirmed vulnerability.

## Establish the case

For each candidate issue, determine:

1. **Asset and boundary** — what is protected and which trust transition changed.
2. **Preconditions** — attacker capability, configuration, identity, and state
   required before the path is reachable.
3. **Reachability** — the concrete data or control-flow path from influenced input
   to the sensitive operation.
4. **Existing controls** — validation, authorization, CEL policy, bounds,
   redaction, Temporal semantics, or deployment assumptions that block or reduce
   the scenario.
5. **Effect** — the behavior that actually occurs, not the behavior a nearby API
   name suggests.
6. **Impact** — confidentiality, integrity, availability, tenant isolation,
   durable-history exposure, or operator consequence.
7. **Evidence and confidence** — reproducer, test, trace, code proof, and the
   remaining uncertainty.

Promote an observation to a finding only when the behavior, reachable path, and
material consequence are supported. State when the result depends on deployment
configuration or a disputed trust assumption.

## Flowstate-specific lenses

- Does any resolved secret, token, or sensitive fixture enter workflow history,
  diagnostics, logs, generated examples, or an error path?
- Does missing identity, policy state, or evaluator failure deny?
- Do local and Temporal paths enforce the same authorization and redaction?
- Can author-controlled input drive unbounded work, memory, retries, diagnostics,
  or payload growth before a limit stops it?
- Does a cache, registry, or snapshot cross tenant, namespace, run, or interpreter
  boundaries without the identity needed to separate them?
- Does a compatibility change reinterpret durable state written by an older
  worker?
- Do tests prove isolation in the negative direction: A cannot reach B?

## Output

For each material finding: severity, location, observation, preconditions,
reachable path, impact, evidence, confidence, existing mitigation, and the
smallest credible remediation. Deduplicate shared root causes. Return `PASS` when
no material issue is supported; do not manufacture security theater.
