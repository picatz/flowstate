# Acting on a token somebody sent you

A run holds a token: a callback from a build system, a webhook body, a partner's
request. Two questions have to be answered before it acts, and keeping them
apart is what this example is about.

1. **Is this token real?** `jose.verify` — against the issuers in
   [`trust-policy.yaml`](trust-policy.yaml), which the *operator* wrote. The
   workflow cannot name an issuer, a key set or an algorithm; a workflow that
   could would be choosing what counts as valid.
2. **May this subject do this?** A CEL expression over the verified claims, in
   the workflow, where the workflow's own policy lives.

Verification is not authorization. A task that answered both would be deciding
the second question by answering the first.

```console
$ mkdir -p ./plugins
$ go build -o ./plugins/flowstate-plugin-jose ./plugins/jose
$ flow worker --plugin-dir ./plugins \
    --plugin-env jose=FLOWSTATE_JOSE_TRUST=$PWD/examples/plugins/jose/trust-policy.yaml
$ flow run examples/plugins/jose/workflow.yaml --input callback_token="$TOKEN"
```

## The two test cases

[`workflow.test.yaml`](workflow.test.yaml) runs both halves of the split with no
issuer and no plugin process:

- a token that verifies **and** is for the expected ref → the run acts;
- a token that verifies **and is not** → the run refuses, and says so with the
  subject the issuer vouched for.

The second case is the one worth reading. Nothing failed: the token was real,
and the workflow declined anyway.

## The trust policy is the server's own

[`trust-policy.yaml`](trust-policy.yaml) is the same document shape
`flow server --auth-policy` reads. A deployment that already trusts an issuer
for its own API can point both at one file — and the plugin's own reachability
test asks the *running* plugin whether this file loads, which is how a typo in
it fails a test rather than a runbook.
