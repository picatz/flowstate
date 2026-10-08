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
$ go -C plugins/jose build -o ../../plugins/flowstate-plugin-jose .
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --plugin-env jose=FLOWSTATE_JOSE_TRUST=$PWD/examples/plugins/jose/trust-policy.yaml &
$ flow server --insecure-no-auth --plugin-dir ./plugins &
$ flow run examples/plugins/jose/workflow.yaml --input callback_token="$TOKEN"
```

The server takes `--plugin-dir` too, because it checks each task the file names,
and its `plugins:` block, against the plugins it launched itself.
`--insecure-no-auth` makes this a rehearsal: every caller is anonymous, which is
only right on a machine nobody else can reach.

`flow run` asks the server it submits to which tasks it can run (`GetCatalog`), so a plugin task the
server loaded validates on the client without the client launching anything. Against a server whose
policy denies that call, or one you cannot reach, pass `--plugin-catalog` with the output of
`flow plugins --plugin-dir ./plugins --output json`.

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
