# Verifying an image before it is deployed

What this example is about: **a tag is a name somebody can move, and a digest is
the bytes.** Everything after the first step here is about the same bytes, which
is the property a supply-chain gate depends on and the one a workflow loses the
moment it passes a tag to two different steps.

Run it:

```console
$ mkdir -p ./plugins
$ go -C plugins/oci build -o ../../plugins/flowstate-plugin-oci .
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --egress-policy examples/plugins/oci/egress-policy.yaml &
$ flow server --plugin-dir ./plugins --auth-policy /path/to/auth-policy.yaml \
    --rpc-resource https://flowstate.example.com/rpc --identity-claim team &
$ flow run examples/plugins/oci/workflow.yaml \
    --input image=ghcr.io/acme/api:1.4.2 \
    --input platform=linux/amd64 \
    --input expected_approver=release-manager@example.com \
    --token-file /path/to/starter.token
```

The server takes `--plugin-dir` too, because the file declares `plugins:` and
the server resolves that block against the plugins it launched itself. It takes
an `--auth-policy` trusting a real issuer, with the `--rpc-resource` its tokens
are minted for, rather than `--insecure-no-auth`, because the approval below is
a signal only an attested release manager other than the starter may send.
It keeps the `team` claim (`--identity-claim team`), because the signal's rule
reads it and a server persists only the claims it names. Every client command
below authenticates with `--token-file` (or `--credential-source`): an
authenticated server refuses an anonymous caller.

`flow run` refuses this file today (#1548): it checks the file against its own
build's task registry, takes no `--plugin-dir`, and so reports the `oci.*`
tasks as ones nothing registered before the server sees it. Until that is
fixed, `flow run local` with the worker's `--plugin-dir` and `--egress-policy`
runs it in one process, and an agent host running
`flow mcp --plugin-dir ./plugins --token-file /path/to/starter.token` submits
it to this server with `flowstate_compile` then `flowstate_run`.

The run stops at `approval` and waits - durably, for up to a day - until someone
answers:

```console
$ flow signal <run> digest-approved --data '{"approved": true}' \
    --token-file /path/to/approver.token
```

The approver's token must be issued by `https://issuer.example.com` to the
subject `expected_approver` names, with `team: release-managers`;
`distinct_from_starter: true` refuses the starter's own.

## The four steps, and why each is separate

1. **`pin`** (`oci.resolve`) turns the tag into `ghcr.io/acme/api@sha256:…`,
   once. Every step below carries `steps.pin.reference`. If the tag moves
   between this step and the deploy, nothing downstream notices, because nothing
   downstream ever looks at the tag again.

2. **`attached`** (`oci.referrers`) asks what is attached to those exact bytes.
   It takes the pinned reference because a referrer set is a statement about
   specific bytes - `oci.referrers` refuses a tag outright rather than answering
   about whatever the tag pointed at when the call landed.

3. **`attestation`** (`oci.blob`) fetches the first referrer by its digest,
   and refuses it unless the bytes hash to that digest. A referrer's digest
   names its manifest, which points at the attestation in its layers, so what
   comes back is that manifest rather than the in-toto statement itself. This
   is the step an `http:` call cannot stand in for: a generic client has no way
   to know what the bytes were supposed to be.

4. **`approval`** (`wait_for_signal`) puts the digest in front of a person. The
   prompt names the pinned reference, so what a human approved and what a
   deployment pulls are the same string.

## What fails closed here

- An **unattested image still reaches the human**, marked as unattested, rather
  than being blocked or silently passed. The second test case in
  [`workflow.test.yaml`](workflow.test.yaml) is that path.
- A **registry that cannot answer** the referrers question fails the step rather
  than returning an empty set. "This registry does not implement referrers" and
  "nothing is attached" are different facts, and a gate that conflated them
  would pass an unattested image on a registry that simply could not say.
- A **platform that an index does not hold** fails `pin`, naming the platforms
  the index does hold, rather than handing back the index's digest.

## Testing it without a registry

[`workflow.test.yaml`](workflow.test.yaml) stubs all three plugin steps, so the
control flow - the skipped steps, the signal, the outputs - is exercised with no
network and no plugin process:

```console
$ flow test examples/plugins/oci/
```

## The egress policy

[`egress-policy.yaml`](egress-policy.yaml) is the operator's half. The plugin
has no registry allowlist of its own: which registries it may read is the
deployment's policy, and this file is what narrows it to one.
