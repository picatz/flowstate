# Verifying an image before it is deployed

What this example is about: **a tag is a name somebody can move, and a digest is
the bytes.** Everything after the first step here is about the same bytes, which
is the property a supply-chain gate depends on and the one a workflow loses the
moment it passes a tag to two different steps.

Run it:

```console
$ mkdir -p ./plugins
$ go build -o ./plugins/flowstate-plugin-oci ./plugins/oci
$ flow worker --plugin-dir ./plugins --egress-policy examples/plugins/oci/egress-policy.yaml
$ flow run examples/plugins/oci/workflow.yaml \
    --input image=ghcr.io/acme/api:1.4.2 \
    --input platform=linux/amd64 \
    --input expected_approver=release-manager@example.com
```

The run stops at `approval` and waits - durably, for up to a day - until someone
answers:

```console
$ flow signal <run> digest-approved --payload approved=true
```

## The four steps, and why each is separate

1. **`pin`** (`oci.resolve`) turns the tag into `ghcr.io/acme/api@sha256:…`,
   once. Every step below carries `steps.pin.reference`. If the tag moves
   between this step and the deploy, nothing downstream notices, because nothing
   downstream ever looks at the tag again.

2. **`attached`** (`oci.referrers`) asks what is attached to those exact bytes.
   It takes the pinned reference because a referrer set is a statement about
   specific bytes - `oci.referrers` refuses a tag outright rather than answering
   about whatever the tag pointed at when the call landed.

3. **`attestation`** (`oci.blob`) fetches the in-toto statement by its own
   digest, and refuses it unless the bytes hash to that digest. This is the step
   an `http:` call cannot stand in for: a generic client has no way to know what
   the bytes were supposed to be.

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
