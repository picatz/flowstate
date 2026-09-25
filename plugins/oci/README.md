# flowstate-plugin-oci

Read-side OCI registry tasks: `oci.resolve` (a reference to the digest the
registry serves today), `oci.referrers` (what is attached to those exact bytes -
signatures, SBOMs, in-toto attestations), and `oci.blob` (one
content-addressed document, verified against the digest it was asked for).

Built on the [OCI distribution
specification](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)
over `net/http` and the SDK's governed client - no registry client library, no
subprocess, and no dependency outside what the SDK already brings.

An example that runs all three lives at
[`examples/plugins/oci`](../../examples/plugins/oci); read that first if you
want to see it work rather than read about it.

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-oci ./plugins/oci
```

## Tasks

| Task | Reads/Writes | Idempotent | Needs a credential |
| --- | --- | --- | --- |
| `oci.resolve` | reads | yes | only for a private repository |
| `oci.referrers` | reads | yes | only for a private repository |
| `oci.blob` | reads | yes (the digest fixes the answer) | only for a private repository |

Every task is a read, which is why none of them can return
`OutcomeUnknown`: a GET that failed halfway left nothing behind to reconcile, so
a transport failure is retryable and a registry's refusal is permanent. A write
task would need the opposite posture, which is why one has not been added
quietly beside these.

## Why this is a plugin rather than three `http:` steps

Three things, none of which a generic HTTP client can do:

1. **The token dance.** A registry answers an anonymous read with `401` and a
   `WWW-Authenticate` header naming a realm; the read is retried with the token
   that realm mints. As Flowfile steps this is a conditional, a second request,
   a header built by string concatenation, and a credential in durable history.
2. **Verification.** A blob is content-addressed: the caller knows what the
   bytes must hash to *before* asking. `oci.blob` hashes what it reads and
   refuses what does not match. An `http:` step cannot, because it does not know
   what was expected.
3. **Content negotiation.** Which manifest a registry serves depends on `Accept`,
   and which image an index means depends on a platform the caller matches
   itself - including skipping the `unknown/unknown` entries that carry
   attestations, which are not images.

## Authentication

`username` is an ordinary input; `password` is declared in `secret_inputs` and
`required_secret_inputs`, so a Flowfile writes it as a whole secret reference -
`${secret('env:GHCR_TOKEN')}` - and a literal is refused before the
specification can enter durable history. The host resolves it under the caller's
namespace and scrubs it from errors and outputs.

Both or neither: a username with no password reads as an authenticated call and
makes an anonymous one, which shows up as a `404` on a private repository rather
than as an error anyone can act on. Registries that take a token as the password
document the account name to send with it, and this plugin refuses to invent one.

## Where the authority comes from

Which registries this plugin may reach is the **deployment's egress policy**,
granted at launch and applied on the real dial path. There is no allowlist of
this plugin's own to drift from it, and
[`examples/plugins/oci/egress-policy.yaml`](../../examples/plugins/oci/egress-policy.yaml)
is the file an operator writes to narrow it.

One consequence stated plainly: the token endpoint is named by the *registry*,
in its own challenge, and Docker Hub's is on a different host than the registry
itself. A credential is therefore sent to a host this plugin did not choose -
but only ever to one the operator's policy permits, and only over HTTPS. There
is no input and no operator setting that makes this plugin speak cleartext.

## Bounds

Every one of these is a limit on bytes another party controls:

| What | Limit |
| --- | --- |
| a reference, before parsing | 1 KiB |
| a manifest or index | 4 MiB (the specification's own ceiling) |
| a referrers index | 4 MiB |
| a token endpoint's response | 64 KiB |
| a resolved credential | 4 KiB |
| referrers returned | 50 by default, 200 maximum |
| annotations per descriptor | 32, each key 256 B and value 1 KiB |
| a blob | 1 MiB by default, 8 MiB maximum |

A `limit` or `max_bytes` over the ceiling is **refused, never lowered**. A
silently clamped limit would answer "two hundred attachments" for an image with
more, indistinguishably from an image with exactly two hundred, and a truncated
document read as a complete one is how a workflow decides on evidence it never
received.

## Design decisions

**No signature verification.** Reading a sigstore bundle is not the same as
validating a Fulcio certificate chain against a trust root and a Rekor inclusion
proof. A task that half-did it would be worse than one that does not claim to:
a workflow would read "verified" where nothing was. Verification belongs behind
its own design, with its own operator-configured trust roots.

**No implicit registry.** `alpine:3` does not parse. Tooling that infers the
registry decides on a workflow's behalf whose servers a deployment talks to, and
the decision is invisible in the file under review. The one host mapping this
plugin does perform is `docker.io` to `registry-1.docker.io`, written in
`reference.go` rather than hidden in a dialer, because an operator's egress
policy has to name what is actually dialed.

**No default tag.** A reference with neither tag nor digest is refused rather
than resolved as `:latest` - a workflow that meant `:latest` can say so.

**An unmatched platform is a refusal, not an output.** `oci.resolve` with a
`platform` that an index does not hold fails, naming the platforms it does hold.
Returning the index's own digest with `platform_matched: false` would leave a
workflow deploying the index while believing it had pinned a platform.

**A 404 from the referrers API is reported, never read as "nothing is
attached".** A registry that does not implement the referrers API answers the
same way as a repository that does not exist, and a deploy gate that treated
either as "no attestation" would fail open.

## What was left undone, and why

- **Push, tag, copy, delete.** This is the read side, which is the side a
  decision is made from. A write task needs the unknown-outcome posture
  described above and a credential story with more than pull scope.
- **Signature and attestation *verification*.** See above; it needs trust roots,
  which are operator configuration this plugin deliberately does not yet have.
- **Pagination beyond the first page of referrers.** The `Link` header is read
  only to report `truncated`. An image with more than two hundred attachments is
  worth looking at directly rather than paging through from a workflow.
