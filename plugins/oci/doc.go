// Command flowstate-plugin-oci reads an OCI registry: what a reference
// actually is, what is attached to it, and what one attachment says.
//
// # Why this is a plugin and not three http steps
//
// Everything here is HTTPS GETs, and the built-in http task can make an HTTPS
// GET. What it cannot do is the three things that make a registry read mean
// something:
//
//   - The token dance. A registry answers an anonymous read with 401 and a
//     WWW-Authenticate header naming a realm to exchange credentials at; the
//     read is retried with the bearer token that returns. Written as Flowfile
//     steps this is a conditional, a second request, a header built by string
//     concatenation, and a credential in durable history.
//   - Verification. A blob is content-addressed: the caller knows what the
//     bytes must hash to before it asks for them. This plugin hashes what it
//     reads and refuses what does not match, which is a property no generic
//     HTTP client can have, because it does not know what was expected.
//   - Content negotiation. Which manifest a registry serves depends on the
//     Accept header, and which image an index means depends on a platform the
//     caller has to match itself.
//
// # What it is for
//
// The deploy gate: resolve a tag to a digest, ask what is attached to that
// digest, read the attestation that comes back, decide with CEL, and let a
// human approve the digest rather than the tag. Every one of those steps is
// about the same bytes, which is the whole point of a digest travelling
// between them.
//
// # What it deliberately does not do
//
//   - It does not verify signatures. Reading a sigstore bundle is not the same
//     as validating a Fulcio certificate chain against a trust root and a Rekor
//     inclusion proof, and a task that half-did it would be worse than one that
//     does not claim to: a workflow would read "verified" where nothing was.
//     Verification belongs behind its own design, with its own trust roots.
//   - It does not push, tag, delete, or copy. This is the read side, which is
//     the side a decision is made from.
//   - It speaks HTTPS only. There is no input, and no operator setting, that
//     makes it talk to a registry in cleartext.
//
// # Where the authority comes from
//
// Which registries this plugin may reach is the deployment's egress policy,
// which the worker grants at launch and which governs every request here on
// its real dial path - there is no second allowlist of this plugin's own to
// keep in sync with it. Credentials arrive as resolved secret inputs, per call,
// under the calling workload's namespace; this plugin holds no credential
// store and has no scheme of its own to fish with.
//
// One consequence worth stating plainly: the token endpoint a registry names
// in its WWW-Authenticate header is chosen by the registry, not by this plugin,
// and Docker Hub's is on a different host than the registry itself. A
// credential is therefore sent to a host the *registry* named - but only ever
// to one the operator's egress policy permits, and only over HTTPS. A
// deployment that wants that surface narrower narrows it where every other
// destination in the system is narrowed.
package main
