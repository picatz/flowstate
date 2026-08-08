package flowstatev1

import (
	"crypto/sha256"
	"encoding/hex"
)

// One spelling of "these exact bytes", for every surface that needs to name a
// content by its hash.
//
// The spelling was already settled by the `digest:` pin on a `call:` step: the
// algorithm, a colon, and the hash in lower-case hex, which is the form
// [Call.SourceDigest] records and the form a container image reference uses. It
// lived in the flowfile compiler as an unexported helper, because at the time the
// compiler was the only thing that hashed anything.
//
// It is here now because there is a second reader. `flow mcp` versions the UI
// fragment it serves by the hash of the fragment's bytes, and a second hashing
// helper written beside it would be exactly the shape this repository keeps
// refinding: a value with one meaning, written down twice, with nothing importing
// both so nothing can compare them. This package is what every one of those
// readers already imports, so putting it here is the same move `DefaultCostLimit`
// and the default attempt count made.

// ContentDigestPrefix is the algorithm label a digest carries, separator
// included. A reader that wants the bare hex cuts this prefix; a reader that
// wants to compare compares the whole string.
const ContentDigestPrefix = "sha256:"

// ContentDigestHexLen is how many hex characters follow [ContentDigestPrefix].
const ContentDigestHexLen = sha256.Size * 2

// ContentDigest renders bytes as the digest this tree writes everywhere:
// algorithm first, then lower-case hex.
//
// One function because there is one spelling. A pin is compared against what this
// returns and every diagnostic prints what this returns, so the digest a reader is
// told to adopt is the digest that will then match.
func ContentDigest(data []byte) string {
	sum := sha256.Sum256(data)

	return ContentDigestPrefix + hex.EncodeToString(sum[:])
}
