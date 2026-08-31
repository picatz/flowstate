package flowstatev1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
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

// ContentDigestOf is [ContentDigest] for bytes nobody should hold all of.
//
// The same spelling, computed without the whole content in memory. The reader
// that needs this is a plugin binary: the file sits in an operator-controlled
// discovery directory, it is hashed at every launch, and a very large or sparse
// one would otherwise be allocated in full before a worker has finished starting.
// [io.Copy]'s buffer is the whole of what this costs, whatever the size of r.
func ContentDigestOf(r io.Reader) (string, error) {
	sum := sha256.New()
	if _, err := io.Copy(sum, r); err != nil {
		return "", err
	}

	return ContentDigestPrefix + hex.EncodeToString(sum.Sum(nil)), nil
}

// ValidateContentDigest checks that digest has the one canonical spelling
// [ContentDigest] writes. It validates a content identifier, not what the
// identified bytes mean and not whether those bytes are available.
//
// Readers need this independently of writers: plugin distribution pins and OCI
// image references both arrive as operator- or workflow-authored text before
// either can be compared with measured content. Keeping the shape here avoids
// each boundary accepting a subtly different spelling of the same digest.
func ValidateContentDigest(digest string) error {
	digits, ok := strings.CutPrefix(digest, ContentDigestPrefix)
	if !ok {
		return fmt.Errorf("does not begin with %q", ContentDigestPrefix)
	}
	if len(digits) != ContentDigestHexLen {
		return fmt.Errorf("carries %d hex characters, want %d", len(digits), ContentDigestHexLen)
	}
	for i := 0; i < len(digits); i++ {
		c := digits[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return fmt.Errorf("holds %q, which is not lower-case hexadecimal", string(c))
	}
	return nil
}
