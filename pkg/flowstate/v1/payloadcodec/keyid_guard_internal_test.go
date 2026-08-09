package payloadcodec

// The invariant these tests hold is the one-name rule, applied to a map this
// project does not own.
//
// Payload metadata is a flat map shared with the SDK, with whatever else is in
// the codec chain, and with any other tool that has ever touched the payload.
// The key id is the entry Decode reads to choose a key, so two spellings of it
// is not an inconsistency to tidy up later: a codec writing one spelling and
// reading another decodes nothing, and two codecs writing the same *bare* name
// select each other's keys. There is therefore one constant, it is namespaced,
// and it has one writer.
//
// A grep is the right enforcement, for the reason the converter guard next door
// gives: the mistake is textual. No type can forbid a string literal, and a test
// that asserts what today's one call site does cannot see the second one added
// next year.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"

	"github.com/stretchr/testify/assert"
)

// keyIDDeclaration is the one line in this package tree allowed to spell the
// metadata key out.
const keyIDDeclaration = `KeyIDMetadataKey = "picatz.github.io/flowstate.keyId"`

// TestTheKeyIDMetadataKeyCannotCollideWithAnybodyElses holds the uniqueness
// half, against the names it could actually collide with.
//
// The SDK reserves bare lowercase names for itself and documents none of the
// space as ours, so a bare name is the collision that cannot be detected: two
// codecs stamping "keyId" produce payloads that look decodable to the wrong
// codec. Namespacing removes the possibility instead of making it unlikely.
func TestTheKeyIDMetadataKeyCannotCollideWithAnybodyElses(t *testing.T) {
	t.Parallel()

	require.Contains(t, KeyIDMetadataKey, "/",
		"the key id metadata name is a bare name, which is the space the SDK reserves for itself")
	require.True(t, strings.HasPrefix(KeyIDMetadataKey, "picatz.github.io/flowstate."),
		"the key id metadata name does not carry this project's prefix, which is how the tree "+
			"answers `who owns this member of somebody else's map` (see cmd/flow/mcpui.go's "+
			"contentDigest key)")

	for _, reserved := range []string{
		converter.MetadataEncoding,
		converter.MetadataMessageType,
		"picatz.github.io/flowstate.contentDigest",
	} {
		require.NotEqual(t, reserved, KeyIDMetadataKey,
			"the key id shares a metadata name with something else that writes payload metadata")
	}
}

// TestTheKeyIDMetadataKeyHasOneSpellingAndOneWriter holds the use half.
//
// Two rules, and they fail differently. A source file spelling the literal out
// has forked the constant, and the fork is invisible until a rename lands on one
// copy. A second writer is worse: the id on a payload is what Decode trusts to
// choose a key, so a second place that stamps one is a second answer to which
// key encrypted this, and only one of them can be right.
func TestTheKeyIDMetadataKeyHasOneSpellingAndOneWriter(t *testing.T) {
	t.Parallel()

	root, err := os.Getwd()
	require.NoError(t, err)

	literals := 0
	writers := 0
	checked := 0

	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}

		source, readErr := os.ReadFile(filepath.Clean(path))
		if readErr != nil {
			return readErr
		}
		checked++

		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}

		for _, line := range strings.Split(string(source), "\n") {
			trimmed := strings.TrimSpace(line)
			// A comment naming the key is documentation, not a use of it.
			if strings.HasPrefix(trimmed, "//") {
				continue
			}

			if strings.Contains(trimmed, `"picatz.github.io/flowstate.keyId"`) {
				literals++
				assert.Equal(t, "const "+keyIDDeclaration, trimmed,
					"%s: %s\n\nrefer to payloadcodec.KeyIDMetadataKey instead of spelling the "+
						"metadata name out. A second spelling is a fork of the constant, and it "+
						"stays invisible until a rename lands on one copy and the codec starts "+
						"writing one name and reading another.", relative, trimmed)
			}

			// A map-literal entry or an assignment: the two ways a key id gets
			// written onto a payload. Reads spell it inside an index expression
			// and do not match.
			if strings.Contains(trimmed, "KeyIDMetadataKey:") || strings.Contains(trimmed, "KeyIDMetadataKey] =") {
				writers++
				assert.Equal(t, filepath.Join("toycodec", "toycodec.go"), relative,
					"%s: %s\n\nthe key id is stamped in one place, toycodec's metadataFor, so that "+
						"what Encode writes and what MaxEncodedSize measures cannot drift apart. A "+
						"second writer is a second answer to which key encrypted a payload, and "+
						"Decode trusts the answer.", relative, trimmed)
			}
		}
		return nil
	})
	require.NoError(t, err)

	require.Greater(t, checked, 1, "this guard read no source files, so it proves nothing")

	// And the permitted sites have to still be there. A guard that passes
	// because the declaration was deleted, or because the toy codec stopped
	// stamping ids altogether, would prove the opposite of what it says.
	require.Equal(t, 1, literals, "the metadata name is no longer declared exactly once")
	require.Equal(t, 1, writers,
		"nothing in this package tree writes a key id any more, so the contract's executable "+
			"specification has stopped specifying it")

	declaration, err := os.ReadFile("payloadcodec.go")
	require.NoError(t, err)
	require.Contains(t, string(declaration), keyIDDeclaration,
		"the metadata name moved out of payloadcodec.go, which is the package that owns it")
}
