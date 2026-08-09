package payloadcodec_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// keylessCodec is a codec that encrypts and says nothing about what with.
//
// It is the shape the startup check exists to refuse, and it is a shape that
// looks fine: it encodes, it decodes, it declares a size, and every payload it
// writes is ciphertext nobody can ever attribute to a key. The failure is years
// away and silent, a `flow shred` that reports success and destroys nothing, so
// it has to be refused now, by the process, before there is history.
type keylessCodec struct{}

func (keylessCodec) Name() string { return "keyless" }

func (keylessCodec) CurrentKeyID() string { return "" }

func (keylessCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (keylessCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (keylessCodec) MaxEncodedSize(plain int) int { return plain + 64 }

// TestACodecThatNamesNoKeyIsRefusedAtStartup is the fail-closed direction for
// the contract: a codec that transforms payloads has a key and says which.
func TestACodecThatNamesNoKeyIsRefusedAtStartup(t *testing.T) {
	t.Parallel()

	err := payloadcodec.Config{Codec: keylessCodec{}}.Validate()

	require.Error(t, err, "a codec that stamps no key id was allowed to start writing history")

	msg := err.Error()
	require.Contains(t, msg, `"keyless"`, "the refusal does not name the codec")
	require.Contains(t, msg, payloadcodec.KeyIDMetadataKey,
		"the refusal does not say where the id belongs, which is the one thing an implementer needs")
	require.Contains(t, msg, "flow shred",
		"the refusal does not say what is lost, so it reads as a formality rather than as the "+
			"capability it protects")
}

// TestTheNullCodecNamesNoKeyAndStartsAnyway pins the one exemption, which is not
// really one: the null codec is asked the question and answers "no key", which
// is true of it and false of everything else.
func TestTheNullCodecNamesNoKeyAndStartsAnyway(t *testing.T) {
	t.Parallel()

	require.Empty(t, payloadcodec.Null().CurrentKeyID())
	require.NoError(t, payloadcodec.Config{}.Validate())
}

// TestAKeyIDOutsideTheGrammarIsRefusedAtStartup is the other half of the
// startup check, and the bound half: the id is stamped on every payload the
// codec writes, so an id nobody bounded is expansion nobody checked.
func TestAKeyIDOutsideTheGrammarIsRefusedAtStartup(t *testing.T) {
	t.Parallel()

	t.Run("longer than the bound", func(t *testing.T) {
		t.Parallel()

		err := payloadcodec.Config{Codec: expandingCodec{
			name:     "long-id",
			declared: func(plain int) int { return plain },
			keyID:    strings.Repeat("k", payloadcodec.MaxKeyIDBytes+1),
		}}.Validate()

		require.Error(t, err, "a codec with an oversized key id was allowed to start")
		require.Contains(t, err.Error(), `"long-id"`)
		require.Contains(t, err.Error(), strconv.Itoa(payloadcodec.MaxKeyIDBytes))
		require.Contains(t, err.Error(), strconv.Itoa(payloadcodec.MaxKeyIDBytes+1),
			"the refusal does not say how long the id actually is")
	})

	t.Run("exactly at the bound is allowed", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, payloadcodec.Config{Codec: expandingCodec{
			name:     "boundary-id",
			declared: func(plain int) int { return plain },
			keyID:    strings.Repeat("k", payloadcodec.MaxKeyIDBytes),
		}}.Validate(), "an id of exactly the permitted length was refused")
	})

	t.Run("unspellable", func(t *testing.T) {
		t.Parallel()

		err := payloadcodec.Config{Codec: expandingCodec{
			name:     "shell-id",
			declared: func(plain int) int { return plain },
			keyID:    "key one",
		}}.Validate()

		require.Error(t, err, "a key id with a space in it was allowed to start")
		require.Contains(t, err.Error(), `"shell-id"`)
		require.Contains(t, err.Error(), "flow shred",
			"the refusal does not say why the grammar is what it is")
	})
}

// TestTheKeyIDIsCheckedBeforeTheSize decides an order rather than leaving it to
// whichever check somebody typed first.
//
// The id is a term in the size: it is stamped on every payload, so it is spent
// out of the same budget the ciphertext is. A codec with both problems is told
// about the id, because the size it declared was computed with an id that is not
// allowed to exist.
func TestTheKeyIDIsCheckedBeforeTheSize(t *testing.T) {
	t.Parallel()

	err := payloadcodec.Config{Codec: expandingCodec{
		name:     "both-wrong",
		declared: func(plain int) int { return plain * 2 },
		keyID:    strings.Repeat("k", payloadcodec.MaxKeyIDBytes+1),
	}}.Validate()

	require.Error(t, err)
	require.Contains(t, err.Error(), "key id",
		"a codec whose id is not allowed to exist was told about the size that id is part of")
}

// TestValidateKeyIDPinsTheGrammar states the grammar as a set of examples, since
// it is a contract other codecs, including the plugin that comes next, have to
// meet without reading this package's source.
func TestValidateKeyIDPinsTheGrammar(t *testing.T) {
	t.Parallel()

	for _, id := range []string{
		"a",
		"0f3c9a1b2d4e5f60",
		"tenant-a.2026-08",
		"AWS_KMS_v3",
		strings.Repeat("k", payloadcodec.MaxKeyIDBytes),
	} {
		require.NoError(t, payloadcodec.ValidateKeyID(id), "rejected a usable id: %q", id)
	}

	for name, id := range map[string]string{
		"empty":                "",
		"over the bound":       strings.Repeat("k", payloadcodec.MaxKeyIDBytes+1),
		"a space":              "key one",
		"a newline":            "key\none",
		"a quote":              `key"one`,
		"path structure":       "tenant/a",
		"scheme structure":     "kms:key",
		"a control byte":       "key\x00one",
		"not ASCII":            "clé",
		"a shell substitution": "$(rm -rf /)",
	} {
		require.Error(t, payloadcodec.ValidateKeyID(id), "accepted an unusable id: %s", name)
	}
}

// TestValidateKeyIDNeverEchoesTheID matters because the other caller is a decode
// path, where the id came off a payload somebody else wrote. An error that
// quoted it back would put attacker-chosen bytes into a log line, and the length
// bound is checked by the same function that would do the quoting.
func TestValidateKeyIDNeverEchoesTheID(t *testing.T) {
	t.Parallel()

	for _, id := range []string{
		strings.Repeat("no-really-do-not-print-me", 400),
		"\n\nFATAL: everything is fine, stop looking",
	} {
		err := payloadcodec.ValidateKeyID(id)
		require.Error(t, err)
		require.NotContains(t, err.Error(), id, "the grammar's own refusal echoed the id")
	}
}
