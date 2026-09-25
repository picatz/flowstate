package flowdebug

import (
	"testing"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRedactedKeyNameFailsClosedOnANonStringRedaction is the negative
// direction of the structural key seam: [Session.SetValueRedactor]'s
// contract is `func(any) any`, so nothing requires a redacted key to still
// be a string. An embedder recognising a sensitive key could hand back nil
// or a type of its own rather than this package's "[redacted]" marker, and
// the earlier version of [redactedKeyName] fell back to the *original* name
// whenever the result was not a string — redacting a key only when a
// caller's own redactor happened to answer in the one shape it expected,
// and handing the secret straight through otherwise (Codex, #2011 review,
// fifth round).
func TestRedactedKeyNameFailsClosedOnANonStringRedaction(t *testing.T) {
	const secret = "hunter2"

	// A redactor that recognises the sensitive key but represents "redact
	// this" as nil rather than a string marker — a reachable, spec-legal
	// shape this package's own SetValueRedactor contract does not forbid.
	redact := func(value any) any {
		if text, ok := value.(string); ok && text == secret {
			return nil
		}

		return value
	}

	got := redactedKeyName(secret, redact)
	assert.NotEqual(t, secret, got,
		"the redactor recognised this key as sensitive, but its answer was not a "+
			"string, so the original, unredacted key was handed back")
	assert.Equal(t, "[redacted]", got,
		"a redacted key that cannot be represented as the redactor's own string "+
			"should fail closed to the fixed marker")
}

// TestRedactedKeyNameLeavesAnOrdinaryKeyAlone is the positive direction
// [TestRedactedKeyNameFailsClosedOnANonStringRedaction] needs beside it: a
// fail-closed default that redacted every key regardless of the redactor's
// answer would pass the negative test just as well as the correct fix, and
// would make every scope and step listing unreadable.
func TestRedactedKeyNameLeavesAnOrdinaryKeyAlone(t *testing.T) {
	redact := func(value any) any { return value }

	assert.Equal(t, "ordinary", redactedKeyName("ordinary", redact),
		"a redactor that recognises nothing changed the key it was not asked to")
	assert.Equal(t, "no-redactor", redactedKeyName("no-redactor", nil),
		"no redactor installed at all must not touch the key either")
}

// trackingMapper counts calls to Find and Iterator, so a test can assert
// which of them a caller actually reached rather than only what the answer
// was — the same distinction [redactingMapper] exists to make: a lazy
// caller and an eager one can return identical answers for one lucky key
// and differ only in what else they touched to get there.
type trackingMapper struct {
	traits.Mapper
	finds       []ref.Val
	iteratorHit int
}

func (m *trackingMapper) Find(key ref.Val) (ref.Val, bool) {
	m.finds = append(m.finds, key)

	return m.Mapper.Find(key)
}

func (m *trackingMapper) Iterator() traits.Iterator {
	m.iteratorHit++

	return m.Mapper.Iterator()
}

// TestRedactingMapperFindStaysLazy is the laziness half of
// [redactingMapper]'s own reason for existing: an earlier fix
// (redactedMapNative) converted every entry through Iterator before CEL
// ever selected one, which fixed the correctness half of the finding that
// asked for this type — one step's unconvertible output no longer breaking
// every other step's read — while leaving the cost-accounting half open,
// because it still walked and converted every completed step regardless of
// which one an expression named (Codex, #2011 review, third and fifth
// rounds). Find is what an ordinary `steps.<id>` expression compiles to, so
// this asserts it reaches the underlying map exactly once, for the one key
// asked, and never touches Iterator at all.
func TestRedactingMapperFindStaysLazy(t *testing.T) {
	base := types.NewRefValMap(types.DefaultTypeAdapter, map[ref.Val]ref.Val{
		types.String("good"):    types.String("fine"),
		types.String("another"): types.String("also fine"),
	})
	tracked := &trackingMapper{Mapper: base}

	wrapper := redactingMapper{Mapper: tracked}

	value, found := wrapper.Find(types.String("good"))
	require.True(t, found, "the wrapper lost a key the underlying map has")
	assert.Equal(t, "fine", value.Value())

	assert.Equal(t, 0, tracked.iteratorHit,
		"Find walked the whole map through Iterator instead of looking up one key")
	require.Len(t, tracked.finds, 1,
		"Find should reach the underlying map exactly once for the key actually asked")
	assert.Equal(t, types.String("good"), tracked.finds[0])
}

// TestRedactingMapperIteratorRedactsEachKey is
// [TestRedactedKeyNameFailsClosedOnANonStringRedaction] proved at the
// [redactingMapper] level rather than the helper it calls: `exists(k, ...)`
// and a `for` both resolve through Iterator, which is the surface that has
// to hand back redacted names rather than the raw ones an author never
// typed.
func TestRedactingMapperIteratorRedactsEachKey(t *testing.T) {
	const secret = "hunter2"

	base := types.NewRefValMap(types.DefaultTypeAdapter, map[ref.Val]ref.Val{
		types.String(secret):    types.Bool(true),
		types.String("visible"): types.Bool(true),
	})

	wrapper := redactingMapper{
		Mapper: base,
		redactValue: func(value any) any {
			if text, ok := value.(string); ok && text == secret {
				return "[redacted]"
			}

			return value
		},
	}

	it := wrapper.Iterator()

	var keys []string
	for it.HasNext() == types.True {
		key, ok := it.Next().(types.String)
		require.True(t, ok, "a map with only string keys yielded a non-string one")
		keys = append(keys, string(key))
	}

	assert.NotContains(t, keys, secret,
		"the map's own key reached the iterator's answer with the secret still in it")
	assert.Contains(t, keys, "[redacted]")
	assert.Contains(t, keys, "visible",
		"the walk redacted a key that was never sensitive")
}
