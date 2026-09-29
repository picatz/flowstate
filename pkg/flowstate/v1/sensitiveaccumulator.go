package flowstatev1

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"maps"
	"math"
	"reflect"
	"slices"
)

// SensitiveAccumulator gathers the sets a run's steps are told to withhold
// into one: each value and substring held once, whatever order and however
// often the sets arrive (#2211). A reader rendering a whole run after it is
// over — `flow test`'s transcript and report — is told a set per step, and
// mostly the same sets over and over.
//
// [SensitiveValues.Merge] appends, and a few hundred repeats of one set
// reach [maxSensitiveDescendants], which withholds everything. Comparing each
// arriving set against everything gathered would format or compare every
// held value on every step (Copilot, #2215). So the accumulator skips a set
// it has already been given (by identity, without reading it), and indexes
// the values it holds by a hash, so a set it has not seen costs its own size,
// not the accumulator's.
//
// The hash is FNV-1a, unseeded, rather than [hash/maphash]: a merge runs in
// workflow-side code too (the engine's debug holds), which must not depend on
// a per-process random seed even where nothing it produces is ordered by one
// (Codex, #2215).
//
// The zero value is ready to use. It is not safe for concurrent use; its
// owner locks. Its state sits behind a pointer, which reflection prints as an
// address, for the reason [SensitiveValues] closes over its own.
type SensitiveAccumulator struct {
	state *sensitiveAccumulation
}

// sensitiveAccumulation is a [SensitiveAccumulator]'s state.
type sensitiveAccumulation struct {
	// hasher and scratch hash one value at a time: its canonical encoding is
	// built in scratch, reused, so hashing allocates nothing once it has
	// grown to the largest value's.
	hasher  hash.Hash64
	scratch []byte

	// seen holds the identities of sets already gathered, up to
	// maxSeenSensitiveSets. Past that a set is read again, which is correct
	// and only slower.
	seen map[*sensitiveIdentity]struct{}

	// index groups the gathered values by hash; equal hashes are told apart
	// by [sameSensitiveValue].
	index      map[uint64][]any
	substrings map[string]struct{}
	values     []any
	subs       []string

	withholdAll bool

	// built is the gathered set as a [SensitiveValues], rebuilt only when
	// something new arrived since it was last asked for.
	built SensitiveValues
	stale bool
}

// maxSensitiveBucket bounds how many unequal values share one hash. Values
// that happen to collide in 64 bits among the few a set may hold never come
// near it; values built to collide, which an unkeyed hash allows, reach it
// and withhold everything, so no addition compares against more than this.
const maxSensitiveBucket = 8

// maxSeenSensitiveSets bounds how many set identities an accumulator
// remembers. A loop of calls makes a set per call, and remembering each
// would be memory the loop's length decides.
const maxSeenSensitiveSets = 4096

// Add gathers s.
func (a *SensitiveAccumulator) Add(s SensitiveValues) {
	if s.Empty() {
		return
	}
	if a.state == nil {
		a.state = &sensitiveAccumulation{
			hasher:     fnv.New64a(),
			seen:       map[*sensitiveIdentity]struct{}{},
			index:      map[uint64][]any{},
			substrings: map[string]struct{}{},
		}
	}
	st := a.state
	if st.withholdAll {
		return
	}
	if _, seen := st.seen[s.identity]; seen {
		return
	}

	held := s.held()
	// A set holding more values than the bound fails closed before any of
	// them is read, as an appending merge of it did: the union it adds to can
	// stay small however many repeats it carries, so the check after the loop
	// never bounds the work the loop does (Codex, #2215).
	if held.withholdAll || len(held.values) > maxSensitiveDescendants {
		*st = sensitiveAccumulation{withholdAll: true}

		return
	}
	for _, value := range held.values {
		key := st.hash(value)
		bucket := st.index[key]
		if slices.ContainsFunc(bucket, func(other any) bool { return sameSensitiveValue(value, other) }) {
			continue
		}
		if len(bucket) >= maxSensitiveBucket {
			// Fail closed rather than scan a bucket that grows: the hash is
			// unkeyed, so values colliding in it can be chosen (Codex, #2215).
			*st = sensitiveAccumulation{withholdAll: true}

			return
		}
		st.index[key] = append(st.index[key], value)
		st.values = append(st.values, value)
		st.stale = true
	}
	for _, substring := range held.substrings {
		if _, held := st.substrings[substring]; held {
			continue
		}
		st.substrings[substring] = struct{}{}
		st.subs = append(st.subs, substring)
		st.stale = true
	}
	if len(st.values) > maxSensitiveDescendants {
		// Fail closed, as a set built past the bound does.
		*st = sensitiveAccumulation{withholdAll: true}

		return
	}
	if len(st.seen) < maxSeenSensitiveSets {
		st.seen[s.identity] = struct{}{}
	}
}

// Values is everything gathered, as one set: empty if nothing was, and
// withholding everything if any set gathered could not be built.
func (a *SensitiveAccumulator) Values() SensitiveValues {
	st := a.state
	switch {
	case st == nil:
		return SensitiveValues{}
	case st.withholdAll:
		return WithheldSensitiveValues()
	case st.stale:
		st.built = sensitiveValuesOf(sensitiveState{
			values:     slices.Clone(st.values),
			substrings: slices.Clone(st.subs),
		})
		st.stale = false
	}

	return st.built
}

// sameSensitiveValue is [isSensitiveValue]'s equality, with every NaN equal
// to every other, at any depth: a NaN never equals anything under
// [reflect.DeepEqual], so a set holding one — alone, or inside a list or a
// map — would be gathered anew each time until the bound withheld
// everything. [sensitiveAccumulation.hash] hashes by the same relation.
func sameSensitiveValue(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)

		return ok && (x == y || math.IsNaN(x) && math.IsNaN(y))
	case []any:
		y, ok := b.([]any)

		return ok && slices.EqualFunc(x, y, sameSensitiveValue)
	case map[string]any:
		y, ok := b.(map[string]any)

		return ok && maps.EqualFunc(x, y, sameSensitiveValue)
	}

	return reflect.DeepEqual(a, b)
}

// hash hashes one value of the shapes [LiteralToGo] produces, in a canonical
// order, without formatting it. Unequal values may share a hash, which
// [sameSensitiveValue] settles; equal ones never differ.
func (st *sensitiveAccumulation) hash(value any) uint64 {
	st.scratch = appendSensitiveHash(st.scratch[:0], value)
	st.hasher.Reset()
	st.hasher.Write(st.scratch)

	return st.hasher.Sum64()
}

// appendSensitiveHash appends what [sensitiveAccumulation.hash] hashes of
// value to b: a tag for its shape, every fixed-width part at its full width,
// and every variable-width part led by its length, so two different values
// never encode alike and only a collision in
// the hash itself puts them in one bucket (Codex, #2215).
func appendSensitiveHash(b []byte, value any) []byte {
	switch value := value.(type) {
	case nil:
		b = append(b, 0)
	case string:
		b = appendSensitiveBytes(append(b, 1), value)
	case []byte:
		b = appendSensitiveBytes(append(b, 2), value)
	case bool:
		// One byte either way: a `false` shorter than a `true` would make
		// the two a prefix of each other and the encoding ambiguous.
		flag := byte(0)
		if value {
			flag = 1
		}
		b = append(b, 3, flag)
	case int64:
		b = binary.LittleEndian.AppendUint64(append(b, 4), uint64(value))
	case uint64:
		b = binary.LittleEndian.AppendUint64(append(b, 5), value)
	case float64:
		// By [sameSensitiveValue]'s relation: every NaN one value whatever
		// its payload, and -0 the same as 0, which compare equal.
		bits := math.Float64bits(value)
		switch {
		case math.IsNaN(value):
			bits = math.Float64bits(math.NaN())
		case value == 0:
			bits = 0
		}
		b = binary.LittleEndian.AppendUint64(append(b, 6), bits)
	case []any:
		b = binary.LittleEndian.AppendUint64(append(b, 7), uint64(len(value)))
		for _, element := range value {
			b = appendSensitiveHash(b, element)
		}
	case map[string]any:
		b = binary.LittleEndian.AppendUint64(append(b, 8), uint64(len(value)))
		for _, key := range slices.Sorted(maps.Keys(value)) {
			b = appendSensitiveBytes(b, key)
			b = appendSensitiveHash(b, value[key])
		}
	default:
		// No other shape reaches here from [LiteralToGo]. Hashed by type
		// alone, so every such value shares a bucket and equality decides.
		b = appendSensitiveBytes(append(b, 9), reflect.TypeOf(value).String())
	}

	return b
}

// appendSensitiveBytes appends payload to b, led by its length.
func appendSensitiveBytes[P string | []byte](b []byte, payload P) []byte {
	return append(binary.LittleEndian.AppendUint64(b, uint64(len(payload))), payload...)
}
