package flowstatev1

import (
	"encoding/binary"
	"hash/maphash"
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
// The zero value is ready to use. It is not safe for concurrent use; its
// owner locks. Its state sits behind a pointer, which reflection prints as an
// address, for the reason [SensitiveValues] closes over its own.
type SensitiveAccumulator struct {
	state *sensitiveAccumulation
}

// sensitiveAccumulation is a [SensitiveAccumulator]'s state.
type sensitiveAccumulation struct {
	seed maphash.Seed

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
			seed:       maphash.MakeSeed(),
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
	if held.withholdAll {
		*st = sensitiveAccumulation{withholdAll: true}

		return
	}
	for _, value := range held.values {
		key := hashSensitiveValue(st.seed, value)
		if slices.ContainsFunc(st.index[key], func(other any) bool { return sameSensitiveValue(value, other) }) {
			continue
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

// sameSensitiveValue is [isSensitiveValue]'s equality, and a NaN equal to
// itself: a NaN never equals anything under [reflect.DeepEqual], so it would
// be gathered anew from every set until the bound withheld everything.
func sameSensitiveValue(a, b any) bool {
	if x, ok := a.(float64); ok && math.IsNaN(x) {
		y, ok := b.(float64)

		return ok && math.IsNaN(y)
	}

	return reflect.DeepEqual(a, b)
}

// hashSensitiveValue hashes one value of the shapes [LiteralToGo] produces,
// in a canonical order, without formatting it. Unequal values may share a
// hash, which [sameSensitiveValue] settles; equal ones never differ.
func hashSensitiveValue(seed maphash.Seed, value any) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	writeSensitiveHash(&h, value)

	return h.Sum64()
}

func writeSensitiveHash(h *maphash.Hash, value any) {
	var number [8]byte
	switch value := value.(type) {
	case nil:
		h.WriteByte(0)
	case string:
		h.WriteByte(1)
		h.WriteString(value)
	case []byte:
		h.WriteByte(2)
		h.Write(value)
	case bool:
		h.WriteByte(3)
		if value {
			h.WriteByte(1)
		}
	case int64:
		h.WriteByte(4)
		binary.LittleEndian.PutUint64(number[:], uint64(value))
		h.Write(number[:])
	case uint64:
		h.WriteByte(5)
		binary.LittleEndian.PutUint64(number[:], value)
		h.Write(number[:])
	case float64:
		h.WriteByte(6)
		binary.LittleEndian.PutUint64(number[:], math.Float64bits(value))
		h.Write(number[:])
	case []any:
		h.WriteByte(7)
		binary.LittleEndian.PutUint64(number[:], uint64(len(value)))
		h.Write(number[:])
		for _, element := range value {
			writeSensitiveHash(h, element)
		}
	case map[string]any:
		h.WriteByte(8)
		binary.LittleEndian.PutUint64(number[:], uint64(len(value)))
		h.Write(number[:])
		for _, key := range slices.Sorted(maps.Keys(value)) {
			h.WriteString(key)
			h.WriteByte(0)
			writeSensitiveHash(h, value[key])
		}
	default:
		// No other shape reaches here from [LiteralToGo]. Hashed by type
		// alone, so every such value shares a bucket and equality decides.
		h.WriteByte(9)
		h.WriteString(reflect.TypeOf(value).String())
	}
}
