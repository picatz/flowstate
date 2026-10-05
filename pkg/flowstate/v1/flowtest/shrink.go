package flowtest

import (
	"slices"
)

// MaxShrinkRuns bounds how many times a violating seed's faults are re-run
// while looking for a smaller set that still violates. Each probe is a whole run
// of the case, and a seed can fire many faults, so the search spends a fixed
// budget and reports the smallest set it found rather than running unbounded.
const MaxShrinkRuns = 256

// shrinkResult is what [shrinkFaults] found.
type shrinkResult struct {
	// Pins are the smallest violating set found, in the form of the input.
	Pins []Fault
	// From is how many fault firings the input held; len(atoms of Pins) is how
	// many remain.
	From int
	// Runs is how many probes were spent.
	Runs int
	// Reproduced reports that the input violated when replayed on its own. When
	// it did not, nothing was shrunk and Pins is the input.
	Reproduced bool
	// Minimal reports that removing any single firing from Pins stopped the
	// violation; false when the budget ended the search first.
	Minimal bool
}

// faultAtom is one firing: fault index i fires on its n-th eligible invocation.
type faultAtom struct{ fault, n int }

func atomsOf(pins []Fault, authored []bool) []faultAtom {
	var atoms []faultAtom
	for i, pin := range pins {
		if authored[i] {
			continue
		}
		for _, n := range pin.On {
			atoms = append(atoms, faultAtom{i, n})
		}
	}

	return atoms
}

// pinsOf rebuilds a fault list holding only the given firings, keeping the
// order and every other property of the faults they came from. A fault with no
// firing left is dropped.
func pinsOf(from []Fault, authored []bool, atoms []faultAtom) []Fault {
	var out []Fault
	for i, pin := range from {
		if authored[i] {
			out = append(out, pin)

			continue
		}
		var on []int
		for _, a := range atoms {
			if a.fault == i {
				on = append(on, a.n)
			}
		}
		if len(on) == 0 {
			continue
		}
		slices.Sort(on)
		pin.On = on
		out = append(out, pin)
	}

	return out
}

// shrinkFaults reduces pins, a violating set of pinned faults, to a smaller one
// that still violates, by delta debugging (Zeller and Hildebrandt, "Simplifying
// and Isolating Failure-Inducing Input", TSE 2002) over the set of firings.
//
// Pins the case declared itself (authored[i]) are fixed: they are the world
// the case describes, and only the firings a seed drew on top of them are
// shrunk.
//
// violates must report whether a run with exactly that fault list breaks the
// case, and ok=false when the run was cut off (a cancelled context, the case's
// time bound) and so answers nothing. The input is probed first: a set that does not reproduce by itself, which
// would mean the seed's violation depended on something a pin does not carry, is
// returned unchanged with Minimal false, because a "smaller" set found from a
// starting point that does not reproduce would not be a shrink of anything.
//
// The result is 1-minimal when Minimal is true: no single firing can be removed
// from it. It is a local minimum, not the smallest violating set there is, and
// it may violate a different invariant than the input did; the probe asks only
// whether the case breaks.
//
// At most maxRuns probes are spent, including the first.
func shrinkFaults(pins []Fault, authored []bool, maxRuns int, violates func([]Fault) (violated, ok bool)) shrinkResult {
	atoms := atomsOf(pins, authored)
	result := shrinkResult{Pins: pins, From: len(atoms)}

	exhausted := false
	probe := func(subset []faultAtom) bool {
		if result.Runs >= maxRuns {
			exhausted = true

			return false
		}
		result.Runs++

		violated, ok := violates(pinsOf(pins, authored, subset))
		if !ok {
			// The probe was cut off, so its "no" says nothing about the
			// subset; the search ends there rather than reading it as a
			// removal that stopped the violation.
			exhausted = true

			return false
		}

		return violated
	}

	if len(atoms) == 0 || !probe(atoms) {
		return result
	}
	result.Reproduced = true

	granularity := 2
	for len(atoms) >= 2 && !exhausted {
		chunks := splitAtoms(atoms, granularity)
		reduced := false

		// A single chunk that violates on its own.
		for _, chunk := range chunks {
			if probe(chunk) {
				atoms, granularity, reduced = chunk, 2, true

				break
			}
		}
		// Otherwise the input without one chunk.
		// (At two chunks the complements are the chunks, already probed.)
		for i := 0; !reduced && granularity > 2 && i < len(chunks); i++ {
			rest := slices.Concat(chunks[:i]...)
			rest = append(rest, slices.Concat(chunks[i+1:]...)...)
			if len(rest) > 0 && probe(rest) {
				atoms, granularity, reduced = rest, max(granularity-1, 2), true
			}
		}
		if reduced {
			continue
		}
		if granularity >= len(atoms) {
			break
		}
		granularity = min(granularity*2, len(atoms))
	}

	result.Pins = pinsOf(pins, authored, atoms)
	// Every removal of a single firing was probed and stopped the violation,
	// unless the budget cut the search off before it could say so.
	result.Minimal = !exhausted

	return result
}

// splitAtoms divides atoms into n contiguous chunks of near-equal size.
func splitAtoms(atoms []faultAtom, n int) [][]faultAtom {
	chunks := make([][]faultAtom, 0, n)
	size, extra := len(atoms)/n, len(atoms)%n
	start := 0
	for i := range n {
		end := start + size
		if i < extra {
			end++
		}
		chunks = append(chunks, atoms[start:end])
		start = end
	}

	return chunks
}
