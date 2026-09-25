// Package nearest is the one did-you-mean rule: how far a typed name may be
// from a real one and still be offered as the thing that was probably meant.
//
// It exists because that rule was written four times (#435): once for
// Flowfile keys, once for cobra commands, once for cobra flags, once for task
// names. Each copy carried the same threshold, none imported another, and the
// last pair collided as a redeclaration during a merge rather than being
// noticed as a duplicate. That is the one-constant lesson from CLAUDE.md
// wearing a function's clothes, so the rule lives here and every caller reads
// it: one constant cannot disagree with itself.
//
// # Why here rather than in the schema package
//
// Both callers that needed it already import `pkg/flowstate/v1` (`flowfile`
// parses into its types; `cmd/flow` runs its tasks), so growing that package
// would have worked. It is a leaf package instead for two reasons. The schema
// package describes the execution model, and the descriptors in it are what
// out-of-process plugins compile against, so its exported surface is a
// contract `buf breaking` guards; a spelling heuristic is neither part of that
// model nor something worth pinning beside it. And the rule has no
// dependencies at all: keeping it in a package that imports nothing but the
// standard library is what lets it be read, tested, and tuned on its own,
// which is precisely what four scattered copies made impossible.
//
// It sits under `pkg/flowstate/v1/` rather than a module-root `internal/`
// because `cmd/flow` lives outside `pkg/flowstate/`, so Go's internal rule
// would put it out of reach of half its callers, and because the repo's
// format and vet gates are pointed at `./cmd` and `./pkg` (CLAUDE.md, "run
// what CI runs"): code outside those two trees is code no local gate checks.
//
// # What does not live here
//
// Only distance and the acceptance threshold. Every caller keeps its own
// presentation and its own bounds, because those genuinely differ:
//
//   - `flowfile` answers with a positioned diagnostic and a rename edit.
//   - `cmd/flow`'s command and flag suggestions rank a capped list, fold case
//     for commands, accept a shared prefix beyond the distance threshold for
//     commands only, and bound the typed side before scanning, because it
//     arrives from argv (#428).
//   - `cmd/flow`'s unknown-task path names a single closest match.
//   - `flowfile`'s `nearestChoice` suggests more willingly against a small
//     closed set, where a wrong suggestion costs a glance.
//
// Nothing in this package bounds the length of what it is given. A caller
// handed input an outside party sizes has to bound it before calling, at the
// edge where the input arrives, rather than trusting a bound buried in here to
// have guessed the right resource.
//
// [Name] does refuse a candidate whose length alone puts it out of reach before
// measuring the distance to it, which is the difference between scanning a
// megabyte-long name once per candidate and building a table that wide for each
// one. That is the same answer arrived at for less work, not a bound: a name the
// length of a candidate is still measured in full, so a caller holding input an
// outside party sizes still owes its own limit.
package nearest

import "unicode/utf8"

// MaxDistance is the most single-character edits a typed name may be from a
// real one and still be worth suggesting.
//
// Two is generous enough to catch a transposition or a dropped letter
// (`lst` -> `list` is one, `validte` -> `validate` is one) and tight enough
// that a short unrelated word does not turn up as a confident answer to
// something nobody typed.
const MaxDistance = 2

// Limit is the largest distance a candidate name accepts: at most a third of
// the name wrong, and never more than [MaxDistance].
//
// The proportion is what keeps the cap honest at the short end. Two edits into
// a four-letter name is half of it, which is not a typo but a different word,
// so a name pays for its own leniency by being long enough to have letters
// left over. Measured in runes, so a name spelled in a script whose letters
// take several bytes each gets the same third of itself an ASCII one does.
func Limit(name string) int {
	return min(utf8.RuneCountInString(name)/3+1, MaxDistance)
}

// Within reports whether distance is close enough to the candidate name for
// that name to be worth offering.
//
// Callers that compute distances themselves, because they rank or fold case,
// ask this rather than comparing against a threshold of their own.
func Within(name string, distance int) bool {
	return distance <= Limit(name)
}

// Name returns the name in known closest to got, when one is close enough to
// be worth suggesting.
//
// Ties go to the earlier name in known, so a caller that hands over a sorted
// or a document-ordered list gets an answer that does not move between runs.
func Name(got string, known []string) (string, bool) {
	best, bestDistance := "", 0

	// Counted once, outside the loop, because the check below needs it for
	// every candidate and it costs a pass over got to answer.
	length := utf8.RuneCountInString(got)

	for _, name := range known {
		// Two strings whose lengths differ by more than one name's limit cannot
		// be within it: every edit changes the length by at most one, so the
		// difference in lengths is itself a lower bound on the distance. Asked
		// before [Distance] rather than after, because that lower bound is two
		// counts while the distance is the product of the two lengths — and the
		// answer is the same either way, so this is the scan being skipped
		// rather than the threshold being changed.
		//
		// What it bounds is the caller that was handed a name an outside party
		// sized: an unknown key or function a megabyte long is measured against
		// each candidate and refused, instead of building a dynamic-programming
		// table a megabyte wide for every one of them (#1119).
		if difference := length - utf8.RuneCountInString(name); max(difference, -difference) > Limit(name) {
			continue
		}

		distance := Distance(got, name)
		if !Within(name, distance) {
			continue
		}
		if best == "" || distance < bestDistance {
			best, bestDistance = name, distance
		}
	}
	return best, best != ""
}

// Distance returns the Levenshtein distance between a and b: the fewest single
// character insertions, deletions, and substitutions that turn one into the
// other.
//
// Compared by rune rather than by byte, so an accented letter counts as the
// one character it looks like on the author's screen instead of the two or
// three it takes to encode. The work is proportional to the product of the two
// lengths; see the package doc on where that has to be bounded.
func Distance(a, b string) int {
	ar, br := []rune(a), []rune(b)

	prev := make([]int, len(br)+1)
	curr := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(ar); i++ {
		curr[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			curr[j] = min(prev[j]+1, curr[j-1]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}

	return prev[len(br)]
}
