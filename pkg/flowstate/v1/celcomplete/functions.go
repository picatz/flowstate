package celcomplete

import (
	"slices"
	"strings"
	"sync"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// An expression's completions once offered names and no functions.
//
// Inside `${...}` an author was shown the bindings in scope and the `steps`
// root, which is everything they can *reference* and nothing they can *do*. So
// the editor answered "what values are here" and never "what can I write" — and
// the second is the question somebody has when they are stuck, since a reference
// is usually already known and a function usually is not.
//
// The listing `flow tasks` prints is the same answer for the terminal, and it
// was added for the same reason: a profile is a membership, and one nobody can
// enumerate is one nobody can write against. This is that answer where somebody
// is actually typing — in an editor, and now at a debugger's prompt.

// byProfile caches each profile's candidate list.
//
// Per profile rather than once, because the profile is a property of what is
// being completed rather than of this build: an editor completes against
// [v1.CurrentProfile] while a debugger completes against the profile the run
// was compiled with, and a run compiled by an older build has a smaller
// vocabulary that this must not silently widen.
var byProfile sync.Map // string -> []Candidate

// FunctionCandidates are a profile's functions, ready to offer.
//
// Two shapes, because a name and a qualifier complete differently.
// `upperAscii` is written whole; `math` is only ever the front of something, so
// it is offered with its dot and the member list arrives on the next keystroke.
func FunctionCandidates(profile string) []Candidate {
	if cached, ok := byProfile.Load(profile); ok {
		return cached.([]Candidate)
	}

	var (
		bare       []Candidate
		namespaces []Candidate
		seen       = map[string]bool{}
	)

	for _, fn := range v1.ProfileFunctions(profile) {
		qualifier, _, qualified := strings.Cut(fn.Name, ".")

		// A macro's Name is not its call form, which [v1.LibraryFunction] says
		// in as many words: cel-go identifies a macro by the name after the
		// dot, so `math.greatest(1, 2)` arrives here called `greatest`. Offered
		// as a bare name it inserted `greatest`, and every one of the twelve
		// macros the profile adds answered `no function called "greatest"` from
		// the validator on the very next keystroke — the editor completing a
		// name into a diagnostic.
		//
		// Where a macro *is* writable is decided from its example rather than
		// from its name, because the name cannot tell the two shapes apart. See
		// [macroQualifier].
		if fn.Macro {
			if ns, ok := macroQualifier(fn); ok && !seen[ns] {
				seen[ns] = true
				namespaces = append(namespaces, namespaceCandidate(ns))
			}

			continue
		}

		if !qualified {
			bare = append(bare, Candidate{
				Name:   fn.Name,
				Detail: functionDetail(fn),
				Docs:   functionDocs(fn),
				Kind:   KindFunction,
			})

			continue
		}
		if seen[qualifier] {
			continue
		}
		seen[qualifier] = true

		namespaces = append(namespaces, namespaceCandidate(qualifier))
	}

	slices.SortFunc(namespaces, func(a, b Candidate) int { return strings.Compare(a.Name, b.Name) })

	// Bare names first. A qualifier is a detour — it commits to a namespace
	// before saying anything about what is in it — and the functions somebody
	// reaches for most (`size`, `join`, `upperAscii`) are the unqualified ones.
	// Clipped so that a caller appending to what it is handed reallocates
	// instead of writing into the cache every other caller shares.
	out := slices.Clip(append(bare, namespaces...))
	byProfile.Store(profile, out)

	return out
}

// namespaceCandidate is a qualifier offered with the dot that follows it.
func namespaceCandidate(qualifier string) Candidate {
	return Candidate{
		Name:   qualifier,
		Detail: "functions",
		// Not attributed to a library, deliberately. A qualifier and a library
		// are different things with overlapping names: `json.encode` is
		// declared by `encoders`, and there is also a library called `json` —
		// so "a namespace the encoders library declares", though true, reads as
		// a contradiction next to it. What somebody needs here is how to write
		// the next character.
		Docs: "A namespace. Its functions are written " + qualifier + ".<name>(...); " +
			"type the dot to see them.",
		// The dot comes with it, the way the `steps` root's does: a qualifier
		// is never the whole of an expression, and stopping at it would leave
		// the author to type the one character that produces the next list.
		Insert: qualifier + ".",
		Kind:   KindNamespace,
	}
}

// macroQualifier returns the namespace a macro is written on, when it has one.
//
// Two shapes hide behind one name, and cel-go's API does not distinguish them —
// [v1.LibraryFunction] says so and provides the example that does. `greatest`
// is written `math.greatest(1, 2)`, on a namespace; `sortBy` is written
// `[3, 1, 2].sortBy(v, v)`, on a value. The first belongs in a namespace's
// member list. The second belongs after a dot on an expression, which there is
// no completion surface for — better absent than offered in a spelling that
// does not compile.
//
// Decided by asking whether what precedes the call in the example is a plain
// identifier. `math` is; `[3, 1, 2]`, `{'a': 1}` and `optional.of(2)` are not,
// the last one because a namespace is a name and not a call.
func macroQualifier(fn v1.LibraryFunction) (string, bool) {
	call := "." + fn.Name + "("

	at := strings.Index(fn.Example, call)
	if at <= 0 {
		return "", false
	}

	return fn.Example[:at], isIdentifier(fn.Example[:at])
}

// isIdentifier reports whether s is a plain CEL identifier.
func isIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for i, r := range s {
		alpha := r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		if alpha || (i > 0 && r >= '0' && r <= '9') {
			continue
		}

		return false
	}

	return true
}

// FunctionsAfter returns what may be written after a qualifier, or nil when the
// qualifier names no namespace the profile has.
//
// Nil rather than an empty list on purpose: the caller distinguishes "this is a
// namespace with nothing matching" from "this is not a namespace", and offering
// nothing for a bare binding is the correct behaviour for the second.
func FunctionsAfter(profile, qualifier string) []Candidate {
	var out []Candidate

	for _, fn := range v1.ProfileFunctions(profile) {
		member := fn.Name

		if fn.Macro {
			// The other half of the same correction. A macro's name has no
			// qualifier in it, so matching on the name alone left
			// `math.greatest` — a spelling the validator accepts — offered
			// nowhere at all, while the spelling it rejects was offered at the
			// top level.
			ns, ok := macroQualifier(fn)
			if !ok || ns != qualifier {
				continue
			}
		} else {
			prefix, name, qualified := strings.Cut(fn.Name, ".")
			if !qualified || prefix != qualifier {
				continue
			}
			member = name
		}

		out = append(out, Candidate{
			Name:   member,
			Detail: functionDetail(fn),
			Docs:   functionDocs(fn),
			Kind:   KindFunction,
		})
	}

	slices.SortFunc(out, func(a, b Candidate) int { return strings.Compare(a.Name, b.Name) })

	return out
}

// functionDetail is the one line beside a name in a listing.
func functionDetail(fn v1.LibraryFunction) string {
	if fn.Macro {
		return fn.Library + " macro"
	}

	return fn.Library
}

// functionDocs says the one thing about a function that is not obvious from its
// name, which is only ever true of a macro.
//
// A signature would be better and is not available: cel-go's declarations carry
// overloads in a form that would have to be rendered into CEL's own type syntax
// to be worth reading, and a rendering that is subtly wrong about which
// overloads exist is worse than none — it is the shape of wrongness this whole
// area keeps producing. So this says where a name comes from and, for a macro,
// when it is resolved.
func functionDocs(fn v1.LibraryFunction) string {
	if !fn.Macro {
		return "From the " + fn.Library + " library, available to every expression in the file."
	}

	docs := "A macro from the " + fn.Library + " library, expanded when the file compiles, " +
		"so what a run carries is the expansion rather than this spelling."

	// The example rather than a general remark about where macros are written.
	// It is the whole reason a macro's entry needs prose at all: its name is
	// not its call form, and the two macros with no example are the two nobody
	// can write anyway.
	if fn.Example != "" {
		docs += " Written: " + fn.Example
	}

	return docs
}
