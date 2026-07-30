package lsp

import (
	"slices"
	"strings"
	"sync"

	lsp "github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// An expression's completions offered names and no functions.
//
// Inside `${...}` an author was shown the bindings in scope and the `steps` root,
// which is everything they can *reference* and nothing they can *do*. So the editor
// answered "what values are here" and never "what can I write" — and the second is
// the question somebody has when they are stuck, since a reference is usually
// already known and a function usually is not.
//
// The listing `flow tasks` prints is the same answer for the terminal, and it was
// added for the same reason: a profile is a membership, and one nobody can enumerate
// is one nobody can write against. This is that answer where an author is actually
// typing.

// functionCandidates are the profile's functions, ready to offer, computed once.
//
// Two shapes, because a name and a qualifier complete differently. `upperAscii` is
// written whole; `math` is only ever the front of something, so it is offered with
// its dot and the member list arrives on the next keystroke.
var functionCandidates = sync.OnceValue(func() (out []refCandidate) {
	var (
		bare       []refCandidate
		namespaces []refCandidate
		seen       = map[string]bool{}
	)

	for _, fn := range v1.ProfileFunctions(v1.CurrentProfile) {
		qualifier, _, qualified := strings.Cut(fn.Name, ".")
		if !qualified {
			bare = append(bare, refCandidate{
				name:   fn.Name,
				detail: functionDetail(fn),
				docs:   functionDocs(fn),
				kind:   lsp.CIKFunction,
			})

			continue
		}
		if seen[qualifier] {
			continue
		}
		seen[qualifier] = true

		namespaces = append(namespaces, refCandidate{
			name:   qualifier,
			detail: "functions",
			// Not attributed to a library, deliberately. A qualifier and a library
			// are different things with overlapping names: `json.encode` is declared
			// by `encoders`, and there is also a library called `json` — so "a
			// namespace the encoders library declares", though true, reads as a
			// contradiction next to it. What an author needs here is how to write
			// the next character.
			docs: "A namespace. Its functions are written " + qualifier + ".<name>(...); " +
				"type the dot to see them.",
			// The dot comes with it, the way the `steps` root's does: a qualifier is
			// never the whole of an expression, and stopping at it would leave the
			// author to type the one character that produces the next list.
			insert: qualifier + ".",
			kind:   lsp.CIKModule,
		})
	}

	// Bare names first. A qualifier is a detour — it commits to a namespace before
	// saying anything about what is in it — and the functions somebody reaches for
	// most (`size`, `join`, `upperAscii`) are the unqualified ones.
	return append(bare, namespaces...)
})

// functionsAfter returns what may be written after a qualifier, or nil when the
// qualifier names no namespace the profile has.
//
// Nil rather than an empty list on purpose: the caller distinguishes "this is a
// namespace with nothing matching" from "this is not a namespace", and offering
// nothing for a bare binding is the existing, correct behaviour.
func functionsAfter(qualifier string) []refCandidate {
	var out []refCandidate

	for _, fn := range v1.ProfileFunctions(v1.CurrentProfile) {
		prefix, member, qualified := strings.Cut(fn.Name, ".")
		if !qualified || prefix != qualifier {
			continue
		}
		out = append(out, refCandidate{
			name:   member,
			detail: functionDetail(fn),
			docs:   functionDocs(fn),
			kind:   lsp.CIKFunction,
		})
	}

	slices.SortFunc(out, func(a, b refCandidate) int { return strings.Compare(a.name, b.name) })

	return out
}

// functionDetail is the one line beside a name in the popup.
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
// overloads in a form that would have to be rendered into CEL's own type syntax to
// be worth reading, and a rendering that is subtly wrong about which overloads exist
// is worse than none — it is the shape of wrongness this whole area keeps producing.
// So this says where a name comes from and, for a macro, when it is resolved.
func functionDocs(fn v1.LibraryFunction) string {
	if !fn.Macro {
		return "From the " + fn.Library + " library, available to every expression in the file."
	}

	return "A macro from the " + fn.Library + " library. It is written on something — " +
		"math.greatest(1, 2), [3,1,2].sortBy(v, v) — and is expanded when the file compiles, " +
		"so what a run carries is the expansion rather than this spelling."
}
