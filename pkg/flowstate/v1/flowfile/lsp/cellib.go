package lsp

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The set of CEL extension libraries a cel step may enable lives in the
// evaluator, and so does the environment each one produces. This file reads both:
// the names come from the evaluator's list, and what a library actually provides
// is discovered by building an environment with it and comparing the declarations
// against the base environment.
//
// Deriving the function list matters because these libraries come from cel-go and
// grow between releases. A hand-written list would promise functions an upgraded
// dependency no longer has, or omit ones it gained.

// A celLibrary describes one CEL extension library for hover and completion.
type celLibrary struct {
	// Name is the value written in a cel step's libs input.
	Name string

	// Summary is a one-line description of what the library is for.
	//
	// This is the one thing here that is not derived: cel-go's extension
	// packages carry no machine-readable summary. Names with no entry get an
	// empty summary rather than a guess, so an added library is under-described
	// rather than mis-described.
	Summary string

	// Provides names the functions and macros the library adds to the base
	// environment, discovered by diffing environments.
	Provides []string
}

// librarySummaries describes what each extension library is for.
//
// The keys must be names the evaluator recognizes; any that are not simply go
// unused, and any evaluator name missing here is reported without a summary.
var librarySummaries = map[string]string{
	"bindings":       "Bind intermediate results to names within one expression.",
	"comprehensions": "Two-variable comprehensions over lists and maps.",
	"encoders":       "Base64 encoding and decoding.",
	"json":           "Parse a JSON string or bytes into CEL values with json_parse.",
	"lists":          "List helpers: sorting, flattening, ranges, and slicing.",
	"math":           "Numeric helpers: min, max, rounding, sign, and bit operations.",
	"optional":       "Optional values, for lookups that may not be present.",
	"protos":         "Read and test Protobuf extension fields.",
	"regex":          "Regular expression matching, extraction, and replacement.",
	"sets":           "Set operations over lists: membership, union, intersection.",
	"strings":        "String helpers: case, trimming, splitting, joining, formatting.",
}

// celLibraries returns every extension library a cel step may enable, keyed by
// name. It is computed once, because building an environment type-checks every
// declaration in it.
var celLibraries = sync.OnceValue(func() map[string]celLibrary {
	ev := v1.DefaultEvaluator()

	baseFuncs := map[string]bool{}
	baseMacros := map[string]bool{}
	if base, err := ev.Env(); err == nil {
		for name := range base.Functions() {
			baseFuncs[name] = true
		}
		for _, m := range base.Macros() {
			baseMacros[m.Function()] = true
		}
	}

	libs := make(map[string]celLibrary)
	for _, name := range v1.ExtensionLibraries() {
		lib := celLibrary{Name: name, Summary: librarySummaries[name]}

		if env, err := ev.Env(name); err == nil {
			var added []string
			for fn := range env.Functions() {
				if !baseFuncs[fn] {
					added = append(added, fn)
				}
			}
			for _, m := range env.Macros() {
				if !baseMacros[m.Function()] {
					added = append(added, m.Function())
				}
			}
			slices.Sort(added)
			lib.Provides = slices.Compact(added)
		}

		libs[name] = lib
	}
	return libs
})

// lookupCELLibrary returns the description of a named extension library.
func lookupCELLibrary(name string) (celLibrary, bool) {
	lib, ok := celLibraries()[strings.ToLower(name)]
	return lib, ok
}

// hover renders a library's documentation.
func (l celLibrary) hover() string {
	var b strings.Builder
	fmt.Fprintf(&b, "**CEL library `%s`**", l.Name)
	if l.Summary != "" {
		fmt.Fprintf(&b, "\n\n%s", l.Summary)
	}
	if len(l.Provides) > 0 {
		fmt.Fprintf(&b, "\n\nProvides: `%s`", strings.Join(l.Provides, "`, `"))
	}
	fmt.Fprintf(&b, "\n\nEnable it in a `cel` step with `libs: [%s]`.", l.Name)
	return b.String()
}
