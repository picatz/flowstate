package flowstatev1

import (
	"maps"
	"slices"
	"strings"
)

// What a profile *contains* was not discoverable from anywhere.
//
// `flow tasks` printed the library names — `bindings, comprehensions, encoders,
// json, lists, math, optional, protos, regex, sets, strings` — which say what is
// switched on and nothing about what any of them offers. Nothing else printed more.
// So an author who wanted to sort a list had no way to learn that `sortBy` exists,
// short of reading cel-go's extension documentation and guessing which parts of it
// this build enables.
//
// That was survivable while the answer was "everything cel-go has". It stopped being
// survivable the moment a profile became a *membership* — the point of naming one is
// that it is a subset, and a subset nobody can enumerate is a subset nobody can
// write against.

// LibraryFunction is one name an expression may call.
type LibraryFunction struct {
	// Library is the profile library that declares it.
	Library string

	// Name is written exactly as it is called: `math.greatest`, `upperAscii`.
	Name string

	// Macro says the parser expands this rather than the evaluator calling it.
	//
	// Worth surfacing rather than levelling away, for two reasons. The difference is
	// visible to an author: a macro is settled when the file compiles, so it is
	// frozen into the compiled workflow, where a function is looked up by whatever
	// worker evaluates the run.
	//
	// And Name is not the whole spelling for one. cel-go identifies a macro by the
	// name after the dot, so `math.greatest(1, 2)` is reported as `greatest` and
	// `[3,1,2].sortBy(v, v)` as `sortBy` — a receiver style whose receiver is a
	// namespace in the first case and a value in the second, and cel-go's API does
	// not say which. So a caller must not print a macro as though it were a call
	// form; it says how one is written once, rather than guessing per entry.
	Macro bool
}

// ProfileFunctions returns every name the profile's libraries add, sorted by library
// and then by name.
//
// Derived by asking each library's environment what it declares and subtracting what
// cel-go declares on its own, which is the only construction that cannot drift: a
// list maintained beside the profile would be a second place to forget, and the
// first place has already been forgotten once — every macro here was unreachable
// from a Flowfile until the compiler was told which libraries a profile has.
//
// An empty result means the environment could not be built, which is a defect in the
// build rather than something a caller can act on. Returning nothing is right for
// every caller this has: a listing prints a shorter section, and none of them can do
// anything useful with an error about CEL environment construction.
func ProfileFunctions(profile string) []LibraryFunction {
	libs, err := ProfileLibraries(profile)
	if err != nil {
		return nil
	}

	// What cel-go brings before any library is added. `size`, `has`, `filter` and
	// the operators are not a library's contribution and listing them under one
	// would be wrong about where they come from.
	base := declaredNames()

	// A name is listed once, under the first library that declares it.
	//
	// Two libraries genuinely declare the same names: this build's `regex` entry
	// pulls in cel-go's optional types, so an undeduplicated listing repeats all
	// thirteen of `optional` under `regex`, and `reverse` appears under both `lists`
	// and `strings`. A profile is one membership rather than a set of switches, so
	// which library a name arrived through is a fact about this build's wiring, not
	// something an author acts on — and printing it twice reads as two functions.
	claimed := map[string]bool{}

	var out []LibraryFunction
	for _, lib := range libs {
		declared := declaredNames(lib)
		for _, name := range slices.Sorted(maps.Keys(declared)) {
			macro := declared[name]
			if claimed[name] {
				continue
			}
			// Presence, not the value. The map's value says whether a name is a
			// macro, so `base[name]` is false for every ordinary function cel-go
			// declares — and the first version of this line subtracted only the
			// macros, putting `size`, `string` and thirty others under every one of
			// the eleven libraries.
			if _, standard := base[name]; standard {
				continue
			}
			if !isCallableName(name) {
				continue
			}
			claimed[name] = true
			out = append(out, LibraryFunction{Library: lib, Name: name, Macro: macro})
		}
	}

	slices.SortFunc(out, func(a, b LibraryFunction) int {
		if a.Library != b.Library {
			return strings.Compare(a.Library, b.Library)
		}

		return strings.Compare(a.Name, b.Name)
	})

	return out
}

// declaredNames returns the function and macro names an environment declares, each
// mapped to whether it is a macro.
func declaredNames(libs ...string) map[string]bool {
	env, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil
	}

	out := map[string]bool{}
	for name := range env.Functions() {
		out[name] = false
	}
	for _, macro := range env.Macros() {
		// A macro and a function of the same name is not a case cel-go has, but if
		// it arrives, the macro is what the parser reaches first and so is what an
		// author is actually writing.
		out[macro.Function()] = true
	}

	return out
}

// isCallableName reports whether a declared name is one somebody can write.
//
// cel-go declares three kinds of name and only one of them is a function an author
// calls. The operators are spelled with placeholders — `_+_`, `_[?_]` — because they
// are written as syntax rather than as calls. And a macro's expansion is declared
// under a reserved name with an `@` in it, `math.@max` for `math.greatest`, which is
// deliberately unwritable so that nothing can call the internal form directly.
//
// Both would be worse than noise in a listing: an author who tried either would get
// a parse error from a name this command had just told them about.
func isCallableName(name string) bool {
	if name == "" {
		return false
	}

	for _, segment := range strings.Split(name, ".") {
		if segment == "" {
			return false
		}
		for i, r := range segment {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			case r >= '0' && r <= '9' && i > 0:
			default:
				return false
			}
		}
	}

	return true
}
