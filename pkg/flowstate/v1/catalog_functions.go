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

	// Example is a complete expression calling this, set only for a macro.
	//
	// A macro's own API will not say whether its Name is written on a namespace or
	// a value — see [macroExamples] — so this exists to answer that question by
	// hand for a macro specifically. An ordinary function does not need it: its
	// overloads answer the same question, and Signature carries what they say,
	// including the arity and argument order Example does not give either kind.
	Example string

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

	// Signature is this function's call form, one entry per overload: argument
	// order, arity and types, and — unlike Name alone — whether it is written on a
	// namespace or a value. `string.charAt(int) -> string` for a member function,
	// `math.abs(double) -> double` for a namespaced one. Empty for a macro, where
	// Example carries the call form instead.
	//
	// Derived, not written down, which is the difference from Example. Example is
	// a written table precisely because cel-go's `Macro` type exposes no way to
	// tell a namespace receiver from a value one; an ordinary function's own
	// overload does not have that gap — `OverloadDecl.IsMemberFunction` names the
	// receiver directly — so [functionSignatures] reads it straight off the
	// profile's compiled environment instead of a table somebody has to keep
	// (#702).
	Signature []string
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

	// Signatures come from the whole profile's environment, not from the one
	// library a name is listed under. A name is *listed* once (see claimed,
	// below), but its overloads can come from several libraries — `reverse`
	// is `list().reverse()` from lists and `string.reverse()` from strings —
	// and an author reading the row for the claiming library still calls
	// every overload the profile actually compiles. Per-library signature
	// maps were how the listing came to advertise only the first library's
	// half of a shared name.
	sigs := functionSignatures(libs...)

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
			out = append(out, LibraryFunction{
				Library:   lib,
				Name:      name,
				Macro:     macro,
				Example:   macroExamples[name],
				Signature: sigs[name],
			})
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

// functionSignatures returns, for every ordinary function an environment
// declares, its overloads formatted as call forms: argument order, arity and
// whether it is written on a namespace or a value.
//
// No macro in this profile is also registered as a function under the same
// name, so a macro's entry here is simply absent — this walks
// `env.Functions()` alone and does not consult `env.Macros()` at all. That
// is not a case this function forbids, only one it has never seen: exactly
// the possibility [declaredNames]' own doc comment already names ("a macro
// and a function of the same name is not a case cel-go has, but if it
// arrives..."). [LibraryFunction.Signature] staying empty for every macro is
// what [TestOnlyAnOrdinaryFunctionCarriesASignature] checks, so a future
// collision would fail there rather than pass silently.
//
// cel-go's own [decls.FunctionDecl.Documentation] does the formatting: it
// walks each overload and asks [decls.OverloadDecl.IsMemberFunction], which
// is the fact a macro's API cannot give up (see [macroExamples]'s doc
// comment) but an ordinary function's overload always carries.
func functionSignatures(libs ...string) map[string][]string {
	env, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil
	}

	out := map[string][]string{}
	for name, fn := range env.Functions() {
		doc := fn.Documentation()
		if doc == nil {
			continue
		}
		for _, overload := range doc.Children {
			if overload.Signature == "" {
				continue
			}
			out[name] = append(out[name], overload.Signature)
		}
		slices.Sort(out[name])
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

// macroExamples is a working expression for every macro the profile has, keyed by
// the name cel-go reports.
//
// Written out, which this file otherwise refuses to do. Everything else here is
// derived, and this is derived too where it can be: the *set* comes from the
// environment, and [TestEveryMacroHasAnExample] fails on a macro that has no entry,
// so a library added to a profile cannot bring in a macro nobody has written a call
// for.
//
// What cannot be derived is the call itself. cel-go's `Macro` exposes `Function`,
// `ArgCount`, `IsReceiverStyle` and `MacroKey`; none of them names the receiver. So
// `greatest` is reported for `math.greatest(1, 2)` and `sortBy` for
// `[3,1,2].sortBy(v, v)` — a namespace in one case and a value in the other, and
// nothing in the API tells them apart.
//
// Pairing each library's macros with the internal `@` names it declares was tried:
// `math.@max` beside `greatest` gives `math`, and `cel.@block` beside `bind` gives
// `cel`, both right. It is silently wrong for `comprehensions`, which declares
// `cel.@mapInsert` while `transformList` is written on a value. A derivation wrong
// for one library in five is worse than a written table, because nothing about it
// says which one.
//
// Every entry is *evaluated* by its test rather than eyeballed, so an example that
// stops working stops passing.
var macroExamples = map[string]string{
	// Standard, from cel-go itself.
	"has":        `has({'a': 1}.a)`,
	"all":        `[1, 2].all(v, v > 0)`,
	"exists":     `[1, 2].exists(v, v > 1)`,
	"exists_one": `[1, 2].exists_one(v, v > 1)`,
	"map":        `[1, 2].map(v, v * 2)`,
	"filter":     `[1, 2].filter(v, v > 1)`,

	// bindings.
	"bind": `cel.bind(x, 2, x + 1)`,

	// comprehensions. Written on a value despite the library declaring `cel.@…`,
	// which is the case that sank deriving these.
	"existsOne":         `[1, 2].existsOne(i, v, v > 1)`,
	"transformList":     `[1, 2].transformList(i, v, v * 2)`,
	"transformMap":      `{'a': 1}.transformMap(k, v, v * 10)`,
	"transformMapEntry": `{'a': 1}.transformMapEntry(k, v, {k: v * 2})`,

	// lists.
	"sortBy": `[3, 1, 2].sortBy(v, v)`,
	"sum":    `[1, 2, 3].sum()`,

	// math. Written on the namespace.
	"greatest": `math.greatest(1, 2)`,
	"least":    `math.least(3, 4)`,

	// optional.
	"optMap":     `optional.of(2).optMap(v, v * 3)`,
	"optFlatMap": `optional.of(2).optFlatMap(v, optional.of(v * 3))`,

	// protos. The only two with no example: both take a protobuf extension field,
	// which is a name in a descriptor rather than a value an expression can write,
	// so there is no complete call to give. Present as empty entries rather than
	// absent, because absent is indistinguishable from forgotten — and that is the
	// distinction the completeness test needs in order to mean anything.
	"getExt": "",
	"hasExt": "",
}
