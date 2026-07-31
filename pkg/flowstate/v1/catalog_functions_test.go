package flowstatev1

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEveryListedFunctionIsOneSomebodyCanWrite is the whole point of the filter.
//
// cel-go declares three kinds of name and only one is a function an author calls.
// Operators are spelled with placeholders — `_+_`, `_[?_]` — because they are
// written as syntax. And a macro's expansion is declared under a reserved name with
// an `@` in it: `math.@max` is what `math.greatest` becomes, deliberately unwritable
// so nothing can call the internal form.
//
// A listing that printed either would be worse than one that printed nothing, since
// an author who tried a name this command had just shown them would get a parse
// error from it.
func TestEveryListedFunctionIsOneSomebodyCanWrite(t *testing.T) {
	t.Parallel()

	functions := ProfileFunctions(CurrentProfile)
	require.NotEmpty(t, functions, "the profile listed no functions, so this proves nothing")

	for _, fn := range functions {
		assert.NotContains(t, fn.Name, "@",
			"%q is a macro's internal expansion, which no expression may name", fn.Name)
		assert.NotContains(t, fn.Name, "_?",
			"%q is an operator's placeholder spelling rather than a call", fn.Name)
		assert.True(t, isCallableName(fn.Name),
			"%q is not a name an expression can write", fn.Name)
	}
}

// TestWhatCELDeclaresOnItsOwnIsNotAttributedToALibrary keeps the listing honest
// about where a name comes from.
//
// `size`, `string`, `has` and the operators are cel-go's, present with no library
// enabled. Listing them under `bindings` — which is what happens if the subtraction
// tests a map's *value* rather than a key's presence, and is what the first version
// of this did — puts thirty names under each of eleven libraries and tells a reader
// that enabling one is what provides them.
func TestWhatCELDeclaresOnItsOwnIsNotAttributedToALibrary(t *testing.T) {
	t.Parallel()

	listed := map[string]bool{}
	for _, fn := range ProfileFunctions(CurrentProfile) {
		listed[fn.Name] = true
	}

	for _, name := range []string{"size", "string", "int", "has", "filter", "map", "matches", "contains"} {
		assert.NotContains(t, listed, name,
			"%q comes with cel-go rather than with a library, so listing it under one is wrong", name)
	}
}

// TestALibrarysNamesAreNotRepeatedUnderAnother is the deduplication, and why it is
// needed at all.
//
// Two of this build's library entries genuinely declare the same names: `regex`
// pulls in cel-go's optional types, so an undeduplicated listing repeats all
// thirteen of `optional` beneath it, and `reverse` appears under both `lists` and
// `strings`. Printed twice, one function reads as two.
func TestALibrarysNamesAreNotRepeatedUnderAnother(t *testing.T) {
	t.Parallel()

	seen := map[string]string{}
	for _, fn := range ProfileFunctions(CurrentProfile) {
		if first, repeated := seen[fn.Name]; repeated {
			assert.Failf(t, "a name is listed twice",
				"%q is listed under both %q and %q, which reads as two functions", fn.Name, first, fn.Library)

			continue
		}
		seen[fn.Name] = fn.Library
	}

	// The two that make the rule necessary, so a change to the library wiring that
	// removed the overlap would show up here rather than leaving a dedup nobody can
	// tell is still doing anything.
	assert.Equal(t, "optional", seen["optional.of"],
		"`optional.of` moved, so the overlap this deduplicates may have changed shape")
	assert.Equal(t, "lists", seen["reverse"],
		"`reverse` moved, so the overlap this deduplicates may have changed shape")
}

// TestTheListingAgreesWithTheEnvironmentItDescribes is the check that this is
// derived rather than remembered.
//
// Every listed name has to be something the profile's environment actually declares,
// and a macro has to be flagged as one — because the flag decides what the listing
// prints beside it, and a function shown as a macro tells an author it is frozen
// into the compiled workflow when it is not.
func TestTheListingAgreesWithTheEnvironmentItDescribes(t *testing.T) {
	t.Parallel()

	libs, err := ProfileLibraries(CurrentProfile)
	require.NoError(t, err)

	env, err := DefaultEvaluator().Env(libs...)
	require.NoError(t, err)

	functions := map[string]bool{}
	for name := range env.Functions() {
		functions[name] = true
	}
	macros := map[string]bool{}
	for _, macro := range env.Macros() {
		macros[macro.Function()] = true
	}

	for _, fn := range ProfileFunctions(CurrentProfile) {
		if fn.Macro {
			assert.Contains(t, macros, fn.Name,
				"%q is listed as a macro and the environment does not declare one", fn.Name)

			continue
		}
		assert.Contains(t, functions, fn.Name,
			"%q is listed and the environment declares no such function", fn.Name)
		assert.NotContains(t, macros, fn.Name,
			"%q is a macro and is listed as an ordinary function, which says the wrong thing "+
				"about when it is resolved", fn.Name)
	}
}

// TestTheThingsAnAuthorCameLookingForAreListed is the failure that started this.
//
// `flow tasks` printed the library names and stopped, so somebody who wanted to sort
// a list had no way to learn that `sortBy` exists. These four are the answers to
// questions this repo has actually had to answer by reading cel-go's source: how to
// sort, how to pick the larger of two numbers, how to render a structure, and how to
// name a value inside one expression.
func TestTheThingsAnAuthorCameLookingForAreListed(t *testing.T) {
	t.Parallel()

	listed := map[string]bool{}
	for _, fn := range ProfileFunctions(CurrentProfile) {
		listed[fn.Name] = true
	}

	for _, name := range []string{"sortBy", "greatest", "json.encode", "bind", "regex.replace", "upperAscii"} {
		assert.Contains(t, listed, name,
			"%q is in the profile and `flow tasks` does not name it", name)
	}
}

// TestAnUnknownProfileListsNothing is the fail-quiet direction.
//
// A profile this build does not know is a defect in the build or a spec from a newer
// one, and neither is something a *listing* can act on. Every caller of this prints
// a section; none can do anything with an error about CEL environment construction,
// and a listing that refused to print the task catalog because it could not name a
// function would be a worse answer than a shorter page.
func TestAnUnknownProfileListsNothing(t *testing.T) {
	t.Parallel()

	assert.Empty(t, ProfileFunctions("not-a-profile"),
		"an unknown profile produced a function listing, so the names came from somewhere else")
}

// TestIsCallableNameRefusesTheSpellingsNobodyCanType covers the filter directly, for
// the shapes cel-go produces that no current library happens to declare.
func TestIsCallableNameRefusesTheSpellingsNobodyCanType(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		want bool
	}{
		{name: "upperAscii", want: true},
		{name: "regex.replace", want: true},
		{name: "json_parse", want: true},
		{name: "_", want: true},

		{name: "", want: false},
		{name: "_+_", want: false},
		{name: "_[?_]", want: false},
		{name: "math.@max", want: false},
		{name: "cel.@block", want: false},
		{name: "a..b", want: false},
		{name: "9lives", want: false},
		{name: "has spaces", want: false},
	} {
		t.Run(strings.ReplaceAll(test.name, " ", "_"), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, isCallableName(test.name))
		})
	}
}

// TestEveryMacroHasAnExample is what makes a written table acceptable here.
//
// The set comes from the environment, so a library added to a profile brings its
// macros into this check the same day, and one with no entry fails naming itself.
// That is the half a maintained list normally cannot have.
//
// An entry may be empty, and that is not the same as missing: `proto.getExt` takes a
// protobuf extension field, which is a name in a descriptor rather than a value an
// expression can write, so there is no complete call to give. Present-and-empty says
// somebody decided; absent says nobody looked.
func TestEveryMacroHasAnExample(t *testing.T) {
	t.Parallel()

	libs, err := ProfileLibraries(CurrentProfile)
	require.NoError(t, err)

	env, err := DefaultEvaluator().Env(libs...)
	require.NoError(t, err)

	require.NotEmpty(t, env.Macros(), "no macros found, so this test checks nothing")

	for _, macro := range env.Macros() {
		_, described := macroExamples[macro.Function()]
		assert.True(t, described,
			"the profile has a macro %q and no example calls it; add one, or an empty entry "+
				"saying no expression can", macro.Function())
	}
}

// TestEveryMacroExampleEvaluates is the other half, and the reason the table can be
// trusted at all.
//
// Evaluated rather than inspected. An example is a string in a map, so nothing about
// writing one proves it is a call anybody can make — and this table exists precisely
// because the *machine-readable* catalog hands these to a consumer that will try to
// use them. An entry that stops working stops passing.
func TestEveryMacroExampleEvaluates(t *testing.T) {
	t.Parallel()

	libs, err := ProfileLibraries(CurrentProfile)
	require.NoError(t, err)

	for name, example := range macroExamples {
		if example == "" {
			continue
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			out, err := DefaultEvaluator().EvalString(t.Context(), example, libs, map[string]any{})
			require.NoError(t, err, "the example for %q does not evaluate: %s", name, example)
			require.NotNil(t, out, "the example for %q evaluated to nothing: %s", name, example)
		})
	}
}

// TestOnlyAMacroCarriesAnExample keeps the field meaning one thing.
//
// A function's name *is* its call form, so an example beside one would be a second
// way of saying what `name` already says — and two spellings of one fact is how they
// come to disagree.
func TestOnlyAMacroCarriesAnExample(t *testing.T) {
	t.Parallel()

	for _, fn := range ProfileFunctions(CurrentProfile) {
		if fn.Macro {
			continue
		}
		assert.Empty(t, fn.Example,
			"%q is an ordinary function and carries an example, which its name already is", fn.Name)
	}
}
