package lsp

import (
	"fmt"
	"strings"
	"testing"

	lsp "github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A macro's name is not the way it is written, and completion offered the name.
//
// cel-go identifies a macro by the segment after the dot, so `math.greatest(1, 2)`
// arrives as `greatest` and `[3, 1, 2].sortBy(v, v)` as `sortBy`.
// [v1.LibraryFunction] says exactly this and carries an example for it. Completion
// read neither: it split on a dot the name does not contain, put all twelve in the
// bare list, and made the insert text the name — so accepting one typed a spelling
// the validator refuses with `no function called "greatest"`, while `math.greatest`
// was offered nowhere.
//
// The general form of the mistake is worth naming, because this package has made it
// before: a value that is a *key* being printed as if it were *source*. The catalog
// already separated the two, and completion used the wrong one.

// TestEveryOfferedNameCompiles is the property, asserted against the validator
// rather than against a list kept here.
//
// A list would be a second place to update, and this one has already been wrong in
// exactly that way. Asking the checker means a library added to the profile is
// covered the day it arrives.
func TestEveryOfferedNameCompiles(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `edition: v2026.2
name: offered
steps:
  - id: only
    log:
      message: ${|}
`

	clean, pos := splitCursor(t, src)
	c.open("file:///offered.yaml", clean)

	for _, item := range c.complete("file:///offered.yaml", pos.Line, pos.Character).Items {
		// A namespace completes to `math.`, which is a prefix rather than an
		// expression; what may follow it is the next test.
		if item.Kind == lsp.CIKModule {
			continue
		}
		// References — the `steps` root, a loop's iterator — are names rather than
		// callables, and whether they resolve is what the scoping tests are for.
		if item.Kind != lsp.CIKFunction {
			continue
		}

		t.Run(item.Label, func(t *testing.T) {
			assertWritable(t, item.Label)
		})
	}
}

// TestANamespacesMembersCompile is the same question one keystroke later.
//
// This is where the twelve had to end up for the ones that have a namespace, so
// asserting the bare list is clean is only half of it: a fix that dropped them
// everywhere would pass the test above and leave `math.greatest` unreachable.
func TestANamespacesMembersCompile(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	for _, namespace := range namespacesOffered(t, c) {
		t.Run(namespace, func(t *testing.T) {
			for _, member := range functionsAfter(namespace) {
				t.Run(member.name, func(t *testing.T) {
					assertWritable(t, namespace+"."+member.name)
				})
			}
		})
	}
}

// TestTheNamespacedMacrosAreReachable pins the names the defect lost.
//
// The test above asks that whatever is offered compiles, which a fix offering
// nothing satisfies. These are the specific spellings that were offered nowhere
// while their unwritable form was offered at the top level.
func TestTheNamespacedMacrosAreReachable(t *testing.T) {
	t.Parallel()

	for _, want := range []struct{ namespace, member string }{
		{namespace: "math", member: "greatest"},
		{namespace: "math", member: "least"},
		{namespace: "cel", member: "bind"},
	} {
		t.Run(want.namespace+"."+want.member, func(t *testing.T) {
			t.Parallel()

			var offered []string
			for _, member := range functionsAfter(want.namespace) {
				offered = append(offered, member.name)
			}

			assert.Contains(t, offered, want.member,
				"`%s.%s` is accepted by the validator and offered nowhere", want.namespace, want.member)
		})
	}
}

// TestNoMacroIsOfferedInASpellingItCannotBeWrittenIn is the negative direction,
// stated over the catalog rather than over the completion list.
//
// The list is what an author sees; the catalog is where the mistake was. A macro
// written on a *value* has no namespace, and there is no completion surface for a
// dot on an expression — so it must appear in no list at all rather than in the
// bare one.
func TestNoMacroIsOfferedInASpellingItCannotBeWrittenIn(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `edition: v2026.2
name: no-bare-macros
steps:
  - id: only
    log:
      message: ${|}
`

	clean, pos := splitCursor(t, src)
	c.open("file:///no-bare-macros.yaml", clean)
	got := labels(c.complete("file:///no-bare-macros.yaml", pos.Line, pos.Character).Items)

	var macros int
	for _, fn := range v1.ProfileFunctions(v1.CurrentProfile) {
		if !fn.Macro {
			continue
		}
		macros++

		assert.NotContains(t, got, fn.Name,
			"the macro %q is offered bare; it is written %q, and accepting the offer types a "+
				"name the validator refuses", fn.Name, fn.Example)
	}

	require.NotZero(t, macros,
		"the profile declares no macros, so this asserts nothing; the catalog is probably empty")
}

// assertWritable checks that the given spelling names something the checker knows.
//
// The call it builds comes from the profile's own example for a macro, and is a
// one-argument placeholder otherwise. The difference is not cosmetic: a macro is
// dispatched by *arity* at parse time, so `cel.bind(0)` is not a macro call at all
// and falls through to a function lookup that fails — which would make this report
// the very defect it exists to detect, on a correct spelling.
//
// What is under test is whether the name resolves. An argument-type complaint is a
// pass; `no function called` is the failure, because that is what an author sees
// after accepting a completion for something that is not there.
func assertWritable(t *testing.T, spelling string) {
	t.Helper()

	call := spelling + "(0)"
	if fn, found := catalogEntry(spelling); found && fn.Macro {
		require.NotEmpty(t, fn.Example,
			"%q is a macro with no example, so it has no writable form and must not be offered",
			spelling)
		call = fn.Example
	}

	src := fmt.Sprintf(`edition: v2026.2
name: writable
steps:
  - id: only
    log:
      message: ${string(%s)}
`, call)

	diags, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err, "the generated fixture does not parse")

	for _, d := range diags {
		assert.NotContains(t, d.Message, "no function called",
			"completion offers %q and the validator does not know it: %s", spelling, d.Message)
	}
}

// catalogEntry finds the profile function an offered spelling refers to.
//
// A macro is looked up by the segment after the dot, because that is the only name
// the catalog has for it — the same asymmetry the fix above is about, met here from
// the reading side.
func catalogEntry(spelling string) (v1.LibraryFunction, bool) {
	_, member, qualified := strings.Cut(spelling, ".")

	for _, fn := range v1.ProfileFunctions(v1.CurrentProfile) {
		if fn.Name == spelling {
			return fn, true
		}
		if qualified && fn.Macro && fn.Name == member {
			return fn, true
		}
	}

	return v1.LibraryFunction{}, false
}

// namespacesOffered returns the qualifiers completion offers at the start of an
// expression.
func namespacesOffered(t *testing.T, c *client) []string {
	t.Helper()

	const src = `edition: v2026.2
name: namespaces
steps:
  - id: only
    log:
      message: ${|}
`

	clean, pos := splitCursor(t, src)
	c.open("file:///namespaces.yaml", clean)

	var out []string
	for _, item := range c.complete("file:///namespaces.yaml", pos.Line, pos.Character).Items {
		if item.Kind == lsp.CIKModule {
			out = append(out, strings.TrimSuffix(item.Label, "."))
		}
	}

	require.NotEmpty(t, out, "no namespaces are offered at all")

	return out
}
