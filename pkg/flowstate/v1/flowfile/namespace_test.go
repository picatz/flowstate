package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A profile's namespaced functions were reported as unknown names, everywhere.
//
// cel-go parses `regex.replace(s, a, b)` as a select over the identifier `regex`, so the
// qualifier reaches the reference walk looking exactly like a name nobody bound. Every
// use of one was refused — in a step input, in an `if:`, in a `vars:` value — with a
// diagnostic naming a step the author never wrote.
//
// The functions are documented, `flow tasks` prints them, and `flow validate` refused
// them. That is the failure mode a diagnostic can least afford: a tool that is wrong
// about working files is one people learn to run with their eyes closed.

// TestAProfileFunctionIsNotAnUnknownName covers every position an expression can be
// written in, because the check they share had no idea about namespaces and each of
// them reached it by a different path.
func TestAProfileFunctionIsNotAnUnknownName(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
	}{
		{
			name: "a task input",
			src: `edition: v2026.4
name: t
steps:
  - id: s
    log:
      message: ${regex.replace("ab", "a", "c")}
`,
		},
		{
			name: "a workflow var",
			src: `edition: v2026.4
name: t
vars:
  shouted: ${regex.replace("ab", "a", "c")}
steps:
  - id: s
    log:
      message: ${vars.shouted}
`,
		},
		{
			name: "a step's own var",
			src: `edition: v2026.4
name: t
steps:
  - id: s
    vars:
      biggest: ${math.greatest(1, 2)}
    log:
      message: ${string(biggest)}
`,
		},
		{
			name: "a condition",
			src: `edition: v2026.4
name: t
steps:
  - id: s
    if: ${math.greatest(1, 2) == 2}
    log:
      message: hi
`,
		},
		{
			name: "a loop's items expression",
			src: `edition: v2026.4
name: t
steps:
  - id: each
    for_each:
      items: ${lists.range(3)}
      as: n
      steps:
        - id: inner
          log:
            message: ${string(n)}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Empty(t, diagnose(t, test.src),
				"a documented profile function was reported as an unknown name")
		})
	}
}

// TestAQualifierIsExemptAndALibraryNameIsNot is the distinction the first version of
// this got wrong in both directions at once.
//
// The exempt set was read from `v1.ExtensionLibraries()`, which are *registration* names
// rather than the qualifiers their functions hang from. They coincide often enough to
// look right — `regex`, `math`, `sets` — and then do not: `encoders` declares
// `base64.encode`, `protos` declares `proto.getExt`, `bindings` declares `cel.bind`.
//
// So a valid `${base64.encode(b)}` was still refused, and `${string(encoders)}` — a name
// that means nothing anywhere — was quietly accepted. Both are asserted here, because a
// set derived from the wrong thing passes a test that only checks one direction.
func TestAQualifierIsExemptAndALibraryNameIsNot(t *testing.T) {
	t.Parallel()

	using := func(expr string) string {
		return "edition: v2026.4\nname: t\nsteps:\n  - id: s\n    log:\n      message: ${" +
			expr + "}\n"
	}

	require.Empty(t, diagnose(t, using(`base64.encode(b"hi")`)),
		"a real qualifier from the encoders library was reported as an unknown name")

	require.Contains(t, diagnose(t, using("string(encoders)")), `references unknown name "encoders"`,
		"the library's registration name is not a qualifier and must not be exempt")
}

// TestTheExemptSetComesFromTheProfile keeps the derivation honest.
//
// What makes this correct rather than lucky is that the qualifiers are read off the
// environment's own declarations — the part of a declared function's name before its
// last dot — rather than from a list this package maintains. A library added to a
// profile is then covered the day it is added, and one whose qualifier differs from its
// name cannot be wrong again.
func TestTheExemptSetComesFromTheProfile(t *testing.T) {
	t.Parallel()

	env, err := v1.DefaultEvaluator().ProfileEnv(v1.CurrentProfile)
	require.NoError(t, err)

	qualifiers := map[string]bool{}
	for name := range env.Functions() {
		if at := strings.LastIndex(name, "."); at > 0 {
			qualifiers[name[:at]] = true
		}
	}
	require.NotEmpty(t, qualifiers, "the profile declares no namespaced functions, so this checks nothing")

	for qualifier := range qualifiers {
		src := "edition: v2026.4\nname: t\nsteps:\n  - id: s\n    log:\n      message: ${string(" +
			qualifier + ")}\n"

		// Whether the expression type-checks is CEL's business — `string(base64)` may
		// well not. What is pinned is that the reference walk does not call a
		// qualifier the profile declares an unknown step.
		require.NotContains(t, diagnose(t, src), "references unknown name",
			"the profile declares functions under %q and the validator calls it an unknown name", qualifier)
	}
}

// TestAnUnknownNameIsStillUnknown is the negative direction.
//
// An exemption is only worth having if it exempts exactly what it names. A bare word
// that is not a library, not a binding and not a root is still a mistake, and still gets
// the diagnostic that explains what a bare name can be.
func TestAnUnknownNameIsStillUnknown(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.4
name: t
steps:
  - id: s
    log:
      message: ${nonsense.thing()}
`

	require.Contains(t, diagnose(t, src), `references unknown name "nonsense"`,
		"exempting the profile's namespaces also exempted everything else")
}

// TestANamespaceIsRefusedAsAValue closes the other direction of the exemption: the
// qualifier of a call is not a reference, but the same word written as a value is
// a name that resolves to nothing, and used to fail only at run time.
func TestANamespaceIsRefusedAsAValue(t *testing.T) {
	t.Parallel()

	using := func(expr string) string {
		return "edition: v2026.4\nname: t\nsteps:\n  - id: s\n    log:\n      message: ${" +
			expr + "}\n"
	}

	require.Contains(t, diagnose(t, using("string(math)")),
		"`math` is a function namespace, not a value; call one of its functions, as `math.<function>(...)`")

	// Valid uses of the same word stay valid: as a call qualifier, nested, and
	// as an iterator a comprehension binds.
	for _, expr := range []string{
		"string(math.greatest(1, 2))",
		"string(math.greatest(math.least(1, 2), 0))",
		`[1, 2].map(math, string(math))[0]`,
	} {
		require.NotContains(t, diagnose(t, using(expr)), "function namespace", expr)
	}
}

// TestAWebhookRefusesAnUnknownEventField pins the closed shape of `event` and
// its negative direction: headers and body, however they are selected, pass.
func TestAWebhookRefusesAnUnknownEventField(t *testing.T) {
	t.Parallel()

	using := func(with string) string {
		return `edition: v2026.4
name: t
inputs:
  order_id: { type: string, required: true }
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.body.id}
    with:
      order_id: ` + with + `
steps:
  - id: s
    log:
      message: ${inputs.order_id}
`
	}

	require.Contains(t, diagnose(t, using("${event.nonsense}")),
		"references unknown field \"nonsense\" of `event`; `event` has headers, body")
	require.Contains(t, diagnose(t, using("${event.bdoy.id}")),
		"references unknown field \"bdoy\" of `event`; did you mean \"body\"?")
	require.Contains(t, diagnose(t, using("${string(math)}")),
		"`math` is a function namespace, not a value")

	for _, with := range []string{
		"${event.body.order_id}",
		`${event.headers["x"]}`,
		"${has(event.body.order_id) ? event.body.order_id : ''}",
		"${math.greatest(1, 2) > 1 ? 'a' : 'b'}",
	} {
		got := diagnose(t, using(with))
		require.NotContains(t, got, "of `event`", with)
		require.NotContains(t, got, "function namespace", with)
	}
}

// TestANamespaceValueIsRefusedInVarsAndConcurrency covers the two positions that
// used to exempt every namespace themselves, plus the valid call in each, and a
// qualifier in front of a function it does not declare.
func TestANamespaceValueIsRefusedInVarsAndConcurrency(t *testing.T) {
	t.Parallel()

	vars := func(expr string) string {
		return "edition: v2026.4\nname: t\nvars:\n  x: ${" + expr +
			"}\nsteps:\n  - id: s\n    log:\n      message: ${vars.x}\n"
	}
	key := func(expr string) string {
		return "edition: v2026.4\nname: t\nconcurrency:\n  key: ${" + expr +
			"}\n  on_conflict: reject\nsteps:\n  - id: s\n    log:\n      message: hi\n"
	}
	want := "`math` is a function namespace, not a value; call one of its functions, as `math.<function>(...)`"

	for name, src := range map[string]func(string) string{"vars": vars, "concurrency key": key} {
		require.Contains(t, diagnose(t, src("string(math)")), want, name)
		require.Contains(t, diagnose(t, src("string(math.size())")), want, name+": math declares no size")
		require.NotContains(t, diagnose(t, src("string(math.greatest(1, 2))")), "function namespace", name)
	}
}
