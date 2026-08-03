package flowfile_test

import (
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// FuzzCELCompile fuzzes the path a Flowfile expression takes through cel-go's
// parser and type checker, by way of [flowfile.ValidateSource].
//
// [flowfile.FuzzRoundTrip] never reaches this: it exercises Unmarshal, and
// checkExpressionTypes only runs from Validate (validate.go), which
// ValidateSource is the entry point for — the same one `flow validate`, the
// language server, and every RPC that checks a source before compiling it
// call. So this is coverage FuzzRoundTrip does not have: an outside party's
// bytes reaching env.Parse and env.Check with the profile environment
// [envDeclaring] builds, across every kind of expression the language accepts
// — `if:`, `vars:`, task inputs, `for_each.items`, `wait_until`.
//
// The invariant is the one CLAUDE.md states for every parser here: no panic,
// no unbounded memory or time, and — since cel-go's own parser is the thing
// actually walking the expression text — no stack overflow on a deeply
// nested one. Measured before writing this (manually, against ValidateSource):
// cel-go's own parser refuses expressions nested a few hundred levels deep with
// an ordinary parse error rather than recursing further, so the deep-nesting
// seed below is exercising a bound that already holds rather than guessing at
// one that might not.
func FuzzCELCompile(f *testing.F) {
	for _, seed := range []string{
		// A file with nothing for the checker to catch.
		`edition: v2026.2
name: ok
steps:
- id: a
  if: ${true}
  vars:
    n: 1
  http:
    url: https://example.com
    headers:
      A: ${string(1)}
- id: b
  log:
    message: ${steps.a.body}
`,
		// The four shapes documented as what this catches: a function that does
		// not exist, an addition with no overload, a call with the wrong argument
		// type, and string() applied to a structure.
		`edition: v2026.2
name: nofunc
steps:
- id: a
  if: ${nosuchfunc(1)}
  log:
    message: hi
`,
		`edition: v2026.2
name: badadd
steps:
- id: a
  vars:
    x: ${1 + 'a'}
  log:
    message: ${x}
`,
		`edition: v2026.2
name: badsize
steps:
- id: a
  log:
    message: ${string(size(1))}
`,
		`edition: v2026.2
name: badstring
steps:
- id: a
  log:
    fields: "${ {'a': string({'b': 1})} }"
`,
		// A qualified function next to a variable of the same name, which is the
		// bug referencedNames documents: only a call target that resolves to a
		// real qualified function is skipped, everything else stays a variable.
		`edition: v2026.2
name: qualified
steps:
- id: a
  vars:
    json: loud
  log:
    message: ${json}
    fields: "${ {'m': math.greatest(1, 2)} }"
`,
		// Every position checkNodeExpressions walks: a loop's items, a branch
		// inside a parallel, and a wait's until.
		`edition: v2026.2
name: positions
steps:
- id: loop
  for_each:
    items: ${nosuch(1)}
    as: n
    steps:
    - id: body
      log:
        message: ${n}
- id: fan
  parallel:
  - steps:
    - id: waiter
      wait:
        until: ${nosuch(2)}
`,
		// Deep nesting in the expression text itself, up to and a little past
		// where cel-go's own parser refuses rather than recurses further. This is
		// the shape a document-level depth bound (maxDepth in parse.go) cannot
		// see, because the expression is one YAML scalar — the recursion is
		// inside the CEL source cel-go parses, not the YAML tree.
		"edition: v2026.2\nname: deep\nsteps:\n- id: a\n  if: ${" +
			strings.Repeat("(", 600) + "1" + strings.Repeat(")", 600) + "}\n  log:\n    message: hi\n",
		// The bare (unfenced) condition spelling alongside the fenced one, since
		// both reach the same checker.
		`edition: v2026.2
name: bare
steps:
- id: a
  if: nosuchfunc(1) == true
  log:
    message: hi
`,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// ValidateSource must never panic, hang, or blow the stack on any byte
		// sequence: it is what an editor calls on every keystroke and what
		// flowstate_validate calls on whatever an agent submits. Diagnostics,
		// including none, are always an acceptable answer; a crash is not.
		ds, err := flowfile.ValidateSource([]byte(input))

		// err is the parse's own reporting of a malformed document — expected
		// for most fuzzed input — while ds is nil exactly when parsing failed
		// far enough that Validate never ran. Neither is asserted on beyond
		// "did not panic": there is no oracle here for what a diagnostic should
		// say, only that producing one (or an error, or neither) must not cost
		// more than parsing a one-megabyte file should.
		_ = ds
		_ = err
	})
}
