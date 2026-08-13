package flowfile_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// interpolated compiles one `log:` message and returns the expression source the
// compiler built for it, failing the test if the file did not compile.
func interpolated(t *testing.T, message string) string {
	t.Helper()

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: ` + message + "\n"))
	if err != nil {
		t.Fatalf("compiling %s: %v", message, err)
	}

	value := wf.GetSteps()[0].GetTask().GetInputs()["message"]
	if value.GetExpr() == nil {
		t.Fatalf("%s compiled to a literal, not an expression: %v", message, value)
	}

	return exprString(t, value)
}

// exprString renders a compiled value's expression the way it would be written
// back out, which is how a test compares what the compiler built against a
// spelling a person can read.
func exprString(t *testing.T, value *v1.Value) string {
	t.Helper()

	text, err := cel.AstToString(cel.ParsedExprToAst(value.GetExpr()))
	if err != nil {
		t.Fatalf("writing the expression back: %v", err)
	}

	return text
}

// TestInterpolationDesugarsToOneExpression pins what a mixed scalar compiles to.
//
// The desugaring is the whole design: one CEL expression, evaluated under one
// cost budget by two drivers that know nothing about interpolation. Asserting
// the *source* rather than the result is what makes that checkable — a test that
// only ran the workflow would pass just as well on a driver-side implementation,
// which is the thing this deliberately is not.
func TestInterpolationDesugarsToOneExpression(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "text before an expression",
			message: `"cost is ${1 + 2}"`,
			want:    `"cost is " + string(1 + 2)`,
		},
		{
			name:    "text on both sides",
			message: `"cost is ${1} dollars"`,
			want:    `"cost is " + string(1) + " dollars"`,
		},
		{
			name:    "two expressions",
			message: `"${1} and ${2}"`,
			want:    `string(1) + " and " + string(2)`,
		},
		{
			name:    "adjacent expressions with no text between",
			message: `"${1}${2}"`,
			want:    `string(1) + string(2)`,
		},
		{
			// The escape produces text, and text alone is a literal, so this one
			// is not an expression at all — which is why it is checked in
			// TestEscapedFenceIsLiteralText rather than here.
			name:    "an escape beside a real fence",
			message: `"$${not} but ${1}"`,
			want:    `"${not} but " + string(1)`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := interpolated(t, test.message); got != test.want {
				t.Errorf("%s compiled to\n  %s\nwant\n  %s", test.message, got, test.want)
			}
		})
	}
}

// TestInterpolationDesugarsToTheSharedDriverCase keeps [tests.InterpolationSource]
// honest.
//
// That constant is the expression both drivers are held to, and it is written
// out by hand in a package that cannot compile a Flowfile. Nothing but this test
// connects it to the scalar it claims to be the compilation of, so without it
// the shared cases could go on agreeing perfectly about an expression the
// compiler had stopped producing.
func TestInterpolationDesugarsToTheSharedDriverCase(t *testing.T) {
	t.Parallel()

	const scalar = `"kinds: s=${inputs.s} i=${inputs.i} u=${uint(7)} d=${2.5} ` +
		`b=${true} dur=${duration('90s')} t=${timestamp('2026-08-12T00:00:00Z')}"`

	// Compared after a round trip through the CEL writer on both sides, since
	// the constant is written in the spelling an author would use and the
	// compiler stores the normalized one. What must match is the expression,
	// not the quoting.
	want := exprString(t, v1.NewExpr(tests.InterpolationSource))

	if got := interpolatedInputs(t, scalar); got != want {
		t.Errorf("the compiler now produces\n  %s\nbut tests.InterpolationSource is\n  %s\n"+
			"one of the two moved; the shared driver cases are only meaningful while they agree", got, want)
	}
}

// interpolatedInputs is [interpolated] for a workflow that declares the inputs
// the scalar references.
func interpolatedInputs(t *testing.T, message string) string {
	t.Helper()

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
inputs:
  s:
    type: string
    required: true
  i:
    type: int
    required: true
steps:
  - id: a
    log:
      message: ` + message + "\n"))
	if err != nil {
		t.Fatalf("compiling %s: %v", message, err)
	}

	return exprString(t, wf.GetSteps()[0].GetTask().GetInputs()["message"])
}

// TestWholeValueFenceKeepsItsType is the boundary the whole design rests on.
//
// `${0}` is the integer zero and `${0} ` is the string "0 ". One syntactic test
// decides which, so the sharp edge is a consequence rather than a special case —
// and, more to the point, every value in every shipped file is on the side of it
// it was always on.
func TestWholeValueFenceKeepsItsType(t *testing.T) {
	t.Parallel()

	whole, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
vars:
  n: ${0}
  t: "${0} "
steps:
  - id: a
    log:
      message: one
`))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	if got := whole.GetVars()["n"]; got.GetExpr() == nil {
		t.Errorf("a whole-value fence stopped being an expression: %v", got)
	}

	// The interpolated one is an expression too, but of a different kind: it
	// builds text. Running the pair is what tells them apart, and that is what
	// the shared driver cases do; here the claim is that the compiler treated
	// them differently at all.
	text := whole.GetVars()["t"]
	source := exprString(t, text)
	if !strings.Contains(source, "string(") {
		t.Errorf("an interpolated value compiled to %q, which does not stringify anything", source)
	}
}

// TestEscapedFenceIsLiteralText covers `$${`, the spelling of a literal `${`.
//
// It has no migration burden — a value holding a literal `${` was refused before
// #413, so no shipped file contains one — which is exactly why it needs a test:
// nothing in the corpus would notice if it stopped working.
func TestEscapedFenceIsLiteralText(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: "write $${a.b} plainly"
`))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	got := wf.GetSteps()[0].GetTask().GetInputs()["message"]
	if got.GetExpr() != nil {
		t.Fatalf("an escaped fence compiled to an expression: %v", got)
	}
	if want := "write ${a.b} plainly"; got.GetLiteral().GetStringValue() != want {
		t.Errorf("escaped fence read as %q, want %q", got.GetLiteral().GetStringValue(), want)
	}
}

// TestSecretInMixedPositionIsRefused is the containment this feature could
// quietly have broken.
//
// A `${secret(...)}` is compiled to a reference the worker resolves, never to
// something the workflow evaluates. Interpolation desugars every fence into a
// `string(...)` call inside one expression — so a secret left to that path would
// be *evaluated*, in workflow code, and its value would land in history. It has
// to be refused instead, and it has to be refused wherever it is written: as the
// whole of a mixed scalar's one fence, and as any fence among several.
func TestSecretInMixedPositionIsRefused(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		message string
	}{
		{name: "the only fence, beside text", message: `"Bearer ${secret('env:T')}"`},
		{name: "the first of several", message: `"${secret('env:T')} and ${1}"`},
		{name: "the last of several", message: `"${1} and ${secret('env:T')}"`},
		{name: "buried in a larger expression", message: `"x ${'B ' + secret('env:T')}"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
      headers:
        Authorization: ` + test.message + "\n"))
			if err == nil {
				t.Fatal("a secret reference in an interpolated value compiled, so the workflow would evaluate it")
			}

			// The sentence as well as the refusal. Two other rules also end in a
			// refusal here — "an expression cannot share a structure with a
			// reference", and the last-resort check over the built expression —
			// and either would leave this passing while telling the author to do
			// something that is not the fix. Naming the one that belongs is what
			// makes the walk over every fence load-bearing rather than
			// redundant.
			if !strings.Contains(err.Error(), "has to be the whole value of a task input") {
				t.Errorf("refused with the wrong sentence: %v", err)
			}
			t.Logf("reported: %v", err)
		})
	}
}

// TestSensitiveInputInMixedPositionIsReported covers the lint trap #413's own
// design pass named.
//
// The sensitive-in-log lint refuses a value that reaches the log verbatim and
// deliberately leaves anything wrapped in a call alone. Interpolation wraps every
// fence in `string()`, so without care the plainest spelling of the leak —
// `message: token is ${inputs.token}` — would compile to something the lint reads
// as derived, and the lint would go quiet exactly where the new syntax made the
// mistake easiest to make.
//
// The second case is the one worth writing out: the sensitive reference is not
// the first fence. A check that looked only at the head of the `+` chain would
// pass it.
func TestSensitiveInputInMixedPositionIsReported(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		message string
		want    bool
	}{
		{name: "the only fence, beside text", message: `"token is ${inputs.token}"`, want: true},
		{name: "the second of several", message: `"user ${inputs.who} token ${inputs.token}"`, want: true},
		{
			// The boundary the lint draws, unmoved: a value the author derived
			// on purpose is not the value, and reporting it would teach an
			// author the lint is wrong.
			name:    "a derived fact about it is not the value",
			message: `"token supplied ${inputs.token != \"\"}"`,
			want:    false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: t
inputs:
  who:
    type: string
    required: true
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: a
    log:
      message: ` + test.message + "\n"))
			if err != nil {
				t.Fatalf("ValidateSource() error: %v", err)
			}

			var found bool
			for _, d := range ds {
				if d.Code == v1.DiagnosticCodeSensitiveInLog {
					found = true
				}
			}
			if found != test.want {
				t.Errorf("sensitive-in-log reported = %v, want %v; diagnostics:\n%s", found, test.want, ds)
			}
		})
	}
}

// TestFixRootsEveryFenceInAnInterpolatedValue is the `flow fix` half of #413,
// and it is asserted on bytes.
//
// `flow fix` rewrites a pre-v2026.2 bare step reference into its `steps.`-rooted
// spelling. It found those by asking the whole-value question, so a scalar that
// interpolates would have had its references left bare while the edition was
// stamped onto the file — the exact failure CLAUDE.md records from the last
// time: `flow fix` exiting zero on a file the validator then rejects.
//
// Compared byte for byte rather than by re-validating the output, which is what
// CLAUDE.md says let both previous `flow fix` corruptions through: a file that
// still validates is not the same claim as a file that says what it said. The
// two identical fences in the third case are the reason the splice walks a
// cursor instead of searching — a search from the value's start would rewrite
// the first one twice and never reach the second.
func TestFixRootsEveryFenceInAnInterpolatedValue(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "one fence beside text",
			src:  "name: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: hello ${who.result}\n",
			want: "edition: v2026.3\nname: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: hello ${steps.who.result}\n",
		},
		{
			name: "two different fences",
			src:  "name: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: what\n    log:\n      message: hi\n  - id: b\n    log:\n      message: ${who.result} did ${what.result}\n",
			want: "edition: v2026.3\nname: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: what\n    log:\n      message: hi\n  - id: b\n    log:\n      message: ${steps.who.result} did ${steps.what.result}\n",
		},
		{
			name: "the same fence written twice",
			src:  "name: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: ${who.result} and ${who.result}\n",
			want: "edition: v2026.3\nname: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: ${steps.who.result} and ${steps.who.result}\n",
		},
		{
			// The escape holds the bytes of a fence and is not one, which is the
			// third way this rewriter can know less than the language does. The
			// scanner reports one fence here and the rewriter used to find its
			// text by searching the line, so it rewrote the *escaped* copy: `flow
			// fix` exiting zero having silently changed what the literal half of
			// the value prints, on a file that was valid before and after. That
			// is the corruption CLAUDE.md records, and the reason this table
			// compares bytes rather than asking whether the output still
			// validates — it always did.
			name: "an escaped lookalike before the fence it duplicates",
			src:  "name: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: \"$${who.result} and ${who.result}\"\n",
			want: "edition: v2026.3\nname: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: \"$${who.result} and ${steps.who.result}\"\n",
		},
		{
			// The mirror image, so the fix cannot be "skip the first match": here
			// the real fence comes first and the escape second.
			name: "an escaped lookalike after the fence it duplicates",
			src:  "name: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: \"${who.result} and $${who.result}\"\n",
			want: "edition: v2026.3\nname: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: \"${steps.who.result} and $${who.result}\"\n",
		},
		{
			// A value that is nothing but escapes has no fence at all, so the
			// rewriter has nothing to do and must leave every byte alone.
			name: "escapes only",
			src:  "name: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: \"$${who.result}$${who.result}\"\n",
			want: "edition: v2026.3\nname: t\nsteps:\n  - id: who\n    log:\n      message: hi\n  - id: b\n    log:\n      message: \"$${who.result}$${who.result}\"\n",
		},
		{
			// The names the grammar binds are not step references, wherever they
			// are written — and interpolation is a new "wherever". A loop's
			// `item` beside a step of the same id is deliberately legal, and
			// rewriting it into a reference to that step is the corruption class
			// CLAUDE.md records twice.
			name: "a loop binding among text is not a step reference",
			src:  "name: t\nsteps:\n  - id: item\n    log:\n      message: a step sharing the binding's name\n  - id: loop\n    for_each:\n      items: ${['a', 'b']}\n      steps:\n        - id: use\n          log:\n            message: saw ${item} here\n",
			want: "edition: v2026.3\nname: t\nsteps:\n  - id: item\n    log:\n      message: a step sharing the binding's name\n  - id: loop\n    for_each:\n      items: ${['a', 'b']}\n      steps:\n        - id: use\n          log:\n            message: saw ${item} here\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(test.src))
			if err != nil {
				t.Fatalf("Fix() error: %v", err)
			}
			if len(result.Refusals) != 0 {
				t.Fatalf("Fix() refused: %v", result.Refusals)
			}
			if got := string(result.Source); got != test.want {
				t.Errorf("Fix() wrote\n%q\nwant\n%q", got, test.want)
			}

			// Compiling the output as well, which is the half that catches a
			// rewrite subtracting a name too widely: a reference left bare under
			// a stamped edition can satisfy a byte comparison written to the
			// wrong expectation, and cannot satisfy this.
			if _, err := flowfile.Unmarshal(result.Source); err != nil {
				t.Errorf("Fix() produced a file that does not compile: %v", err)
			}
		})
	}
}

// TestFenceDiagnosticSkipsAnEscapedLookalike pins the position a CEL error in a
// multi-fence value is reported at, when an earlier escape holds the same bytes.
//
// The span used to be found by searching the scalar for `${` + the fence's own
// source + `}`, which finds the escaped copy first: the diagnostic named a
// column inside literal text the author wrote on purpose, and said the CEL
// parser had failed there. A diagnostic pointing at the wrong span is the false
// diagnostic this repository ranks as worse than a missing one, so the column is
// asserted here rather than only the message.
func TestFenceDiagnosticSkipsAnEscapedLookalike(t *testing.T) {
	t.Parallel()

	// `    value: "$${ ] } then ${ ] }"` — the escaped `]` is at column 17 and
	// the real fence's at 29. Both are broken CEL; only the second is CEL at all.
	const src = `edition: v2026.3
name: t
steps:
  - id: say
    value: "$${ ] } then ${ ] }"
`

	// The compile diagnostics come back as the error rather than in the list,
	// because a value that does not compile stops the parse.
	_, err := flowfile.ValidateSource([]byte(src))
	if err == nil {
		t.Fatal("ValidateSource() accepted a value whose fence is broken CEL")
	}

	var diags flowfile.Diagnostics
	if !errors.As(err, &diags) {
		t.Fatalf("ValidateSource() error is not a Diagnostics: %#v", err)
	}
	if len(diags) != 1 {
		t.Fatalf("ValidateSource() gave %d diagnostics, want 1: %v", len(diags), diags)
	}
	if got := diags[0].Column; got != 29 {
		t.Errorf("the CEL error is reported at column %d, want 29 — column 17 is the `]` inside the $${ escape: %v",
			got, err)
	}
}

// TestFormattingNeverInvalidatesAnEscapedTextField is `flow fmt`'s promise, on
// the fields that are read when the workflow is compiled.
//
// [flowfile.compiler.text] resolves `$${` to a literal `${`, so
// `description: show $${TOKEN}` is held as `show ${TOKEN}`. Writing those bytes
// back unescaped produces a real fence in a position that cannot hold one, and
// the next compile refuses the file the formatter just wrote. Bytes are compared
// rather than validity alone, because the first half of that failure is a
// document that still parses and means something else.
func TestFormattingNeverInvalidatesAnEscapedTextField(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: t
description: show $${TOKEN} to interpolate
steps:
  - id: a
    description: and $${here} too
    log:
      message: hi
`

	wf, err := flowfile.Unmarshal([]byte(src))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	if want := "show ${TOKEN} to interpolate"; wf.GetDescription() != want {
		t.Fatalf("description read as %q, want %q", wf.GetDescription(), want)
	}

	written, err := flowfile.Format([]byte(src), wf)
	if err != nil {
		t.Fatalf("Format() error: %v", err)
	}

	if !strings.Contains(string(written), "description: show $${TOKEN} to interpolate") {
		t.Errorf("Format() wrote the description unescaped:\n%s", written)
	}

	// The half that says the round trip is a round trip rather than merely legal.
	again, err := flowfile.Unmarshal(written)
	if err != nil {
		t.Fatalf("Format() produced a file that does not compile: %v\n%s", err, written)
	}
	if again.GetDescription() != wf.GetDescription() {
		t.Errorf("description became %q across a format, was %q", again.GetDescription(), wf.GetDescription())
	}
	if again.GetSteps()[0].GetDescription() != wf.GetSteps()[0].GetDescription() {
		t.Errorf("step description became %q, was %q",
			again.GetSteps()[0].GetDescription(), wf.GetSteps()[0].GetDescription())
	}
}

// TestOpenFenceIgnoresAnEscape is the editor half of the same rule.
//
// A completion request carries the text before the cursor, so the question is
// which fence — if any — the cursor is inside. Searching back for the last `${`
// finds the escape's, and the editor then offers step and input completions in
// the middle of literal text.
func TestOpenFenceIgnoresAnEscape(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		before string
		want   string
		open   bool
	}{
		{name: "inside a real fence", before: "hello ${inputs.ver", want: "inputs.ver", open: true},
		{name: "inside an escape", before: "write $${inputs.ver", open: false},
		{name: "after a closed fence", before: "${inputs.a} then ", open: false},
		{name: "a real fence after an escape", before: "$${lit} and ${inputs.v", want: "inputs.v", open: true},
		{name: "an escape after a closed fence", before: "${inputs.a} $${lit", open: false},
		{name: "no fence at all", before: "just words", open: false},
		{
			// A brace inside a CEL map does not close the fence, which the old
			// search for any `}` got wrong in the other direction.
			name:   "a map literal is not a close",
			before: "${ {'k': 1} , inputs.v",
			want:   " {'k': 1} , inputs.v",
			open:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, open := flowfile.OpenFence(test.before)
			if open != test.open {
				t.Fatalf("OpenFence(%q) open = %v, want %v", test.before, open, test.open)
			}
			if open && got != test.want {
				t.Errorf("OpenFence(%q) = %q, want %q", test.before, got, test.want)
			}
		})
	}
}
