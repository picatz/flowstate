package flowfile

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestScanInterpolation covers the scanner's own answers: where each fence
// begins and ends, what the escape produces, and what is refused.
//
// The cases that earn their place are the ones where a brace does not mean what
// it looks like. A naive count closes `${'}'}` on the quoted brace and leaves
// `'}` behind as text, and the value then *compiles* — as a different
// expression. That is the `flow fix` corruption shape one surface over: not a
// crash, a wrong answer that still validates.
func TestScanInterpolation(t *testing.T) {
	t.Parallel()

	// want is the segments as "lit:<text>" and "fence:<source>", which reads
	// closer to the value than a struct literal per segment does.
	for _, test := range []struct {
		name string
		in   string
		want []string
		err  string
	}{
		{name: "no fence at all", in: "plain text", want: []string{"lit:plain text"}},
		{name: "one whole fence", in: "${a.b}", want: []string{"fence:a.b"}},
		{
			name: "text before and after",
			in:   "cost is ${n} dollars",
			want: []string{"lit:cost is ", "fence:n", "lit: dollars"},
		},
		{
			name: "two fences with text between",
			in:   "${a} and ${b}",
			want: []string{"fence:a", "lit: and ", "fence:b"},
		},
		{
			name: "adjacent fences",
			in:   "${a}${b}",
			want: []string{"fence:a", "fence:b"},
		},
		{
			// A map literal's braces nest, so the fence closes at the last one.
			name: "a map literal inside a fence",
			in:   "x ${ {'k': 1} } y",
			want: []string{"lit:x ", "fence: {'k': 1} ", "lit: y"},
		},
		{
			// The case a brace count gets wrong, and gets wrong silently.
			name: "a brace inside a string literal",
			in:   "a ${'}'} b",
			want: []string{"lit:a ", "fence:'}'", "lit: b"},
		},
		{
			name: "a brace inside a raw string literal",
			in:   `a ${r'\}'} b`,
			want: []string{"lit:a ", `fence:r'\}'`, "lit: b"},
		},
		{
			name: "a brace inside a triple-quoted string literal",
			in:   `a ${'''}'''} b`,
			want: []string{"lit:a ", `fence:'''}'''`, "lit: b"},
		},
		{
			// A `b` before a quote is a bytes literal, not an identifier beside a
			// string, so the brace in it is text.
			name: "a brace inside a bytes literal",
			in:   "a ${size(b'}')} b",
			want: []string{"lit:a ", "fence:size(b'}')", "lit: b"},
		},
		{
			// Reachable in a block scalar, the one value shape holding a newline.
			name: "a brace inside a comment",
			in:   "a ${x // }\n} b",
			want: []string{"lit:a ", "fence:x // }\n", "lit: b"},
		},
		{
			name: "an escaped fence is literal text",
			in:   "write $${a} plainly",
			want: []string{"lit:write ${a} plainly"},
		},
		{
			name: "an escape beside a real fence",
			in:   "$${lit} and ${real}",
			want: []string{"lit:${lit} and ", "fence:real"},
		},
		{
			// The escape is spelled only in literal text, so a CEL string holding
			// the characters needs no second escape.
			name: "an escape sequence inside a fence is not an escape",
			in:   "${'$${'}",
			want: []string{"fence:'$${'"},
		},
		{
			// A lone `$$` is two dollars. Only `$${` is the escape, so a shell
			// variable or a price stays what it was.
			name: "a double dollar not opening a fence is literal",
			in:   "cost $$5 and $x",
			want: []string{"lit:cost $$5 and $x"},
		},
		{
			name: "a fence at the very start and another at the very end",
			in:   "${a} mid ${b}",
			want: []string{"fence:a", "lit: mid ", "fence:b"},
		},
		{
			name: "an unterminated fence among text",
			in:   "hello ${a and more",
			err:  "unterminated expression",
		},
		{
			// The count is the bound, and one over it is refused.
			name: "more fences than the bound allows",
			in:   strings.Repeat("${a}", maxFencesPerValue+1),
			err:  "more than 64",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			segs, err := scanInterpolation(test.in)
			if test.err != "" {
				if err == nil {
					t.Fatalf("scanInterpolation(%q) = %v, want an error containing %q", test.in, segs, test.err)
				}
				if !strings.Contains(err.Error(), test.err) {
					t.Fatalf("scanInterpolation(%q) error = %v, want it to contain %q", test.in, err, test.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("scanInterpolation(%q) error = %v", test.in, err)
			}

			var got []string
			for _, sg := range segs {
				kind := "lit:"
				if sg.fence {
					kind = "fence:"
				}
				got = append(got, kind+sg.text)

				// Every segment's offsets have to name the bytes it was cut
				// from, because a diagnostic's column and `flow fix`'s splice
				// are both computed from them. A segment whose text is right and
				// whose offsets are wrong is the wrong-position failure this
				// package treats as worse than no position at all.
				if sg.fence {
					if want := fenceOpen + sg.text + fenceClose; test.in[sg.open:sg.end] != want {
						t.Errorf("fence %q spans %q at [%d,%d), want %q",
							sg.text, test.in[sg.open:sg.end], sg.open, sg.end, want)
					}
					if test.in[sg.start:sg.start+len(sg.text)] != sg.text {
						t.Errorf("fence %q does not begin at offset %d", sg.text, sg.start)
					}
				}
			}
			if strings.Join(got, "|") != strings.Join(test.want, "|") {
				t.Errorf("scanInterpolation(%q) = %v, want %v", test.in, got, test.want)
			}
		})
	}
}

// TestMaxFencesPerValueIsTheBoundThatIsReached checks the bound both ways: one
// under it passes, one over it is refused.
//
// Asserting the bound is *reached* and not only that it is not exceeded, which
// CLAUDE.md names after the paging bug: a scanner that gave up after two fences
// would satisfy "no more than 64" perfectly and quietly refuse honest files.
func TestMaxFencesPerValueIsTheBoundThatIsReached(t *testing.T) {
	t.Parallel()

	// A literal count, not one derived from the constant. A test that built its
	// input from maxFencesPerValue would pass at every value of it, including a
	// value far below any honest message — which is the under-scanning half of
	// the bound, and the half that hides real files rather than costing anything.
	// Eight is comfortably above the busiest scalar in the corpus.
	const honest = 8
	segs, err := scanInterpolation(strings.Repeat("${a}", honest))
	if err != nil {
		t.Fatalf("a value with %d fences was refused, which no honest message should be: %v", honest, err)
	}
	if len(segs) != honest {
		t.Errorf("scanned %d fences, want %d", len(segs), honest)
	}
	if maxFencesPerValue < honest {
		t.Errorf("maxFencesPerValue is %d, below the %d an ordinary message may hold", maxFencesPerValue, honest)
	}

	at := strings.Repeat("${a}", maxFencesPerValue)
	if segs, err := scanInterpolation(at); err != nil {
		t.Fatalf("a value with exactly %d fences was refused: %v", maxFencesPerValue, err)
	} else if len(segs) != maxFencesPerValue {
		t.Errorf("scanned %d fences at the bound, want %d", len(segs), maxFencesPerValue)
	}

	if _, err := scanInterpolation(at + "${a}"); err == nil {
		t.Errorf("a value with %d fences was accepted, and the bound is %d",
			maxFencesPerValue+1, maxFencesPerValue)
	}
}

// FuzzScanAgreesWithSplitFence is the compatibility proof #413's implementation
// is asked to start with, run as a property rather than a table.
//
// The claim: wherever a value is a whole-value fence today *and* what is inside
// it compiles, the scanner answers with exactly that one fence and exactly that
// source. Every such value is a value some shipped file may contain, and every
// one of them has to keep meaning what it means — the typing, the secret
// placement, and the `flow fix` rewrites all hang off that one question being
// answered the same way it was before.
//
// The inverse is deliberately not claimed. A value the old rule refused is free
// to mean something now, which is the whole of the feature.
func FuzzScanAgreesWithSplitFence(f *testing.F) {
	for _, seed := range []string{
		"${a.b}",
		"${ {'k': 1} }",
		"${'}'}",
		"${r'\\}'}",
		"${'''}'''}",
		"${a} ${b}",
		"${a}${b}",
		"$${a}",
		"$$${a}",
		"hello ${a} and ${b}",
		"${a // }\n}",
		"plain",
		"}",
		"${",
		"${}",
		"${'never closed}",
		"a $ b } c",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, in string) {
		inner, whole := SplitFence(in)
		if !whole || v1.NewExpr(inner).Error() != nil {
			// Not a value the old rule accepted, so nothing is owed about it
			// beyond the scan not panicking, which reaching here proves.
			_, _ = scanInterpolation(in)

			return
		}

		segs, err := scanInterpolation(in)
		if err != nil {
			t.Fatalf("scanInterpolation(%q) refused a value the old rule accepted: %v", in, err)
		}

		got, ok := wholeValueFence(segs, in)
		if !ok {
			t.Fatalf("scanInterpolation(%q) = %v, want the one whole-value fence SplitFence found", in, segs)
		}
		if got != inner {
			t.Fatalf("scanInterpolation(%q) read the fence as %q, want %q", in, got, inner)
		}
	})
}

// FuzzEscapeFencesRoundTrips pins the other direction of the escape: any string
// at all can be written out and read back as itself.
//
// It is what makes Marshal total on literal strings, which it was not before
// #413 — a literal holding `${` could not be written at all. An escaper that
// dropped a fence, or one that escaped a `${` it had itself just produced, fails
// here rather than in somebody's round-tripped workflow.
func FuzzEscapeFencesRoundTrips(f *testing.F) {
	for _, seed := range []string{"", "plain", "${a}", "$${a}", "$$${a}", "$", "${", "a${b${c"} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, in string) {
		escaped := escapeFences(in)

		segs, err := scanInterpolation(escaped)
		if err != nil {
			t.Fatalf("escapeFences(%q) = %q, which does not scan: %v", in, escaped, err)
		}
		if hasFence(segs) {
			t.Fatalf("escapeFences(%q) = %q, which still holds an expression", in, escaped)
		}
		if got := literalText(segs); got != in {
			t.Fatalf("escapeFences(%q) read back as %q", in, got)
		}
	})
}
