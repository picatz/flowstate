package secrets

import (
	"math/rand/v2"
	"strings"
	"testing"
)

// The scan in [Scrubber.ScrubWith] skips over text no needle can begin in, and
// copies what it keeps one span at a time rather than one byte at a time. Both
// are changes to how the answer is reached and to nothing about the answer,
// which is the kind of claim that is only worth as much as the check behind it.
//
// So the check is differential: the algorithm the scan replaced is kept here,
// verbatim, and the two are run against the same inputs. A divergence is a
// leak or a mangled diagnostic, and neither announces itself in a fixed case
// chosen by whoever wrote the faster loop.

// scrubBytewise is the byte-at-a-time scan this package used before the
// candidate index, kept as the reference the current one must agree with. It
// inspects every byte, writes every byte it keeps on its own, and charges the
// comparison budget exactly where the current scan does.
//
// It builds its own index from the needles rather than reading the one under
// test. Sharing [indexNeedles] would make the two sides agree about a needle
// that was filed under the wrong byte or not filed at all, which is the one
// way a redaction can go missing that a differential check should be able to
// see.
func scrubBytewise(needles []string, text, replacement string) string {
	var byFirst [256][]string
	for _, needle := range needles {
		byFirst[needle[0]] = append(byFirst[needle[0]], needle)
	}

	var out strings.Builder
	compareBytes := 0
	for i := 0; i < len(text); {
		matched := ""
		for _, needle := range byFirst[text[i]] {
			if len(needle) > maxScrubCompareBytes-compareBytes {
				return Redacted
			}
			compareBytes += len(needle)
			if strings.HasPrefix(text[i:], needle) {
				matched = needle
				break
			}
		}
		if matched != "" {
			out.WriteString(replacement)
			i += len(matched)
			continue
		}
		out.WriteByte(text[i])
		i++
	}

	return out.String()
}

// TestTheCandidateScanAgreesWithTheByteWiseOne runs both scans over a small
// alphabet, where needles collide, nest, and abut far more often than they do
// in real text. A four-letter alphabet is the point: it makes a candidate byte
// that begins no match — the case the skip has to get right — the common
// outcome rather than a rare one.
func TestTheCandidateScanAgreesWithTheByteWiseOne(t *testing.T) {
	t.Parallel()

	// The second alphabet carries bytes above 0x7f, where a needle's first byte
	// is a UTF-8 continuation or lead byte rather than a letter. Nothing in the
	// scan reads runes, so this is the same question asked of the half of the
	// byte space the first alphabet never reaches.
	alphabets := map[string]string{
		"four letters":      "abcd",
		"bytes above ASCII": "ab\xc3\xa9\x80",
	}

	for name, alphabet := range alphabets {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(20260913, 1))
			word := func(maxLen int) string {
				b := make([]byte, 1+rng.IntN(maxLen))
				for i := range b {
					b[i] = alphabet[rng.IntN(len(alphabet))]
				}
				return string(b)
			}

			for run := range 4000 {
				scrubber := &Scrubber{}
				for range 1 + rng.IntN(3) {
					scrubber.AddValue(word(5))
				}

				text := word(1 + rng.IntN(120))
				want := scrubBytewise(scrubber.state().needles, text, Redacted)
				if got := scrubber.Scrub(text); got != want {
					t.Fatalf("run %d: Scrub(%q) = %q, the byte-wise scan gives %q", run, text, got, want)
				}
			}
		})
	}
}

// TestACandidateByteThatBeginsNoMatchDoesNotHideOneAfterIt is the specific way
// a skipping scan goes wrong: it finds a byte that starts a needle, fails to
// complete the match, and resumes past the bytes it already looked at. A match
// beginning inside the failed attempt is then never tried.
func TestACandidateByteThatBeginsNoMatchDoesNotHideOneAfterIt(t *testing.T) {
	t.Parallel()

	scrubber := &Scrubber{}
	scrubber.AddValue("abcd")

	tests := []struct {
		name string
		text string
		want string
	}{
		{
			name: "a failed attempt overlapping a real one",
			text: "abcabcd",
			want: "abc" + Redacted,
		},
		{
			name: "a run of starts before the match",
			text: "aaaabcd",
			want: "aaa" + Redacted,
		},
		{
			name: "back to back matches",
			text: "abcdabcd",
			want: Redacted + Redacted,
		},
		{
			name: "a match at each end with clear text between",
			text: "abcd....abcd",
			want: Redacted + "...." + Redacted,
		},
		{
			name: "a truncated needle at the very end",
			text: "....abc",
			want: "....abc",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := scrubber.Scrub(test.text); got != test.want {
				t.Errorf("Scrub(%q) = %q, want %q", test.text, got, test.want)
			}
		})
	}
}

// TestTextWithNothingToRedactCostsNoAllocation states the property the whole
// scan is arranged around. A reply that carries no registered value is the
// overwhelmingly common one, and it should now cost a read of the text and
// nothing else.
//
// The text is chosen to be the hard case rather than the easy one: it is full
// of bytes that begin one of the registered encodings — `t` from the value, `d`
// from its base64, `7` from its hex — so the scan stops at candidate after
// candidate and completes none of them. Text with no candidate byte at all
// would pass this without the builder ever having been the thing under test.
//
// Not parallel: [testing.AllocsPerRun] panics when it is, because a count taken
// while other tests are allocating is not a count of this one.
func TestTextWithNothingToRedactCostsNoAllocation(t *testing.T) {
	scrubber := &Scrubber{}
	scrubber.AddValue("tok-live-9f8e7d6c")

	const clean = `{"status":"ok","detail":"the upstream did not do that","id":"7f3a"}`
	if got := scrubber.Scrub(clean); got != clean {
		t.Fatalf("Scrub(%q) = %q, want it unchanged", clean, got)
	}

	// Asked of the index rather than of a hand-derived list of bytes: a list
	// spelled out here keeps passing after the registered value changes, while
	// no longer describing that value's candidates — which is the vacuity this
	// check exists to prevent.
	state := scrubber.state()
	candidates := 0
	for i := range len(clean) {
		if len(state.byFirst[clean[i]]) != 0 {
			candidates++
		}
	}
	if candidates == 0 {
		t.Fatal("no byte of the text begins a registered encoding, so a scan that never " +
			"stops would pass this without the builder ever having been under test")
	}

	if allocs := testing.AllocsPerRun(100, func() { scrubber.Scrub(clean) }); allocs != 0 {
		t.Errorf("Scrub allocated %v times on text with nothing to redact, want 0", allocs)
	}
}

// shrinkSink keeps the result of a [shrinkToFit] call reachable.
//
// Without it the compiler sees the value discarded and deletes the copy, and
// `testing.AllocsPerRun` reports zero allocations for both branches — a test
// that passes whatever shrinkToFit does. The first draft of the test below did
// exactly that.
var shrinkSink string

// TestAHeavilyRedactedResultDoesNotRetainTheBufferItWasBuiltIn pins the bound
// on what a scrubbed string keeps alive.
//
// The builder is sized for the text, because the common shape answers at about
// that length. A body that is mostly one registered value answers in a few
// bytes instead, and [strings.Builder] hands out its backing array — so
// without the copy, ten bytes of answer would hold a megabyte of buffer for as
// long as anything referenced them.
//
// Asked of [shrinkToFit] rather than of a whole scrub, because the property is
// "a fresh string sized for the answer" and an allocation is what that looks
// like from outside. Not parallel, for the reason `AllocsPerRun` is never
// parallel.
func TestAHeavilyRedactedResultDoesNotRetainTheBufferItWasBuiltIn(t *testing.T) {
	tests := []struct {
		name     string
		result   string
		capacity int
		copies   float64
	}{
		{
			name:     "a short answer built in a buffer sized for a megabyte",
			result:   Redacted,
			capacity: 1 << 20,
			copies:   1,
		},
		{
			name:     "an answer nearly as long as its buffer",
			result:   strings.Repeat("x", 1<<20),
			capacity: 1 << 20,
			copies:   0,
		},
		{
			// A large answer that is still leaving a large buffer behind. The
			// decision is on the slack alone, and this is the row that says so:
			// short-circuiting on the answer's own size instead — plausible,
			// because in this band the copy reclaims a small fraction of what
			// it moves — restores the retention at exactly the size that
			// matters, and every other row here still passes.
			name:     "a large answer still leaving a large buffer",
			result:   strings.Repeat("x", 512<<10),
			capacity: 1 << 20,
			copies:   1,
		},
		{
			name:     "slack exactly at the bound is kept",
			result:   "x",
			capacity: maxScrubRetainedSlack + 1,
			copies:   0,
		},
		{
			name:     "slack one byte past the bound is copied",
			result:   "x",
			capacity: maxScrubRetainedSlack + 2,
			copies:   1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shrinkToFit(test.result, test.capacity); got != test.result {
				t.Fatalf("shrinkToFit changed the value: got %q, want %q", got, test.result)
			}

			allocs := testing.AllocsPerRun(50, func() {
				shrinkSink = shrinkToFit(test.result, test.capacity)
			})
			if allocs != test.copies {
				t.Errorf("shrinkToFit allocated %v times, want %v", allocs, test.copies)
			}
		})
	}
}

// TestScrubbingDoesNotHandBackTheBufferItWasBuiltIn is the wiring check for
// [shrinkToFit], and the one the test above cannot be.
//
// That test proves the decision; this proves it is asked with the right
// question. Passing [strings.Builder.Len] instead of [strings.Builder.Cap] at
// the call site makes the slack identically zero, so nothing ever shrinks and
// the retention this head exists to remove comes straight back — with every
// other test in this package, that one included, still green. It was found by
// review rather than by a test, which is the reason this one exists.
//
// Two allocations: the builder sized for the text, and the copy that returns
// what it did not use. One means the copy did not happen.
func TestScrubbingDoesNotHandBackTheBufferItWasBuiltIn(t *testing.T) {
	const value = "tok-live-9f8e7d6c-4b21-4e5a-9c3d-7a1f08e6b2d4"

	scrubber := &Scrubber{}
	scrubber.AddValue(value)

	// Wall to wall values, so the answer is a small fraction of the text and
	// the slack is far past [maxScrubRetainedSlack]. 64 KiB keeps the test
	// quick while leaving about 49 KiB of slack, which is comfortably over it.
	saturated := strings.Repeat(value, (64<<10)/len(value))
	want := strings.Repeat(Redacted, (64<<10)/len(value))
	if got := scrubber.Scrub(saturated); got != want {
		t.Fatalf("Scrub of a saturated body gave %d bytes, want %d", len(got), len(want))
	}
	if slack := len(saturated) - len(want); slack <= maxScrubRetainedSlack {
		t.Fatalf("the body leaves only %d bytes of slack, which is inside the bound: this would pass without shrinking", slack)
	}

	allocs := testing.AllocsPerRun(20, func() {
		shrinkSink = scrubber.Scrub(saturated)
	})
	if allocs != 2 {
		t.Errorf("scrubbing a saturated body allocated %v times, want 2 (the builder, and the copy that returns its unused tail)", allocs)
	}
}
