package flowstatev1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The set-building tests moved here with the walk they cover (from
// flowtest's stubsensitive_internal_test.go and transcript_internal_test.go),
// because the mechanism they pin is this package's now and a test that has to
// reach unexported state belongs beside it. What stayed in flowtest is what
// is about flowtest: how its transcript and its stub diagnostics *use* the
// answer.

// literalStringList builds a sensitive-input-shaped literal list of n
// distinct strings.
func literalStringList(n int) *Value {
	values := make([]*expr.Value, 0, n)
	for i := range n {
		values = append(values, &expr.Value{Kind: &expr.Value_StringValue{StringValue: fmt.Sprintf("element-%d", i)}})
	}

	return &Value{Kind: &Value_Literal{Literal: &expr.Value{
		Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: values}},
	}}}
}

// oneSensitiveInput is the smallest set this walk builds: one input, declared
// sensitive, under one name.
func oneSensitiveInput(name string, value *Value) SensitiveValues {
	return SensitiveInputValues(map[string]*Value{name: value}, map[string]bool{name: true})
}

// A `for_each` over a `sensitive:` list binds each element to the loop's
// `as:` name and runs the body with it in scope, and the engine attaches that
// bound value to a tolerated failure as [StepErrorItemOutput]. Nothing at the
// loop says `sensitive:`, and nothing has to: the item is a descendant of the
// list it was drawn from, so the set built from the declaration already holds
// it — which is what "sensitivity propagates through bindings" means
// concretely (#974).
func TestALoopItemIsSensitiveBecauseTheListItCameFromIs(t *testing.T) {
	t.Parallel()

	customers := NewLiteralList("alice@corp.example", "bob@corp.example")
	sensitive := oneSensitiveInput("customers", customers)

	// The whole list, the way an unbound `${inputs.customers}` renders.
	require.True(t, sensitive.IsSensitive([]any{"alice@corp.example", "bob@corp.example"}))

	// And each bound item on its own, which is the value the loop puts in
	// scope and the engine records beside a tolerated failure. This is the
	// half a name-keyed rule cannot reach: the item has no declaration.
	for _, item := range []string{"alice@corp.example", "bob@corp.example"} {
		require.True(t, sensitive.IsSensitive(item), "the bound item %q is a descendant of the list it came from", item)
	}

	// A failure sentence composed around the item — an http task naming the
	// URL it was given — is cleared by the substring backstop, because the
	// item never appears in it as a value, only as text inside a larger one.
	failure := `task "http": GET http://enrich.invalid/alice@corp.example returned status 404`
	require.Equal(t,
		`task "http": GET http://enrich.invalid/[redacted] returned status 404`,
		sensitive.RedactSubstrings(failure))
	require.NotContains(t, sensitive.RedactSubstrings(failure), "alice@corp.example")
}

// A value derived from a sensitive one by a step's `vars:` or a loop's
// `state:` is the same value under another name, and the set compares by
// content, so it is caught wherever it surfaces — including as a map key.
func TestASensitiveValueIsCaughtUnderWhateverNameItSurfacesUnder(t *testing.T) {
	t.Parallel()

	sensitive := oneSensitiveInput("creds", NewLiteralMap(map[string]any{
		"token":   "shh-secret-value",
		"account": "acct-9931",
	}))

	redacted, ok := sensitive.RedactTree(map[string]any{
		"carried": map[string]any{"copied_token": "shh-secret-value"},
		"kept":    "visible",
	}).(map[string]any)
	require.True(t, ok)
	require.Equal(t, "visible", redacted["kept"])
	require.Equal(t, map[string]any{"copied_token": SensitiveMarker}, redacted["carried"])
}

// CLAUDE.md's containment shapes, applied to the holder rather than to the
// value: printing a [SensitiveValues] — or any struct or slice holding one —
// must not print the material it was built to keep off the screen. A struct
// field would, because [fmt] reaches an unexported field by reflection and
// prints it rather than calling a method on it; the closure is what makes
// this hold, exactly as it does for secrets.Scrubber.
func TestPrintingASensitiveValuesSetNeverPrintsItsMaterial(t *testing.T) {
	t.Parallel()

	const material = "shh-secret-value"

	sensitive := oneSensitiveInput("creds", NewLiteralMap(map[string]any{"token": material}))
	require.True(t, sensitive.IsSensitive(material), "the set has to actually hold it for this to prove anything")

	type holder struct {
		Name      string
		Sensitive SensitiveValues
	}

	subjects := map[string]any{
		"the value":     sensitive,
		"a pointer":     &sensitive,
		"in a struct":   holder{Name: "run", Sensitive: sensitive},
		"in a slice":    []SensitiveValues{sensitive},
		"in a map":      map[string]SensitiveValues{"run": sensitive},
		"in a slice of": []holder{{Name: "run", Sensitive: sensitive}},
	}

	for name, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			rendered := fmt.Sprintf(verb, subject)
			require.NotContainsf(t, rendered, material,
				"%s rendered with %s leaked the material it holds: %s", name, verb, rendered)
		}
	}
}

// A withholding set is the fail-closed answer, and it has to hold under the
// same shapes: it can enumerate nothing, so it redacts everything it is asked
// about rather than passing a value through for want of a match.
func TestAWithholdingSetRedactsEverythingItIsAsked(t *testing.T) {
	t.Parallel()

	withheld := WithheldSensitiveValues()

	require.True(t, withheld.WithholdAll())
	require.False(t, withheld.Empty(), "a set that withholds everything is not a set that changes nothing")
	require.Equal(t, SensitiveMarker, withheld.RedactTree(map[string]any{"anything": "at all"}))
	require.Equal(t, "[withheld]", withheld.RedactText("secret-material-here", "[withheld]"))
}

// The zero value is the common case — a workflow declaring nothing sensitive
// — and must be usable without a constructor, changing nothing it is given.
func TestTheZeroSensitiveValuesSetChangesNothing(t *testing.T) {
	t.Parallel()

	var none SensitiveValues

	require.True(t, none.Empty())
	require.False(t, none.WithholdAll())
	require.False(t, none.IsSensitive("anything"))
	require.Equal(t, "a failure", none.RedactSubstrings("a failure"))
	require.Equal(t, "a failure", none.RedactText("a failure", "[withheld]"))
	require.Equal(t, map[string]any{"kept": "visible"}, none.RedactTree(map[string]any{"kept": "visible"}))
}

// WithValues adds a plaintext that is sensitive without being a declared
// input — a test case's own `secrets:` value — to both halves, and returns a
// new set rather than mutating one its holders already copied.
func TestWithValuesAddsToBothHalvesAndDoesNotMutate(t *testing.T) {
	t.Parallel()

	base := oneSensitiveInput("creds", NewLiteral("declared-value"))
	extended := base.WithValues("added-value", "")

	require.True(t, extended.IsSensitive("added-value"))
	require.Equal(t, "x [redacted] y", extended.RedactSubstrings("x added-value y"))
	require.True(t, extended.IsSensitive("declared-value"), "the original set is carried, not replaced")

	require.False(t, base.IsSensitive("added-value"), "the set a holder already copied must not change under it")
	require.Equal(t, "x added-value y", base.RedactSubstrings("x added-value y"))

	require.Equal(t, "unchanged", extended.RedactSubstrings("unchanged"),
		"an empty plaintext registers nothing: it occurs at every position of every string")
}

// TestWithValuesHoldsAOneRuneValueToTheSubstringFloor is the shredder case
// [minSensitiveSubstringRunes] argues, on the path that used to skip the
// floor: a case's `secrets: {env:TOKEN: e}` marked every `e` of every
// rendered line — `authenticated: true` came back as
// `auth[redacted]nticat[redacted]d: tru[r[redacted]dact[redacted]d]`, the
// marker itself re-shredded — destroying the diagnostic while protecting
// nothing the value comparison had not already caught.
func TestWithValuesHoldsAOneRuneValueToTheSubstringFloor(t *testing.T) {
	t.Parallel()

	set := SensitiveValues{}.WithValues("e")

	require.True(t, set.IsSensitive("e"),
		"the value comparison holds at every length: a rendered value equal to the plaintext still redacts")
	require.Equal(t, SensitiveMarker, set.RedactTree("e"),
		"and the redaction itself, not only set membership: a value equal to the plaintext "+
			"renders as the marker at any length")
	require.Equal(t, "authenticated: true", set.RedactSubstrings("authenticated: true"),
		"a one-rune plaintext must not join the substring backstop: replacing every occurrence "+
			"of one rune is a shredder, not a redaction")

	twoRunes := SensitiveValues{}.WithValues("ab")
	require.Equal(t, "Bearer [redacted]", twoRunes.RedactSubstrings("Bearer ab"),
		"the floor is a floor: at two runes the composite backstop still works")
}

// A sensitive input this cannot read withholds everything rather than
// dropping out of the set: skipping it would leave *nothing* about that input
// redacted anywhere, which is an allow-on-error in the one function whose job
// is to deny (CLAUDE.md, "fail closed").
func TestASensitiveInputThatCannotBeReadWithholdsEverything(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value *Value
	}{
		{
			// Not a literal at all: GetLiteral is nil, so there is no value
			// to compare anything against.
			name:  "an unresolved secret reference",
			value: &Value{Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "TOKEN"}}},
		},
		{
			// A literal [LiteralToGo] refuses: a map keyed by an integer has
			// no Go map[string]any spelling, and it fails closed rather than
			// collapsing every entry into object[""].
			name: "a literal with a non-string map key",
			value: &Value{Kind: &Value_Literal{Literal: &expr.Value{
				Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{{
					Key:   &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 1}},
					Value: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "shh-secret-value"}},
				}}}},
			}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sensitive := oneSensitiveInput("creds", tc.value)
			require.True(t, sensitive.WithholdAll(), "an unreadable sensitive input must withhold, not be skipped")
			require.Empty(t, sensitive.held().values, "a partial set is what withholdAll exists to refuse")
		})
	}
}

// The walk's bound is exact on both sides: a sensitive input whose whole tree
// fits is enumerated normally, and one element more withholds everything
// rather than proceeding with the prefix the walk managed to collect.
func TestTheSensitiveDescendantBoundWithholdsRatherThanTruncates(t *testing.T) {
	t.Parallel()

	// The list itself counts as one of the values, so a list of
	// maxSensitiveDescendants-1 elements is the widest one that fits.
	fits := oneSensitiveInput("bulk", literalStringList(maxSensitiveDescendants-1))
	require.False(t, fits.WithholdAll())
	require.Len(t, fits.held().values, maxSensitiveDescendants, "the container and every element are in the set")

	over := oneSensitiveInput("bulk", literalStringList(maxSensitiveDescendants))
	require.True(t, over.WithholdAll(), "one element past the bound must withhold, not truncate")
	require.Empty(t, over.held().values)
}

// A one-rune leaf is kept out of the textual backstop: replacing every `a` in
// a rendered line would destroy it while protecting nothing the exact-value
// comparison has not already caught. The declared input's own value has no
// such floor, which the second half pins.
func TestOnlyTheDeclaredValueEscapesTheSubstringFloor(t *testing.T) {
	t.Parallel()

	nested := oneSensitiveInput("creds", NewLiteralMap(map[string]any{
		"initial": "a",
		"token":   "shh-secret-value",
	}))
	require.NotContains(t, nested.held().substrings, "a", "a one-rune leaf is a shredder, not a redaction")
	require.Contains(t, nested.held().substrings, "shh-secret-value")
	// It is still compared by value, so the leaf itself never prints.
	require.True(t, nested.IsSensitive("a"))

	declared := oneSensitiveInput("pin", NewLiteral("a"))
	require.Contains(t, declared.held().substrings, "a",
		"the value `sensitive:` names is replaced textually whatever its length: it is what `\"Bearer \" + inputs.pin` needs")
}

// A bytes leaf inside a sensitive structure is withheld where text merely
// contains it, as a string leaf is: `"Bearer " + string(inputs.creds.token)`
// is a string the typed equality never sees (#2231). It is held to the same
// floor as a string of the same length.
func TestASensitiveBytesLeafIsWithheldWhereTextContainsIt(t *testing.T) {
	t.Parallel()

	set := oneSensitiveInput("creds", NewLiteralMap(map[string]any{
		"token": []byte("hunter2-bytes"),
		"tiny":  []byte("a"),
	}))

	require.Equal(t, "Authorization: Bearer [redacted]", set.RedactSubstrings("Authorization: Bearer hunter2-bytes"),
		"text containing the bytes' text is withheld")
	require.NotContains(t, set.held().substrings, "a",
		"bytes shorter than the floor are held to it, as the string \"a\" is")
	require.True(t, set.IsSensitive([]byte("hunter2-bytes")), "and it is still compared by value")
}

// A `sensitive: true` integer converted to text — `${string(inputs.pin)}` —
// matches neither the typed equality nor a string-only substring set, so its
// canonical rendering joins the backstop under the same floor and root
// exemption a string descendant gets.
func TestNonStringSensitiveScalarsJoinTheSubstringBackstop(t *testing.T) {
	t.Parallel()

	set := oneSensitiveInput("pin", NewLiteral(int64(8231)))

	require.Contains(t, set.held().substrings, "8231",
		"the number's canonical text must be replaceable wherever a conversion strands it in a string")
	require.Equal(t, "code [redacted] here", set.RedactSubstrings("code 8231 here"))
}

// A nested numeric descendant's converted text enters the substring set at
// the same two-rune floor as any descendant — `creds: {pin: 12}` renders "12"
// redacted — and a one-rune numeric stays out for the floor's own documented
// reason: replacing every occurrence of a single digit shreds the line (every
// `t=7m` timestamp included) while protecting a ten-value guessing space.
func TestShortNumericDescendantsJoinTheBackstopAtTheFloor(t *testing.T) {
	t.Parallel()

	set := oneSensitiveInput("creds", NewLiteralMap(map[string]any{"pin": int64(12)}))

	require.Contains(t, set.held().substrings, "12",
		"a two-rune converted numeric descendant is at the floor, not under it")
	require.Equal(t, "code [redacted] here", set.RedactSubstrings("code 12 here"))
}

// With secrets `abcd` and `abcdef`, replacing the shorter first splits the
// longer into `[redacted]ef` — a partial leak decided by map iteration order.
// The union of matches has no order to get wrong.
func TestOverlappingSensitiveSubstringsRedactWhole(t *testing.T) {
	t.Parallel()

	for _, order := range [][]string{
		{"abcd", "abcdef"},
		{"abcdef", "abcd"},
	} {
		got := redactSensitiveSubstrings("token abcdef here", order)
		require.Equal(t, "token [redacted] here", got,
			"order %v must not leak a suffix of the longer secret", order)
	}
}

// Two secrets that intersect without containment — `ABCDE` and `CDEFG`
// across derived text `ABCDEFG` — leak a fragment under sequential
// replacement in either order. Self-overlapping matches are covered by the
// same union.
func TestIntersectingSensitiveSubstringsRedactWhole(t *testing.T) {
	t.Parallel()

	for _, order := range [][]string{
		{"ABCDE", "CDEFG"},
		{"CDEFG", "ABCDE"},
	} {
		got := redactSensitiveSubstrings("xx ABCDEFG yy", order)
		require.Equal(t, "xx [redacted] yy", got,
			"order %v must not leak either secret's fragment", order)
	}

	require.Equal(t, "[redacted]", redactSensitiveSubstrings("aaa", []string{"aa"}),
		"self-overlapping matches all enter the union")
}

func TestSensitiveSubstringMatcherPreservesTheUnionOfEveryMatch(t *testing.T) {
	t.Parallel()

	patternSets := [][]string{
		{"ab", "bc"},
		{"aa"},
		{"ab", "abc"},
		{"aba", "bab", "bc"},
		{"", "ab", "ab"},
		// Backward-extending overlaps: a short pattern that ends before a long
		// one which started earlier. Every set above happens to be one where
		// the matcher's end-offset order is also its start-offset order, which
		// is why the corpus agreed with the reference while the prefix of a
		// longer secret was printing in the clear (#1119).
		{"aa", "baaa"},
		{"c", "abc"},
		{"bc", "aabc"},
	}
	for _, patterns := range patternSets {
		for length := range 7 {
			count := 1
			for range length {
				count *= 3
			}
			for encoded := range count {
				text := make([]byte, length)
				value := encoded
				for i := range text {
					text[i] = "abc"[value%3]
					value /= 3
				}
				want := referenceSensitiveSubstringRedaction(string(text), patterns)
				require.Equal(t, want, redactSensitiveSubstrings(string(text), patterns),
					"patterns %q over text %q", patterns, text)
			}
		}
	}
}

func referenceSensitiveSubstringRedaction(text string, patterns []string) string {
	redacted := make([]bool, len(text))
	for _, pattern := range patterns {
		if pattern == "" || len(pattern) > len(text) {
			continue
		}
		for from := 0; from <= len(text)-len(pattern); {
			offset := strings.Index(text[from:], pattern)
			if offset < 0 {
				break
			}
			start := from + offset
			for i := start; i < start+len(pattern); i++ {
				redacted[i] = true
			}
			from = start + 1
		}
	}

	var b strings.Builder
	for i := 0; i < len(text); {
		if !redacted[i] {
			b.WriteByte(text[i])
			i++
			continue
		}
		b.WriteString(SensitiveMarker)
		for i < len(text) && redacted[i] {
			i++
		}
	}
	return b.String()
}

func TestSensitiveSubstringRedactionBoundsAttackerShapedWork(t *testing.T) {
	t.Parallel()

	rendered := strings.Repeat("a", maxSensitiveSubstringRedactionWork)
	longOverlap := strings.Repeat("a", len(rendered)/2)
	require.Equal(t, SensitiveMarker, redactSensitiveSubstrings(rendered, []string{longOverlap}),
		"a long secret at many overlapping offsets must be matched in linear time")

	duplicates := make([]string, 1024)
	for i := range duplicates {
		duplicates[i] = "aa"
	}
	require.Equal(t, SensitiveMarker,
		redactSensitiveSubstrings(strings.Repeat("a", 100_000), duplicates),
		"duplicate descendants must cost one search")

	distinct := make([]string, 11)
	for i := range distinct {
		distinct[i] = fmt.Sprintf("secret-%d", i)
	}
	require.Equal(t, strings.Repeat("a", 100_000),
		redactSensitiveSubstrings(strings.Repeat("a", 100_000), distinct),
		"a multi-pattern matcher must scan the rendered value once")
	require.Equal(t, SensitiveMarker,
		redactSensitiveSubstrings(strings.Repeat("a", maxSensitiveSubstringRedactionWork+1), distinct),
		"a rendered value past the absolute bound must be withheld")
	require.Equal(t, "unchanged", redactSensitiveSubstrings("unchanged", nil),
		"the common empty-set path must not allocate a redaction mask")
}

func TestSensitiveSubstringMatcherIsReusedAcrossATranscriptSizedRendering(t *testing.T) {
	t.Parallel()

	patterns := make([]string, maxSensitiveDescendants)
	for i := range patterns {
		patterns[i] = fmt.Sprintf("secret-%04d", i)
	}
	sensitive := SensitiveValues{}.WithValues(patterns...)
	require.False(t, sensitive.WithholdAll())

	line := strings.Repeat("x", 800)
	for worker := range 8 {
		t.Run(fmt.Sprintf("worker-%d", worker), func(t *testing.T) {
			t.Parallel()
			for range 1_250 {
				require.Equal(t, line, sensitive.RedactSubstrings(line))
			}
		})
	}
}

// redactSensitiveTree redacted values at every depth but preserved map keys,
// so a sensitive key nested inside a structured value printed — including one
// below the substring floor. Keys redact by exact match at every level.
func TestNestedSensitiveKeysRedact(t *testing.T) {
	t.Parallel()

	got := redactSensitiveTree(map[string]any{
		"outer": map[string]any{"zq": "v", "kept": "w"},
	}, []any{"zq"})

	outer, ok := got.(map[string]any)["outer"].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, outer, "zq")
	require.Contains(t, outer, SensitiveMarker)
	require.Contains(t, outer, "kept")
}

// A declaration naming an input the run does not carry adds nothing, and a
// run carrying inputs no declaration names redacts nothing: the set is the
// intersection, built from what was actually bound.
func TestOnlyDeclaredInputsEnterTheSet(t *testing.T) {
	t.Parallel()

	set := SensitiveInputValues(
		map[string]*Value{
			"secret": NewLiteral("hidden-value"),
			"public": NewLiteral("shown-value"),
		},
		map[string]bool{"secret": true, "absent": true},
	)

	require.True(t, set.IsSensitive("hidden-value"))
	require.False(t, set.IsSensitive("shown-value"))
	require.Equal(t, "shown-value", set.RedactSubstrings("shown-value"))
	require.False(t, strings.Contains(set.RedactSubstrings("hidden-value"), "hidden-value"))
}

// TestABackwardOverlapRedactsTheWholeSecret is #1119's leak in the shape it
// reaches a person: two sensitive values where the short one is a substring of
// the long one but does not start where it starts.
//
// The matcher reports a match at the position it *ends*, so `aa` is announced
// before the `topsecret-aaa` containing it, and a high-water mark of what is
// already covered then treats the longer secret as having only its final byte
// left to redact. What printed was `topsecret-[redacted]` — the whole
// distinguishing part of the value, in a failure message on a terminal, in CI
// output, or in a test report an agent reads back.
//
// The single-pattern case is the control: it is what makes this a claim about
// the overlap rather than about the long value being in the set at all.
func TestABackwardOverlapRedactsTheWholeSecret(t *testing.T) {
	t.Parallel()

	const text = "failure: topsecret-aaa"

	alone := SensitiveValues{}.WithValues("topsecret-aaa")
	require.Equal(t, "failure: [redacted]", alone.RedactSubstrings(text),
		"the long value alone must redact whole; if this fails the case below proves nothing")

	both := SensitiveValues{}.WithValues("aa", "topsecret-aaa")
	require.Equal(t, "failure: [redacted]", both.RedactSubstrings(text),
		"adding a shorter sensitive value must not expose the longer one's prefix")

	// The order the values were added in is not what decides it: the matcher
	// walks the text, not the list.
	reversed := SensitiveValues{}.WithValues("topsecret-aaa", "aa")
	require.Equal(t, "failure: [redacted]", reversed.RedactSubstrings(text))
}

// TestMergeExtendsRatherThanReplaces is #2079's mechanism, pinned directly:
// two sets built independently — the shape a caller has before a run's
// inputs bind and the shape built from binding them — each hold a value the
// other does not, and [SensitiveValues.Merge] must hold both afterward. A
// caller that instead assigned one set over the other, the pattern #2079
// tracks, would lose whichever side it discarded; this fails against that
// shape and passes against Merge's.
func TestMergeExtendsRatherThanReplaces(t *testing.T) {
	t.Parallel()

	preBind := SensitiveValues{}.WithValues("pre-bind-only-secret")
	postBind := SensitiveValues{}.WithValues("post-bind-only-secret")

	merged := preBind.Merge(postBind)

	require.True(t, merged.IsSensitive("pre-bind-only-secret"),
		"a value only the pre-bind set held must survive the merge")
	require.True(t, merged.IsSensitive("post-bind-only-secret"),
		"and a value only the post-bind set held")

	require.Equal(t, "[redacted] and [redacted]",
		merged.RedactSubstrings("pre-bind-only-secret and post-bind-only-secret"),
		"the substring backstop must hold both sides too, not only the value comparison")

	// Order must not matter: extending in either direction reaches the same
	// answer, since a caller with two independently built sets has no reason
	// to prefer one side as the base.
	require.True(t, postBind.Merge(preBind).IsSensitive("pre-bind-only-secret"))
}

// TestMergeWithholdsWhenEitherSideDoes is Merge's fail-closed half
// (CLAUDE.md, "fail closed at trust boundaries"): a set that could not be
// built completely must still withhold everything after merging with one
// that could, whichever side of the call it is on — a caller has no reason
// to know which side is the one that could not decide.
func TestMergeWithholdsWhenEitherSideDoes(t *testing.T) {
	t.Parallel()

	held := SensitiveValues{}.WithValues("built-fine")
	withheld := WithheldSensitiveValues()

	require.True(t, held.Merge(withheld).WithholdAll())
	require.True(t, withheld.Merge(held).WithholdAll())
}

// TestMergeWithholdsPastTheCombinedDescendantBound is Codex's finding that
// Merge appended both sides' values with no bound of its own: two sets each
// valid on their own — a maximum-size structured input's own set, say,
// merged with a run's pre-bind secrets — combine into one larger than
// [maxSensitiveDescendants], the bound [SensitiveInputValues] enforces while
// building either side so that [SensitiveValues.RedactTree] never costs more
// per node than that many [reflect.DeepEqual] comparisons. Merge must answer
// the identical bound on the combined count, not only on each side alone,
// or a chain of otherwise-valid merges grows the excess further with every
// call.
func TestMergeWithholdsPastTheCombinedDescendantBound(t *testing.T) {
	t.Parallel()

	half := maxSensitiveDescendants/2 + 1 // each side alone stays under the bound; combined, over it

	distinct := func(prefix string, n int) []string {
		values := make([]string, n)
		for i := range values {
			values[i] = fmt.Sprintf("%s-%04d", prefix, i)
		}
		return values
	}

	a := SensitiveValues{}.WithValues(distinct("a", half)...)
	b := SensitiveValues{}.WithValues(distinct("b", half)...)
	require.False(t, a.WithholdAll(), "one side alone must stay under the bound")
	require.False(t, b.WithholdAll(), "and the other")

	require.True(t, a.Merge(b).WithholdAll(),
		"two sets each under the bound combined past it and Merge did not fail closed")
	require.True(t, b.Merge(a).WithholdAll(), "order must not matter")
}

// TestAnAccumulatorHoldsEachValueOnce: a set gathered step by step is told
// the same sets over and over (#2211). The accumulator holds each value once,
// however often and in however many separately built sets it is told, so it
// stays under the bound — 1025 repeats of even a one-value set would pass it
// if each were held again. Merge, a union built on it, does the same
// (#2215).
func TestAnAccumulatorHoldsEachValueOnce(t *testing.T) {
	t.Parallel()

	var gathered SensitiveAccumulator
	var merged SensitiveValues
	for range maxSensitiveDescendants + 1 {
		// Built afresh each time, so only equality, not identity, can see
		// that nothing is new.
		gathered.Add(oneSensitiveInput("token", NewLiteral("hunter2-token")))
		gathered.Add(oneSensitiveInput("codes", NewLiteralList(7, 8)))
		merged = merged.Merge(oneSensitiveInput("token", NewLiteral("hunter2-token")))
	}
	require.False(t, merged.WithholdAll(), "merging the same set again and again reached the bound")
	assert.Len(t, merged.held().values, len(oneSensitiveInput("token", NewLiteral("hunter2-token")).held().values))

	all := gathered.Values()
	token, codes := oneSensitiveInput("token", NewLiteral("hunter2-token")), oneSensitiveInput("codes", NewLiteralList(7, 8))
	require.False(t, all.WithholdAll(), "gathering the same two sets reached the bound")
	assert.Len(t, all.held().values, len(token.held().values)+len(codes.held().values))
	assert.Len(t, all.held().substrings, len(token.held().substrings)+len(codes.held().substrings))
	assert.True(t, all.IsSensitive("hunter2-token"))
	assert.Equal(t, "the [redacted] travels", all.RedactText("the hunter2-token travels", "[redacted]"))
	assert.Equal(t, SensitiveMarker, all.RedactTree(int64(7)), "a structured set's short descendant was lost")
}

// TestAnAccumulatorToldASetAgainDoesNoWork: the cost Copilot measured on
// #2215 — every step formatting everything gathered — is gone. A set already
// gathered is skipped by identity, and asking for the result again rebuilds
// nothing.
func TestAnAccumulatorToldASetAgainDoesNoWork(t *testing.T) {
	large := oneSensitiveInput("key", NewLiteral(strings.Repeat("s3cr3t-", 5000)))
	var gathered SensitiveAccumulator
	gathered.Add(large)
	first := gathered.Values()

	allocs := testing.AllocsPerRun(100, func() {
		gathered.Add(large)
		_ = gathered.Values()
	})
	assert.Zero(t, allocs, "gathering a set already gathered did work")
	assert.Same(t, first.identity, gathered.Values().identity, "the gathered set was rebuilt with nothing new")
}

// TestAnAccumulatorFailsClosed: a set that could not be built withholds
// everything from then on, whichever order it arrives in, and an accumulator
// told nothing withholds nothing.
func TestAnAccumulatorFailsClosed(t *testing.T) {
	t.Parallel()

	var before, after, empty SensitiveAccumulator
	before.Add(WithheldSensitiveValues())
	before.Add(oneSensitiveInput("token", NewLiteral("hunter2-token")))
	after.Add(oneSensitiveInput("token", NewLiteral("hunter2-token")))
	after.Add(WithheldSensitiveValues())
	assert.True(t, before.Values().WithholdAll())
	assert.True(t, after.Values().WithholdAll())
	assert.True(t, empty.Values().Empty())
}

// TestAnAccumulatorHoldsANaNOnce: a NaN never equals itself under
// reflect.DeepEqual, so without its own rule it would be gathered anew from
// every set until the bound withheld everything.
func TestAnAccumulatorHoldsANaNOnce(t *testing.T) {
	t.Parallel()

	var gathered SensitiveAccumulator
	for range maxSensitiveDescendants + 1 {
		gathered.Add(sensitiveValuesOf(sensitiveState{values: []any{math.NaN()}}))
	}
	require.False(t, gathered.Values().WithholdAll())
	assert.Len(t, gathered.Values().held().values, 1)
}

// TestAnAccumulatorHashesByItsOwnEquality: values its equality calls one —
// NaNs whatever their payload, -0 and 0, a NaN inside a list — must hash
// alike, or equality is never asked and each copy is gathered again until the
// bound withholds everything (Codex, #2215).
func TestAnAccumulatorHashesByItsOwnEquality(t *testing.T) {
	t.Parallel()

	payloads := []float64{math.NaN(), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0xfff8000000000002)}
	var gathered SensitiveAccumulator
	for i := range maxSensitiveDescendants + 1 {
		nan := payloads[i%len(payloads)]
		zero := 0.0
		if i%2 == 1 {
			zero = math.Copysign(0, -1)
		}
		gathered.Add(sensitiveValuesOf(sensitiveState{values: []any{nan, zero, []any{nan}, map[string]any{"z": zero}}}))
	}
	require.False(t, gathered.Values().WithholdAll(), "values equal by the accumulator's own relation reached the bound")
	assert.Len(t, gathered.Values().held().values, 4)
}

// TestMergingWithNothingKeepsTheSet: a merge where one side adds nothing is
// the other side itself, so a step whose failure carries nothing reports the
// same set its position holds, and a reader can recognize it.
func TestMergingWithNothingKeepsTheSet(t *testing.T) {
	t.Parallel()

	token := oneSensitiveInput("token", NewLiteral("hunter2-token"))
	assert.Same(t, token.identity, token.Merge(SensitiveValues{}).identity)
	assert.Same(t, token.identity, SensitiveValues{}.Merge(token).identity)
	assert.True(t, token.Merge(WithheldSensitiveValues()).WithholdAll())
}

// TestAnAccumulatorHashesAlikeInEveryProcess: a merge runs workflow-side, so
// its index must not hang on a per-process random seed. Two accumulators hash
// one value alike, and to the same number every process computes (Codex,
// #2215).
func TestAnAccumulatorHashesAlikeInEveryProcess(t *testing.T) {
	t.Parallel()

	value := map[string]any{"token": "hunter2", "pins": []any{int64(7), 1.5, true, nil, []byte("b")}}
	var first, second SensitiveAccumulator
	first.Add(oneSensitiveInput("token", NewLiteral("first")))
	second.Add(oneSensitiveInput("token", NewLiteral("second")))
	assert.Equal(t, uint64(0xc01e540087d0a1ca), first.state.hash(value))
	assert.Equal(t, first.state.hash(value), second.state.hash(value))
}

// TestAnAccumulatorRefusesASetPastTheBoundUnread: a set holding more values
// than the bound withholds everything, however few distinct values it holds,
// as an appending merge of it did. Deduplicated first, a thousand repeats of
// one secret kept the union small, and the bound never limited the work of
// reading them (Codex, #2215).
func TestAnAccumulatorRefusesASetPastTheBoundUnread(t *testing.T) {
	t.Parallel()

	repeated := SensitiveValues{}.WithValues(slices.Repeat([]string{"a"}, maxSensitiveDescendants+1)...)
	var gathered SensitiveAccumulator
	gathered.Add(repeated)
	assert.True(t, gathered.Values().WithholdAll())
	assert.True(t, oneSensitiveInput("token", NewLiteral("hunter2-token")).Merge(repeated).WithholdAll())
	// And merged with nothing, where the set would otherwise come back as
	// it was (Codex, #2215).
	assert.True(t, repeated.Merge(SensitiveValues{}).WithholdAll())
	assert.True(t, SensitiveValues{}.Merge(repeated).WithholdAll())

	atTheBound := SensitiveValues{}.WithValues(slices.Repeat([]string{"a"}, maxSensitiveDescendants)...)
	var within SensitiveAccumulator
	within.Add(atTheBound)
	assert.False(t, within.Values().WithholdAll())
}

// TestAnAccumulatorFramesWhatItHashes: values split differently across their
// strings encode differently, so they do not share a bucket unless the hash
// itself collides (Codex, #2215).
func TestAnAccumulatorFramesWhatItHashes(t *testing.T) {
	t.Parallel()

	var gathered SensitiveAccumulator
	gathered.Add(oneSensitiveInput("token", NewLiteral("hunter2-token")))
	for _, pair := range [][2]any{
		{[]any{"a", "\x01b"}, []any{"a\x01", "b"}},
		{map[string]any{"a": "\x01b"}, map[string]any{"a\x01": "b"}},
		{[]any{[]byte("a"), "b"}, []any{[]byte("ab")}},
		// A bool's width, a map key's length and a byte string's length, each
		// on its own: a key ended by a NUL, as it once was, let a key absorb
		// its value's encoding.
		{[]any{false, "\x00"}, []any{true, ""}},
		{map[string]any{"a": ""}, map[string]any{"a\x00\x01\x00\x00\x00\x00\x00\x00": nil}},
		{map[string]any{"a": "\x00"}, map[string]any{"a\x01": ""}},
		{[]any{[]byte("a"), []byte("\x02")}, []any{[]byte("a\x02"), []byte("")}},
	} {
		assert.NotEqual(t, gathered.state.hash(pair[0]), gathered.state.hash(pair[1]), "%#v and %#v", pair[0], pair[1])
	}
}

// TestAnAccumulatorRefusesABucketThatGrows: the hash is unkeyed, so values
// colliding in it can be chosen. Past a few in one bucket, the accumulator
// withholds everything rather than compare each addition against them all
// (Codex, #2215).
func TestAnAccumulatorRefusesABucketThatGrows(t *testing.T) {
	t.Parallel()

	for _, colliding := range []int{maxSensitiveBucket - 1, maxSensitiveBucket} {
		var gathered SensitiveAccumulator
		gathered.Add(oneSensitiveInput("token", NewLiteral("hunter2-token")))
		// Stand-ins for values built to collide with the one added next.
		key := gathered.state.hash("hunter2-colliding")
		for i := range colliding {
			gathered.state.index[key] = append(gathered.state.index[key], int64(i))
		}
		gathered.Add(oneSensitiveInput("token", NewLiteral("hunter2-colliding")))
		assert.Equal(t, colliding == maxSensitiveBucket, gathered.Values().WithholdAll(), "%d colliding values", colliding)
	}
}

// TestAnAccumulatorTellsANilByteStringFromAnEmptyOne: the redaction's
// equality holds a nil byte string and an empty one apart, so the hash must
// too, or lists differing only in which empty leaves are nil share a bucket
// and reach its cap (Codex, #2215).
func TestAnAccumulatorTellsANilByteStringFromAnEmptyOne(t *testing.T) {
	t.Parallel()

	var gathered SensitiveAccumulator
	for variant := range 16 {
		leaves := make([]any, 4)
		for bit := range leaves {
			if variant&(1<<bit) != 0 {
				leaves[bit] = []byte(nil)
			} else {
				leaves[bit] = []byte{}
			}
		}
		gathered.Add(sensitiveValuesOf(sensitiveState{values: []any{leaves}}))
	}
	require.False(t, gathered.Values().WithholdAll(), "variants unequal under the redaction's equality filled one bucket")
	assert.Len(t, gathered.Values().held().values, 16)
}

// A sensitive duration or timestamp is redacted however an expression spelled it:
// the form the run document writes, and the form CEL's `string(...)` writes.
func TestASensitiveDurationAndTimestampAreRedactedInEverySpelling(t *testing.T) {
	t.Parallel()

	span, err := NormalizeDataKind(InputDeclaration_TYPE_DURATION, &expr.Value{Kind: &expr.Value_StringValue{StringValue: "1h"}})
	require.NoError(t, err)
	stamp, err := NormalizeDataKind(InputDeclaration_TYPE_TIMESTAMP, &expr.Value{Kind: &expr.Value_StringValue{StringValue: "2026-03-01T09:30:00Z"}})
	require.NoError(t, err)

	duration := oneSensitiveInput("d", &Value{Kind: &Value_Literal{Literal: span}})
	for _, line := range []string{"waited 1h0m0s", "waited 3600s"} {
		redacted := duration.RedactSubstrings(line)
		assert.NotContains(t, redacted, "1h0m0s", line)
		assert.NotContains(t, redacted, "3600s", line)
	}

	moment := oneSensitiveInput("t", &Value{Kind: &Value_Literal{Literal: stamp}})
	assert.NotContains(t, moment.RedactSubstrings("at 2026-03-01T09:30:00Z"), "2026-03-01")
}

// A sensitive list of timestamps and a fractional duration are redacted in the
// spelling CEL writes, not only the one the run document does.
func TestASensitiveNestedAndFractionalDataKindIsRedactedInCELSpelling(t *testing.T) {
	t.Parallel()

	span, err := NormalizeDataKind(InputDeclaration_TYPE_DURATION, &expr.Value{Kind: &expr.Value_StringValue{StringValue: "1500ms"}})
	require.NoError(t, err)
	fractional := oneSensitiveInput("d", &Value{Kind: &Value_Literal{Literal: span}})
	assert.NotContains(t, fractional.RedactSubstrings("waited 1.5s"), "1.5s")

	stamp, err := NormalizeDataKind(InputDeclaration_TYPE_TIMESTAMP, &expr.Value{Kind: &expr.Value_StringValue{StringValue: "2026-03-01T09:30:00.5+02:00"}})
	require.NoError(t, err)
	list := &Value{Kind: &Value_Literal{Literal: &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: []*expr.Value{stamp}}}}}}
	nested := oneSensitiveInput("ts", list)
	assert.NotContains(t, nested.RedactSubstrings("at 2026-03-01T07:30:00.5Z"), "2026-03-01")
	assert.NotContains(t, nested.RedactSubstrings("at 2026-03-01T07:30:00Z"), "2026-03-01")

	assert.Equal(t, "-0.000000001s", exactSeconds(&durationpb.Duration{Nanos: -1}))
	assert.Equal(t, "3600s", exactSeconds(&durationpb.Duration{Seconds: 3600}))
}

// TestRedactSubstringsCoversEveryEncodingARendererMayApply pins #2081: a
// renderer that encodes a sensitive value before the backstop reads it — `%q`,
// [json.Marshal] with or without HTML escaping — prints bytes the plaintext
// substring never matches, so every set-building path holds those spellings.
func TestRedactSubstringsCoversEveryEncodingARendererMayApply(t *testing.T) {
	t.Parallel()

	value := "pa\"ss\\w<o>rd&é\nx"

	htmlEscaped, err := json.Marshal(value)
	require.NoError(t, err)

	var plain bytes.Buffer

	enc := json.NewEncoder(&plain)
	enc.SetEscapeHTML(false)
	require.NoError(t, enc.Encode(value))

	// The quotes a renderer adds around the body are its own, not the value's.
	bodyOf := func(quoted string) string { return quoted[1 : len(quoted)-1] }

	spellings := map[string]string{
		"plaintext":    value,
		"percent-q":    bodyOf(strconv.Quote(value)),
		"json":         bodyOf(string(htmlEscaped)),
		"json-no-html": bodyOf(strings.TrimSuffix(plain.String(), "\n")),
	}

	sets := map[string]SensitiveValues{
		"declared input": oneSensitiveInput("token", &Value{Kind: &Value_Literal{Literal: &expr.Value{
			Kind: &expr.Value_StringValue{StringValue: value},
		}}}),
		"with values": SensitiveValues{}.WithValues(value),
	}

	for setName, set := range sets {
		for spellingName, spelling := range spellings {
			t.Run(setName+"/"+spellingName, func(t *testing.T) {
				t.Parallel()

				got := set.RedactSubstrings("line: " + spelling + " end")

				assert.Equal(t, "line: "+SensitiveMarker+" end", got)
			})
		}
	}
}
