package flowstatev1

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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
