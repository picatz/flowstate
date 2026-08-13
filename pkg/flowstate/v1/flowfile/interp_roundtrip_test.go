package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The round trip this file pins is the one two of #513's review findings were
// each half of: for any scalar the parser accepts, marshalling the workflow and
// reading it back has to produce the same workflow, and the bytes written have
// to compile.
//
// It is a property rather than a table of cases because the failure it covers is
// a *missing* place rather than a wrong one. `$${` made the bytes `${` appear in
// text that is not a fence, and every consumer that finds a fence by searching
// for those bytes is wrong in the same way — so the useful question is not
// "does description round-trip" but "does any field not". A table answers the
// first. Substituting one probe into every position a scalar can occupy, and
// letting the comparison be proto equality, answers the second: a field added
// tomorrow that forgets to escape what it writes fails here without anyone
// having to remember to add it. That is the shape #508 found four times in a
// week — a walk that did not know about a new branch.
//
// Three properties, because they fail independently:
//
//   - The bytes re-parse without a diagnostic. This is the one `flow fmt`'s
//     promise rests on, and the one the description finding broke: formatting a
//     valid file must never invalidate it.
//   - The workflow read back equals the one written. Bytes that parse are not
//     enough — a value that comes back meaning something else parses perfectly.
//   - Marshalling twice is a fixed point, so `flow fmt` settles rather than
//     walking a file somewhere new on each run.

// interpProbes are scalars that put `${`-shaped bytes in every arrangement the
// escape makes possible. Each is substituted into every position in
// [probeDocument] that a scalar can occupy.
//
// The list is deliberately weighted to the arrangements a substring search gets
// wrong: an escape alone, an escape *before* a real fence (where a search finds
// the lookalike first), two identical fences either side of an escape, and
// escapes with nothing between them.
var interpProbes = []string{
	"plain text with no fence at all",
	"$${TOKEN}",
	"${steps.who.value}",
	"$${who.said} and ${steps.who.value}",
	"${steps.who.value} and $${who.said}",
	"$${a}$${b}",
	"$$${a}",
	"$${a} ${steps.who.value} $${a}",
	"${steps.who.value} ${steps.who.value}",
	"before $${x} between ${steps.who.value} after",
	"$$",
	"$",
	"${",
	"}",
	"a } b",
	"${'$' + '{'}",
	"${'}'} $${}",
	"$${} ${steps.who.value}",
}

// probeDocument renders a workflow with the probe in one named position.
//
// Every position here is a scalar the compiler reads through a path that resolves
// the escape: the compile-time text fields (`description` in its three places,
// an input's `must`) and the evaluated value fields. `must` earns its slot
// because it is read by the same [compiler.text] the descriptions are and is the
// one of those that is not prose, which is exactly the sort of field an
// enumeration by hand leaves out.
func probeDocument(position, probe string) (string, bool) {
	fields := map[string]string{
		"workflow description": "",
		"step description":     "",
		"input description":    "",
		"output description":   "",
		"calendar comment":     "",
		"log message":          "hello",
		"value":                "'hello'",
	}
	if _, ok := fields[position]; !ok {
		return "", false
	}
	fields[position] = probe

	quote := func(s string) string { return "\"" + strings.ReplaceAll(s, `"`, `\"`) + "\"" }

	return fmt.Sprintf(`edition: %s
name: probe
description: %s
triggers:
  - schedule:
      calendars:
        - hour: 9
          comment: %s
inputs:
  who:
    type: string
    description: %s
outputs:
  answer:
    description: %s
    value: ${steps.who.value}
steps:
  - id: who
    description: %s
    value: "'hello'"
  - id: valued
    value: %s
  - id: shown
    log:
      message: %s
`,
		flowfile.CurrentEdition,
		quote(fields["workflow description"]),
		quote(fields["calendar comment"]),
		quote(fields["input description"]),
		quote(fields["output description"]),
		quote(fields["step description"]),
		quote(fields["value"]),
		quote(fields["log message"]),
	), true
}

var probePositions = []string{
	"workflow description",
	"step description",
	"input description",
	"output description",
	"calendar comment",
	"log message",
	"value",
}

// mustRoundTrip are the probes every position has to actually accept, rather
// than skip as refused.
//
// The list exists because every assertion below is conditional on the parser
// accepting the document, so a harness that malforms its own document skips each
// case and passes green. This one did exactly that, for as long as its template
// wrote `calendar:` where the grammar spells `calendars:`: eighteen probes times
// seven positions, every one declined, the suite green, and the bug the file was
// written for still there. Asserting that the interesting cases ran is the same
// habit CLAUDE.md records for bounds — check the bound was *reached*, not only
// that it was not exceeded.
//
// These and not the whole corpus, because most of the corpus is legitimately
// refused somewhere: a bare `${...}` cannot go in a field read at compile time,
// and `${` alone is unterminated everywhere. What a position must accept is text
// carrying the escape, which is the case both round-trip findings were about.
//
// `value:` is the exception, and it is the language rather than the harness. A
// field the schema types as an expression holds CEL source, where `$${` is not
// an escape but two dollars and a brace — the compiler says so in
// [compiler.scalarString], and reading it as text there would give the language
// two rules about what `${` means, decided by which field it is in. So the
// expression positions promise only that ordinary text survives, and the escape
// probes are asserted where the escape exists.
var mustRoundTrip = map[string][]string{
	"": {
		"plain text with no fence at all",
		"$${TOKEN}",
		"$${a}$${b}",
		"$$${a}",
	},
	// An expression position's probes have to be CEL, which is the same point
	// said from the other side: `plain text with no fence at all` is not an
	// expression, so it is refused here and asserting it would be asserting the
	// language is something it is not. A single fence and two fences with text
	// between them are the two shapes that matter, and the second is the one
	// whose spans a substring search got wrong.
	"value": {
		"${steps.who.value}",
		"${steps.who.value} ${steps.who.value}",
	},
}

func mustRoundTripIn(position string) []string {
	if probes, ok := mustRoundTrip[position]; ok {
		return probes
	}
	return mustRoundTrip[""]
}

func TestMarshalRoundTripsEveryScalarPosition(t *testing.T) {
	t.Parallel()

	for _, position := range probePositions {
		t.Run(position, func(t *testing.T) {
			t.Parallel()

			ran := map[string]bool{}
			for _, probe := range interpProbes {
				t.Run(probe, func(t *testing.T) {
					source, ok := probeDocument(position, probe)
					require.True(t, ok)
					ran[probe] = requireSourceRoundTrip(t, []byte(source))
				})
			}
			for _, probe := range mustRoundTripIn(position) {
				require.True(t, ran[probe],
					"%q was never round-tripped in %q, so nothing here was actually asserted about it; "+
						"the usual cause is the harness's own document no longer parsing",
					probe, position)
			}
		})
	}
}

// requireSourceRoundTrip asserts the three properties on one document, and
// reports whether the document was one the parser accepted and so one the
// properties had anything to say about.
//
// A refused document is not a failure here: the corpus deliberately holds
// scalars that are not legal in every position — `${` alone is an unterminated
// fence, and a bare expression is refused where a field is read at compile time.
// What the round trip owes is about documents the parser *accepts*, and asserting
// on the others would be asserting which ones those are, which is a different
// test that already exists. The caller counts the acceptances instead; see
// [exercised].
func requireSourceRoundTrip(t *testing.T, source []byte) bool {
	t.Helper()

	first, _, err := flowfile.Parse(source)
	if err != nil {
		return false
	}
	if diags, err := flowfile.ValidateSource(source); err != nil || len(diags) > 0 {
		return false
	}

	written, err := flowfile.Marshal(first)
	require.NoError(t, err, "a workflow the parser accepted could not be written back")

	// Property one: what was written is a file this build accepts. `flow fmt`
	// writes these bytes over the author's file, so a diagnostic here is
	// formatting a valid file into an invalid one.
	diags, err := flowfile.ValidateSource(written)
	require.NoError(t, err, "the marshalled document does not parse:\n%s", written)
	require.Empty(t, diags, "the marshalled document does not validate:\n%s", written)

	// Property two: it says the same thing. Bytes that parse are not enough,
	// because a value that comes back meaning something else parses perfectly —
	// which is exactly what an unescaped `${` in a description does.
	second, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	require.True(t, proto.Equal(first, second),
		"the workflow changed across a marshal round trip\n--- written ---\n%s\n--- before ---\n%v\n--- after ---\n%v",
		written, first, second)

	// Property three: a fixed point, so `flow fmt` settles.
	again, err := flowfile.Marshal(second)
	require.NoError(t, err)
	require.Equal(t, string(written), string(again), "marshalling twice is not a fixed point")

	return true
}

// FuzzMarshalRoundTrip is the same three properties over a scalar the fuzzer
// chooses rather than one this file lists.
//
// The document around it is fixed and the scalar is the only variable, because
// the property is about scalars: handing a fuzzer whole YAML documents spends
// almost all of its budget on files the parser refuses at the first line, and
// learns nothing about the round trip, which only has something to say once a
// document is accepted.
func FuzzMarshalRoundTrip(f *testing.F) {
	for _, probe := range interpProbes {
		for _, position := range probePositions {
			f.Add(position, probe)
		}
	}

	f.Fuzz(func(t *testing.T, position, probe string) {
		// A probe carrying a newline or a quote is a question about YAML's own
		// escaping rather than about fences, and the harness's own quoting cannot
		// answer it honestly. The round trip for those is covered where block
		// scalars are.
		if strings.ContainsAny(probe, "\n\r\"\\") || !isPrintable(probe) {
			t.Skip()
		}
		source, ok := probeDocument(position, probe)
		if !ok {
			t.Skip()
		}
		requireSourceRoundTrip(t, []byte(source))
	})
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
