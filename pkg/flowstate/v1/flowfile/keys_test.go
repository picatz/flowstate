package flowfile

import (
	"fmt"
	"slices"
	"testing"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a mapping key may be is a question two passes ask, and they must give one
// answer: [compiler.entries] decides which names a mapping claims for itself
// using keyNameOf, and then reads the same keys again through compiler.keyName.
// A name the first pass fails to see is a name a merged mapping silently
// overrides — a wrong value with no diagnostic, which is the worst shape a bug
// can take in a config language.
//
// keyName is keyNameOf plus a diagnostic for that reason. This test pins the
// vocabulary both of them work over, because the divergence they used to have was
// a branch for a node type the parser never produces: unreachable, and so
// invisible to every test that went through the parser.
func TestKeyNodeVocabulary(t *testing.T) {
	t.Parallel()

	// Every way a key can be spelled, and the node the parser makes of it. A
	// change here is a change in what the DSL accepts, which is why the mapping is
	// written out rather than derived.
	tests := []struct {
		name   string
		src    string
		want   string // the AST node type the parser produces for the key
		spells string // the name keyNameOf reads from it, empty when it reads none
	}{
		{name: "plain", src: "a: 1\n", want: "*ast.StringNode", spells: "a"},
		{name: "double quoted", src: "\"a\": 1\n", want: "*ast.StringNode", spells: "a"},
		{name: "single quoted", src: "'a': 1\n", want: "*ast.StringNode", spells: "a"},
		{name: "number", src: "1: 1\n", want: "*ast.IntegerNode"},
		{name: "boolean", src: "true: 1\n", want: "*ast.BoolNode"},
		{name: "null", src: "~: 1\n", want: "*ast.NullNode"},
		{name: "merge", src: "<<: *x\n", want: "*ast.MergeKeyNode"},
		{name: "anchored", src: "&k a: 1\n", want: "*ast.AnchorNode"},
		{name: "aliased", src: "*k : 1\n", want: "*ast.AliasNode"},

		// Every explicit key arrives wrapped, whatever is inside it. This is the
		// row that matters: a block scalar cannot open a mapping key, so a key is
		// never a *ast.LiteralNode, and a keyNameOf that handled one would be
		// handling a shape that cannot occur.
		{name: "explicit plain", src: "? a\n: 1\n", want: "*ast.MappingKeyNode"},
		{name: "explicit block scalar", src: "? |-\n  a\n: 1\n", want: "*ast.MappingKeyNode"},
		{name: "explicit folded scalar", src: "? >-\n  a\n: 1\n", want: "*ast.MappingKeyNode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := parser.ParseBytes([]byte(tt.src), 0)
			require.NoError(t, err, "this key spelling should parse as YAML")
			require.NotEmpty(t, file.Docs)

			var key ast.Node
			switch body := file.Docs[0].Body.(type) {
			case *ast.MappingNode:
				require.Len(t, body.Values, 1)
				key = body.Values[0].Key
			case *ast.MappingValueNode:
				key = body.Key
			default:
				t.Fatalf("document body is %T, want a mapping", body)
			}

			assert.Equal(t, tt.want, fmt.Sprintf("%T", key))

			name, ok := keyNameOf(key)
			if tt.spells == "" {
				assert.False(t, ok, "keyNameOf must not read a name from %s", tt.want)
				return
			}
			assert.True(t, ok)
			assert.Equal(t, tt.spells, name)
		})
	}
}

// TestKeyNamePlusDiagnosticMatchesKeyNameOf is the property the two functions
// have to hold: keyName accepts exactly what keyNameOf accepts, and reports
// everything else.
//
// Asserted over the vocabulary above rather than over the two implementations,
// because two switches agreeing today is what the previous version also looked
// like.
func TestKeyNamePlusDiagnosticMatchesKeyNameOf(t *testing.T) {
	t.Parallel()

	srcs := []string{
		"a: 1\n", "\"a\": 1\n", "'a': 1\n", "1: 1\n", "true: 1\n", "~: 1\n",
		"&k a: 1\n", "? a\n: 1\n", "? |-\n  a\n: 1\n",
	}
	for _, src := range srcs {
		file, err := parser.ParseBytes([]byte(src), 0)
		require.NoError(t, err)
		var key ast.Node
		switch body := file.Docs[0].Body.(type) {
		case *ast.MappingNode:
			key = body.Values[0].Key
		case *ast.MappingValueNode:
			key = body.Key
		}

		quiet, quietOK := keyNameOf(key)

		c := &compiler{pos: newPositions()}
		loud, loudOK := c.keyName(key, ref{})

		assert.Equal(t, quietOK, loudOK, "the two disagree about whether %q has a name", src)
		assert.Equal(t, quiet, loud, "the two read different names from %q", src)
		if !loudOK {
			assert.NotEmpty(t, c.diags, "a key with no name must be reported: %q", src)
		}
	}
}

// TestExplicitKeyIsReportedInTheAuthorsWords covers the message itself. "a
// mappingkey was written here" is the parser's word for it, and it says keys must
// be strings about a key that is one.
func TestExplicitKeyIsReportedInTheAuthorsWords(t *testing.T) {
	t.Parallel()

	_, _, err := Parse([]byte("name: t\nsteps:\n  - id: a\n    ? echo\n    : {}\n"))
	require.Error(t, err)

	var ds Diagnostics
	require.ErrorAs(t, err, &ds)
	assert.Contains(t, ds.Error(), "an explicit key (`? key` on its own line) is not written here; use `key: value`")
	assert.NotContains(t, ds.Error(), "mappingkey")
}

// The step grammar's vocabulary is written down twice, and this is what holds the
// two copies together.
//
// `flowfile` needs the keys split by meaning and in the order it reports them;
// `flowstatev1` needs one flat set, because [flowstatev1.Registry.Register]
// consults it and a registry cannot import a parser. Neither list can be derived
// from the other, so both are hand-written — and the comment on `stepKeys` already
// claims `ReservedStepKeys` keeps the two halves disjoint, which nothing checked.
//
// What breaks if they drift is not the parser. It is that a key the grammar uses
// stays available as a *task name*, so a plugin can register `handlers` on the day
// `handlers:` becomes a step key, and `handlers:` on a step then means two
// incompatible things with no way for a parser to choose. The failure lands on a
// Flowfile author, months later, as a step that did something else.

// grammarVocabulary is every word the step grammar speaks for: the keys it accepts
// today, and the spellings it still recognizes in order to refuse them.
//
// The retired spellings belong here, and leaving them out was a real hole rather
// than a tidiness point. `task` is not a key any file may use, so it looks like it
// needs no reservation — but the parser rejects it *by name*, so a plugin
// registering a task called `task` gets a registered task no Flowfile can reach,
// and an author who writes `task:` is told about a retired spelling rather than
// about the plugin they installed. Sourced from `retiredStepKeys` rather than
// listed, so retiring a second spelling is covered the day it happens.
func grammarVocabulary() []string {
	words := append(slices.Clone(stepPropertyKeys), nodeKindKeys...)
	for retired := range retiredStepKeys {
		words = append(words, retired)
	}
	return words
}

// TestEveryGrammarKeyIsReserved fails if the parser learns a key the registry will
// still hand out.
func TestEveryGrammarKeyIsReserved(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, retiredStepKeys,
		"grammarVocabulary draws its retired half from retiredStepKeys; empty means this checks less than it reads as")

	reserved := v1.ReservedStepKeys()
	for _, key := range grammarVocabulary() {
		assert.Contains(t, reserved, key,
			"the step grammar speaks for %q and `v1.ReservedStepKeys()` does not list it\n"+
				"  add it to grammarStepKeys in pkg/flowstate/v1/stepkeys.go\n"+
				"  until then a task may be registered under that name, and `%s:` on a step is ambiguous or unreachable",
			key, key)
	}
}

// TestReservedKeysAreEitherGrammarOrFuture is the other direction.
//
// A word in the reserved set that the grammar does not use and no plan claims is a
// task name taken from a plugin author for no reason. The two groups exist so that
// question has an answer for every entry, and this checks that none has fallen
// outside both.
func TestReservedKeysAreEitherGrammarOrFuture(t *testing.T) {
	t.Parallel()

	grammar := grammarVocabulary()

	for _, key := range v1.ReservedStepKeys() {
		if slices.Contains(grammar, key) || v1.IsFutureStepKey(key) {
			continue
		}
		t.Errorf("%q is reserved but is neither current grammar nor held for later\n"+
			"  every reserved word costs a plugin author a name, so each one needs a reason\n"+
			"  either the grammar uses it, or it belongs in futureStepKeys, or it should not be reserved",
			key)
	}
}

// TestAFutureKeyIsNotAlsoGrammar keeps the two groups from overlapping.
//
// A word in both is a contradiction the diagnostics act on: the parser reports a
// future key as unbuilt and holds it back, so a key that is genuinely grammar and
// also listed as future would be refused on every file that uses it correctly.
func TestAFutureKeyIsNotAlsoGrammar(t *testing.T) {
	t.Parallel()

	for _, key := range grammarVocabulary() {
		assert.False(t, v1.IsFutureStepKey(key),
			"%q is spoken for by the grammar and also reserved for later; "+
				"the parser reports a future key as unbuilt, so a step using it correctly would be refused", key)
	}
}

// TestAFutureKeyIsReportedAsUnbuiltRatherThanUnknown covers what an author reads.
//
// `vars:` on a step and `varz:` on a step are not the same mistake, and the
// generic key check cannot tell them apart: it offers the nearest known key and
// then lists the rest, which for a reserved word describes a typo the author did
// not make and sends them looking for one that is not there.
//
// The word is held back from that check for the same reason a retired spelling
// is — so the message about it is the only thing said about it.
func TestAFutureKeyIsReportedAsUnbuiltRatherThanUnknown(t *testing.T) {
	t.Parallel()

	_, err := Unmarshal([]byte("name: t\nsteps:\n  - id: a\n    vars:\n      x: 1\n    echo:\n      message: hi\n"))
	require.Error(t, err, "`vars:` is not a step key in this build and was accepted")

	message := err.Error()
	assert.Contains(t, message, "reserved for a later version of the grammar",
		"a reserved word is reported without saying it is reserved")
	assert.NotContains(t, message, "unknown key",
		"a reserved word is reported as unknown, which reads as a misspelling")
	assert.NotContains(t, message, "did you mean",
		"a reserved word is offered a spelling correction it does not need")
}

// TestAMisspelledKeyStillGetsItsSuggestion is the other direction, and the reason
// it is worth writing: holding words back from the key check could have suppressed
// the suggestion for everything, and the test above would still pass.
func TestAMisspelledKeyStillGetsItsSuggestion(t *testing.T) {
	t.Parallel()

	_, err := Unmarshal([]byte("name: t\nsteps:\n  - id: a\n    timeut: 5s\n    echo:\n      message: hi\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `did you mean "timeout"?`,
		"a genuine misspelling lost its suggestion")
}

// TestAFutureKeyReadsTheSameWhereverItIsWritten covers the positions, not the word.
//
// `vars` is reserved for a block the design places at three levels — the
// workflow, the step, and a loop body — and the first version of this reported it
// only on a step, because that is where the key check for steps lives. So the
// workflow level, which is the first place an author would try it, still answered
// `unknown key "vars"`.
//
// One word reported two ways depending on which line it is on is the tool
// disagreeing with itself, and worse than either message alone: an author who
// moves the block up one level to see if that is the trick gets told it is a
// different kind of wrong.
func TestAFutureKeyReadsTheSameWhereverItIsWritten(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"workflow": "name: t\nvars:\n  x: 1\nsteps:\n  - id: a\n    echo:\n      message: hi\n",
		"step":     "name: t\nsteps:\n  - id: a\n    vars:\n      x: 1\n    echo:\n      message: hi\n",
		"loop body": "name: t\nsteps:\n  - id: loop\n    for_each:\n      items: [1, 2]\n      steps:\n" +
			"        - id: inner\n          vars:\n            x: 1\n          echo:\n            message: hi\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := Unmarshal([]byte(src))
			require.Error(t, err, "`vars:` is not grammar in this build and was accepted at the %s level", name)

			message := err.Error()
			assert.Contains(t, message, "reserved for a later version of the grammar",
				"at the %s level a reserved word is reported without saying it is reserved", name)
			assert.NotContains(t, message, "unknown key",
				"at the %s level a reserved word is reported as unknown, which reads as a misspelling", name)
		})
	}
}

// TestAnUnknownWorkflowKeyIsStillUnknown is the other direction at the level the
// fix reached last.
//
// Running the reserved check over workflow keys could have swallowed genuine
// unknown keys there, and every test above would still pass.
func TestAnUnknownWorkflowKeyIsStillUnknown(t *testing.T) {
	t.Parallel()

	_, err := Unmarshal([]byte("name: t\nnonsense: 1\nsteps:\n  - id: a\n    echo:\n      message: hi\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown key "nonsense"`,
		"a genuinely unknown workflow key stopped being reported")
}
