package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLevenshteinKnownDistances pins the edit-distance function against a few
// hand-checked pairs, including the zero and empty-string edges the DP table
// has to get right at its boundary.
func TestLevenshteinKnownDistances(t *testing.T) {
	for _, test := range []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"", "abc", 3},
		{"abc", "", 3},
		{"list", "list", 0},
		{"lst", "list", 1},
		{"validte", "validate", 1},
		{"kitten", "sitting", 3},
		{"adress", "address", 1},
	} {
		assert.Equal(t, test.want, levenshtein(test.a, test.b), "levenshtein(%q, %q)", test.a, test.b)
	}
}

// TestCommandSuggestionsRanksTheCloseNameAboveTheFarOne is the positive
// direction #372 asks for: a near-miss surfaces the command it was probably
// meant to be, ranked ahead of a more distant match, and capped rather than
// dumping the whole tree.
func TestCommandSuggestionsRanksTheCloseNameAboveTheFarOne(t *testing.T) {
	root := newRootCommand()

	got := commandSuggestions(root, "lst")
	require.NotEmpty(t, got, "expected at least one candidate for \"lst\"")
	assert.Equal(t, "list", got[0], "the one-edit match should rank ahead of anything further")
	assert.LessOrEqual(t, len(got), maxSuggestions, "more candidates were offered than maxSuggestions allows")
}

// TestCommandSuggestionsEmptyForGarbage is the negative direction: a typo
// that resembles nothing in the tree gets no suggestion rather than an
// invented one.
func TestCommandSuggestionsEmptyForGarbage(t *testing.T) {
	root := newRootCommand()

	got := commandSuggestions(root, "zzzzzqqqq123")
	assert.Empty(t, got, "a name sharing nothing with the command tree should suggest nothing")
}

// TestFlagSuggestionsFindsTheNearMiss is #372's flag half: --adress is one
// edit from --address on a command that declares it.
func TestFlagSuggestionsFindsTheNearMiss(t *testing.T) {
	root := newRootCommand()
	root.SetArgs([]string{"list"})
	listCmd, _, err := root.Find([]string{"list"})
	require.NoError(t, err)

	got := flagSuggestions(listCmd, "adress")
	require.NotEmpty(t, got, "expected --address to be suggested for --adress")
	assert.Equal(t, "address", got[0])
}

// TestFlagSuggestionsEmptyForGarbage is the negative direction for flags.
func TestFlagSuggestionsEmptyForGarbage(t *testing.T) {
	root := newRootCommand()
	listCmd, _, err := root.Find([]string{"list"})
	require.NoError(t, err)

	got := flagSuggestions(listCmd, "zzzzzqqqq123")
	assert.Empty(t, got)
}

// TestDidYouMeanFormatsOneAndTwoCandidates pins the exact prose renderError
// draws, since that text is this CLI's own voice rather than cobra's or
// pflag's: a reader should never see "Did you mean this?" or a bare name
// with no `flow ` in front of it.
func TestDidYouMeanFormatsOneAndTwoCandidates(t *testing.T) {
	one := &suggestedError{
		err:        errors.New("unknown flag: --adress"),
		spelling:   func(name string) string { return "--" + name },
		candidates: []string{"address"},
	}
	assert.Equal(t, "did you mean `--address`?", didYouMean(one))

	two := &suggestedError{
		err:        errors.New(`unknown command "lst" for "flow"`),
		spelling:   func(name string) string { return "flow " + name },
		candidates: []string{"list", "lsp"},
	}
	assert.Equal(t, "did you mean `flow list` or `flow lsp`?", didYouMean(two))

	assert.Equal(t, "", didYouMean(errors.New("unknown command \"zzz\" for \"flow\"")),
		"an error carrying no suggestedError should draw no line at all")
}

// TestSuggestedErrorPreservesTheOriginalMessage is the same verbatim
// guarantee [TestNewUsageErrorMarksWithoutChangingTheMessage] pins for
// newUsageError, extended to the wrapper renderError draws its did-you-mean
// line from: the text a person or a script reads must stay exactly what
// cobra or pflag wrote, with the suggestion drawn as a separate line rather
// than appended into the message itself.
func TestSuggestedErrorPreservesTheOriginalMessage(t *testing.T) {
	const text = `unknown command "lst" for "flow"`

	wrapped := &suggestedError{
		err:        newUsageError(errors.New(text)),
		spelling:   func(name string) string { return "flow " + name },
		candidates: []string{"list"},
	}

	assert.Equal(t, text, wrapped.Error(), "suggestedError changed the message it was given")
	assert.True(t, isUsageError(wrapped), "a suggestedError lost its usage-error classification")
}

// TestAnImplausiblyLongTypoGetsNoScan pins the input bound: a typed name
// longer than any command or flag this tree could plausibly hold is refused
// before the edit-distance scan, because the cost of that scan grows with
// the product of the two strings and the typed side arrives from argv.
func TestAnImplausiblyLongTypoGetsNoScan(t *testing.T) {
	root := newRootCommand()
	listCmd, _, err := root.Find([]string{"list"})
	require.NoError(t, err)

	typed := strings.Repeat("x", 64*1024)

	assert.Nil(t, commandSuggestions(root, typed),
		"a 64KB argument should be refused before the scan, not scanned")
	assert.Nil(t, flagSuggestions(listCmd, typed),
		"the flag half carries the same bound")
}
