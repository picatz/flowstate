package main

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// suggestionMaxDistance is how many single-character edits a typed name may
// be from a real one and still be offered as a correction. Two is generous
// enough to catch a transposition or a dropped letter (`lst` -> `list` is
// one) and tight enough that an unrelated three-letter command does not show
// up beside it.
const suggestionMaxDistance = 2

// maxSuggestions caps how many candidates a did-you-mean line offers.
//
// Cobra's own suggestion machinery (SuggestionsFor, used by findSuggestions)
// returns every command within the distance threshold above, or sharing a
// prefix, in whatever order Commands() holds them: unranked and uncapped.
// For a three-letter typo like `lst` that was six names, "fmt, test, get,
// list, lsp, jwt", one of which ("jwt") shares no letters at all with what
// was typed. A person wants the name they probably meant, not a browse of
// the command tree, so this ranks by distance and keeps only the closest
// couple.
const maxSuggestions = 2

// commandSuggestions ranks parent's immediate subcommands by how close their
// name is to typed, closest first, capped at maxSuggestions.
//
// root.DisableSuggestions is set so cobra never runs this computation itself
// and bakes the result into err.Error(): see execute.go's comment on why
// that text has to stay cobra's own and unchanged, and why the ranked
// version is drawn separately, in this CLI's own voice.
func commandSuggestions(parent *cobra.Command, typed string) []string {
	if utf8.RuneCountInString(typed) > maxSuggestionInput {
		return nil
	}

	type candidate struct {
		name     string
		distance int
	}

	var candidates []candidate
	for _, cmd := range parent.Commands() {
		if !cmd.IsAvailableCommand() {
			continue
		}

		name := cmd.Name()
		distance := levenshtein(strings.ToLower(typed), strings.ToLower(name))
		if distance > suggestionMaxDistance && !strings.HasPrefix(strings.ToLower(name), strings.ToLower(typed)) {
			continue
		}

		candidates = append(candidates, candidate{name: name, distance: distance})
	}

	return rankedNames(candidates, func(c candidate) (string, int) { return c.name, c.distance })
}

// flagSuggestions ranks cmd's own flags (local and inherited, already merged
// by the time a ParseFlags error reaches [flagErrorFunc]) by how close their
// name is to typed, closest first, capped at maxSuggestions.
func flagSuggestions(cmd *cobra.Command, typed string) []string {
	if utf8.RuneCountInString(typed) > maxSuggestionInput {
		return nil
	}

	type candidate struct {
		name     string
		distance int
	}

	var candidates []candidate
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Hidden || f.Deprecated != "" {
			return
		}

		distance := levenshtein(typed, f.Name)
		if distance > suggestionMaxDistance {
			return
		}

		candidates = append(candidates, candidate{name: f.Name, distance: distance})
	})

	return rankedNames(candidates, func(c candidate) (string, int) { return c.name, c.distance })
}

// rankedNames sorts candidates by ascending distance, then name, and returns
// at most maxSuggestions of their names.
//
// Generic over the candidate shape so the two callers above, one ranking
// commands and one ranking flags, share the sort and cap instead of each
// carrying its own copy that could drift out of step with the other.
func rankedNames[T any](candidates []T, key func(T) (name string, distance int)) []string {
	sort.Slice(candidates, func(i, j int) bool {
		ni, di := key(candidates[i])
		nj, dj := key(candidates[j])
		if di != dj {
			return di < dj
		}
		return ni < nj
	})

	if len(candidates) > maxSuggestions {
		candidates = candidates[:maxSuggestions]
	}

	names := make([]string, len(candidates))
	for i, c := range candidates {
		names[i], _ = key(c)
	}
	return names
}

// maxSuggestionInput bounds the typed name a suggestion is computed for. The
// candidates are this CLI's own short names, but the typo arrives from a
// command line a script may have assembled, and the distance scan below is
// work proportional to its length times every candidate: input an outside
// party sizes gets a bound matched to what it can spend (CLAUDE.md). Nothing
// within two edits of a real name can be longer than the longest name plus
// two, so refusing long input costs no suggestion anybody could have earned.
const maxSuggestionInput = 64

// levenshtein returns the edit distance between a and b: the fewest single
// character insertions, deletions, and substitutions that turn one into the
// other. Callers bound the typed side by [maxSuggestionInput]; candidate
// names bound themselves.
func levenshtein(a, b string) int {
	ar, br := []rune(a), []rune(b)

	prev := make([]int, len(br)+1)
	curr := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(ar); i++ {
		curr[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			curr[j] = min(prev[j]+1, curr[j-1]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}

	return prev[len(br)]
}

// suggestedError marks err as carrying ranked candidate corrections, so
// renderError draws them as its own styled element rather than them arriving
// baked into err.Error() in cobra's or pflag's own voice.
//
// Wraps a [usageError] rather than the raw cobra/pflag error, so isUsageError
// still classifies it correctly through the Unwrap chain: a mistyped
// command or flag is exactly the invocation-mistake case that function
// exists to mark. Error and Unwrap both forward, so the text a person or a
// script reads never includes the suggestion prose; only renderError adds
// that, and only on the stream meant for a person.
type suggestedError struct {
	err error

	// spelling turns a bare candidate name into what an author would type:
	// "flow " prepended for a command, "--" for a flag, so the did-you-mean
	// line reads as a full answer rather than a name with no context.
	spelling   func(name string) string
	candidates []string
}

func (e *suggestedError) Error() string { return e.err.Error() }
func (e *suggestedError) Unwrap() error { return e.err }

// didYouMean is the styled line renderError prints under an error that
// carries ranked candidates, or "" when there are none to show.
func didYouMean(err error) string {
	var suggested *suggestedError
	if !errors.As(err, &suggested) || len(suggested.candidates) == 0 {
		return ""
	}

	spelled := make([]string, 0, len(suggested.candidates))
	for _, name := range suggested.candidates {
		spelled = append(spelled, "`"+suggested.spelling(name)+"`")
	}

	if len(spelled) == 1 {
		return fmt.Sprintf("did you mean %s?", spelled[0])
	}

	return fmt.Sprintf("did you mean %s or %s?", spelled[0], spelled[1])
}

// flagErrorFunc reports an unknown or misspelled flag with the closest match
// among cmd's own flags, rather than pflag's bare "unknown flag: --adress"
// with nothing pointing at "--address".
//
// Registered once on the root command: cobra's FlagErrorFunc walks up to the
// nearest ancestor that set one, so every subcommand inherits this without
// declaring its own.
func flagErrorFunc(cmd *cobra.Command, err error) error {
	marked := newUsageError(err)

	var notExist *pflag.NotExistError
	if !errors.As(err, &notExist) {
		return marked
	}

	// Shorthand flags are a single character, so "closest match" is not a
	// question worth asking: every other single-character flag is
	// equidistant. Only the long form gets a suggestion.
	if notExist.GetSpecifiedShortnames() != "" {
		return marked
	}

	candidates := flagSuggestions(cmd, notExist.GetSpecifiedName())
	if len(candidates) == 0 {
		return marked
	}

	return &suggestedError{
		err:        marked,
		spelling:   func(name string) string { return "--" + name },
		candidates: candidates,
	}
}

// commandSuggestionError reports an unknown top-level command with the
// closest match among root's own subcommands, rather than cobra's own
// unranked "Did you mean this?" block, which [newRootCommand] turns off
// with DisableSuggestions so this is the only source of one.
//
// typed and cmdPath are parsed back out of cobra's own error text
// (`unknown command %q for %q`) rather than threaded through as separate
// values, because [execute] only has the error ExecuteContextC returned to
// work from: Find's internal call to legacyArgs is not a hook this package
// can reach. The format is fixed by cobra and already pinned by
// TestCobraUsageErrorsMatchIsUsageError; a cobra upgrade that reworded it
// leaves this simply finding no suggestions, never a wrong one, since a
// failed parse returns "" as unquoted and the search below then matches
// nothing.
func commandSuggestionError(root *cobra.Command, err error) error {
	marked := newUsageError(err)

	typed, cmdPath, ok := parseUnknownCommandError(err.Error())
	if !ok {
		return marked
	}

	// cmdPath is the command whose subcommands were being searched, root
	// itself for every case reachable today, since legacyArgs only runs
	// against the command with no parent. Matched by CommandPath rather than
	// assumed, so a future nested case that reused this wording still finds
	// the right subcommand list instead of silently searching root's.
	target := root
	if root.CommandPath() != cmdPath {
		if found, _, findErr := root.Find(strings.Fields(cmdPath)[1:]); findErr == nil {
			target = found
		}
	}

	candidates := commandSuggestions(target, typed)
	if len(candidates) == 0 {
		return marked
	}

	prefix := target.CommandPath() + " "
	return &suggestedError{
		err:        marked,
		spelling:   func(name string) string { return prefix + name },
		candidates: candidates,
	}
}

// parseUnknownCommandError reverses cobra's `unknown command %q for %q`
// formatting, returning the two quoted values and whether the text matched
// that shape at all.
func parseUnknownCommandError(text string) (typed, cmdPath string, ok bool) {
	const prefix = "unknown command "
	if !strings.HasPrefix(text, prefix) {
		return "", "", false
	}
	rest := text[len(prefix):]

	const sep = " for "
	i := strings.Index(rest, sep)
	if i < 0 {
		return "", "", false
	}
	typedQuoted, cmdPathQuoted := rest[:i], rest[i+len(sep):]

	typed, err := strconv.Unquote(typedQuoted)
	if err != nil {
		return "", "", false
	}
	cmdPath, err = strconv.Unquote(cmdPathQuoted)
	if err != nil {
		return "", "", false
	}

	return typed, cmdPath, true
}
