package flowdebug

// VocabularyEntry is one verb of the command table as the external tests read
// it: every spelling, the fronts that answer it, and what a front that does not
// says instead.
type VocabularyEntry struct {
	Verb      string
	Spellings []string
	Prompt    bool
	Driver    bool
	Autopsy   bool
	Elsewhere string
}

// Vocabulary returns the whole command table.
func Vocabulary() []VocabularyEntry {
	out := make([]VocabularyEntry, 0, len(commands))
	for _, c := range commands {
		out = append(out, VocabularyEntry{
			Verb:      c.verb,
			Spellings: append([]string{c.verb}, c.aliases...),
			Prompt:    c.onFront(frontPrompt),
			Driver:    c.onFront(frontDriver),
			Autopsy:   c.onFront(frontAutopsy),
			Elsewhere: c.elsewhere,
		})
	}

	return out
}
