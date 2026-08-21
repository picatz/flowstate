package flowfile_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// docs/STYLE.md is the style charter (#543, landed by #646) and
// .claude/skills/flowfile-style is its agent-facing companion. Prose is not
// testable, but two things about the pair are, and both are ways the artifact can
// quietly stop being what it claims to be.
//
// The first is that its examples are real. Every positive example is a whole
// Flowfile and is compiled by TestREADMEWorkflowsCompile alongside the README's,
// which is the harness that already existed; nothing here duplicates it. What is
// checked here is the convention that makes that harness safe to point at a
// document teaching by counterexample: a negative example is a fragment, so it is
// never compiled, because some negatives are legal-but-poor-style and would pass
// while others are refused outright, and a checker that cannot tell the halves
// apart either rejects the document or teaches nothing.
//
// The second is that the skill and the document agree. They deliberately are not
// two copies: the document holds the rule text, the skill holds the rule index and
// cites the document for everything else. The index is the part that goes stale,
// because adding or renaming a rule is exactly when nobody remembers the second
// file. So the index is derived from the document here and compared.

// styleGuide is docs/STYLE.md, from this package's directory.
var styleGuide = filepath.Join("..", "..", "..", "..", "docs", "STYLE.md")

// styleSkill is the companion skill's only file.
var styleSkill = filepath.Join("..", "..", "..", "..", ".claude", "skills", "flowfile-style", "SKILL.md")

// styleRuleHeading matches a rule heading in the charter: `### R5. Decompose ...`.
var styleRuleHeading = regexp.MustCompile(`(?m)^### (R([0-9]+)\. .+?)\s*$`)

// skillRuleMention matches any rule number the skill names, so a rule the skill
// claims and the document does not have is caught as well as the reverse.
var skillRuleMention = regexp.MustCompile(`\bR([0-9]+)\.`)

// negativeExample is how the charter marks an example that must not be copied.
const negativeExample = "# Not this"

// fencedYAML matches the contents of every ```yaml block, which is where an
// example lives and prose about examples does not.
//
// Looking for the marker anywhere in the document instead is the failure this
// test exists to catch, wearing the test's own clothes: the conventions section
// names `# Not this:` in a sentence, so a search over the whole file stays green
// after every negative example has been deleted. Codex found that on #849, and it
// is the "green by not running" shape CLAUDE.md legislates against.
var fencedYAML = regexp.MustCompile("(?s)```yaml\n(.*?)```")

// TestStyleGuideShowsBothKinds holds up the convention the compile harness
// depends on: positive examples are whole Flowfiles, negative ones are fragments.
//
// Without this, the document could lose its examples entirely and every check
// over it would still pass, which is the "green by not running" shape CLAUDE.md
// legislates against; or a negative example could be written as a whole file and
// be compiled as though it were something to copy.
func TestStyleGuideShowsBothKinds(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(styleGuide)
	require.NoError(t, err, "docs/STYLE.md moved and this test did not")
	source := string(data)

	blocks := completeWorkflow.FindAllStringSubmatch(source, -1)
	assert.NotEmpty(t, blocks,
		"docs/STYLE.md shows no complete workflow; either it lost its positive examples or the pattern stopped matching them")

	for i, block := range blocks {
		assert.NotContains(t, block[1], negativeExample,
			"docs/STYLE.md positive example %d is marked %q: a negative example must be a fragment, "+
				"or the compile harness will present it as something to copy", i+1, negativeExample)
	}

	var negatives int
	for _, block := range fencedYAML.FindAllStringSubmatch(source, -1) {
		if strings.Contains(block[1], negativeExample) {
			negatives++
		}
	}
	assert.NotZero(t, negatives,
		"no fenced yaml block in docs/STYLE.md is marked %q; the charter teaches by \"do not write this, "+
			"write that\", and half of that is missing. Prose naming the marker does not count: the "+
			"conventions section names it, so a search over the whole document would pass with every "+
			"negative example deleted", negativeExample)
}

// TestStyleSkillIndexMatchesTheCharter is the pin between the two artifacts.
//
// It fails when a rule is added to the charter and not to the skill's index, when
// a rule is renamed on one side only, and when the skill names a rule the charter
// does not have. It deliberately says nothing about the rule *text*, which lives
// in one place and so cannot disagree with itself.
func TestStyleSkillIndexMatchesTheCharter(t *testing.T) {
	t.Parallel()

	guide, err := os.ReadFile(styleGuide)
	require.NoError(t, err, "docs/STYLE.md moved and this test did not")

	skill, err := os.ReadFile(styleSkill)
	require.NoError(t, err, "the flowfile-style skill moved and this test did not")

	headings := styleRuleHeading.FindAllStringSubmatch(string(guide), -1)
	require.NotEmpty(t, headings,
		"docs/STYLE.md carries no `### R<n>.` rule headings; either the charter lost its rules or they are spelled differently now")

	charter := map[string]bool{}
	for _, heading := range headings {
		full, number := heading[1], heading[2]
		charter[number] = true

		assert.Contains(t, string(skill), full,
			"the flowfile-style skill does not name rule R%s as the charter spells it (%q); "+
				"the index is the one thing the two files share, so add or correct the row", number, full)
	}

	for _, mention := range skillRuleMention.FindAllStringSubmatch(string(skill), -1) {
		number := mention[1]
		assert.True(t, charter[number],
			"the flowfile-style skill names rule R%s, which docs/STYLE.md does not have; "+
				"a rule that exists only in the skill is a rule nobody can read", number)
	}
}

// TestStyleGuideLinksAreReachable checks the relative links this pair points at,
// because the two files exist to be navigated between and a dead link between them
// is the failure that makes an agent reason from memory instead.
func TestStyleGuideLinksAreReachable(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		dir    string
	}{
		{source: styleGuide, dir: filepath.Join("..", "..", "..", "..", "docs")},
		{source: styleSkill, dir: filepath.Join("..", "..", "..", "..", ".claude", "skills", "flowfile-style")},
	} {
		t.Run(filepath.Base(filepath.Dir(test.source))+"/"+filepath.Base(test.source), func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(test.source)
			require.NoError(t, err)

			for _, target := range markdownLinkTargets(string(data)) {
				if strings.HasPrefix(target, "http") || strings.HasPrefix(target, "#") {
					continue
				}
				path, _, _ := strings.Cut(target, "#")
				if path == "" {
					continue
				}
				_, err := os.Stat(filepath.Join(test.dir, path))
				assert.NoError(t, err, "%s links to %q, which is not there", test.source, target)
			}
		})
	}
}

// markdownLinkTargets returns the target of every inline markdown link in source.
func markdownLinkTargets(source string) []string {
	var targets []string
	for _, match := range markdownLink.FindAllStringSubmatch(source, -1) {
		targets = append(targets, match[1])
	}
	return targets
}

// markdownLink matches the target of an inline markdown link.
var markdownLink = regexp.MustCompile(`\]\(([^)\s]+)\)`)
