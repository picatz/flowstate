package agentconfig

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var styleRuleHeading = regexp.MustCompile(`^### (R[0-9]+\..+)$`)

func TestStyleSkillIndexMatchesTheCharter(t *testing.T) {
	root := repoRoot(t)
	charter := read(t, filepath.Join(root, "docs", "STYLE.md"))
	skill := read(t, filepath.Join(root, ".agents", "skills", "flowfile-style", "SKILL.md"))

	want := styleRuleHeadings(string(charter))
	got := styleSkillRules(t, string(skill))
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("flowfile-style rule index differs from docs/STYLE.md:\n got: %q\nwant: %q", got, want)
	}
}

func styleRuleHeadings(source string) []string {
	var headings []string
	for _, line := range strings.Split(source, "\n") {
		match := styleRuleHeading.FindStringSubmatch(strings.TrimSpace(line))
		if match != nil {
			headings = append(headings, match[1])
		}
	}
	return headings
}

func styleSkillRules(t *testing.T, source string) []string {
	t.Helper()
	section, ok := markdownSection(source, "## Rule index")
	if !ok {
		t.Fatal("flowfile-style skill has no Rule index section")
	}

	var rules []string
	for _, line := range strings.Split(section, "\n") {
		cells := strings.Split(line, "|")
		if len(cells) != 4 {
			continue
		}
		rule := strings.TrimSpace(cells[2])
		if styleRuleHeading.MatchString("### " + rule) {
			rules = append(rules, rule)
		}
	}
	return rules
}

func markdownSection(source, heading string) (string, bool) {
	start := strings.Index(source, heading+"\n")
	if start < 0 {
		return "", false
	}
	start += len(heading) + 1
	rest := source[start:]
	if end := strings.Index(rest, "\n## "); end >= 0 {
		rest = rest[:end]
	}
	return rest, true
}
