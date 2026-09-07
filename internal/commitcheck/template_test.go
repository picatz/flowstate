package commitcheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/testkit"
)

// TestThePullRequestTemplateCarriesTheSkillsHeadings keeps the template a
// web-opened PR starts from and the comms-pr skill's default shape one list:
// the six numbered items of the skill are the six `##` headings of the
// template, in order, so neither can drift from the other unnoticed.
func TestThePullRequestTemplateCarriesTheSkillsHeadings(t *testing.T) {
	t.Parallel()

	root := testkit.RepoRoot(t)

	skill, err := os.ReadFile(filepath.Join(root, ".agents", "skills", "comms-pr", "SKILL.md"))
	require.NoError(t, err)
	item := regexp.MustCompile(`(?m)^\d+\. \*\*([^*]+)\*\*`)
	var want []string
	for _, m := range item.FindAllStringSubmatch(string(skill), -1) {
		want = append(want, m[1])
	}
	require.NotEmpty(t, want, "the skill lists no numbered sections; the shape this test pins has moved")

	template, err := os.ReadFile(filepath.Join(root, ".github", "PULL_REQUEST_TEMPLATE.md"))
	require.NoError(t, err)
	var got []string
	for _, line := range strings.Split(string(template), "\n") {
		if strings.HasPrefix(line, "## ") {
			got = append(got, strings.TrimPrefix(line, "## "))
		}
	}

	assert.Equal(t, want, got, "the PR template's headings and the comms-pr skill's sections differ; change both or neither")
}
