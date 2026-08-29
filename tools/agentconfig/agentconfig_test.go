package agentconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const (
	maxAgentsBytes     = 12 << 10
	maxClaudeBytes     = 2 << 10
	maxFieldIndexBytes = 4 << 10
)

type ampPermission struct {
	Tool    string         `json:"tool"`
	Action  string         `json:"action"`
	Matches map[string]any `json:"matches"`
}

func TestGuidanceStaysLayered(t *testing.T) {
	root := repoRoot(t)
	agents := read(t, filepath.Join(root, "AGENTS.md"))
	claude := read(t, filepath.Join(root, "CLAUDE.md"))
	index := read(t, filepath.Join(root, "AGENT_FIELD_NOTES.md"))
	legacy := read(t, filepath.Join(root, "AGENT_FIELD_NOTES_LEGACY.md"))

	if len(agents) > maxAgentsBytes {
		t.Fatalf("AGENTS.md is %d bytes; keep always-loaded guidance under %d", len(agents), maxAgentsBytes)
	}
	if len(claude) > maxClaudeBytes {
		t.Fatalf("CLAUDE.md is %d bytes; keep the Claude adapter under %d", len(claude), maxClaudeBytes)
	}
	if firstNonBlankLine(string(claude)) != "@AGENTS.md" {
		t.Fatal("CLAUDE.md must import AGENTS.md as its first non-blank line")
	}
	if bytes.Contains(agents, []byte("@AGENT_FIELD_NOTES")) || bytes.Contains(claude, []byte("@AGENT_FIELD_NOTES")) {
		t.Fatal("historical field notes must not be imported into always-loaded guidance")
	}
	if len(index) > maxFieldIndexBytes {
		t.Fatalf("AGENT_FIELD_NOTES.md is %d bytes; keep it as an index under %d", len(index), maxFieldIndexBytes)
	}
	if !bytes.Contains(index, []byte("AGENT_FIELD_NOTES_LEGACY.md")) {
		t.Fatal("field-notes index must link to the preserved legacy guidance")
	}
	if len(legacy) <= maxAgentsBytes {
		t.Fatal("legacy guidance no longer looks like the preserved detailed archive")
	}
}

func TestPortableSkillsMirrorClaude(t *testing.T) {
	root := repoRoot(t)
	portableRoot := filepath.Join(root, ".agents", "skills")
	claudeRoot := filepath.Join(root, ".claude", "skills")
	portable := skillNames(t, portableRoot)
	claude := skillNames(t, claudeRoot)

	if fmt.Sprint(portable) != fmt.Sprint(claude) {
		t.Fatalf("skill sets differ:\n.agents: %v\n.claude: %v", portable, claude)
	}
	for _, name := range portable {
		t.Run(name, func(t *testing.T) {
			portableSkill := read(t, filepath.Join(portableRoot, name, "SKILL.md"))
			claudeSkill := read(t, filepath.Join(claudeRoot, name, "SKILL.md"))
			if !bytes.Equal(portableSkill, claudeSkill) {
				t.Fatalf("skill mirrors differ for %s", name)
			}
			meta := frontmatter(t, portableSkill)
			if meta["name"] != name {
				t.Fatalf("frontmatter name %q does not match directory %q", meta["name"], name)
			}
			if strings.TrimSpace(meta["description"]) == "" {
				t.Fatal("skill description is empty")
			}
		})
	}
}

func TestReplacedGuidanceKeepsFieldNotes(t *testing.T) {
	root := repoRoot(t)
	for _, name := range []string{
		"comms-commit", "comms-issue", "comms-pr", "comms-review",
		"comms-session", "flowfile-style", "pre-pr-review",
	} {
		t.Run("skill/"+name, func(t *testing.T) {
			archive := filepath.Join(root, ".agent-history", "skills", name, "SKILL.md")
			if _, err := os.Stat(archive); err != nil {
				t.Fatalf("missing archived skill: %v", err)
			}
			want := "../../../.agent-history/skills/" + name + "/SKILL.md"
			for _, host := range []string{".agents", ".claude"} {
				active := read(t, filepath.Join(root, host, "skills", name, "SKILL.md"))
				if !bytes.Contains(active, []byte(want)) {
					t.Fatalf("%s skill does not link to %s", host, want)
				}
			}
		})
	}

	for _, name := range []string{"both-drivers", "ci-check", "test-fast"} {
		t.Run("command/"+name, func(t *testing.T) {
			if _, err := os.Stat(filepath.Join(root, ".agent-history", "commands", name+".md")); err != nil {
				t.Fatalf("missing archived command: %v", err)
			}
			alias := read(t, filepath.Join(root, ".claude", "commands", name+".md"))
			if len(alias) > 1024 {
				t.Fatalf("compatibility command is %d bytes; keep procedure in a skill", len(alias))
			}
		})
	}
}

func TestAmpSettingsUsePortableSkillsAndGuardWrites(t *testing.T) {
	data := read(t, filepath.Join(repoRoot(t), ".amp", "settings.json"))
	var settings struct {
		DisableClaudeSkills bool            `json:"amp.skills.disableClaudeCodeSkills"`
		Permissions         []ampPermission `json:"amp.permissions"`
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatalf("parse .amp/settings.json: %v", err)
	}
	if !settings.DisableClaudeSkills {
		t.Fatal("Amp must use .agents/skills without also loading Claude mirrors")
	}

	guards := []struct {
		tool        string
		matchKey    string
		matchValue  string
		description string
	}{
		{tool: "Bash", matchKey: "cmd", matchValue: "*git*push*", description: "shell-level git push"},
		{tool: "Bash", matchKey: "cmd", matchValue: "*gh*pr*merge*", description: "GitHub CLI pull-request merge"},
		{tool: "Bash", matchKey: "cmd", matchValue: "*gh*release*", description: "GitHub CLI release mutation"},
		{tool: "mcp__github__merge_pull_request", description: "GitHub MCP pull-request merge"},
		{tool: "Bash", matchKey: "cmd", matchValue: "*git*reset*--hard*", description: "hard git reset"},
		{tool: "Bash", matchKey: "cmd", matchValue: "*git*clean*-f*", description: "forced git clean"},
		{tool: "Bash", matchKey: "cmd", matchValue: "*rm*rf*", description: "recursive forced removal"},
	}
	for _, guard := range guards {
		if !hasAmpPermission(settings.Permissions, guard.tool, "ask", guard.matchKey, guard.matchValue) {
			t.Errorf("Amp permissions must ask before %s", guard.description)
		}
	}
}

func hasAmpPermission(rules []ampPermission, tool, action, matchKey, matchValue string) bool {
	for _, rule := range rules {
		if rule.Tool != tool || rule.Action != action {
			continue
		}
		if matchKey == "" || strings.Contains(fmt.Sprint(rule.Matches[matchKey]), matchValue) {
			return true
		}
	}
	return false
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func firstNonBlankLine(source string) string {
	for _, line := range strings.Split(source, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return ""
}

func skillNames(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, entry.Name(), "SKILL.md")); err != nil {
			t.Fatalf("skill %s has no SKILL.md: %v", entry.Name(), err)
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func frontmatter(t *testing.T, source []byte) map[string]string {
	t.Helper()
	lines := strings.Split(string(source), "\n")
	if len(lines) < 3 || strings.TrimSpace(lines[0]) != "---" {
		t.Fatal("SKILL.md must begin with YAML frontmatter")
	}
	values := map[string]string{}
	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "---" {
			return values
		}
		key, value, ok := strings.Cut(line, ":")
		if ok {
			values[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	t.Fatal("SKILL.md frontmatter is not closed")
	return nil
}
