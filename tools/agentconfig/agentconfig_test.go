package agentconfig

import (
	"bytes"
	"crypto/sha1"
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

// preservedGuidanceBlobs pins the exact bytes migrated from the base tree.
// These are Git blob object IDs: SHA-1("blob <size>\x00" + file bytes). They are
// provenance fingerprints, not a cryptographic security mechanism.
var preservedGuidanceBlobs = []struct {
	path   string
	blobID string
}{
	{path: "AGENT_FIELD_NOTES_LEGACY.md", blobID: "1c89ae496958a04fdf5a863e97c07a183f7f4eb2"},
	{path: ".agent-history/commands/both-drivers.md", blobID: "9e6c4035e3fcf6fdc3b5882b18e7c97c0b13b7eb"},
	{path: ".agent-history/commands/ci-check.md", blobID: "0caf1ef08cf2ec870116d4fc1ff8357a25a4f6f7"},
	{path: ".agent-history/commands/test-fast.md", blobID: "6313525eb392a4893459c2cf5c18dad3b703ea80"},
	{path: ".agent-history/skills/comms-commit/SKILL.md", blobID: "1d0dcaf8ddf66d055c1bb18c1b3464a5be7e71ff"},
	{path: ".agent-history/skills/comms-issue/SKILL.md", blobID: "7a62615da7475184b4094033ec30c5f6798cdda8"},
	{path: ".agent-history/skills/comms-pr/SKILL.md", blobID: "ab0c5cae5fd9d3a7a09572b65d8a83177e6214c3"},
	{path: ".agent-history/skills/comms-review/SKILL.md", blobID: "15e47d71407da1391fccac53771557179986a04a"},
	{path: ".agent-history/skills/comms-session/SKILL.md", blobID: "4743327c120ecdca22439df06de97b20315e438a"},
	{path: ".agent-history/skills/flowfile-style/SKILL.md", blobID: "fbc8e68093120fe3c1905a4af9e38be91a42df67"},
	{path: ".agent-history/skills/pre-pr-review/SKILL.md", blobID: "1bd12a66c9f0831a7075837a8c7c14b2f4094a48"},
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

func TestArchivedGuidancePreservesMigratedBytes(t *testing.T) {
	root := repoRoot(t)
	for _, archived := range preservedGuidanceBlobs {
		t.Run(archived.path, func(t *testing.T) {
			data := read(t, filepath.Join(root, filepath.FromSlash(archived.path)))
			if got := gitBlobID(data); got != archived.blobID {
				t.Fatalf("archived guidance changed bytes: got Git blob %s, want %s", got, archived.blobID)
			}
		})
	}
}

func TestGitBlobIDNormalizesCheckoutLineEndings(t *testing.T) {
	lf := []byte("first\nsecond\n")
	crlf := []byte("first\r\nsecond\r\n")
	if got, want := gitBlobID(crlf), gitBlobID(lf); got != want {
		t.Fatalf("CRLF checkout hashed as %s; want canonical LF blob %s", got, want)
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

func TestAmpSettingsUsePortableSkillsWithoutRepositoryPermissionPrompts(t *testing.T) {
	data := read(t, filepath.Join(repoRoot(t), ".amp", "settings.json"))
	var settings struct {
		DisableClaudeSkills bool              `json:"amp.skills.disableClaudeCodeSkills"`
		Permissions         []json.RawMessage `json:"amp.permissions"`
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatalf("parse .amp/settings.json: %v", err)
	}
	if !settings.DisableClaudeSkills {
		t.Fatal("Amp must use .agents/skills without also loading Claude mirrors")
	}
	if len(settings.Permissions) != 0 {
		t.Fatalf("Flowstate must not add repository-specific Amp approval prompts; got %d permission rules", len(settings.Permissions))
	}
}

// gitBlobID returns the Git blob identity of canonical repository text. Git may
// materialize text files with CRLF on some worktrees; the pinned repository blobs
// use LF, so normalize the checkout representation before constructing the blob.
func gitBlobID(data []byte) string {
	data = bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))
	h := sha1.New()
	_, _ = h.Write([]byte(fmt.Sprintf("blob %d\x00", len(data))))
	_, _ = h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
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
