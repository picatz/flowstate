package agentconfig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The Claude-only layers that sit beside the mirrored skills: subagents that
// carry the repository's review and verification rubrics into a fresh
// context, path-scoped rules that load a procedure only while its files are
// being edited, and the permission allow list that removes prompts for the
// commands the repository itself prescribes. Each is bounded here the way
// AGENTS.md and CLAUDE.md are bounded above, so a layer meant to keep the
// always-loaded contract small cannot quietly become the next large prompt.
const (
	maxSubagentBytes = 4 << 10
	maxRuleBytes     = 2 << 10
)

// subagentTools are the tool names a checked-in subagent may request. Read,
// Grep, Glob, and Bash are what a reviewer or verifier needs; the editing
// tools are listed so a future agent can name them, and the reviewer test
// below is what keeps them out of the reviewer.
var subagentTools = map[string]bool{
	"Read": true, "Grep": true, "Glob": true, "Bash": true,
	"Edit": true, "Write": true, "NotebookEdit": true,
	"Skill": true, "Agent": true, "WebFetch": true, "WebSearch": true,
}

var subagentModels = map[string]bool{
	"inherit": true, "sonnet": true, "opus": true, "haiku": true, "fable": true,
}

// TestClaudeSubagentsCarryRepositoryRubrics checks each subagent under
// .claude/agents: its frontmatter names its file, it says when Claude should
// delegate to it, every tool and model it asks for is one Claude Code
// understands, every skill it preloads exists in the Claude mirror (Claude
// Code skips a missing preloaded skill with only a debug warning, so this
// test is what notices), and its body stays a lightweight guide rather
// than a second contract.
func TestClaudeSubagentsCarryRepositoryRubrics(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, ".claude", "agents")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".md")
		names = append(names, name)
		t.Run(name, func(t *testing.T) {
			data := read(t, filepath.Join(dir, entry.Name()))
			if len(data) > maxSubagentBytes {
				t.Fatalf("subagent is %d bytes; keep it under %d", len(data), maxSubagentBytes)
			}
			meta := frontmatter(t, data)
			if meta["name"] != name {
				t.Fatalf("frontmatter name %q does not match file %q", meta["name"], name)
			}
			if strings.TrimSpace(meta["description"]) == "" {
				t.Fatal("subagent description is empty; it is what Claude delegates on")
			}
			for _, tool := range splitList(meta["tools"]) {
				if !subagentTools[tool] {
					t.Errorf("tool %q is not one Claude Code subagents can be given", tool)
				}
			}
			if model, ok := meta["model"]; ok && !subagentModels[model] {
				t.Errorf("model %q is not a Claude Code model alias", model)
			}
			for _, skill := range splitList(meta["skills"]) {
				if _, err := os.Stat(filepath.Join(root, ".claude", "skills", skill, "SKILL.md")); err != nil {
					t.Errorf("preloads skill %q that does not exist in .claude/skills: %v", skill, err)
				}
			}
		})
	}
	for _, required := range []string{"flowstate-reviewer", "flowstate-verifier", "flowstate-pr-tidy"} {
		if !slices.Contains(names, required) {
			t.Errorf("subagent %q is missing; CLAUDE.md and the ship skill delegate to it", required)
		}
	}
}

// TestClaudeReviewerCannotEdit pins the two mechanisms behind the reviewer's
// independence: the dedicated editing tools are withheld, and the agent runs
// in a throwaway worktree, so a shell command it runs can change only that
// worktree and never the checkout under review. A reviewer that fixes what
// it finds is reviewing its own work.
func TestClaudeReviewerCannotEdit(t *testing.T) {
	meta := frontmatter(t, read(t, filepath.Join(repoRoot(t), ".claude", "agents", "flowstate-reviewer.md")))
	if meta["isolation"] != "worktree" {
		t.Errorf("isolation = %q, want worktree: with a shell available, the worktree is what keeps the reviewer from changing the checkout under review", meta["isolation"])
	}
	tools := splitList(meta["tools"])
	if len(tools) == 0 {
		t.Fatal("the reviewer must list its tools explicitly; an omitted list inherits every tool, including the editing ones")
	}
	for _, tool := range tools {
		switch tool {
		case "Edit", "Write", "NotebookEdit":
			t.Errorf("the reviewer can %s; an independent review must be read-only", tool)
		}
	}
}

// TestClaudeRulesLoadOnlyByPath keeps .claude/rules from becoming a second
// always-loaded contract: every rule declares the paths it applies to, so it
// enters context only while one of those files is being edited, and stays
// small enough to be a procedure rather than a manual.
func TestClaudeRulesLoadOnlyByPath(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, ".claude", "rules")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	found := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		found = true
		t.Run(entry.Name(), func(t *testing.T) {
			data := read(t, filepath.Join(dir, entry.Name()))
			if len(data) > maxRuleBytes {
				t.Fatalf("rule is %d bytes; keep it under %d", len(data), maxRuleBytes)
			}
			meta := frontmatter(t, data)
			paths := strings.TrimSpace(meta["paths"])
			if !strings.HasPrefix(paths, "[") || !strings.HasSuffix(paths, "]") || paths == "[]" {
				t.Fatalf("rule has no inline `paths:` list; a rule without paths loads at every session start, which is AGENTS.md's job")
			}
		})
	}
	if !found {
		t.Fatal("no rules under .claude/rules; CLAUDE.md points at agent-config.md")
	}
}

// allowedCommands is the reviewed set of commands the allow list may carry,
// spelled exactly as a rule's command part: program and subcommand, with
// no wildcard before the subcommand. A denylist of verbs would let
// `Bash(git:*)` through, and that rule pre-approves every git command,
// push included. Adding a command here is a review decision, which is the
// point: the allow list removes prompts from the checks the repository
// prescribes and never from the actions AGENTS.md reserves for the host's
// authorization contract.
var allowedCommands = map[string]bool{
	"go build": true, "go vet": true, "go test": true, "go list": true, "go doc": true,
	"go env": true, "go version": true,
	"go run ./tools/gate": true, "go run ./tools/testsum": true, "go run ./tools/shipcheck": true,
	"go run ./cmd/flow validate": true, "go run ./cmd/flow lint": true,
	"make gate": true, "make check": true, "make fmt": true, "make test": true,
	"make test-fast": true, "make docs": true,
	"git status": true, "git diff": true, "git log": true, "git show": true, "git blame": true,
	"git ls-files": true, "git rev-parse": true, "git fetch origin": true,
}

// allowRuleCommand reads the command an allow rule pre-approves and reports
// whether it is one of the reviewed commands. A rule that is not a Bash
// rule, that carries a wildcard or shell metacharacter anywhere but the
// trailing `:*`, or whose command is not in allowedCommands is not ok.
func allowRuleCommand(entry string) (string, bool) {
	if !strings.HasPrefix(entry, "Bash(") || !strings.HasSuffix(entry, ")") {
		return "", false
	}
	command := strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(entry, "Bash("), ")"), ":*")
	if command == "" || strings.ContainsAny(command, "*$`;|&<>") {
		return command, false
	}
	return command, allowedCommands[command]
}

// TestClaudePermissionsOnlyRemovePromptsFromVerification parses the checked-in
// allow list and holds every entry to allowedCommands, then proves the
// negative direction on the shapes a verb denylist would have let through.
// It also refuses `deny` and `ask` entries here: the hooks are the
// repository's blocking controls, and a second spelling of them in
// permissions would be the competing-mechanism problem
// docs/agents/README.md exists to prevent.
func TestClaudePermissionsOnlyRemovePromptsFromVerification(t *testing.T) {
	data := read(t, filepath.Join(repoRoot(t), ".claude", "settings.json"))
	var settings struct {
		Permissions struct {
			Allow []string        `json:"allow"`
			Deny  json.RawMessage `json:"deny"`
			Ask   json.RawMessage `json:"ask"`
		} `json:"permissions"`
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatalf("parse .claude/settings.json: %v", err)
	}
	if len(settings.Permissions.Allow) == 0 {
		t.Fatal("no permissions.allow entries; the verification commands should not prompt")
	}
	if settings.Permissions.Deny != nil || settings.Permissions.Ask != nil {
		t.Fatal("permissions.deny/ask are set; blocking controls are hooks, which say why they block")
	}
	for _, entry := range settings.Permissions.Allow {
		if command, ok := allowRuleCommand(entry); !ok {
			t.Errorf("allow entry %q (command %q) is not one of the reviewed verification commands; add it to allowedCommands only if it cannot push, merge, commit, or destroy work", entry, command)
		}
	}
	for _, required := range []string{"Bash(go run ./tools/gate:*)", "Bash(make fmt)", "Bash(go test:*)"} {
		if !slices.Contains(settings.Permissions.Allow, required) {
			t.Errorf("allow list lacks %q, the check AGENTS.md asks for before every handoff", required)
		}
	}
	for _, rejected := range []string{
		"Bash(*)", "Bash(git:*)", "Bash(go:*)", "Bash(make:*)", "Bash(git push:*)",
		"Bash(git commit:*)", "Bash(git reset --hard)", "Bash(bash -c:*)",
		"Bash(go test ./... && git push)", "Bash(gh pr merge:*)", "Bash(rm -rf:*)",
		"Edit", "mcp__github__merge_pull_request",
	} {
		if _, ok := allowRuleCommand(rejected); ok {
			t.Errorf("%q would be accepted into the allow list", rejected)
		}
	}
}

// splitList reads an inline frontmatter list: `Read, Grep, Glob` or
// `[Read, Grep]`, which is the form the agentconfig frontmatter parser sees.
func splitList(value string) []string {
	value = strings.Trim(strings.TrimSpace(value), "[]")
	if value == "" {
		return nil
	}
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
