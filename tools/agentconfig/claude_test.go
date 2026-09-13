package agentconfig

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"regexp"
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
// no wildcard before the subcommand. The value says whether the rule may
// end in `:*` and so accept arguments, and the rule for that is whether the
// command needs arguments to do its job:
//
//   - A build, test, or gate run takes packages and flags, and running the
//     checkout's own code is the point. Its writes land where a contributor's
//     own run lands (the build cache, a `-coverprofile` path), so the allow
//     list assumes the checkout is one the operator trusts.
//   - A command that reads must stay exact. `git fetch origin` with arguments
//     accepts a forced refspec and `--update-head-ok` that move the
//     checked-out ref, and `git diff`, `log`, `show`, and `blame` accept
//     `--output=<file>`, which truncates that file and writes the report into
//     it. Reading needs neither, so neither is pre-approved.
//
// A denylist of verbs would let `Bash(git:*)` through, and that rule
// pre-approves every git command, push included. Adding a command here is a
// review decision, which is the point: the allow list removes prompts from
// the checks the repository prescribes and never from the actions AGENTS.md
// reserves for the host's authorization contract.
var allowedCommands = map[string]bool{
	"go build": true, "go vet": true, "go test": true, "go list": true, "go doc": true,
	"go env": false, "go version": false,
	"go run ./tools/gate": true, "go run ./tools/testsum": true, "go run ./tools/shipcheck": true,
	"go run ./cmd/flow validate": true, "go run ./cmd/flow lint": true,
	"make gate": false, "make check": false, "make fmt": false, "make test": false,
	"make test-fast": false, "make docs": false,
	"git status": true, "git ls-files": true, "git rev-parse": true,
	"git diff": false, "git log": false, "git show": false, "git blame": false,
	"git fetch origin": false, "git fetch origin main": false,
}

// allowRuleCommand reads the command an allow rule pre-approves and reports
// whether it is one of the reviewed commands in a permitted form. A rule
// that is not a Bash rule, that carries a wildcard or shell metacharacter
// anywhere but the trailing `:*`, whose command is not in allowedCommands,
// or that accepts arguments where the command must stay exact is not ok.
func allowRuleCommand(entry string) (string, bool) {
	if !strings.HasPrefix(entry, "Bash(") || !strings.HasSuffix(entry, ")") {
		return "", false
	}
	command := strings.TrimSuffix(strings.TrimPrefix(entry, "Bash("), ")")
	arguments := strings.HasSuffix(command, ":*")
	command = strings.TrimSuffix(command, ":*")
	if command == "" || strings.ContainsAny(command, "*$`;|&<>") {
		return command, false
	}
	argumentsAllowed, known := allowedCommands[command]
	return command, known && (!arguments || argumentsAllowed)
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
		"Bash(git fetch origin:*)", "Bash(git fetch origin +main:work --update-head-ok)",
		"Bash(git diff:*)", "Bash(git diff --output=AGENTS.md HEAD)",
		"Bash(git log:*)", "Bash(git show:*)", "Bash(git blame:*)",
		"Bash(go env:*)", "Bash(make gate:*)",
		"Edit", "mcp__github__merge_pull_request",
	} {
		if _, ok := allowRuleCommand(rejected); ok {
			t.Errorf("%q would be accepted into the allow list", rejected)
		}
	}
}

// Anthropic's hosted review surfaces sit beside the CLI's own layers. The
// managed Code Review reads CLAUDE.md and a root REVIEW.md on a pull request
// and loads no skill, so REVIEW.md is where the comms-review rubric reaches it;
// the security-guidance plugin reads its own guidance file on every model-backed
// review. Both are prose a reviewer acts on, so both are bounded for the reason
// the layers above are: a long file's real cost is paid by the rules in it that
// matter most. The plugin's own 8 KiB cap is combined across the user, project,
// and project-local guidance files it concatenates, so this bounds only the copy
// the repository controls; a contributor's user-scope file spends the rest.
const (
	maxReviewBytes           = 4 << 10
	maxSecurityGuidanceBytes = 8 << 10
)

// officialMarketplace is the only plugin source this repository enables for
// everyone, and officialMarketplaceRepo is the GitHub repository it must resolve
// to. A plugin named here runs in every clone and every cloud session, including
// for a contributor who never chose it, so both halves are pinned: the name a
// plugin key is spelled with, and the repository that name fetches from. Without
// the second, a settings edit could keep these plugin names and serve them from
// a fork.
//
// Claude Code registers the official marketplace on its own, but not on a
// machine whose first launch is non-interactive — a cloud session is exactly
// that — so extraKnownMarketplaces is what makes the checked-in enablement
// resolve there rather than silently doing nothing.
const (
	officialMarketplace     = "claude-plugins-official"
	officialMarketplaceRepo = "anthropics/claude-plugins-official"
)

// reviewedPlugins is the set .claude/settings.json may enable, each spelled
// exactly as Claude Code resolves it. Both are defense in depth around the
// review the shipping gate already requires, not a replacement for it:
// security-guidance reviews a change as it is written, and claude-security is
// an on-demand deep scan that costs nothing until it is invoked. Adding an entry
// is a review decision, which is the point — an enabled plugin can add hooks
// and commands to every session in this repository.
var reviewedPlugins = []string{
	"claude-security@" + officialMarketplace,
	"security-guidance@" + officialMarketplace,
}

// reviewedPlugin reports whether an enabledPlugins key names one of the
// reviewed plugins from the official marketplace. The marketplace is checked
// separately from the list so the negative direction below proves the rule
// rather than only the spelling.
func reviewedPlugin(key string) bool {
	name, marketplace, ok := strings.Cut(key, "@")
	if !ok || name == "" || marketplace != officialMarketplace {
		return false
	}
	return slices.Contains(reviewedPlugins, key)
}

// TestClaudeEnabledPluginsAreTheReviewedOfficialOnes parses the checked-in
// enabledPlugins block, holds every entry to reviewedPlugins, and proves the
// negative direction on the shapes an unpinned marketplace would let through.
// It also couples security-guidance to its guidance file: with the plugin
// enabled and the file missing, the model-backed reviews read this repository as
// generic Go and spend tokens rediscovering its invariants.
func TestClaudeEnabledPluginsAreTheReviewedOfficialOnes(t *testing.T) {
	root := repoRoot(t)
	var settings struct {
		EnabledPlugins         map[string]bool `json:"enabledPlugins"`
		ExtraKnownMarketplaces map[string]struct {
			Source struct {
				Source string `json:"source"`
				Repo   string `json:"repo"`
			} `json:"source"`
		} `json:"extraKnownMarketplaces"`
	}
	if err := json.Unmarshal(read(t, filepath.Join(root, ".claude", "settings.json")), &settings); err != nil {
		t.Fatalf("parse .claude/settings.json: %v", err)
	}
	for _, key := range slices.Sorted(maps.Keys(settings.EnabledPlugins)) {
		enabled := settings.EnabledPlugins[key]
		if !reviewedPlugin(key) {
			t.Errorf("enabledPlugins carries %q, which is not a reviewed plugin from the %s marketplace", key, officialMarketplace)
		}
		if !enabled {
			t.Errorf("enabledPlugins sets %q to false; remove the entry instead of shipping a disabled one", key)
		}
	}
	for _, required := range reviewedPlugins {
		if !settings.EnabledPlugins[required] {
			t.Errorf("enabledPlugins lacks %q; a user-scoped install does not carry into a cloud session", required)
		}
	}
	for _, rejected := range []string{
		"security-guidance", "@" + officialMarketplace, "security-guidance@",
		"security-guidance@community-marketplace",
		"claude-security@" + officialMarketplace + "-fork",
		"unreviewed-plugin@" + officialMarketplace,
	} {
		if reviewedPlugin(rejected) {
			t.Errorf("%q would be accepted into enabledPlugins", rejected)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(settings.ExtraKnownMarketplaces)) {
		source := settings.ExtraKnownMarketplaces[name].Source
		if name != officialMarketplace || source.Source != "github" || source.Repo != officialMarketplaceRepo {
			t.Errorf("extraKnownMarketplaces registers %q from %+v; this repository pins only %q from the github repo %q", name, source, officialMarketplace, officialMarketplaceRepo)
		}
	}
	if _, ok := settings.ExtraKnownMarketplaces[officialMarketplace]; !ok {
		t.Errorf("extraKnownMarketplaces does not register %q; a cloud session may not have it and the enabled plugins would resolve to nothing", officialMarketplace)
	}
	if settings.EnabledPlugins["security-guidance@"+officialMarketplace] {
		if _, err := os.Stat(filepath.Join(root, ".claude", "claude-security-guidance.md")); err != nil {
			t.Errorf("security-guidance is enabled without .claude/claude-security-guidance.md: %v", err)
		}
	}
}

// driftingCitation matches a `path.go:123` citation. tools/citations checks
// those against the tree for docs/ and a fixed list of root documents that
// includes neither file checked below, so a line number in one of them goes
// stale silently. Instructions to a reviewer have no reason to cite a line
// anyway, so this refuses them rather than widening that checker.
var driftingCitation = regexp.MustCompile(`\.go:\d+`)

// TestHostedReviewGuidanceStaysBoundedAndSelfContained checks the two files a
// hosted reviewer reads as instructions. Both are bounded, neither may use the
// `@import` syntax — Code Review reads REVIEW.md as-is and does not expand an
// import, and the plugin concatenates its guidance files without expanding one
// either, so an import is a silently missing rule rather than an error — and
// neither may carry a line-numbered citation that nothing checks. The required
// substrings pin the rules each file exists to deliver, so a rewrite that drops
// one fails here rather than quietly changing what reviewers are told.
func TestHostedReviewGuidanceStaysBoundedAndSelfContained(t *testing.T) {
	root := repoRoot(t)
	for _, tc := range []struct {
		path     string
		max      int
		required []string
	}{
		{
			path: "REVIEW.md",
			max:  maxReviewBytes,
			required: []string{
				"no findings is a valid",
				"at most five nits",
				"cmd/flow/internal/reference/mirror/",
				"is not a new finding",
				"fails closed",
				"shared conformance",
				"one-line tally",
			},
		},
		{
			path: filepath.Join(".claude", "claude-security-guidance.md"),
			max:  maxSecurityGuidanceBytes,
			required: []string{
				"on missing state and on evaluation error",
				"belongs to nobody and is refused",
				"only as a reference",
				"deterministic",
				"does not bound the walk that produced",
				"textContent",
			},
		},
	} {
		t.Run(tc.path, func(t *testing.T) {
			data := read(t, filepath.Join(root, tc.path))
			if len(data) == 0 {
				t.Fatal("file is empty; a hosted reviewer would read no instructions")
			}
			if len(data) > tc.max {
				t.Fatalf("file is %d bytes; keep it under %d, because length dilutes the rules that matter", len(data), tc.max)
			}
			for line := range strings.SplitSeq(string(data), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "@") {
					t.Errorf("line %q uses the @import syntax, which a hosted review does not expand; inline the rule", line)
				}
			}
			if match := driftingCitation.FindString(string(data)); match != "" {
				t.Errorf("carries the line-numbered citation %q, which nothing checks against the tree", match)
			}
			for _, required := range tc.required {
				if !strings.Contains(string(data), required) {
					t.Errorf("does not contain required guidance %q", required)
				}
			}
		})
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
	for item := range strings.SplitSeq(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
