package agentconfig

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
)

const (
	maxAgentsBytes     = 8 << 10
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
		})
	}

	// The two verification aliases keep their old names; both-drivers has no
	// alias because a command and a skill with one name shadow each other.
	for _, name := range []string{"ci-check", "test-fast"} {
		t.Run("alias/"+name, func(t *testing.T) {
			alias := read(t, filepath.Join(root, ".claude", "commands", name+".md"))
			if len(alias) > 1024 {
				t.Fatalf("compatibility command is %d bytes; keep procedure in a skill", len(alias))
			}
		})
	}
	for _, name := range skillNames(t, filepath.Join(root, ".claude", "skills")) {
		if _, err := os.Stat(filepath.Join(root, ".claude", "commands", name+".md")); err == nil {
			t.Errorf("command %s.md duplicates the skill of the same name; one of them is shadowed", name)
		}
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

func TestAmpShipProcedurePinsFinalHeadEvidence(t *testing.T) {
	raw := string(read(t, filepath.Join(repoRoot(t), ".agents", "ship.md")))
	ship := strings.Join(strings.Fields(raw), " ")
	for _, required := range []string{
		"without auto-merge",
		"provider-neutral AI review",
		"fresh-context review",
		"code-security",
		"PASS/no-actionable-findings",
		"Do not request a vendor review bot",
		"Only a material defect",
		"earns a new head",
		"does not restart these gates",
		"base-to-head diff",
		"false positive with evidence",
		"searched, non-duplicate scoped issue",
		"if one arrives after merge",
		"flowstate-independent-review:v1",
		"./tools/shipcheck",
		"--match-head-commit",
		"explicit human authorization",
		"Before the next autonomous merge",
	} {
		if !strings.Contains(ship, required) {
			t.Errorf(".agents/ship.md does not contain required process control %q", required)
		}
	}
	if strings.Contains(ship, "gh pr merge --auto") && !strings.Contains(ship, "Never use `gh pr merge --auto`") {
		t.Error(".agents/ship.md appears to recommend auto-merge")
	}
}

func TestClaudeSessionPreparesPinnedToolchainAndHooks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Claude session hook requires Bash and POSIX symlinks")
	}

	root := repoRoot(t)
	data := read(t, filepath.Join(root, ".claude", "settings.json"))
	type hookGroup struct {
		Hooks []struct {
			Command string `json:"command"`
		} `json:"hooks"`
	}
	var settings struct {
		Hooks struct {
			SessionStart []hookGroup `json:"SessionStart"`
			PreToolUse   []hookGroup `json:"PreToolUse"`
			PostToolUse  []hookGroup `json:"PostToolUse"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatalf("parse .claude/settings.json: %v", err)
	}
	guardedCommand := func(command string) string {
		return `bash -c 'if [[ -z "${CLAUDE_PROJECT_DIR:-}" || ! -d "${CLAUDE_PROJECT_DIR}" ]]; then printf "CLAUDE_PROJECT_DIR does not name a checkout directory.\n" >&2; exit 2; fi; if ! bash "${CLAUDE_PROJECT_DIR}/.claude/hooks/` + command + `; then printf "Flowstate Claude hook entrypoint could not run.\n" >&2; exit 2; fi'`
	}
	hookCommand := guardedCommand(`session-env.sh"`)
	if len(settings.Hooks.SessionStart) != 1 || len(settings.Hooks.SessionStart[0].Hooks) != 1 ||
		settings.Hooks.SessionStart[0].Hooks[0].Command != hookCommand {
		t.Fatalf("Claude SessionStart must run %q", hookCommand)
	}

	temp := t.TempDir()
	baseBin := filepath.Join(temp, "base-go", "bin")
	if err := os.MkdirAll(baseBin, 0o755); err != nil {
		t.Fatal(err)
	}
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	var goVersion string
	for _, line := range strings.Split(string(read(t, filepath.Join(root, "go.mod"))), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "go" {
			goVersion = fields[1]
			break
		}
	}
	if goVersion == "" {
		t.Fatal("go.mod has no go directive")
	}
	goEnv := exec.Command(goBinary, "env", "GOROOT")
	goEnv.Env = append(os.Environ(), "GOTOOLCHAIN=go"+goVersion)
	output, err := goEnv.CombinedOutput()
	if err != nil {
		t.Fatalf("resolve pinned GOROOT: %v\n%s", err, output)
	}
	want := filepath.Join(strings.TrimSpace(string(output)), "bin", "gofmt")
	if err := os.Symlink(goBinary, filepath.Join(baseBin, "go")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(baseBin, "gofmt"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	basePath := baseBin + ":/usr/bin:/bin"
	envFile := filepath.Join(temp, "claude-env")
	hook := exec.Command("bash", filepath.Join(root, ".claude", "hooks", "session-env.sh"))
	hook.Env = append(os.Environ(),
		"CLAUDE_PROJECT_DIR="+root,
		"CLAUDE_ENV_FILE="+envFile,
		"PATH="+basePath,
	)
	if output, err := hook.CombinedOutput(); err != nil {
		t.Fatalf("run Claude SessionStart hook: %v\n%s", err, output)
	}

	check := exec.Command("bash", "-c", `source "$1"; command -v gofmt`, "bash", envFile)
	check.Env = append(os.Environ(), "PATH="+basePath)
	output, err = check.CombinedOutput()
	if err != nil {
		t.Fatalf("source Claude session environment: %v\n%s", err, output)
	}
	got := strings.TrimSpace(string(output))
	if got != want {
		t.Fatalf("Claude session gofmt = %q, want pinned toolchain formatter %q", got, want)
	}

	hookDir := filepath.Join(root, ".claude", "hooks", ".bin")
	wantCommands := map[string]int{}
	for _, name := range []string{"genguard", "gofmtcheck", "pidguard", "mergeguard"} {
		path := filepath.Join(hookDir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("SessionStart did not build %s: %v", name, err)
		} else if info.Mode()&0o111 == 0 {
			t.Errorf("SessionStart built non-executable hook %s", path)
		}
		wantCommands[guardedCommand(fmt.Sprintf(`run-hook.sh" %s`, name))] = 1
	}
	if _, err := os.Stat(filepath.Join(hookDir, ".ready")); err != nil {
		t.Errorf("SessionStart did not mark the complete hook build ready: %v", err)
	}
	// mergeguard is shared by Bash and the native GitHub merge tool. Neither
	// call site fails open when the guard cannot be built, because refusing a
	// merge stands between nobody and repairing a broken tree; the merge
	// tool's entry keeps saying `strict` so the policy reads at the call site
	// as well as in the launcher.
	wantCommands[guardedCommand(`run-hook.sh" mergeguard strict`)] = 1

	gotCommands := map[string]int{}
	configuredCommands := []string{hookCommand}
	for _, groups := range [][]hookGroup{settings.Hooks.PreToolUse, settings.Hooks.PostToolUse} {
		for _, group := range groups {
			for _, hook := range group.Hooks {
				gotCommands[hook.Command]++
				configuredCommands = append(configuredCommands, hook.Command)
				if strings.Contains(hook.Command, "go run") {
					t.Errorf("per-tool Claude hook recompiles through go run: %q", hook.Command)
				}
			}
		}
	}
	if !reflect.DeepEqual(gotCommands, wantCommands) {
		t.Fatalf("Claude per-tool commands = %v, want the session-built hooks %v", gotCommands, wantCommands)
	}
	for _, command := range configuredCommands {
		for _, projectDir := range []string{"", "/definitely/not/a/checkout", t.TempDir()} {
			cmd := exec.Command("bash", "-c", command)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "CLAUDE_PROJECT_DIR=" + projectDir}
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 || len(output) == 0 {
				t.Errorf("configured hook with project dir %q = %v, want explained exit 2; output:\n%s", projectDir, err, output)
			}
		}
	}
}

func TestClaudeHookLauncherFailsClosedWithoutACompleteBuild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Claude hooks require Bash")
	}

	root := repoRoot(t)
	project := t.TempDir()
	hookDir := filepath.Join(project, ".claude", "hooks", ".bin")
	if err := os.MkdirAll(hookDir, 0o700); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(root, ".claude", "hooks", "run-hook.sh")
	sentinel := filepath.Join(project, "stale-hook-ran")
	for _, script := range []string{launcher, filepath.Join(root, ".claude", "hooks", "session-env.sh")} {
		cmd := exec.Command("bash", script)
		cmd.Env = []string{"PATH=/usr/bin:/bin"}
		output, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 || !strings.Contains(string(output), "CLAUDE_PROJECT_DIR") {
			t.Fatalf("%s without CLAUDE_PROJECT_DIR = %v, want explained exit 2; output:\n%s", script, err, output)
		}
	}
	// The launcher answers a stale build in one of two ways, and which one is
	// the policy under test here. A tree that will not compile has not refused
	// anything, so it warns and lets the call through, in the neutral shape the
	// guards themselves use: a systemMessage and no permission decision, which
	// would otherwise skip the prompt rather than stay neutral. Anything else
	// denies. Neither may run the stale binary.
	runLauncherWarns := func(wantMessage string, paths ...string) {
		t.Helper()
		path := os.Getenv("PATH")
		if len(paths) != 0 {
			path = paths[0]
		}
		cmd := exec.Command("bash", launcher, "genguard")
		cmd.Env = []string{"CLAUDE_PROJECT_DIR=" + project, "HOME=" + os.Getenv("HOME"), "PATH=" + path}
		output, err := cmd.Output()
		if err != nil {
			t.Fatalf("launcher with an uncompilable tree = %v, want a neutral warning; output:\n%s", err, output)
		}
		var warning struct {
			SystemMessage string `json:"systemMessage"`
		}
		if err := json.Unmarshal(output, &warning); err != nil {
			t.Fatalf("launcher warning is not the hook JSON shape: %v\n%s", err, output)
		}
		if !strings.Contains(warning.SystemMessage, wantMessage) {
			t.Fatalf("launcher warning = %q, want it to mention %q", warning.SystemMessage, wantMessage)
		}
		if strings.Contains(string(output), "permissionDecision") {
			t.Fatalf("a blind check decided the permission rather than staying neutral:\n%s", output)
		}
		if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale hook ran while its sources did not compile: %v", err)
		}
	}
	runLauncher := func(wantMessage string, paths ...string) {
		t.Helper()
		path := os.Getenv("PATH")
		if len(paths) != 0 {
			path = paths[0]
		}
		cmd := exec.Command("bash", launcher, "mergeguard")
		cmd.Env = []string{"CLAUDE_PROJECT_DIR=" + project, "HOME=" + os.Getenv("HOME"), "PATH=" + path}
		output, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 {
			t.Fatalf("launcher without a ready build = %v, want exit 2; output:\n%s", err, output)
		}
		if !strings.Contains(string(output), wantMessage) {
			t.Fatalf("launcher did not explain how to restore hooks: %s", output)
		}
	}

	// A missing build denies instead of returning the shell's non-blocking
	// 127. The launcher rebuilds first; with no module here at all the build
	// cannot even name a toolchain, which is incoherent rather than a tree
	// mid-edit, so it denies and says why.
	runLauncher("could not be rebuilt")

	stale := []byte("#!/bin/sh\ntouch " + strconv.Quote(sentinel) + "\n")
	writeStaleBuild := func() {
		t.Helper()
		if err := os.Remove(sentinel); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		for _, hook := range []string{"mergeguard", "genguard"} {
			if err := os.WriteFile(filepath.Join(hookDir, hook), stale, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(hookDir, ".ready"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A SessionStart that fails must leave no usable generation behind. What
	// the next tool call then sees depends on why the build failed, so each
	// case says which policy it expects; what none of them may do is run the
	// binary the failed build invalidated.
	runFailedSession := func(thenTheLauncher func(), path string, extraEnv ...string) {
		t.Helper()
		session := exec.Command("bash", filepath.Join(root, ".claude", "hooks", "session-env.sh"))
		session.Env = append(os.Environ(), append([]string{"CLAUDE_PROJECT_DIR=" + project, "PATH=" + path}, extraEnv...)...)
		if output, err := session.CombinedOutput(); err == nil {
			t.Fatalf("SessionStart unexpectedly accepted a failed hook build; output:\n%s", output)
		}
		thenTheLauncher()
		if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalidated stale hook executed: %v", err)
		}
	}
	deniesTheCall := func() { t.Helper(); runLauncher("could not be rebuilt") }
	warnsAndContinues := func() { t.Helper(); runLauncherWarns("do not compile right now") }

	// Even setup failures before compilation must invalidate an older build.
	writeStaleBuild()
	// No module at all: the build cannot name a toolchain, which is incoherent.
	runFailedSession(deniesTheCall, "/usr/bin:/bin") // go.mod is deliberately absent.

	// Every script the hooks call each other through has to exist in the
	// fixture. A missing one dies at 127 inside the launcher, which looks
	// exactly like the denial each case below means to assert while proving
	// nothing about the mechanism under test.
	for _, script := range []string{"source-id.sh", "build-hooks.sh"} {
		data, err := os.ReadFile(filepath.Join(root, ".claude", "hooks", script))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(project, ".claude", "hooks", script), data, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	sourceIDScript := filepath.Join(project, ".claude", "hooks", "source-id.sh")
	if err := os.WriteFile(filepath.Join(project, "go.mod"), []byte("module example.com/hooks\n\ngo 1.27.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, "go.sum"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	toolSource := filepath.Join(project, "tools", "hooks", "dummy.go")
	testSource := filepath.Join(project, "tools", "hooks", "dummy_test.go")
	unicodeSource := filepath.Join(project, "tools", "hooks", "règle.go")
	dependencySource := filepath.Join(project, "internal", "commitcheck", "dummy.go")
	textboundSource := filepath.Join(project, "internal", "textbound", "dummy.go")
	for _, source := range []string{toolSource, testSource, unicodeSource, dependencySource, textboundSource} {
		if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(source, []byte("package hooks\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"init", "--quiet"},
		{"add", "go.mod", "go.sum", "tools/hooks/dummy.go", "tools/hooks/dummy_test.go", "tools/hooks/règle.go", "internal/commitcheck/dummy.go", "internal/textbound/dummy.go"},
		{"-c", "user.name=Flowstate Test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "fixture"},
	} {
		if output, err := exec.Command("git", append([]string{"-C", project}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	// Every configured hook needs a package that exists: a package that is
	// absent is a removed control and denies, which would mask the cases below
	// that are about a tree mid-edit. These compile only once repaired.
	writeBrokenHookPackages := func() {
		t.Helper()
		for _, hook := range []string{"genguard", "gofmtcheck", "pidguard", "mergeguard"} {
			dir := filepath.Join(project, "tools", "hooks", hook)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nthis is not go\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeBrokenHookPackages()

	sourceDirs := filepath.Join(hookDir, ".source-dirs")
	if err := os.WriteFile(sourceDirs, []byte("internal/commitcheck\ninternal/textbound\ntools/hooks\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sourceID := func() string {
		t.Helper()
		cmd := exec.Command("bash", sourceIDScript, sourceDirs)
		cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("compute hook source identity: %v\n%s", err, output)
		}
		return strings.TrimSpace(string(output))
	}
	baselineSourceID := sourceID()
	if err := os.WriteFile(testSource, []byte("package hooks\n\nconst testOnlyChange = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := sourceID(); got != baselineSourceID {
		t.Fatalf("test-only edit changed hook build identity: got %s, want %s", got, baselineSourceID)
	}
	if output, err := exec.Command("git", "-C", project, "checkout", "--", "tools/hooks/dummy_test.go").CombinedOutput(); err != nil {
		t.Fatalf("restore fixture test source: %v\n%s", err, output)
	}
	if err := os.WriteFile(unicodeSource, []byte("package hooks\n\nconst unicodeChange = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := sourceID(); got == baselineSourceID {
		t.Fatal("tracked Unicode-named Go source edit did not change hook build identity")
	}
	if output, err := exec.Command("git", "-C", project, "checkout", "--", "tools/hooks/règle.go").CombinedOutput(); err != nil {
		t.Fatalf("restore fixture Unicode source: %v\n%s", err, output)
	}
	// `go build` does not read .gitignore, so a Go source the repository
	// ignores is compiled like any other. An identity that enumerated tracked
	// and merely-untracked files would skip it and keep accepting a guard
	// built before it changed.
	ignoredSource := filepath.Join(project, "tools", "hooks", "ignored.go")
	if err := os.WriteFile(filepath.Join(project, ".gitignore"), []byte("/tools/hooks/ignored.go\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ignoredSource, []byte("package hooks\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", project, "check-ignore", "-q", "tools/hooks/ignored.go").CombinedOutput(); err != nil {
		t.Fatalf("fixture source is not ignored by git: %v\n%s", err, output)
	}
	withIgnored := sourceID()
	if withIgnored == baselineSourceID {
		t.Fatal("a git-ignored Go source the compiler reads did not change the hook build identity")
	}
	if err := os.WriteFile(ignoredSource, []byte("package hooks\n\nconst ignoredChange = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if sourceID() == withIgnored {
		t.Fatal("editing a git-ignored Go source did not change the hook build identity")
	}
	if err := os.Remove(ignoredSource); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(project, ".gitignore")); err != nil {
		t.Fatal(err)
	}
	if sourceID() != baselineSourceID {
		t.Fatal("removing the ignored source did not restore the hook build identity")
	}

	untrackedUnicodeSource := filepath.Join(project, "tools", "hooks", "nøuveau.go")
	if err := os.WriteFile(untrackedUnicodeSource, []byte("package hooks\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := sourceID(); got == baselineSourceID {
		t.Fatal("untracked Unicode-named Go source did not change hook build identity")
	}
	if err := os.Remove(untrackedUnicodeSource); err != nil {
		t.Fatal(err)
	}

	writeStaleBuild()
	cachePath := filepath.Join(project, ".claude", "hooks", ".cache")
	if err := os.WriteFile(cachePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// The cache path is a file, so the build cannot even stage: incoherent.
	runFailedSession(deniesTheCall, "/usr/bin:/bin")
	if err := os.Remove(cachePath); err != nil {
		t.Fatal(err)
	}

	// A compiler failure cannot reactivate the stale generation either.
	writeStaleBuild()
	fakeBin := filepath.Join(project, "fake-bin")
	if err := os.Mkdir(fakeBin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeBin, "go"), []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	// The compiler itself refuses, which is the tree-does-not-compile case.
	runFailedSession(warnsAndContinues, fakeBin+":/usr/bin:/bin")

	// An identity command that fails only after compilation must not publish the
	// hash it happened to print as a ready generation.
	writeStaleBuild()
	postBuildBin := filepath.Join(project, "post-build-bin")
	if err := os.Mkdir(postBuildBin, 0o700); err != nil {
		t.Fatal(err)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	gitCount := filepath.Join(project, "git-identity-count")
	// Each identity check ends in one `git hash-object --stdin` that folds the
	// per-file hashes together. Failing the second one is an identity that
	// succeeds before the build and fails after it, which must not publish the
	// hash it printed the first time.
	postBuildGit := "#!/bin/sh\nstdin=\nfor arg do\n  case \"$arg\" in --stdin) stdin=1 ;; esac\ndone\nif [ -n \"$stdin\" ]; then\n  count=$(cat \"$FLOWSTATE_TEST_GIT_COUNT\" 2>/dev/null || echo 0)\n  count=$((count + 1))\n  printf '%s\\n' \"$count\" > \"$FLOWSTATE_TEST_GIT_COUNT\"\n  if [ \"$count\" -gt 1 ]; then exit 1; fi\nfi\nexec " + strconv.Quote(realGit) + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(postBuildBin, "git"), []byte(postBuildGit), 0o700); err != nil {
		t.Fatal(err)
	}
	postBuildGo := "#!/bin/sh\nfor arg do\n  case \"$arg\" in *EmbedFiles*) exit 0 ;; esac\ndone\nfor arg do\n  if [ \"$arg\" = list ]; then\n    printf '%s\\n' \"$CLAUDE_PROJECT_DIR/tools/hooks\" \"$CLAUDE_PROJECT_DIR/internal/commitcheck\" \"$CLAUDE_PROJECT_DIR/internal/textbound\"\n    exit 0\n  fi\ndone\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = -o ]; then shift; out=$1; break; fi\n  shift\ndone\nfor name in genguard gofmtcheck pidguard mergeguard; do\n  printf '#!/bin/sh\\nexit 0\\n' > \"$out/$name\"\n  chmod 700 \"$out/$name\"\ndone\n"
	if err := os.WriteFile(filepath.Join(postBuildBin, "go"), []byte(postBuildGo), 0o700); err != nil {
		t.Fatal(err)
	}
	// The identity fails only after a successful compile, so SessionStart
	// publishes nothing; the next call then finds no generation to trust.
	runFailedSession(warnsAndContinues, postBuildBin+":/usr/bin:/bin", "FLOWSTATE_TEST_GIT_COUNT="+gitCount)

	// A source change after SessionStart invalidates an otherwise ready binary.
	writeStaleBuild()
	if err := os.WriteFile(filepath.Join(hookDir, ".source-id"), []byte(sourceID()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dependencySource, []byte("package hooks\n\nconst changed = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runLauncherWarns("do not compile right now")

	// The merge guard never fails open, on either call site: refusing a merge
	// stands between nobody and a repair, while the guards that match the
	// tools a repair needs may warn. Asserted here without the argument, so
	// the shell path is covered too.
	strictCmd := exec.Command("bash", launcher, "mergeguard")
	strictCmd.Env = []string{"CLAUDE_PROJECT_DIR=" + project, "HOME=" + os.Getenv("HOME"), "PATH=" + os.Getenv("PATH")}
	strictOutput, strictErr := strictCmd.CombinedOutput()
	var strictExit *exec.ExitError
	if !errors.As(strictErr, &strictExit) || strictExit.ExitCode() != 2 {
		t.Fatalf("merge tool entry with an uncompilable tree = %v, want exit 2; output:\n%s", strictErr, strictOutput)
	}
	if !strings.Contains(string(strictOutput), "does not fail open") {
		t.Fatalf("merge tool denial did not say why it does not fail open:\n%s", strictOutput)
	}

	// Remove one of them: a control that is gone must not fail open.
	if err := os.RemoveAll(filepath.Join(project, "tools", "hooks", "mergeguard")); err != nil {
		t.Fatal(err)
	}
	runLauncher("could not be rebuilt")
	writeBrokenHookPackages()

	// Failure to enumerate untracked sources is an identity failure, not an
	// empty untracked-file set that can accidentally trust the old binary.
	if output, err := exec.Command("git", "-C", project, "checkout", "--", "internal/commitcheck/dummy.go").CombinedOutput(); err != nil {
		t.Fatalf("restore fixture source: %v\n%s", err, output)
	}
	writeStaleBuild()
	if err := os.WriteFile(filepath.Join(hookDir, ".source-id"), []byte(sourceID()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeGitBin := filepath.Join(project, "fake-git-bin")
	if err := os.Mkdir(fakeGitBin, 0o700); err != nil {
		t.Fatal(err)
	}
	fakeGit := "#!/bin/sh\nfor arg do\n  if [ \"$arg\" = hash-object ]; then exit 1; fi\ndone\nexec " + strconv.Quote(realGit) + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(fakeGitBin, "git"), []byte(fakeGit), 0o700); err != nil {
		t.Fatal(err)
	}
	// An identity that cannot be computed is not a tree mid-edit: nothing is
	// known about what the binaries were built from, so this denies. The PATH
	// keeps the real toolchain, because a PATH without it would fail earlier
	// for a different reason and make the assertion depend on the host.
	runLauncher("could not identify", fakeGitBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	// A ready executable that cannot launch after the readiness check is still
	// converted to Claude's blocking exit status.
	if err := os.WriteFile(filepath.Join(hookDir, "mergeguard"), []byte("#!/definitely/missing/interpreter\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hookDir, ".ready"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hookDir, ".source-id"), []byte(sourceID()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runLauncher("could not run")
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
	values, err := parseFrontmatter(source)
	if err != nil {
		t.Fatal(err)
	}
	return values
}

// parseFrontmatter reads the `key: value` lines of a file's YAML frontmatter
// as text, after checking with a real YAML parser that the block is YAML
// at all: a description with an unquoted `: ` inside it reads fine line by
// line and is rejected by every host that parses the block to advertise
// the skill, which the line reader alone would never notice.
func parseFrontmatter(source []byte) (map[string]string, error) {
	lines := strings.Split(strings.ReplaceAll(string(source), "\r\n", "\n"), "\n")
	if len(lines) < 3 || strings.TrimSpace(lines[0]) != "---" {
		return nil, errors.New("the file must begin with YAML frontmatter")
	}
	end := -1
	for i, line := range lines[1:] {
		if strings.TrimSpace(line) == "---" {
			end = i + 1
			break
		}
	}
	if end < 0 {
		return nil, errors.New("the frontmatter is not closed")
	}
	block := strings.Join(lines[1:end], "\n")
	var parsed map[string]any
	if err := yaml.Unmarshal([]byte(block), &parsed); err != nil {
		return nil, fmt.Errorf("the frontmatter is not valid YAML: %v", err)
	}
	values := map[string]string{}
	for _, line := range lines[1:end] {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			values[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	return values, nil
}

// TestFrontmatterRejectsWhatAHostWouldReject pins the YAML check with the
// shape that slipped past the line reader once: a plain-scalar description
// carrying `: ` inside backticks.
func TestFrontmatterRejectsWhatAHostWouldReject(t *testing.T) {
	bad := "---\nname: x\ndescription: the shape is `scope: lowercase imperative` with a body\n---\nbody\n"
	if _, err := parseFrontmatter([]byte(bad)); err == nil {
		t.Fatal("an unquoted `: ` inside a plain scalar was accepted")
	}
	good := "---\nname: x\ndescription: \"the shape is `scope: lowercase imperative`\"\npaths: [\"a/**\", \"b.md\"]\n---\nbody\n"
	values, err := parseFrontmatter([]byte(good))
	if err != nil {
		t.Fatalf("quoted scalar rejected: %v", err)
	}
	if values["name"] != "x" || !strings.HasPrefix(values["paths"], "[") {
		t.Fatalf("values = %v", values)
	}
}

// TestThePullRequestTemplateCarriesTheSkillsHeadings keeps the template a
// web-opened PR starts from and the comms-pr skill's default shape one list:
// the skill's numbered sections are the template's `##` headings, in order,
// so neither drifts from the other unnoticed (#1728). It lives here rather
// than beside tools/commitcheck because this package is the one the gate runs
// for a diff to the agent configuration, which the template now counts as.
// TestClaudeHookBuildRefusesAnIncoherentGeneration drives build-hooks.sh
// directly, because the launcher tests reach it only through failures that
// stop earlier. What is asserted here is the part that decides whether a
// compiled generation may be trusted: the build asks the compiler what it
// depends on before and after compiling, re-derives the identity afterwards,
// and publishes only when both answers still agree. Each case removes one of
// those and must fail, since a published generation that was not built from
// the sources it names is exactly how a stale guard keeps running.
func TestClaudeHookBuildRefusesAnIncoherentGeneration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Claude hooks require Bash")
	}
	root := repoRoot(t)
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}

	// A fixture the real toolchain can build, so only the double under test
	// makes a case fail.
	newProject := func(t *testing.T) string {
		t.Helper()
		project := t.TempDir()
		for _, dir := range []string{".claude/hooks", "internal/commitcheck"} {
			if err := os.MkdirAll(filepath.Join(project, filepath.FromSlash(dir)), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		for _, script := range []string{"source-id.sh", "build-hooks.sh"} {
			data, err := os.ReadFile(filepath.Join(root, ".claude", "hooks", script))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(project, ".claude", "hooks", script), data, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		write := func(path, body string) {
			t.Helper()
			full := filepath.Join(project, filepath.FromSlash(path))
			if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		write("go.mod", "module example.com/hooks\n\ngo "+goDirective(t, root)+"\n")
		write("go.sum", "")
		write("internal/commitcheck/check.go", "package commitcheck\n\n// Shared so the manifest holds more than the commands.\nconst Name = \"commitcheck\"\n")
		for _, hook := range []string{"genguard", "gofmtcheck", "pidguard", "mergeguard"} {
			write("tools/hooks/"+hook+"/main.go",
				"package main\n\nimport _ \"example.com/hooks/internal/commitcheck\"\n\nfunc main() {}\n")
		}
		if output, err := exec.Command("git", "-C", project, "init", "--quiet").CombinedOutput(); err != nil {
			t.Fatalf("git init: %v\n%s", err, output)
		}
		return project
	}

	build := func(t *testing.T, project, path string) (int, string) {
		t.Helper()
		cmd := exec.Command("bash", filepath.Join(project, ".claude", "hooks", "build-hooks.sh"))
		cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project, "PATH="+path)
		output, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		switch {
		case err == nil:
			return 0, string(output)
		case errors.As(err, &exitErr):
			return exitErr.ExitCode(), string(output)
		default:
			t.Fatalf("run build-hooks.sh: %v\n%s", err, output)
			return 0, ""
		}
	}
	published := func(project string) bool {
		_, err := os.Stat(filepath.Join(project, ".claude", "hooks", ".bin", ".ready"))
		return err == nil
	}

	t.Run("a healthy tree publishes", func(t *testing.T) {
		project := newProject(t)
		if status, output := build(t, project, os.Getenv("PATH")); status != 0 {
			t.Fatalf("build = %d, want 0; output:\n%s", status, output)
		}
		if !published(project) {
			t.Fatal("a successful build published no ready generation")
		}
	})

	t.Run("a dependency set that changes while compiling", func(t *testing.T) {
		project := newProject(t)
		bin := filepath.Join(project, "double")
		if err := os.Mkdir(bin, 0o700); err != nil {
			t.Fatal(err)
		}
		// Answers the package-directory query with one more directory the
		// second time, which is what an import added mid-build looks like.
		double := "#!/bin/sh\nfor arg do\n  case \"$arg\" in *EmbedFiles*) exit 0 ;; esac\ndone\nfor arg do\n  if [ \"$arg\" = list ]; then\n    count=$(cat \"$CLAUDE_PROJECT_DIR/list-count\" 2>/dev/null || echo 0)\n    count=$((count + 1))\n    printf '%s\\n' \"$count\" > \"$CLAUDE_PROJECT_DIR/list-count\"\n    printf '%s\\n' \"$CLAUDE_PROJECT_DIR/tools/hooks/genguard\" \"$CLAUDE_PROJECT_DIR/internal/commitcheck\"\n    if [ \"$count\" -gt 1 ]; then printf '%s\\n' \"$CLAUDE_PROJECT_DIR/tools/hooks/pidguard\"; fi\n    exit 0\n  fi\ndone\nexec " + strconv.Quote(realGo) + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(bin, "go"), []byte(double), 0o700); err != nil {
			t.Fatal(err)
		}
		status, output := build(t, project, bin+":"+os.Getenv("PATH"))
		if status != 2 {
			t.Fatalf("build with a changing dependency set = %d, want 2; output:\n%s", status, output)
		}
		if !strings.Contains(output, "changed while they were compiling") {
			t.Fatalf("build did not say the dependencies changed:\n%s", output)
		}
		if published(project) {
			t.Fatal("a build whose dependency set changed published a ready generation")
		}
	})

	t.Run("an embedded input that appears while compiling", func(t *testing.T) {
		project := newProject(t)
		for _, name := range []string{"table.txt", "late.txt"} {
			if err := os.WriteFile(filepath.Join(project, "internal", "commitcheck", name), []byte(name), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		bin := filepath.Join(project, "double")
		if err := os.Mkdir(bin, 0o700); err != nil {
			t.Fatal(err)
		}
		// Names one more embedded file the second time it is asked, which is
		// what a file matching an existing `//go:embed` glob looks like when it
		// lands mid-build. No Go source changes, so the two identities agree:
		// only comparing the input manifests notices, and an identity that
		// omitted the file would keep accepting the guard after it changed.
		double := "#!/bin/sh\nembed=\nfor arg do\n  case \"$arg\" in *EmbedFiles*) embed=1 ;; esac\ndone\nif [ -n \"$embed\" ]; then\n  count=$(cat \"$CLAUDE_PROJECT_DIR/embed-count\" 2>/dev/null || echo 0)\n  count=$((count + 1))\n  printf '%s\\n' \"$count\" > \"$CLAUDE_PROJECT_DIR/embed-count\"\n  printf '%s\\n' \"$CLAUDE_PROJECT_DIR/internal/commitcheck/table.txt\"\n  if [ \"$count\" -gt 1 ]; then printf '%s\\n' \"$CLAUDE_PROJECT_DIR/internal/commitcheck/late.txt\"; fi\n  exit 0\nfi\nexec " + strconv.Quote(realGo) + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(bin, "go"), []byte(double), 0o700); err != nil {
			t.Fatal(err)
		}
		status, output := build(t, project, bin+":"+os.Getenv("PATH"))
		if status != 2 {
			t.Fatalf("build with a changing input set = %d, want 2; output:\n%s", status, output)
		}
		if !strings.Contains(output, "inputs changed while they were compiling") {
			t.Fatalf("build did not say the inputs changed:\n%s", output)
		}
		if published(project) {
			t.Fatal("a build whose embedded inputs changed published a ready generation")
		}
	})

	t.Run("an identity that changes while compiling", func(t *testing.T) {
		project := newProject(t)
		bin := filepath.Join(project, "double")
		if err := os.Mkdir(bin, 0o700); err != nil {
			t.Fatal(err)
		}
		// Each identity check ends in one `hash-object --stdin`; answering the
		// second with a different hash is a source edited mid-build.
		double := "#!/bin/sh\nstdin=\nfor arg do\n  case \"$arg\" in --stdin) stdin=1 ;; esac\ndone\nif [ -n \"$stdin\" ]; then\n  count=$(cat \"$CLAUDE_PROJECT_DIR/id-count\" 2>/dev/null || echo 0)\n  count=$((count + 1))\n  printf '%s\\n' \"$count\" > \"$CLAUDE_PROJECT_DIR/id-count\"\n  cat > /dev/null\n  printf 'identity%s\\n' \"$count\"\n  exit 0\nfi\nexec " + strconv.Quote(realGit) + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(bin, "git"), []byte(double), 0o700); err != nil {
			t.Fatal(err)
		}
		status, output := build(t, project, bin+":"+os.Getenv("PATH"))
		if status != 2 {
			t.Fatalf("build with a changing identity = %d, want 2; output:\n%s", status, output)
		}
		if !strings.Contains(output, "sources changed while they were compiling") {
			t.Fatalf("build did not say the sources changed:\n%s", output)
		}
		if published(project) {
			t.Fatal("a build whose identity changed published a ready generation")
		}
	})

	// The point of building each guard on its own: one that does not compile
	// must not decide anything about the others, which is what the single
	// build it replaced did.
	t.Run("one guard that does not compile leaves the others published", func(t *testing.T) {
		project := newProject(t)
		broken := filepath.Join(project, "tools", "hooks", "genguard", "main.go")
		if err := os.WriteFile(broken, []byte("package main\n\nthis is not go\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if status, output := build(t, project, os.Getenv("PATH")); status != 0 {
			t.Fatalf("build with one broken guard = %d, want 0; output:\n%s", status, output)
		}
		bin := filepath.Join(project, ".claude", "hooks", ".bin")
		for _, hook := range []string{"gofmtcheck", "pidguard", "mergeguard"} {
			if _, err := os.Stat(filepath.Join(bin, hook)); err != nil {
				t.Errorf("guard %s was not published although it compiles: %v", hook, err)
			}
		}
		if _, err := os.Stat(filepath.Join(bin, "genguard")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the guard that does not compile was published: %v", err)
		}
		unbuilt, err := os.ReadFile(filepath.Join(bin, ".unbuilt"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(unbuilt)) != "genguard" {
			t.Fatalf(".unbuilt = %q, want only the guard that failed", unbuilt)
		}
	})

	t.Run("a generation missing a binary is rebuilt rather than trusted", func(t *testing.T) {
		project := newProject(t)
		if status, output := build(t, project, os.Getenv("PATH")); status != 0 {
			t.Fatalf("seed build = %d; output:\n%s", status, output)
		}
		missing := filepath.Join(project, ".claude", "hooks", ".bin", "mergeguard")
		if err := os.Remove(missing); err != nil {
			t.Fatal(err)
		}
		// The identity still matches, so only the binary check can notice.
		if status, output := build(t, project, os.Getenv("PATH")); status != 0 {
			t.Fatalf("rebuild = %d; output:\n%s", status, output)
		}
		if _, err := os.Stat(missing); err != nil {
			t.Fatalf("a generation missing a guard was accepted as current: %v", err)
		}
	})

	t.Run("a failed build invalidates the generation it replaces", func(t *testing.T) {
		project := newProject(t)
		if status, output := build(t, project, os.Getenv("PATH")); status != 0 {
			t.Fatalf("seed build = %d; output:\n%s", status, output)
		}
		if !published(project) {
			t.Fatal("seed build published nothing")
		}
		// The generation has to be stale for the build to do anything: a
		// current one short-circuits after the lock, which is the point of
		// that check.
		if err := os.WriteFile(filepath.Join(project, "internal", "commitcheck", "check.go"),
			[]byte("package commitcheck\n\nconst Name = \"changed\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		bin := filepath.Join(project, "double")
		if err := os.Mkdir(bin, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bin, "go"), []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		if status, _ := build(t, project, bin+":"+os.Getenv("PATH")); status == 0 {
			t.Fatal("a build whose compiler refuses reported success")
		}
		if published(project) {
			t.Fatal("a failed build left the previous generation marked ready")
		}
	})
}

// goDirective reads the Go version the repository pins, so a fixture module
// resolves the same toolchain the hooks are built with.
func goDirective(t *testing.T, root string) string {
	t.Helper()
	for _, line := range strings.Split(string(read(t, filepath.Join(root, "go.mod"))), "\n") {
		if fields := strings.Fields(line); len(fields) == 2 && fields[0] == "go" {
			return fields[1]
		}
	}
	t.Fatal("go.mod has no go directive")
	return ""
}

func TestThePullRequestTemplateCarriesTheSkillsHeadings(t *testing.T) {
	root := repoRoot(t)

	skill := read(t, filepath.Join(root, ".agents", "skills", "comms-pr", "SKILL.md"))
	item := regexp.MustCompile(`(?m)^\d+\. \*\*([^*]+)\*\*`)
	var want []string
	for _, m := range item.FindAllStringSubmatch(string(skill), -1) {
		want = append(want, m[1])
	}
	if len(want) == 0 {
		t.Fatal("the comms-pr skill lists no numbered sections; the shape this test pins has moved")
	}

	template := read(t, filepath.Join(root, ".github", "PULL_REQUEST_TEMPLATE.md"))
	var got []string
	for _, line := range strings.Split(string(template), "\n") {
		if strings.HasPrefix(line, "## ") {
			got = append(got, strings.TrimPrefix(line, "## "))
		}
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("the PR template's headings and the comms-pr skill's sections differ; change both or neither\n skill:    %q\n template: %q", want, got)
	}
}
