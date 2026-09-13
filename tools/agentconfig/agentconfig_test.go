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

	// An identity command that fails only after compilation must not publish
	// the hash it happened to print as a ready generation. The `go` double
	// honours `-o <file>`, because build-hooks.sh names a file there: a double
	// that treated it as a directory would fail every compile and stop at the
	// exit-3 warn path above, never reaching the identity this asserts.
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
	postBuildGo := "#!/bin/sh\nfor arg do\n  case \"$arg\" in *EmbedFiles*) exit 0 ;; esac\ndone\nfor arg do\n  if [ \"$arg\" = list ]; then\n    printf '%s\\n' \"$CLAUDE_PROJECT_DIR/tools/hooks\" \"$CLAUDE_PROJECT_DIR/internal/commitcheck\" \"$CLAUDE_PROJECT_DIR/internal/textbound\"\n    exit 0\n  fi\ndone\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = -o ]; then shift; out=$1; break; fi\n  shift\ndone\nprintf '#!/bin/sh\\nexit 0\\n' > \"$out\"\nchmod 700 \"$out\"\n"
	if err := os.WriteFile(filepath.Join(postBuildBin, "go"), []byte(postBuildGo), 0o700); err != nil {
		t.Fatal(err)
	}
	// The identity fails only after a successful compile, so SessionStart
	// publishes nothing. A build that cannot verify what it just compiled is
	// incoherent rather than merely unfinished, so the next call denies.
	runFailedSession(deniesTheCall, postBuildBin+":/usr/bin:/bin", "FLOWSTATE_TEST_GIT_COUNT="+gitCount)

	// A source change after SessionStart invalidates an otherwise ready binary.
	writeStaleBuild()
	if err := os.WriteFile(filepath.Join(hookDir, ".source-id"), []byte(sourceID()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dependencySource, []byte("package hooks\n\nconst changed = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runLauncherWarns("do not compile right now")

	// mergeguard is wired on Bash as well as on the merge tool, and it returns
	// immediately from every Bash call that is not a merge. So when it cannot
	// be built, the calls it would have ignored must still run -- denying them
	// would take away `go build` and `git`, the tools the repair itself needs,
	// and this entry did not deny them before it was prebuilt -- while
	// anything that could be a merge must not. The guard stays the only thing
	// that decides which merge; the launcher only decides what could be one.
	mergeLauncher := func(payload string, args ...string) (int, string) {
		t.Helper()
		cmd := exec.Command("bash", append([]string{launcher, "mergeguard"}, args...)...)
		cmd.Env = []string{"CLAUDE_PROJECT_DIR=" + project, "HOME=" + os.Getenv("HOME"), "PATH=" + os.Getenv("PATH")}
		cmd.Stdin = strings.NewReader(payload)
		output, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		switch {
		case err == nil:
			return 0, string(output)
		case errors.As(err, &exit):
			return exit.ExitCode(), string(output)
		default:
			t.Fatalf("run the merge guard launcher: %v\n%s", err, output)
			return 0, ""
		}
	}
	bashPayload := func(command string) string {
		return `{"hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"` + command + `"}}`
	}
	for _, allowed := range []string{
		"go build ./tools/hooks/mergeguard",
		"go test ./tools/hooks/mergeguard/...",
		"git status --short",
		"git diff --stat",
		"make fmt",
		"gofmt -l .",
	} {
		if status, output := mergeLauncher(bashPayload(allowed)); status != 0 {
			t.Fatalf("a broken merge guard blocked %q with exit %d; the repair it needs must still run:\n%s", allowed, status, output)
		}
	}
	// The spellings the over-approximation is pinned to cover, not every
	// spelling the guard recognizes: flags are inherited and may sit between
	// `pr` and the subcommand, the executable may come from an expansion, and
	// the word may be split by quoting, a backslash, or a line continuation.
	// Completeness is not claimed and cannot be -- the guard tokenizes, this
	// matches text -- so a spelling found outside this table is the known
	// residual tracked in #1967, not a contradiction. tools/hooks/mergeguard
	// stays the only recognizer; every row here must hold.
	for _, refused := range []string{
		"gh pr merge 1942 -R picatz/flowstate --squash",
		"gh pr merge https://github.com/picatz/flowstate/pull/1942",
		"gh --repo picatz/flowstate pr merge 1942",
		"gh pr --repo picatz/flowstate merge 1942 --match-head-commit abcdef",
		"gh pr -R picatz/flowstate merge 1942",
		"$GHBIN pr merge 1942",
		`gh pr m''erge 1942`,
		`gh pr m\"erge\" 1942`,
		`gh pr m\\erge 1942`,
		`gh pr mer\\\nge 1942 -R picatz/flowstate`,
	} {
		status, output := mergeLauncher(bashPayload(refused))
		if status != 2 {
			t.Fatalf("a broken merge guard let %q through with exit %d:\n%s", refused, status, output)
		}
		if !strings.Contains(output, "tools/hooks/mergeguard") {
			t.Fatalf("the denial did not name what to repair:\n%s", output)
		}
	}
	// The merge tool's own call carries no Bash command, and a payload the
	// launcher cannot read is not evidence of anything. Neither fails open.
	for _, entry := range []struct {
		name    string
		payload string
	}{
		{"the merge tool entry", `{"hook_event_name":"PreToolUse","tool_name":"mcp__github__merge_pull_request","tool_input":{"pullNumber":1942}}`},
		{"an unreadable payload", ""},
	} {
		if status, output := mergeLauncher(entry.payload); status != 2 {
			t.Fatalf("%s with a broken merge guard = %d, want 2:\n%s", entry.name, status, output)
		}
	}
	if status, output := mergeLauncher(bashPayload("go build ./..."), "strict"); status != 2 {
		t.Fatalf("the strict call site failed open with exit %d:\n%s", status, output)
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
		writeHookFixture(t, root, project)
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

// TestClaudeHookLauncherHandsTheGuardItsPayload pins the one property the
// launcher can break silently: a current generation hands the guard the exact
// bytes Claude Code piped in. The launcher reads stdin itself to decide what a
// merge is, and the checks it runs are invoked from /dev/null, so any new read
// added before the guard runs would take the payload with it. If that ever
// happens, hook.Read fails on empty input and genguard, pidguard and
// gofmtcheck each return without a decision -- three controls disabled at
// once, at exit 0, with nothing written anywhere. (The redirects on the
// currency check and the rebuild are belt and braces rather than what this
// catches: neither script reads stdin today.)
func TestClaudeHookLauncherHandsTheGuardItsPayload(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Claude hooks require Bash")
	}
	root := repoRoot(t)
	project := t.TempDir()
	writeHookFixture(t, root, project)
	launcher := filepath.Join(project, ".claude", "hooks", "run-hook.sh")
	data, err := os.ReadFile(filepath.Join(root, ".claude", "hooks", "run-hook.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(launcher, data, 0o700); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("bash", filepath.Join(project, ".claude", "hooks", "build-hooks.sh"))
	build.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the fixture hooks: %v\n%s", err, output)
	}

	// The published guard is replaced by one that records what it was given,
	// which is the only way to observe the payload actually arriving.
	received := filepath.Join(project, "received")
	recorder := "#!/bin/sh\ncat > " + strconv.Quote(received) + "\n"
	if err := os.WriteFile(filepath.Join(project, ".claude", "hooks", ".bin", "genguard"), []byte(recorder), 0o700); err != nil {
		t.Fatal(err)
	}
	payload := `{"hook_event_name":"PreToolUse","tool_name":"Write","tool_input":{"file_path":"x.go"}}`
	cmd := exec.Command("bash", launcher, "genguard")
	cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project)
	cmd.Stdin = strings.NewReader(payload)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("launcher with a current generation: %v\n%s", err, output)
	}
	got, err := os.ReadFile(received)
	if err != nil {
		t.Fatalf("the guard read no payload at all: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("the guard received %q, want the tool call %q", got, payload)
	}
}

// TestClaudeHookLauncherAsksTheRetainedMergeGuard pins the mechanism that
// closes a class the launcher's text test cannot: mergeguard decides what a
// merge is by tokenizing the command the way a shell would, so a subcommand
// assembled by an expansion is a merge to it and is invisible to any pattern.
// Successive review rounds each found one more such spelling, which is what a
// stand-in for a tokenizer buys. The build now retains the last merge guard
// that compiled, and the launcher asks it, so while the sources are mid-repair
// the decision is still made by a recognizer.
//
// What is asserted is the whole shape, because each half is unsafe alone: the
// retained guard refuses what only it can see, the text backstop still runs
// when there is no retained guard or it found nothing, and neither refuses the
// commands a repair needs.
func TestClaudeHookLauncherAsksTheRetainedMergeGuard(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Claude hooks require Bash")
	}
	root := repoRoot(t)
	project := t.TempDir()
	writeHookFixture(t, root, project)
	for _, script := range []string{"run-hook.sh"} {
		data, err := os.ReadFile(filepath.Join(root, ".claude", "hooks", script))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(project, ".claude", "hooks", script), data, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	launcher := filepath.Join(project, ".claude", "hooks", "run-hook.sh")
	retained := filepath.Join(project, ".claude", "hooks", ".lkg")

	// A stand-in for the compiled guard: it refuses exactly the spelling the
	// text backstop cannot reach, so a pass here can only come from the
	// launcher having consulted it.
	expansion := "gh pr m${EMPTY:-}erge 1942 -R picatz/flowstate"
	if err := os.MkdirAll(retained, 0o700); err != nil {
		t.Fatal(err)
	}
	recognizer := "#!/bin/sh\npayload=$(cat)\n" +
		"case \"$payload\" in\n" +
		"  *'m${EMPTY:-}erge'*) printf '%s\\n' '{\"decision\":\"block\",\"hookSpecificOutput\":{\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"retained recognizer saw a merge\"}}' ;;\n" +
		"esac\nexit 0\n"
	writeRetained := func(t *testing.T, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(retained, "mergeguard"), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(retained, ".source-id"), []byte("retainedsourceid\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeRetained(t, recognizer)

	// The guard's own sources must not compile, because that is the only
	// state in which the retained binary is consulted at all.
	if err := os.WriteFile(filepath.Join(project, "tools", "hooks", "mergeguard", "main.go"),
		[]byte("package main\n\nthis is not go\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	run := func(t *testing.T, command string) (int, string, string) {
		t.Helper()
		cmd := exec.Command("bash", launcher, "mergeguard")
		cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project)
		cmd.Stdin = strings.NewReader(
			`{"hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"` + command + `"}}`)
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		var exit *exec.ExitError
		switch {
		case err == nil:
			return 0, stdout.String(), stderr.String()
		case errors.As(err, &exit):
			return exit.ExitCode(), stdout.String(), stderr.String()
		default:
			t.Fatalf("run the launcher: %v", err)
			return 0, "", ""
		}
	}

	t.Run("a spelling only the recognizer can see", func(t *testing.T) {
		status, stdout, stderr := run(t, expansion)
		if status != 0 {
			t.Fatalf("passing the guard's own decision through = %d, want 0; stderr:\n%s", status, stderr)
		}
		if !strings.Contains(stdout, `"deny"`) {
			t.Fatalf("the retained guard's refusal did not reach Claude Code:\n%s", stdout)
		}
		if !strings.Contains(stdout, "retained recognizer saw a merge") {
			t.Fatalf("the guard's own reason was not passed through:\n%s", stdout)
		}
		if !strings.Contains(stderr, "the last build of it that did") {
			t.Fatalf("the operator was not told the decision came from a retained binary:\n%s", stderr)
		}
		// The identity recorded beside the binary is named, so an operator can
		// see which build decided rather than only that an earlier one did.
		recorded, err := os.ReadFile(filepath.Join(retained, ".source-id"))
		if err != nil {
			t.Fatalf("no identity was recorded beside the retained guard: %v", err)
		}
		if !strings.Contains(stderr, strings.TrimSpace(string(recorded))) {
			t.Fatalf("the note did not name the build that decided (%q):\n%s", recorded, stderr)
		}
	})

	t.Run("the commands a repair needs still run", func(t *testing.T) {
		for _, allowed := range []string{
			"go build ./tools/hooks/mergeguard",
			"git status --short",
			"make fmt",
		} {
			status, stdout, stderr := run(t, allowed)
			if status != 0 || strings.Contains(stdout, `"deny"`) {
				t.Fatalf("a retained guard blocked %q (exit %d):\n%s\n%s", allowed, status, stdout, stderr)
			}
		}
	})

	t.Run("the backstop still refuses what the recognizer missed", func(t *testing.T) {
		// This recognizer answers nothing, standing for one too old to know a
		// spelling. The text test must still refuse the plain one.
		writeRetained(t, "#!/bin/sh\ncat > /dev/null\nexit 0\n")
		if status, _, _ := run(t, "gh pr merge 1942 -R picatz/flowstate"); status != 2 {
			t.Fatalf("a silent recognizer let the plain spelling through with exit %d", status)
		}
	})

	t.Run("a retained binary that cannot run does not decide", func(t *testing.T) {
		// Not an executable at all, which is what a binary built for another
		// platform looks like here. It must neither refuse nor be trusted.
		writeRetained(t, "not a binary\n")
		if status, _, _ := run(t, "gh pr merge 1942 -R picatz/flowstate"); status != 2 {
			t.Fatalf("an unusable retained binary broke the backstop, exit %d", status)
		}
		if status, stdout, stderr := run(t, "go build ./tools/hooks/mergeguard"); status != 0 {
			t.Fatalf("an unusable retained binary blocked a repair, exit %d:\n%s\n%s", status, stdout, stderr)
		}
	})

	t.Run("a refusal from a guard that then failed is not a decision", func(t *testing.T) {
		// The real guards always exit 0 and say what they decided in JSON, so
		// a non-zero exit means the run came apart -- and output from a run
		// that came apart is not a judgement, however much of it looks like
		// one. Honouring it would let a crashing binary speak for the guard.
		// The call is still refused here, by the backstop rather than by this
		// output, which is what makes the two distinguishable: the operator is
		// not told a recognizer decided when none did.
		writeRetained(t, "#!/bin/sh\ncat > /dev/null\n"+
			"printf '%s\\n' '{\"hookSpecificOutput\":{\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"half-written\"}}'\n"+
			"exit 3\n")
		status, stdout, stderr := run(t, "gh pr merge 1942 -R picatz/flowstate")
		if status != 2 {
			t.Fatalf("a failed retained guard decided the call, exit %d:\n%s", status, stdout)
		}
		if strings.Contains(stdout, "half-written") {
			t.Fatalf("output from a failed guard was passed off as its decision:\n%s", stdout)
		}
		if strings.Contains(stderr, "the last build of it that did") {
			t.Fatalf("the operator was told a recognizer decided when none did:\n%s", stderr)
		}
		// And it must not block a repair either.
		if status, stdout, stderr := run(t, "go build ./tools/hooks/mergeguard"); status != 0 {
			t.Fatalf("a failed retained guard blocked a repair, exit %d:\n%s\n%s", status, stdout, stderr)
		}
	})

	t.Run("a refusal survives a guard that does not drain the payload", func(t *testing.T) {
		// The guard is fed from a here-string rather than a pipe, so its own
		// exit code is what is read. Through a pipe, a guard that decides
		// without draining a large payload kills the writer with SIGPIPE, and
		// `pipefail` would report that as the guard's failure -- throwing away
		// a refusal it had already made. Today's guard drains, but a retained
		// binary is by design one the launcher cannot inspect.
		writeRetained(t, "#!/bin/sh\nhead -c 200 > /dev/null\n"+
			"printf '%s\\n' '{\"hookSpecificOutput\":{\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"decided early\"}}'\n"+
			"exit 0\n")
		big := strings.Repeat("x", 200000)
		status, stdout, stderr := run(t, "gh pr "+"mer"+"ge 1942 # "+big)
		if status != 0 || !strings.Contains(stdout, "decided early") {
			t.Fatalf("a refusal was discarded because the guard did not drain the payload (exit %d):\n%s\n%s", status, stdout, stderr)
		}
	})

	t.Run("no retained binary leaves the previous behaviour", func(t *testing.T) {
		if err := os.RemoveAll(retained); err != nil {
			t.Fatal(err)
		}
		if status, _, _ := run(t, "gh pr merge 1942 -R picatz/flowstate"); status != 2 {
			t.Fatalf("without a retained guard the plain spelling was allowed, exit %d", status)
		}
		// A payload large enough that the backstop's own grep finishes before
		// the writer does. `grep -q` exits on its first match and the writer
		// dies of SIGPIPE, so under `pipefail` the pipeline reports 141 --
		// which a test of grep's status, rather than a read of it, took for
		// "no match" and allowed. A long heredoc reaches this.
		big := strings.Repeat("x", 700000)
		if status, _, stderr := run(t, "gh pr merge 1942 -R picatz/flowstate # "+big); status != 2 {
			t.Fatalf("a large payload let the plain spelling through, exit %d:\n%s", status, stderr)
		}
		if status, stdout, stderr := run(t, "git status --short"); status != 0 {
			t.Fatalf("without a retained guard a repair was blocked, exit %d:\n%s\n%s", status, stdout, stderr)
		}
	})
}

// TestClaudeHookBuildRetainsTheMergeGuardItCompiled pins the other half: a
// build that produces a merge guard keeps it, so the launcher above has one to
// ask. Only that guard is retained, because it is the only one whose answer to
// being unbuildable is a refusal rather than a warning -- a retained genguard
// could refuse the very edit that repairs it.
func TestClaudeHookBuildRetainsTheMergeGuardItCompiled(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Claude hooks require Bash")
	}
	root := repoRoot(t)
	project := t.TempDir()
	writeHookFixture(t, root, project)
	if output, err := runBuild(t, project, os.Getenv("PATH")); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	retained := filepath.Join(project, ".claude", "hooks", ".lkg")
	info, err := os.Stat(filepath.Join(retained, "mergeguard"))
	if err != nil {
		t.Fatalf("a successful build retained no merge guard: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("the retained merge guard is not executable: %v", info.Mode())
	}
	if _, err := os.Stat(filepath.Join(retained, ".source-id")); err != nil {
		t.Fatalf("the retained guard records no source identity: %v", err)
	}
	for _, other := range []string{"genguard", "gofmtcheck", "pidguard"} {
		if _, err := os.Stat(filepath.Join(retained, other)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s was retained; only the merge guard may be, since a stale one of these could refuse a repair", other)
		}
	}

	// The guard is replaced, never removed and re-created. Publishing the
	// directory wholesale would mean deleting the recognizer before its
	// replacement was in place, and a build killed in that window would leave
	// the next session with none -- falling back to a text test whose
	// incompleteness is exactly what retaining a binary is for. Asserted by
	// watching what the directory holds across a second successful build.
	guard := filepath.Join(retained, "mergeguard")
	first, err := os.ReadFile(guard)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, "internal", "commitcheck", "check.go"),
		[]byte("package commitcheck\n\nconst Name = \"commitcheck2\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	watched := make(chan bool, 1)
	stop := make(chan struct{})
	go func() {
		// Any moment at which the guard is absent is the window this asserts
		// against; a rename leaves no such moment.
		missing := false
		for {
			select {
			case <-stop:
				watched <- missing
				return
			default:
			}
			if _, err := os.Lstat(guard); errors.Is(err, os.ErrNotExist) {
				missing = true
			}
		}
	}()
	if output, err := runBuild(t, project, os.Getenv("PATH")); err != nil {
		close(stop)
		<-watched
		t.Fatalf("second build: %v\n%s", err, output)
	}
	close(stop)
	if <-watched {
		t.Fatal("the retained guard was absent while its replacement was being published")
	}
	second, err := os.ReadFile(guard)
	if err != nil {
		t.Fatalf("the second build left no retained guard: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Log("the two builds produced identical binaries; the replacement is still asserted by the absence check above")
	}

	// Retention runs after the generation is published, so a failure in it
	// must not turn a build that succeeded into one the launcher reads as
	// incoherent -- that denies an unrelated tool call, with a diagnostic
	// naming a cause that has nothing to do with the guards. `set -e` is
	// suppressed for the command in an `if` condition but not for the commands
	// in its body, which is why the whole of retention is a function invoked
	// with `|| true` rather than a bare block.
	t.Run("a build whose retention fails still succeeds", func(t *testing.T) {
		project := t.TempDir()
		writeHookFixture(t, root, project)
		bin := filepath.Join(project, "double")
		if err := os.Mkdir(bin, 0o700); err != nil {
			t.Fatal(err)
		}
		realMktemp, err := exec.LookPath("mktemp")
		if err != nil {
			t.Skip("mktemp is not available")
		}
		// Fails only for retention's own staging template, so every other use
		// of mktemp in the build still works and this isolates the one step.
		double := "#!/bin/sh\nfor arg do\n  case \"$arg\" in *build.lkg.*)" +
			" printf 'mktemp: simulated failure\\n' >&2; exit 1 ;; esac\ndone\nexec " +
			strconv.Quote(realMktemp) + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(bin, "mktemp"), []byte(double), 0o700); err != nil {
			t.Fatal(err)
		}
		output, err := runBuild(t, project, bin+":"+os.Getenv("PATH"))
		if err != nil {
			t.Fatalf("a build whose retention failed reported failure: %v\n%s", err, output)
		}
		// The generation it published must still be complete and current, so
		// the launcher has no reason to deny anything.
		for _, name := range []string{"genguard", "gofmtcheck", "pidguard", "mergeguard", ".ready", ".source-id"} {
			if _, err := os.Stat(filepath.Join(project, ".claude", "hooks", ".bin", name)); err != nil {
				t.Fatalf("the published generation is missing %s: %v", name, err)
			}
		}
	})

	// A later build that cannot compile the guard must leave the retained one
	// alone rather than clearing it -- that is the whole point of keeping it.
	if err := os.WriteFile(filepath.Join(project, "tools", "hooks", "mergeguard", "main.go"),
		[]byte("package main\n\nthis is not go\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(retained, "mergeguard"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runBuild(t, project, os.Getenv("PATH")); err != nil {
		t.Logf("build with a broken guard failed as expected: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(retained, "mergeguard"))
	if err != nil {
		t.Fatalf("a build that could not compile the guard discarded the retained one: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("a build that could not compile the guard replaced the retained one")
	}
}

// TestClaudeHookBuildRunsOnAnOrdinaryCheckout covers the two properties that
// have nothing to do with a broken tree and everything to do with where the
// checkout happens to sit. Both failed silently in exactly the direction that
// matters: the guards are a control, so a build that cannot run, or that
// misjudges its own dependencies, disables them rather than announcing itself.
// CI runs one Linux image, so these are asserted against behavior a developer
// machine can differ on rather than against the host that happens to run them.
func TestClaudeHookBuildRunsOnAnOrdinaryCheckout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Claude hooks require Bash")
	}
	root := repoRoot(t)

	t.Run("a checkout reached through a symbolic link", func(t *testing.T) {
		// `go list` reports a package directory with every link resolved, so a
		// build that compares those against an unresolved CLAUDE_PROJECT_DIR
		// decides every first-party package lives outside the checkout. That
		// is not hypothetical: a macOS temporary directory is under
		// /var -> /private/var, and so is any checkout under one.
		project := t.TempDir()
		real := filepath.Join(project, "real")
		link := filepath.Join(project, "link")
		if err := os.Mkdir(real, 0o700); err != nil {
			t.Fatal(err)
		}
		writeHookFixture(t, root, real)
		if err := os.Symlink(real, link); err != nil {
			t.Skipf("this filesystem does not support symbolic links: %v", err)
		}
		cmd := exec.Command("bash", filepath.Join(link, ".claude", "hooks", "build-hooks.sh"))
		cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+link)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build through a symbolic link: %v\n%s", err, output)
		}
		if _, err := os.Stat(filepath.Join(real, ".claude", "hooks", ".bin", ".ready")); err != nil {
			t.Fatalf("a build through a symbolic link published nothing: %v", err)
		}
	})

	// Both loops that read a command's output through a file: a process
	// substitution's failure reaches neither `set -e` nor `pipefail`, so the
	// loop reads nothing and the identity comes back narrowed at exit 0 --
	// over the manifests alone, or over the directory names alone. Either is
	// stable, so it goes on matching while the sources change underneath it,
	// and the launcher goes on running a guard built from code that no longer
	// exists. That is the one failure this identity exists to prevent, and it
	// is the quietest way to get one, so each must refuse rather than narrow.
	for _, blind := range []struct {
		name    string
		command string
		refusal string
	}{
		{"a walk that cannot enumerate a package", "find", "could not enumerate"},
		{"an order that cannot be established", "sort", "could not order"},
	} {
		t.Run(blind.name, func(t *testing.T) {
			project := t.TempDir()
			writeHookFixture(t, root, project)
			if output, err := runBuild(t, project, os.Getenv("PATH")); err != nil {
				t.Fatalf("seed build: %v\n%s", err, output)
			}
			bin := filepath.Join(project, "double")
			if err := os.Mkdir(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, blind.command),
				[]byte("#!/bin/sh\nprintf '"+blind.command+": broken\\n' >&2\nexit 1\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			manifest := filepath.Join(project, ".claude", "hooks", ".bin", ".source-dirs")
			cmd := exec.Command("bash", filepath.Join(project, ".claude", "hooks", "source-id.sh"), manifest)
			cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project, "PATH="+bin+":"+os.Getenv("PATH"))
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("a blind %s still produced an identity:\n%s", blind.command, output)
			}
			if !strings.Contains(string(output), blind.refusal) {
				t.Fatalf("the identity did not say %s failed:\n%s", blind.command, output)
			}
		})
	}

	t.Run("a mktemp that requires a template", func(t *testing.T) {
		// BSD `mktemp` -- macOS -- is a usage error without one, where GNU
		// coreutils defaults. source-id.sh stands under every guard, so the
		// difference is not a portability nit: it denies every tool call, on a
		// healthy tree, with no tool left to repair anything.
		project := t.TempDir()
		writeHookFixture(t, root, project)
		bin := filepath.Join(project, "double")
		if err := os.Mkdir(bin, 0o700); err != nil {
			t.Fatal(err)
		}
		realMktemp, err := exec.LookPath("mktemp")
		if err != nil {
			t.Skip("mktemp is not available")
		}
		double := "#!/bin/sh\nfor arg do\n  case \"$arg\" in -*) ;; *) exec " +
			strconv.Quote(realMktemp) + " \"$@\" ;; esac\ndone\n" +
			"printf 'usage: mktemp [-d] [-q] [-t prefix] [-u] template ...\\n' >&2\nexit 1\n"
		if err := os.WriteFile(filepath.Join(bin, "mktemp"), []byte(double), 0o700); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("bash", filepath.Join(project, ".claude", "hooks", "build-hooks.sh"))
		cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project, "PATH="+bin+":"+os.Getenv("PATH"))
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build where mktemp requires a template: %v\n%s", err, output)
		}
		if _, err := os.Stat(filepath.Join(project, ".claude", "hooks", ".bin", ".ready")); err != nil {
			t.Fatalf("published nothing where mktemp requires a template: %v", err)
		}
	})

	t.Run("a session start whose build fails still pins the toolchain", func(t *testing.T) {
		// The pin and the prebuild are independent, and only one of them can
		// be recovered later: the launcher rebuilds the hooks on the next tool
		// call, while a session that never got the pin resolves `go` and
		// `gofmt` from the base image for as long as it lives.
		project := t.TempDir()
		writeHookFixture(t, root, project)
		// The shared package, so every guard fails and the build reports the
		// tree does not compile rather than publishing a partial generation.
		if err := os.WriteFile(filepath.Join(project, "internal", "commitcheck", "check.go"),
			[]byte("package commitcheck\n\nthis is not go\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		envFile := filepath.Join(project, "env")
		cmd := exec.Command("bash", filepath.Join(project, ".claude", "hooks", "session-env.sh"))
		cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project, "CLAUDE_ENV_FILE="+envFile)
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("a session start whose hooks did not compile reported success:\n%s", output)
		}
		pin, readErr := os.ReadFile(envFile)
		if readErr != nil {
			t.Fatalf("a failing hook build took the toolchain pin with it: %v\n%s", readErr, output)
		}
		if !strings.Contains(string(pin), "export PATH=") {
			t.Fatalf("the session was left without a pinned toolchain: %q", pin)
		}
	})
}

// runBuild compiles a fixture's guard hooks the way SessionStart does.
func runBuild(t *testing.T, project, path string) (string, error) {
	t.Helper()
	cmd := exec.Command("bash", filepath.Join(project, ".claude", "hooks", "build-hooks.sh"))
	cmd.Env = append(os.Environ(), "CLAUDE_PROJECT_DIR="+project, "PATH="+path)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// writeHookFixture lays down a module the real toolchain can build the four
// guards from, so a case fails only for the reason it is testing.
func writeHookFixture(t *testing.T, root, project string) {
	t.Helper()
	for _, script := range []string{"source-id.sh", "build-hooks.sh", "session-env.sh"} {
		data, err := os.ReadFile(filepath.Join(root, ".claude", "hooks", script))
		if err != nil {
			t.Fatal(err)
		}
		full := filepath.Join(project, ".claude", "hooks", script)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, data, 0o700); err != nil {
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
	write("internal/commitcheck/check.go", "package commitcheck\n\nconst Name = \"commitcheck\"\n")
	for _, hook := range []string{"genguard", "gofmtcheck", "pidguard", "mergeguard"} {
		write("tools/hooks/"+hook+"/main.go",
			"package main\n\nimport _ \"example.com/hooks/internal/commitcheck\"\n\nfunc main() {}\n")
	}
	if output, err := exec.Command("git", "-C", project, "init", "--quiet").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
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

// TestThePullRequestTemplateCarriesTheSkillsHeadings keeps the template a
// web-opened PR starts from and the comms-pr skill's default shape one list:
// the skill's numbered sections are the template's `##` headings, in order,
// so neither drifts from the other unnoticed (#1728). It lives here rather
// than beside tools/commitcheck because this package is the one the gate runs
// for a diff to the agent configuration, which the template now counts as.
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
