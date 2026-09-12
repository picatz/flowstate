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

func TestAmpShipProcedurePinsFinalHeadEvidence(t *testing.T) {
	raw := string(read(t, filepath.Join(repoRoot(t), ".agents", "ship.md")))
	ship := strings.Join(strings.Fields(raw), " ")
	for _, required := range []string{
		"without auto-merge",
		"distinct provider-neutral AI review",
		"code-security",
		"PASS/no-actionable-findings",
		"availability is optional",
		"at most once",
		"feedback is not optional once it arrives",
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
	// mergeguard is shared by Bash and the native GitHub merge tool.
	wantCommands[guardedCommand(`run-hook.sh" mergeguard`)] = 2

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
	for _, script := range []string{launcher, filepath.Join(root, ".claude", "hooks", "session-env.sh")} {
		cmd := exec.Command("bash", script)
		cmd.Env = []string{"PATH=/usr/bin:/bin"}
		output, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 || !strings.Contains(string(output), "CLAUDE_PROJECT_DIR") {
			t.Fatalf("%s without CLAUDE_PROJECT_DIR = %v, want explained exit 2; output:\n%s", script, err, output)
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
	// 127. The launcher tries to rebuild first; this fixture has no module to
	// build, so the attempt fails and the denial reports that rather than
	// telling the operator to restart.
	runLauncher("could not be rebuilt")

	sentinel := filepath.Join(project, "stale-hook-ran")
	stale := []byte("#!/bin/sh\ntouch " + strconv.Quote(sentinel) + "\n")
	writeStaleBuild := func() {
		t.Helper()
		if err := os.Remove(sentinel); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(hookDir, "mergeguard"), stale, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(hookDir, ".ready"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runFailedSession := func(path string, extraEnv ...string) {
		t.Helper()
		session := exec.Command("bash", filepath.Join(root, ".claude", "hooks", "session-env.sh"))
		session.Env = append(os.Environ(), append([]string{"CLAUDE_PROJECT_DIR=" + project, "PATH=" + path}, extraEnv...)...)
		if output, err := session.CombinedOutput(); err == nil {
			t.Fatalf("SessionStart unexpectedly accepted a failed hook build; output:\n%s", output)
		}
		runLauncher("could not be rebuilt")
		if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalidated stale hook executed: %v", err)
		}
	}

	// Even setup failures before compilation must invalidate an older build.
	writeStaleBuild()
	runFailedSession("/usr/bin:/bin") // go.mod is deliberately absent.

	sourceIDScript := filepath.Join(project, ".claude", "hooks", "source-id.sh")
	sourceIDData, err := os.ReadFile(filepath.Join(root, ".claude", "hooks", "source-id.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourceIDScript, sourceIDData, 0o700); err != nil {
		t.Fatal(err)
	}
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
	runFailedSession("/usr/bin:/bin")
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
	runFailedSession(fakeBin + ":/usr/bin:/bin")

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
	gitCount := filepath.Join(project, "git-ls-files-count")
	postBuildGit := "#!/bin/sh\nfor arg do\n  if [ \"$arg\" = ls-files ]; then\n    count=$(cat \"$FLOWSTATE_TEST_GIT_COUNT\" 2>/dev/null || echo 0)\n    count=$((count + 1))\n    printf '%s\\n' \"$count\" > \"$FLOWSTATE_TEST_GIT_COUNT\"\n    if [ \"$count\" -gt 1 ]; then exit 1; fi\n  fi\ndone\nexec " + strconv.Quote(realGit) + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(postBuildBin, "git"), []byte(postBuildGit), 0o700); err != nil {
		t.Fatal(err)
	}
	postBuildGo := "#!/bin/sh\nfor arg do\n  if [ \"$arg\" = list ]; then\n    printf '%s\\n' \"$CLAUDE_PROJECT_DIR/tools/hooks\" \"$CLAUDE_PROJECT_DIR/internal/commitcheck\" \"$CLAUDE_PROJECT_DIR/internal/textbound\"\n    exit 0\n  fi\ndone\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = -o ]; then shift; out=$1; break; fi\n  shift\ndone\nfor name in genguard gofmtcheck pidguard mergeguard; do\n  printf '#!/bin/sh\\nexit 0\\n' > \"$out/$name\"\n  chmod 700 \"$out/$name\"\ndone\n"
	if err := os.WriteFile(filepath.Join(postBuildBin, "go"), []byte(postBuildGo), 0o700); err != nil {
		t.Fatal(err)
	}
	runFailedSession(postBuildBin+":/usr/bin:/bin", "FLOWSTATE_TEST_GIT_COUNT="+gitCount)

	// A source change after SessionStart invalidates an otherwise ready binary.
	writeStaleBuild()
	if err := os.WriteFile(filepath.Join(hookDir, ".source-id"), []byte(sourceID()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dependencySource, []byte("package hooks\n\nconst changed = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runLauncher("could not be rebuilt")
	if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale hook ran after its sources changed: %v", err)
	}

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
	runLauncher("could not be rebuilt", fakeGitBin+":/usr/bin:/bin")
	if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale hook ran after source enumeration failed: %v", err)
	}

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
