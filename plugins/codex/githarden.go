package main

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// gitHardening is everything one run's Git invocations over a
// task-controlled workspace carry: the `-c` overrides, the entire
// environment those commands run with, and the empty directory
// core.hooksPath points at.
//
// It travels as one value rather than as a bare []string of overrides
// because the two halves are not independently safe. An override list
// applied to a command that inherited this worker's environment still runs
// whatever $GIT_EXTERNAL_DIFF, $GIT_CONFIG_GLOBAL or a writable ~/.gitconfig
// names, and a scrubbed environment on a command carrying no overrides still
// runs what the repository's own config names. Neither half is the
// hardening; the pair is.
type gitHardening struct {
	// args is the `-c key=value` prefix, recomputed against the repository's
	// config each time it is used - see hardenedGitConfig.
	args []string
	// env is the child's complete environment. Never derived from
	// os.Environ(): see gitEnv.
	env []string
	// hooksDir is an empty directory this process created, and doubles as
	// $HOME for the invocation - see gitEnv.
	hooksDir string
}

// gitEnv builds the entire environment every Git invocation in this plugin
// runs with, from an explicit allowlist - never os.Environ(), and never
// this process's environment with entries merely added or removed. It is
// the same rule process.go's childEnv applies to the codex subprocess, for
// the same reason stated there, applied to the other subprocess this plugin
// starts.
//
// The `-c` overrides in hardenedGitConfig answer what the *repository*
// configures. This answers the two sources they cannot reach:
//
//   - The worker's own ambient environment. GIT_EXTERNAL_DIFF, GIT_PAGER,
//     GIT_SSH_COMMAND, GIT_PROXY_COMMAND, GIT_ASKPASS, GIT_DIR,
//     GIT_WORK_TREE, GIT_INDEX_FILE and GIT_CONFIG_* each name a program or
//     redirect where Git reads state from, and every one of them was
//     inherited by these commands before this. They are absent here by
//     construction: this list is built from nothing, so a variable has to be
//     named below to survive.
//   - Global and system config. GIT_CONFIG_GLOBAL and GIT_CONFIG_SYSTEM
//     point at /dev/null and GIT_CONFIG_NOSYSTEM refuses the system file
//     outright, with $HOME pointed at an empty directory this process made
//     so that ~/.gitconfig and ~/.config/git/config have nowhere to be read
//     from either. This is not hypothetical for a DANGER_FULL_ACCESS run:
//     that mode can write the worker user's own ~/.gitconfig, and the
//     post-run diff would then read a config file the run itself authored,
//     from outside the workspace the repository sweep looks at.
//
// GIT_ATTR_NOSYSTEM refuses the system gitattributes file for the same
// reason (attributes are how a filter driver gets bound to a path).
// GIT_NO_LAZY_FETCH keeps a partial clone's promisor remote from running its
// transport helper to materialize a missing object, and GIT_ALLOW_PROTOCOL
// set to the empty string is an allowlist naming no protocol at all - the
// environment-side pair of protocol.allow=never. GIT_TERMINAL_PROMPT=0 keeps
// a command that wants credentials from blocking on a terminal that is not
// there.
//
// PATH is the one value read from this process's environment, and is not an
// execution vector in its own right: a repository naming a helper can give
// an absolute path regardless. Git needs it for the helpers it legitimately
// locates that way, and childEnv passes it for the same reason.
//
// On a platform with no /dev/null the config variables name a path Git
// cannot read, which fails the invocation and therefore fails closed - no
// patch - rather than falling back to the file they exist to exclude.
func gitEnv(home string) []string {
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_ATTR_NOSYSTEM=1",
		"GIT_NO_LAZY_FETCH=1",
		"GIT_ALLOW_PROTOCOL=",
		"GIT_TERMINAL_PROMPT=0",
	}
}

// bootstrapGitOverrides is what the two Git commands that run *before* a
// config listing exists carry: the worktree check and the listing itself.
// Both are ordinary Git commands, so both would run a task-controlled
// core.fsmonitor, and both need the ownership answer safe.directory gives
// now that global and system config are excluded (see staticGitOverrides).
// They cannot carry the swept overrides, because computing those is what
// they are for.
func bootstrapGitOverrides(workDir string) []string {
	return []string{"-c", "core.fsmonitor=false", "-c", "safe.directory=" + workDir}
}

// staticGitOverrides is the fixed part of the `-c` prefix: the keys whose
// names are known in advance, each set to a value that names no program.
//
// Every entry is a way a repository asks Git to run something, or to wander
// somewhere it would then run something:
//
//   - core.fsmonitor. A program Git runs to ask what changed in the working
//     tree, consulted by anything that inspects the index. It needs no
//     gitattributes entry: the one config key is the whole attack.
//   - core.hooksPath. `git add` writes the index, which fires
//     post-index-change from wherever this points. There is no "no hooks"
//     value, only a directory with no hooks in it, which is what hooksDir is.
//   - core.alternateRefsCommand, core.sshCommand, core.gitProxy,
//     core.pager, core.editor, sequence.editor, credential.helper,
//     gpg.program, diff.external, uploadpack.packObjectsHook. Each names a
//     command for some Git operation; emptied, none of them does.
//   - protocol.allow and protocol.ext.allow. A promisor remote configured
//     with an `ext::` transport names an arbitrary program Git runs from
//     inside object retrieval, which no filter or hook override reaches.
//     `never` refuses the transport outright.
//   - submodule.recurse. `git status` and `git diff` can walk into an
//     initialized submodule and run Git against *that* checkout's
//     configuration - a second, differently-named set of filters and hooks
//     the superproject sweep never enumerated. Refusing to enter one at all
//     beats trying to recurse hardening into every nested repository.
//
// safe.directory is set separately, to workDir, because its value is not
// fixed: with global and system config excluded above, a deployment's own
// safe.directory entry is no longer visible, and a workspace owned by
// another uid would otherwise fail Git's ownership check. This plugin is
// deliberately operating on that directory, so it says so per invocation
// rather than reading someone's ambient answer.
func staticGitOverrides(hooksDir, workDir string) []string {
	return []string{
		"-c", "core.fsmonitor=false",
		"-c", "core.hooksPath=" + hooksDir,
		"-c", "core.alternateRefsCommand=",
		"-c", "core.sshCommand=",
		"-c", "core.gitProxy=",
		"-c", "core.pager=cat",
		"-c", "core.editor=",
		"-c", "sequence.editor=",
		"-c", "credential.helper=",
		"-c", "gpg.program=",
		"-c", "diff.external=",
		"-c", "uploadpack.packObjectsHook=",
		"-c", "protocol.allow=never",
		"-c", "protocol.ext.allow=never",
		"-c", "submodule.recurse=false",
		"-c", "safe.directory=" + workDir,
	}
}

// hardenedGitConfig computes the `-c` prefix for one repository as it stands
// right now, after checking that the repository is one this plugin is
// willing to touch at all.
//
// # Why the config listing is judged by an allowlist
//
// #682 disabled the repository config keys that were known to execute
// programs, key by key: diff.external, the filter drivers, core.fsmonitor.
// Each fix named a key somebody had thought of, and #700 is about the part
// that method cannot answer - Git's set of program-naming keys is not
// closed, it grows across releases, and the failure mode of a key nobody
// listed is silent: the patch renders and a repository-supplied program has
// run as the worker, outside the Codex sandbox.
//
// So the question this asks of a key is inverted. A key is *recognized* -
// by name, or by belonging to a section where no key names a program - or
// the whole listing fails closed and there is no patch. A key Git gains in
// some future release, or one this plugin's author never heard of, lands in
// the unrecognized case, which refuses. That turns "a key nobody thought of"
// from silent execution into a missing patch, which is the trade this makes
// and states: an unusual but harmless config costs a repository its patch
// output, while files_changed still reports what the run touched.
//
// A recognized key is handled one of three ways: neutralized by a fixed
// override (staticGitOverrides), neutralized by an override naming it
// specifically (the swept keys, whose subsection is the repository's to
// choose and so cannot be written down in advance), or left alone because
// it configures behavior rather than a program.
//
// Fails closed on: a repository whose gitdir is not the plain `.git`
// directory of the workspace itself (see gitWorktreeIsPlain), a config
// listing that cannot be read or that hits its bound, an unrecognized key,
// a key that needs an override but cannot receive one, and a hooks
// directory that does not exist.
func hardenedGitConfig(ctx context.Context, gitBin, workDir string, env []string, hooksDir string) ([]string, bool) {
	if hooksDir == "" {
		return nil, false
	}
	if info, err := os.Stat(hooksDir); err != nil || !info.IsDir() {
		return nil, false
	}
	if !gitWorktreeIsPlain(ctx, gitBin, workDir, env) {
		return nil, false
	}

	out, ok, truncated := runGitBounded(ctx, gitBin, workDir, env, maxGitConfigBytes,
		append(bootstrapGitOverrides(workDir), "config", "--list", "--name-only", "--null")...)
	if !ok || truncated {
		return nil, false
	}

	// Git config keys cannot contain NUL, so --name-only --null gives an
	// unambiguous list even when a task-controlled value contains newlines.
	var swept []string
	for _, key := range strings.Split(strings.TrimSuffix(out, "\x00"), "\x00") {
		if key == "" {
			continue
		}
		switch classifyConfigKey(key) {
		case configKeyInert:
			continue
		case configKeySwept:
			// `-c NAME=VALUE` is parsed by Git splitting the argument text at
			// its first `=`, but a quoted config subsection may itself legally
			// contain `=` - so a key such as `filter.evil=driver.clean` does
			// not round-trip through `key + "="`. Git would read the override
			// as setting `filter.evil` (to the value `driver.clean=`) rather
			// than the driver's own clean command, leaving the attacker's
			// filter enabled under its real name. There is no override
			// spelling that is safe for a key shaped like this, so its
			// presence fails the whole listing closed rather than silently
			// skipping just that one key.
			if strings.Contains(key, "=") {
				return nil, false
			}
			swept = append(swept, key)
		default:
			return nil, false
		}
	}
	sort.Strings(swept)

	overrides := staticGitOverrides(hooksDir, workDir)
	for _, key := range swept {
		overrides = append(overrides, "-c", key+"=")
	}
	return overrides, true
}

// configKeyClass is what hardenedGitConfig decides about one config key.
type configKeyClass int

const (
	// configKeyUnrecognized is the default, and refuses: see
	// hardenedGitConfig's doc comment on why the unknown case is the one
	// that matters.
	configKeyUnrecognized configKeyClass = iota
	// configKeyInert names no program, or names one a fixed override in
	// staticGitOverrides has already emptied.
	configKeyInert
	// configKeySwept names a program under a subsection the repository
	// chose, so the override naming it has to be built from the listing.
	configKeySwept
)

// sweptLeaves are the leaf names that make a three-part key a program the
// repository named: `filter.<driver>.clean`, `diff.<driver>.textconv`,
// `credential.<url>.helper`, `gpg.<format>.program` and their neighbours.
// The middle component is the repository's to choose, which is exactly why
// these cannot be written into staticGitOverrides and have to be read back
// out of the config listing instead.
var sweptLeaves = map[string]map[string]bool{
	"filter":     {"clean": true, "process": true, "smudge": true},
	"diff":       {"command": true, "textconv": true},
	"credential": {"helper": true},
	"gpg":        {"program": true},
}

// inertSections hold no key that names a program for any Git command this
// plugin runs. `lfs` is here for a different reason than the rest: Git never
// reads it at all - git-lfs does, and git-lfs only runs through the
// `filter.lfs.*` drivers, which are swept. Leaving it out would cost every
// Git-LFS repository its patch output for a section Git itself ignores.
var inertSections = map[string]bool{
	"advice": true,
	"color":  true,
	"gc":     true,
	"index":  true,
	"pack":   true,
	"lfs":    true,
}

// inertLeaves lists, per section, the leaf names that configure behavior
// rather than a command. A section appears here only where some *other* key
// in it does name a program, so the section as a whole cannot be inert.
var inertLeaves = map[string]map[string]bool{
	"core": {
		"repositoryformatversion": true, "filemode": true, "bare": true,
		"logallrefupdates": true, "symlinks": true, "ignorecase": true,
		"precomposeunicode": true, "autocrlf": true, "safecrlf": true,
		"eol": true, "quotepath": true, "abbrev": true, "commitgraph": true,
		"untrackedcache": true, "sparsecheckout": true, "sparsecheckoutcone": true,
		"compression": true, "loosecompression": true, "bigfilethreshold": true,
		"trustctime": true, "checkstat": true, "protecthfs": true,
		"protectntfs": true, "fsync": true, "fsyncmethod": true,
		"splitindex": true, "multipackindex": true, "preloadindex": true,
	},
	"extensions": {
		"objectformat": true, "compatobjectformat": true,
		"refstorage": true, "worktreeconfig": true,
	},
	"user":   {"name": true, "email": true, "signingkey": true, "useconfigonly": true},
	"commit": {"gpgsign": true, "template": true, "verbose": true, "cleanup": true},
	"tag":    {"gpgsign": true, "sort": true, "forcesignannotated": true},
	"branch": {"autosetupmerge": true, "autosetuprebase": true, "sort": true},
	"push":   {"default": true, "followtags": true, "autosetupremote": true},
	"pull":   {"rebase": true, "ff": true, "default": true},
	"fetch": {
		"prune": true, "prunetags": true, "parallel": true,
		"recursesubmodules": true, "writecommitgraph": true, "showforcedupdates": true,
	},
	"merge": {"ff": true, "conflictstyle": true, "autostash": true, "renamelimit": true, "renames": true},
	"diff": {
		"renames": true, "renamelimit": true, "algorithm": true,
		"ignoresubmodules": true, "submodule": true, "indentheuristic": true,
		"colormoved": true, "mnemonicprefix": true, "noprefix": true, "relative": true,
	},
	"status": {
		"showuntrackedfiles": true, "submodulesummary": true, "short": true,
		"branch": true, "relativepaths": true, "aheadbehind": true,
	},
	"log":       {"date": true, "decorate": true, "showsignature": true, "abbrevcommit": true},
	"init":      {"defaultbranch": true},
	"rerere":    {"enabled": true, "autoupdate": true},
	"rebase":    {"autostash": true, "autosquash": true, "updaterefs": true},
	"apply":     {"whitespace": true, "ignorewhitespace": true},
	"remote":    {"pushdefault": true},
	"submodule": {"active": true, "recurse": true, "fetchjobs": true},
	"include":   {"path": true},
}

// inertSubsectionLeaves is inertLeaves for three-part keys, where the middle
// component is a name the repository chose: `remote.<name>.url`,
// `branch.<name>.merge`, `submodule.<name>.path`. The omissions are the
// point - remote.<name>.uploadpack, .receivepack, .vcs and .proxy all name
// programs, and submodule.<name>.update accepts a `!command` form, so none
// of them is listed and each therefore refuses.
var inertSubsectionLeaves = map[string]map[string]bool{
	"remote": {
		"url": true, "pushurl": true, "fetch": true, "push": true,
		"mirror": true, "tagopt": true, "prune": true, "prunetags": true,
		"skipfetchall": true, "skipdefaultupdate": true,
		"partialclonefilter": true, "promisor": true, "followremotehead": true,
	},
	"branch": {
		"remote": true, "pushremote": true, "merge": true,
		"rebase": true, "description": true, "mergeoptions": true,
	},
	"submodule": {
		"url": true, "path": true, "active": true, "branch": true,
		"ignore": true, "shallow": true, "fetchrecursesubmodules": true,
	},
	"filter":     {"required": true},
	"diff":       {"xfuncname": true, "binary": true, "wordregex": true, "cachetextconv": true},
	"credential": {"username": true, "usehttppath": true},
	"includeif":  {"path": true},
	"alias":      {},
	"color":      {},
}

// staticallyOverridden are the keys staticGitOverrides already empties by
// name. They appear in the config listing like any other key - including the
// ones this plugin passes on its own command line, which `git config --list`
// reports from the command scope - and are inert precisely because that
// override is always present.
var staticallyOverridden = map[string]bool{
	"core.fsmonitor":             true,
	"core.hookspath":             true,
	"core.alternaterefscommand":  true,
	"core.sshcommand":            true,
	"core.gitproxy":              true,
	"core.pager":                 true,
	"core.editor":                true,
	"sequence.editor":            true,
	"credential.helper":          true,
	"gpg.program":                true,
	"diff.external":              true,
	"uploadpack.packobjectshook": true,
	"protocol.allow":             true,
	"protocol.ext.allow":         true,
	"submodule.recurse":          true,
	"safe.directory":             true,
}

// classifyConfigKey decides what hardenedGitConfig does about one key. It
// works on the lowercased name because Git's own section and leaf names are
// case-insensitive (only a subsection, the middle component, is not) - a
// repository writing `Core.FSMonitor` is naming the same key as
// `core.fsmonitor`, and a classifier that missed that would let a key
// through by capitalization alone.
func classifyConfigKey(key string) configKeyClass {
	lower := strings.ToLower(key)

	if staticallyOverridden[lower] {
		return configKeyInert
	}

	parts := strings.Split(lower, ".")
	if len(parts) < 2 {
		return configKeyUnrecognized
	}
	section, leaf := parts[0], parts[len(parts)-1]

	// alias.<name> reads as a two-part key whose leaf is the alias name, and
	// an alias value may legally begin with `!` and be a shell command - but
	// Git resolves an alias only for a word that is not a built-in command,
	// and every subcommand this plugin runs (config, rev-parse, status, add,
	// diff) is built in. An alias therefore cannot shadow any of them.
	if section == "alias" {
		return configKeyInert
	}

	if inertSections[section] {
		return configKeyInert
	}

	if len(parts) == 2 {
		if inertLeaves[section][leaf] {
			return configKeyInert
		}
		return configKeyUnrecognized
	}

	if sweptLeaves[section][leaf] {
		return configKeySwept
	}
	if inertSubsectionLeaves[section][leaf] {
		return configKeyInert
	}
	return configKeyUnrecognized
}

// gitWorktreeIsPlain reports whether workDir is the top level of an ordinary
// checkout whose gitdir is the plain `.git` directory sitting inside it.
//
// It replaces an earlier check that asked only whether workDir was inside
// *some* working tree, which several task-controlled arrangements answered
// yes to while meaning something quite different:
//
//   - A `.git` that is a file or a symlink. `gitdir: /somewhere/else` in a
//     `.git` file, or a symlinked `.git`, points Git at a repository outside
//     the workspace - whose config, hooks and objects are then what these
//     commands read and write. A run that writes such a file redirects the
//     post-run diff at a repository on the worker that the run itself never
//     had access to, and the diff's contents are returned to the caller.
//   - A real `.git` directory holding a `commondir` file. This is the subtle
//     one: the gitdir is genuinely the workspace's own `.git`, so a check on
//     --absolute-git-dir alone passes - but Git reads `.git/commondir` to set
//     GIT_COMMON_DIR, the location refs and objects are actually resolved
//     from, and a `commondir` naming a second repository makes the add and
//     diff read *that* repository. Confirmed on git 2.43: a clean workspace
//     produced a patch carrying a deleted secret file from the pointed-to
//     repo. So --git-common-dir is required to stay inside the workspace too.
//   - A workspace that is a subdirectory of a larger repository. `git diff`
//     with no pathspec reports the whole repository, so the patch would
//     carry changes from outside the jailed working_context.
//
// All refuse here, which costs a linked worktree, a submodule checkout and
// a subdirectory workspace their patch output - stated rather than hidden,
// and the same fail-closed trade the rest of this file makes.
func gitWorktreeIsPlain(ctx context.Context, gitBin, workDir string, env []string) bool {
	out, ok, truncated := runGitBounded(ctx, gitBin, workDir, env, maxGitConfigBytes,
		append(bootstrapGitOverrides(workDir), "rev-parse", "--show-toplevel", "--absolute-git-dir", "--git-common-dir")...)
	if !ok || truncated {
		return false
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		return false
	}
	toplevel := strings.TrimSpace(lines[0])
	gitDir := strings.TrimSpace(lines[1])
	commonDir := strings.TrimSpace(lines[2])
	if toplevel == "" || gitDir == "" || commonDir == "" {
		return false
	}

	// Git reports the toplevel with symlinks resolved, so the comparison has
	// to be made against a resolved workDir or an entirely ordinary checkout
	// reached through a symlinked path would refuse.
	resolved, err := filepath.EvalSymlinks(workDir)
	if err != nil {
		return false
	}
	if filepath.Clean(toplevel) != filepath.Clean(resolved) {
		return false
	}
	ownGitDir := filepath.Join(filepath.Clean(resolved), ".git")
	if filepath.Clean(gitDir) != ownGitDir {
		return false
	}

	// The gitdir being the workspace's own `.git` is not enough: a real `.git`
	// directory may contain a `commondir` file, and Git reads it to set
	// GIT_COMMON_DIR - the location refs and objects are actually read from.
	// --absolute-git-dir stays inside the workspace while --git-common-dir
	// points wherever `commondir` says, so a workspace whose `.git/commondir`
	// names a second repository has the add and diff below read *that*
	// repository's refs and objects: a clean workspace then produces a patch
	// carrying the pointed-to repository's content (a deleted secret file, in
	// the confirmed repro), which is exactly the cross-checkout exfiltration
	// this function exists to refuse. For an ordinary checkout --git-common-dir
	// reports a path relative to the command's working directory (literally
	// `.git`), while a `commondir` escape reports an absolute path elsewhere -
	// so it is resolved against workDir before comparison and, for a plain
	// repository, resolves to the workspace's own `.git`; anything else fails
	// closed. Compared against a symlink-resolved form because Git may report
	// either dir with symlinks resolved.
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(resolved, commonDir)
	}
	resolvedCommon, err := filepath.EvalSymlinks(commonDir)
	if err != nil {
		return false
	}
	resolvedOwn, err := filepath.EvalSymlinks(ownGitDir)
	if err != nil {
		return false
	}
	if filepath.Clean(resolvedCommon) != filepath.Clean(resolvedOwn) {
		return false
	}

	// Lstat, not Stat: the question is whether `.git` *is* a directory, not
	// whether it leads to one. A symlink to a directory passes the second
	// and is exactly the redirection this refuses.
	info, err := os.Lstat(gitDir)
	if err != nil || !info.Mode().IsDir() {
		return false
	}
	return true
}
