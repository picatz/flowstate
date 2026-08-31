package plugin

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/picatz/flowstate/internal/pathsec"
)

// BinaryPrefix is what a plugin executable's name begins with. The rest of the
// name is the plugin's name, so `flowstate-plugin-vault` provides "vault".
//
// Discovery by naming convention rather than by a manifest file means nothing is
// loaded, parsed, or trusted to find out what is installed: the host asks each
// binary what it does, which is the only answer that can be true.
const BinaryPrefix = "flowstate-plugin-"

// MaxNameLen is the longest plugin name, matching the constraint the schema puts
// on PluginManifest.name.
const MaxNameLen = 64

// Found is a plugin binary discovered on the search path.
type Found struct {
	// Name is the plugin's name, taken from the binary's suffix. It is the
	// host's identity for the plugin everywhere: a plugin cannot rename itself
	// by describing itself differently, so it cannot claim to be another one.
	Name string

	// Path is the absolute path of the executable.
	Path string

	// Dir is the search path entry it was found in.
	Dir string
}

// Discover returns the plugin binaries on the configured search path, sorted by
// name.
//
// A name found in more than one directory resolves to the earliest directory in
// the search path, and the shadowed paths are reported through [Found] entries
// that are simply absent — precedence is decided by configuration order rather
// than by whatever order the filesystem returns entries in, which is not stable
// and is not the operator's choice.
//
// A directory that does not exist is skipped: a deployment reasonably configures
// the same search path across hosts that do not all have plugins installed. A
// directory that exists but is writable by other users is refused, because
// anything that can write there chooses what this process executes. See
// [Config.AllowInsecureSearchPath].
func Discover(cfg Config) ([]Found, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	log := cfg.logger()
	byName := make(map[string]Found)

	// A directory named twice is scanned once. SearchPath is a list and the
	// environment spells it as a path list, so `A:B:A` — or the same entry
	// passed to --plugin-dir twice — is ordinary input rather than a mistake
	// worth refusing. Scanning it again finds every binary already in byName
	// and would report each as shadowed by itself, using and ignoring the
	// identical path: a false warning on every plugin in that directory, on
	// every command and every worker start, which is exactly the "a warning
	// on every ordinary launch is one nobody reads" failure the notice below
	// exists to avoid (Codex, #1361).
	//
	// Compared by cleaned path rather than resolved through symlinks: the
	// former is free and catches how the duplicate is actually written, while
	// the latter would be a filesystem call per entry to catch a case nothing
	// has reported. Two paths that reach one directory by different links are
	// still scanned twice and still shadow each other, which is honest — they
	// are different entries, and the account says which one won.
	scanned := make(map[string]struct{}, len(cfg.SearchPath))

	for _, dir := range cfg.SearchPath {
		if _, already := scanned[filepath.Clean(dir)]; already {
			log.Debug("plugin search path entry appears more than once; scanning it once", "dir", dir)

			continue
		}
		scanned[filepath.Clean(dir)] = struct{}{}

		info, err := os.Stat(dir)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			log.Debug("plugin search path entry does not exist", "dir", dir)
			continue
		case err != nil:
			log.Warn("plugin search path entry cannot be read", "dir", dir, "error", err)
			continue
		case !info.IsDir():
			log.Warn("plugin search path entry is not a directory", "dir", dir)
			continue
		}

		decidable, err := checkPathIsTrusted(info, dir, cfg.AllowInsecureSearchPath)
		if err != nil {
			return nil, err
		}
		if err := checkPathAncestry(dir, cfg.AllowInsecureSearchPath); err != nil {
			return nil, err
		}
		if !decidable {
			warnOwnershipUnchecked(log, dir)
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			log.Warn("plugin search path entry cannot be listed", "dir", dir, "error", err)
			continue
		}

		for _, entry := range entries {
			name, ok := pluginName(entry.Name())
			if !ok {
				continue
			}

			path := filepath.Join(dir, entry.Name())

			// Stat rather than the DirEntry, so that a symlink is resolved to
			// what it points at: the thing that will be executed is what has to
			// be a regular executable file, not the link.
			info, err := os.Stat(path)
			if err != nil {
				log.Warn("plugin binary cannot be read", "plugin", name, "path", path, "error", err)
				continue
			}

			switch {
			case !info.Mode().IsRegular():
				log.Warn("plugin binary is not a regular file", "plugin", name, "path", path)
				continue
			case info.Mode().Perm()&0o111 == 0:
				log.Warn("plugin binary is not executable", "plugin", name, "path", path)
				continue
			}

			if _, err := checkPathIsTrusted(info, path, cfg.AllowInsecureSearchPath); err != nil {
				return nil, err
			}
			// Walked as well as checked, because os.Stat above followed any
			// symbolic link: what gets executed is the target, and the
			// directories leading to a target outside this search path entry
			// are not ones the walk of dir has seen.
			if err := checkPathAncestry(path, cfg.AllowInsecureSearchPath); err != nil {
				return nil, err
			}

			if existing, shadowed := byName[name]; shadowed {
				// Warn rather than Info, because of what is lost rather than
				// how the loser was chosen. First-wins is documented and
				// deliberate (--plugin-dir is "repeatable, in precedence
				// order"), but the shadowed binary takes *every task it
				// provides* with it, silently: a workflow's `plugins:`
				// requirement still resolves, because the winner answers to
				// the same name and version, and a step naming one of the
				// loser's tasks fails as an unknown task with nothing
				// connecting the two. This package already refuses outright
				// when two plugins claim one secret *scheme* ("two answers for
				// one scheme is a configuration error"); one name claimed
				// twice is that error with a larger blast radius, and it was
				// reported below the level every one of this CLI's plugin
				// surfaces logs at.
				log.Warn("plugin binary is shadowed by an earlier search path entry",
					"plugin", name, "using", existing.Path, "ignoring", path)
				continue
			}

			byName[name] = Found{Name: name, Path: path, Dir: dir}
		}
	}

	found := make([]Found, 0, len(byName))
	for _, f := range byName {
		found = append(found, f)
	}
	slices.SortFunc(found, func(a, b Found) int { return strings.Compare(a.Name, b.Name) })

	return found, nil
}

// pluginName returns the plugin name encoded in a file name.
//
// The name is constrained to what the schema permits for a manifest name, so a
// binary whose suffix could not be a valid plugin name is not a plugin — which
// also keeps a file name from carrying a path separator, a leading dash that
// would read as a flag, or anything else that would have to be quoted later.
func pluginName(fileName string) (string, bool) {
	name, ok := strings.CutPrefix(fileName, BinaryPrefix)
	if !ok {
		return "", false
	}
	if !validPluginName(name) {
		return "", false
	}

	return name, true
}

// validPluginName reports whether name is a name a discovered plugin could
// actually have: lower-case alphanumerics and interior hyphens, non-empty, and
// no longer than [MaxNameLen] — the same constraint the schema puts on a
// manifest name.
//
// It is its own function because two callers need the identical rule.
// [pluginName] applies it to a discovered file's suffix; [validateDigestPin]
// applies it to a pin's key, so that a pin naming something no plugin could be
// called — `PinnedDigests["GitHub"]` for a binary discovered as `github` — is a
// startup error rather than a pin that silently never matches and leaves the
// real plugin running unpinned. A security control keyed by a name has to reject
// a key that cannot name the thing it guards, or the guard fails open on a typo.
func validPluginName(name string) bool {
	if name == "" || len(name) > MaxNameLen {
		return false
	}

	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-' && i > 0:
		default:
			return false
		}
	}

	return true
}

// writableByOthers is every permission bit that lets a user who is not the
// owner write: world (0o002) and group (0o020).
//
// The group bit belongs here for the same reason the world bit does. "Any member
// of group staff chooses what this worker executes" is the same sentence with
// one word changed, and a group is not a curated thing from this process's point
// of view — it is a list somebody else maintains, which is exactly what a plugin
// search path may not be. Refusing only the world bit made the guarantee read
// stronger than it was.
const writableByOthers = 0o022

// checkPathIsTrusted refuses a path that somebody other than this worker can
// choose the contents of — by writing to it, or by owning it.
//
// A directory of plugin binaries is a list of programs this process will run
// with the worker's credentials and network reach, so write access to it is
// equivalent to code execution here. The sticky bit does not redeem a
// world-writable directory: it stops one user deleting another's files, and does
// nothing to stop anyone adding a flowstate-plugin-anything of their own.
//
// Ownership is the second half, and permission bits alone do not cover it:
// whoever owns a path can chmod it, or rename and replace an entry inside it,
// through their own owner bits whatever its group and world bits say. A 0755
// directory owned by another unprivileged uid is therefore still that uid's
// choice of what this worker executes. The two refusals are worded separately
// because they are separate things for an operator to fix — one is a chmod, the
// other a chown or an install to a different directory.
//
// This is deliberately stricter than the common 0o775 an installer leaves
// behind, and the escape hatch for a deployment that cannot change it is the one
// that already exists: [Config.AllowInsecureSearchPath], which says out loud
// what is being accepted.
//
// Returns a second value saying whether ownership could be decided at all, so
// [Discover] can tell an operator on a platform without POSIX ownership which
// half of the guarantee they are holding themselves — rather than the check
// looking like it passed.
func checkPathIsTrusted(info fs.FileInfo, path string, allow bool) (decidable bool, err error) {
	mode := info.Mode()

	if !allow && mode.Perm()&writableByOthers != 0 {
		who := "any user"
		if mode.Perm()&0o002 == 0 {
			who = "any member of its group"
		}

		return true, fmt.Errorf(
			"%w: %q is writable by %s (mode %#o), which means they choose what this worker executes; fix its permissions, or set AllowInsecureSearchPath if this is a single-user image",
			ErrSearchPath, path, who, mode.Perm(),
		)
	}

	trusted, decidable := ownedByTrustedUser(path, info)
	if allow || !decidable || trusted {
		return decidable, nil
	}

	return true, fmt.Errorf(
		"%w: %q is owned by another user, who can replace it whatever its permission bits say, and so chooses what this worker executes; install it as root or as this service's own identity, or set AllowInsecureSearchPath if this is a single-user image",
		ErrSearchPath, path,
	)
}

// checkPathAncestry refuses a path whose *ancestors* somebody else can
// replace, which the two checks above cannot see (#972).
//
// [checkPathIsTrusted] asks who owns a path and who may write it. Neither
// question reaches one level up: an `/opt` owned by an untrusted uid lets that
// uid rename `/opt/plugins` and put its own directory there, whatever the
// permissions on the directory that used to be at that path — so the search
// path entry this process reads is that uid's choice, and every binary in it
// with it. It is the argument [checkPathIsTrusted] already makes about owning
// the leaf, applied to each component of the path leading to it.
//
// The walk is [pathsec.Checker.Check], which is `cmd/flow`'s ACME cache walk
// lifted into a package both can import rather than a second implementation
// beside it: it resolves the path the way the kernel does, component by
// component and through symbolic links, and took two corrections (#735, #736)
// to fit real deployments. An ACME account key and a directory of programs
// this worker executes are not stakes that warrant two different standards.
//
// The leaf keeps its own, stricter rule rather than deferring to the walk's:
// [pathsec] exempts a sticky directory, because on one the kernel lets only an
// entry's owner rename it — which protects an *ancestor* and does nothing for
// a search path entry, where the attack is adding a
// `flowstate-plugin-anything` of one's own rather than renaming what is there.
// So both run, and the leaf's refusal is reported first because it is the one
// an operator can usually fix with a chmod.
//
// [Config.AllowInsecureSearchPath] covers this refusal for the reason it
// covers the other two: a deployment that has said out loud it accepts an
// untrusted search path should not then meet a third refusal it cannot turn
// off.
//
// A platform that cannot decide answers [pathsec.ErrUnsupported], which is not
// a refusal — [warnOwnershipUnchecked] is what tells the operator that half of
// this did not run.
func checkPathAncestry(path string, allow bool) error {
	if allow {
		return nil
	}

	err := newPathChecker().Check(path)
	switch {
	case err == nil, errors.Is(err, pathsec.ErrUnsupported):
		return nil
	}

	return fmt.Errorf(
		"%w: %q cannot be resolved safely: %w; whoever can replace a directory on the way to a "+
			"plugin directory chooses what this worker executes, whatever the permissions on the "+
			"directory itself; install plugins under a path owned by root or by this service's own "+
			"identity, or set AllowInsecureSearchPath if this is a single-user image",
		ErrSearchPath, path, err,
	)
}

// warnOwnershipUnchecked says once, per search path entry, that half the
// guarantee did not run.
//
// A platform without POSIX ownership still gets the permission-bit checks, and
// an operator reading documentation that describes both is entitled to know
// which one their deployment actually got. Said once per directory rather than
// once per binary, because the answer is a property of the platform and a line
// per plugin would be noise nobody reads.
func warnOwnershipUnchecked(log *slog.Logger, dir string) {
	log.Warn("plugin search path entry was not checked for ownership, because this platform "+
		"does not expose POSIX ownership; its permission bits were checked, and keeping the "+
		"directory owned by this service's identity is the operator's responsibility here",
		"dir", dir)
}
