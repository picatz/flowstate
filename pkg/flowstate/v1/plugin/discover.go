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

	for _, dir := range cfg.SearchPath {
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

			if existing, shadowed := byName[name]; shadowed {
				log.Info("plugin binary is shadowed by an earlier search path entry",
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

	trusted, decidable := ownedByTrustedUser(info)
	if allow || !decidable || trusted {
		return decidable, nil
	}

	return true, fmt.Errorf(
		"%w: %q is owned by another user, who can replace it whatever its permission bits say, and so chooses what this worker executes; install it as root or as this service's own identity, or set AllowInsecureSearchPath if this is a single-user image",
		ErrSearchPath, path,
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
