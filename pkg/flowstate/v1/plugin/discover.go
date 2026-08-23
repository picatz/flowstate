package plugin

import (
	"errors"
	"fmt"
	"io/fs"
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

		if err := checkWritableByOthers(info, dir, cfg.AllowInsecureSearchPath); err != nil {
			return nil, err
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

			if err := checkWritableByOthers(info, path, cfg.AllowInsecureSearchPath); err != nil {
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
	if !ok || name == "" || len(name) > MaxNameLen {
		return "", false
	}

	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-' && i > 0:
		default:
			return "", false
		}
	}

	return name, true
}

// checkWritableByOthers refuses a path that users other than its owner can
// write to.
//
// A directory of plugin binaries is a list of programs this process will run
// with the worker's credentials and network reach, so write access to it is
// equivalent to code execution here. The sticky bit does not redeem a
// world-writable directory: it stops one user deleting another's files, and does
// nothing to stop anyone adding a flowstate-plugin-anything of their own.
func checkWritableByOthers(info fs.FileInfo, path string, allow bool) error {
	if allow {
		return nil
	}

	mode := info.Mode()
	if mode.Perm()&0o022 == 0 && ownedByTrustedUser(info) {
		return nil
	}

	return fmt.Errorf(
		"%w: %q is not owned by the worker or root, or is writable by another user (mode %#o), which means another user may choose what this worker executes; fix its ownership and permissions, or set AllowInsecureSearchPath if this is a single-user image",
		ErrSearchPath, path, mode.Perm(),
	)
}
