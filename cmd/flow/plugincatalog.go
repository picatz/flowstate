package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// The offline half of #710: what a plugin provides, read from a document,
// with no process launched.
//
// #835 gave `validate`, `tasks` and `fix` --plugin-dir, which answers the
// question by *executing* the plugins. That is the right answer for a person
// with the binaries on their machine and no answer at all for the three
// surfaces #710 names — a browser authoring surface (#102, #242), a
// server-side Validate RPC, and a CI job that validates a repository's plugin
// examples with no plugin binaries in the runner. None of them can exec.
//
// So the same facts arrive as a file. `flow plugins --output json` already
// writes one ([runPlugins]), #854 made a task's descriptors travel in it, and
// [plugin.TaskDefsFromCatalog] rebuilds bounded task definitions out of it that
// refuse everything a launched definition refuses. This file is the flag, the
// bounded read, and the registration — the last span, in the sense
// plugins.go's own opening comment uses.
//
// Two decisions are worth reading before changing anything here, because both
// are the shape #835 argued rather than a new one:
//
//   - It is opt-in from the command line, or from a plugins.lock.json that sits
//     above the very file being checked ([discoverPluginLock], added for the
//     shipped examples and trusted exactly as that file is). Without either,
//     and without --plugin-dir, these verbs consult this build's own tasks, a
//     step naming a plugin task still gets unknownTaskMessage's installation
//     question, and nothing on disk is read.
//   - A catalog that fails to *load* fails the command, naming the file, and
//     nothing is validated, listed or rewritten. Carrying on with the registry
//     there is would report every task the catalog was carrying as an unknown
//     task: a diagnostic about the file, drawn from something that went wrong
//     with a document, and false.

// pluginCatalogFlag names the saved catalog to check against.
const pluginCatalogFlag = "plugin-catalog"

// maxPluginCatalogBytes bounds the document before it is parsed.
//
// [plugin.TaskDefsFromCatalog] bounds the decoded message — how many plugins,
// how many tasks, how many descriptor bytes across all of them — and every one
// of those bounds is checked after something has already unmarshaled the whole
// file into memory. The file itself is chosen by whoever wrote it, so it needs
// a bound of its own, ahead of the parse, in the shape the rest of the tree
// reads untrusted files in ([flowfile.readBoundedSource],
// [readFileBounded]): open once, ask the open file what it is, and read
// through a reader capped at one byte past the limit.
//
// Two times plugin.DefaultMaxCatalogDescriptorBytes, because protojson carries
// descriptor bytes as base64 — four characters per three bytes, plus the field
// names around them — so a catalog exactly at the descriptor bound is roughly
// 1.4 times this size on disk. A file bound tighter than the message bound
// would make the message bound unreachable, and a bound nothing reaches is a
// bound nothing tests.
const maxPluginCatalogBytes = 2 * plugin.DefaultMaxCatalogDescriptorBytes

// addPluginCatalogFlag declares --plugin-catalog on a command.
//
// Deliberately separate from [addPluginFlags] rather than folded into it. The
// launch flags are on `worker`, `server`, `run local` and `task run` as well,
// and a catalog cannot serve any of those: a definition rebuilt from one
// carries a function that refuses to execute ([plugin.ErrCatalogOnly]), so a
// worker registering one would accept a step it can only fail. This flag
// therefore goes on exactly the verbs that read a task definition without ever
// running it.
func addPluginCatalogFlag(cmd *cobra.Command) {
	cmd.Flags().String(pluginCatalogFlag, "",
		"check against a saved plugin catalog (`flow plugins --plugin-dir <dir> --output json`) "+
			"instead of launching plugins; no process is started")
}

// pluginCatalogPath is the file a command was pointed at, or "" for the
// commands that do not take the flag at all.
func pluginCatalogPath(cmd *cobra.Command) string {
	path, _ := cmd.Flags().GetString(pluginCatalogFlag)

	return path
}

// loadPluginCatalog reads the catalog a command was pointed at and registers
// every task in it, returning the catalog so a `plugins:` requirement resolves
// against the same document ([validatePluginRequirements]).
//
// It returns (nil, nil) when no catalog was named and none is discovered above
// the anchors (the files the verb was asked about; none for a verb with no
// file, and none for standard input).
//
// Registration is against [v1.DefaultRegistry] for the reason [startPlugins]
// records: that is the registry every lookup consults, and a registry made for
// the occasion would be a catalog that loaded, parsed, rebuilt every task, and
// answered `unknown task`.
func loadPluginCatalog(cmd *cobra.Command, anchors ...string) (*v1.PluginCatalog, error) {
	path := pluginCatalogPath(cmd)
	if path == "" {
		discovered, err := discoverPluginCatalogFor(cmd, anchors)
		if err != nil || discovered == "" {
			return nil, err
		}

		return registerPluginCatalog(discovered)
	}

	// An ambient search path loses to an explicit catalog, and says so once.
	// Silently ignoring configuration a machine already carries is how somebody
	// comes to believe their plugins were launched: the variable is set, the
	// verb printed a plugin's task, and nothing distinguishes the two sources
	// in the output. On the account stream, so a `-o json` consumer's document
	// is untouched.
	if ambientPluginSearchPath() != "" && !cmd.Flags().Changed("plugin-dir") {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"$%s is set and no plugin was launched: --%s named %s, and this reads it instead.\n",
			pluginSearchPathEnv, pluginCatalogFlag, path)
	}

	return registerPluginCatalog(path)
}

// registerPluginCatalog reads one catalog document and registers its tasks. It
// is the only loader: a catalog named with --plugin-catalog and one discovered
// next to a Flowfile ([discoverPluginLock]) are read, bounded, rebuilt and
// registered by this one function, so a discovered lock can be no more than a
// named one is.
func registerPluginCatalog(path string) (*v1.PluginCatalog, error) {
	catalog, err := readPluginCatalog(path)
	if err != nil {
		return nil, err
	}

	// The same Config a launch runs under, with no field set: the defaults are
	// the worker's, and a catalog reader that admitted a descriptor a launching
	// host would refuse is the asymmetry #854 spent a boundary test on.
	defs, err := plugin.TaskDefsFromCatalog(catalog, plugin.Config{})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	for _, def := range defs {
		if err := v1.DefaultRegistry().Replace(def); err != nil {
			return nil, fmt.Errorf("%s: registering task %q: %w", path, def.Name, err)
		}
	}

	return catalog, nil
}

// readPluginCatalog reads and parses one catalog document.
//
// protojson, and only protojson, because that is the one shape anything in this
// tree writes: `flow plugins --output json`. The cost of not also accepting the
// binary encoding is that a very large catalog travels as base64 in a JSON
// document rather than as bytes; the benefit is that there is one document
// shape, one writer, and no content sniffing between two of them.
func readPluginCatalog(path string) (*v1.PluginCatalog, error) {
	data, err := readBoundedFile(path, "a plugin catalog", maxPluginCatalogBytes)
	if err != nil {
		return nil, err
	}

	catalog := &v1.PluginCatalog{}

	// Unknown fields are refused rather than discarded, which is protojson's
	// default and the right one here: a catalog written by a build that knows a
	// field this one does not is a document this build cannot fully read, and
	// reading it partly is how a task travels with a claim silently dropped.
	// The claims schema version [plugin.TaskDefsFromCatalog] checks is the same
	// guard from the other direction.
	if err := protojson.Unmarshal(data, catalog); err != nil {
		return nil, fmt.Errorf(
			"%s is not a plugin catalog: %w; a catalog is what `flow plugins --plugin-dir <dir> "+
				"--output json` writes", path, err)
	}

	return catalog, nil
}

// errPluginCatalogAndLaunch is the refusal for a command line that names both a
// saved catalog and a way to launch plugins.
//
// Two sources of one fact, and nothing here can tell which the person meant:
// merging them would have to decide what happens when the catalog and the
// binaries disagree about a task's schema, and every answer to that is a
// deployment's answer being invented by an authoring verb. CLAUDE.md's own
// account of this — a parallel declaration of the same facts always eventually
// drifts — is the whole argument, so the command line says which source it
// means and this refuses to guess.
//
// Only flags *given on the command line* count, which is what makes the rule
// livable next to $FLOWSTATE_PLUGIN_DIR: that variable is baked into container
// images and shell profiles, and refusing every --plugin-catalog run on a
// machine that has it set would make the flag unreachable exactly where CI
// wants it. An explicit flag beats an ambient default, and [pluginFlagsOf]
// drops the ambient search path so nothing is launched behind the catalog's
// back.
var errPluginCatalogAndLaunch = errors.New("--" + pluginCatalogFlag + " and the plugin launch flags are two sources of the same fact")

// registerDeploymentCatalog teaches this process the plugin tasks the
// deployment it is about to submit to can run (#1548).
//
// `flow run` validates the file before submitting it, and a plugin task is
// unknown to a client that launched no plugin: the refusal then said the file
// was fine and offered no way to act on it. The deployment is the authority on
// what it will run, and GetCatalog already answers with its plugin snapshot —
// the same one submissions are pinned against — so a plugin the server has is a
// task this client accepts, and one it lacks stays refused.
//
// An explicit --plugin-catalog wins and nothing is fetched. A deployment that
// cannot be asked (unreachable, or its policy denies the RPC) leaves the
// client's own registry in place, which is what this verb did before: the
// submission then reaches the server, whose refusal is the authoritative one.
// Registration uses the same bounded rebuild as a saved catalog, so a server
// cannot hand a client more than a document read from disk could carry.
func registerDeploymentCatalog(cmd *cobra.Command, client flowstatev1connect.WorkflowServiceClient) error {
	if pluginCatalogPath(cmd) != "" {
		_, err := loadPluginCatalog(cmd)

		return err
	}

	resp, err := client.GetCatalog(cmd.Context(), connect.NewRequest(&v1.GetCatalogRequest{}))
	if err != nil {
		// A cancelled command is not an unreachable deployment: carrying on
		// would parse the file and reach for a submission nobody wants.
		return cmd.Context().Err()
	}

	catalog := resp.Msg.GetPlugins()

	// On the account stream, once, so the answer a refusal below rests on has a
	// named source: a plugin the file names and this line does not count is
	// unknown to the deployment, not merely to this process.
	fmt.Fprintf(cmd.ErrOrStderr(), "plugin tasks checked against the deployment's catalog (%d plugin(s)).\n",
		len(catalog.GetPlugins()))
	if len(catalog.GetPlugins()) == 0 {
		return nil
	}

	defs, err := plugin.TaskDefsFromCatalog(catalog, plugin.Config{})
	if err != nil {
		return nil
	}
	for _, def := range defs {
		if _, exists := v1.DefaultRegistry().Lookup(def.Name); exists {
			continue
		}
		if err := v1.DefaultRegistry().Replace(def); err != nil {
			return fmt.Errorf("registering the deployment's task %q: %w", def.Name, err)
		}
	}

	return nil
}

// pluginLockName is the file [discoverPluginLock] looks for: what
// `flow plugins --output json` writes and `make plugin-example-catalog-update`
// keeps for the shipped examples.
const pluginLockName = "plugins.lock.json"

// maxPluginLockDepth bounds the upward walk, counted in directories visited
// including the starting one. A path deeper than this is not a repository
// layout anyone wrote; it is a loop or an attack, and an unbounded walk over a
// path the caller did not choose is work spent where nothing limits it.
const maxPluginLockDepth = 32

// discoverPluginCatalogFor is the discovery half of [loadPluginCatalog]: it
// answers "" (read nothing, today's behavior) unless nothing more explicit has
// spoken. Order of authority:
//
//  1. --plugin-catalog, handled by the caller, always wins.
//  2. Plugins launched by --plugin-dir (or $FLOWSTATE_PLUGIN_DIR) already form
//     the registry, and a catalog-only definition must not replace a launched
//     one, so discovery stands down.
//  3. Otherwise the lock found upward from the files named, if any.
func discoverPluginCatalogFor(cmd *cobra.Command, anchors []string) (string, error) {
	if dirs, _ := cmd.Flags().GetStringArray("plugin-dir"); len(dirs) > 0 {
		return "", nil
	}

	path, err := discoverPluginLock(anchors)
	if err != nil || path == "" {
		return "", err
	}

	if verbose, _ := cmd.Flags().GetBool("verbose"); verbose {
		fmt.Fprintf(cmd.ErrOrStderr(), "plugin tasks checked against %s, found next to the files named.\n", path)
	}

	return path, nil
}

// discoverPluginLock finds the plugins.lock.json that governs the paths named,
// the way `go` finds go.mod: the nearest one in the file's directory or an
// ancestor. It returns "" when there is none.
//
// Trust. A discovered lock is exactly as trusted as the Flowfile beside it: it
// decides which plugin task names, input shapes and outputs this process
// believes in while it checks that file, the same authority the file already
// has over its own steps. It starts no process and runs no plugin code; it is
// read through [registerPluginCatalog], bounded and rebuilt like a named one.
// What it must not do is quietly shrink the answer, so:
//
//   - A lock that is present but is a symbolic link, is not a regular file, or
//     does not load is an error naming it. Never skipped, never searched past:
//     carrying on to a parent would validate against a different document than
//     the one next to the file.
//   - Symbolic links are not followed, so a lock cannot point out of the tree.
//   - The walk visits at most [maxPluginLockDepth] directories per path.
//   - Paths whose nearest locks differ are refused rather than merged, because
//     the registry is one per process; so is naming some files under a lock and
//     some under none. Name --plugin-catalog to say which document you mean.
//
// "-" (standard input) has no location and contributes nothing.
func discoverPluginLock(anchors []string) (string, error) {
	var found, unlocked string

	for _, anchor := range anchors {
		if anchor == "" || anchor == "-" {
			continue
		}

		lock, err := nearestPluginLock(anchor)
		if err != nil {
			return "", err
		}
		switch {
		case lock == "":
			unlocked = anchor
		case found != "" && found != lock:
			return "", newUsageError(fmt.Errorf(
				"the files named are governed by different plugin locks (%s and %s) and one process "+
					"can check against only one; run them separately or name the one you mean with --%s",
				found, lock, pluginCatalogFlag))
		default:
			found = lock
		}
	}

	if found != "" && unlocked != "" {
		return "", newUsageError(fmt.Errorf(
			"the files named are governed by %s but %s is under no plugin lock, and one process cannot "+
				"check some files against a lock and others against none; run them separately or name "+
				"the one you mean with --%s", found, unlocked, pluginCatalogFlag))
	}

	return found, nil
}

// nearestPluginLock walks up from one path, bounded by [maxPluginLockDepth].
func nearestPluginLock(anchor string) (string, error) {
	abs, err := filepath.Abs(anchor)
	if err != nil {
		return "", fmt.Errorf("resolving %s to look for %s: %w", anchor, pluginLockName, err)
	}

	dir := abs
	if info, err := os.Stat(abs); err != nil || !info.IsDir() {
		dir = filepath.Dir(abs)
	}

	for range maxPluginLockDepth {
		candidate := filepath.Join(dir, pluginLockName)

		info, err := os.Lstat(candidate)
		switch {
		case err == nil:
			if info.Mode()&os.ModeSymlink != 0 {
				return "", fmt.Errorf("%s is a symbolic link; a discovered plugin lock is not followed "+
					"out of its directory, so name the real file with --%s", candidate, pluginCatalogFlag)
			}
			if !info.Mode().IsRegular() {
				return "", fmt.Errorf("%s is not a regular file (%s)", candidate, info.Mode().Type())
			}

			return candidate, nil
		case errors.Is(err, fs.ErrNotExist), errors.Is(err, syscall.ENOTDIR):
		default:
			return "", fmt.Errorf("looking for %s: %w", candidate, err)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", nil
		}
		dir = parent
	}

	return "", nil
}

// lockDiscovery is discovery for a long-lived process, `flow lsp`, which sees
// documents one at a time instead of a command line: each file it is asked
// about is looked up with [nearestPluginLock], and a lock is registered once
// per (size, modification time), so a keystroke-rate caller does not re-read
// an unchanged document and an edited lock is picked up without a restart.
// Registration is the one loader, [registerPluginCatalog]. The registry is
// the process's, so locks found for different documents accumulate; an
// editor with two repositories open sees the union, which the one-shot verbs
// refuse ([discoverPluginLock]) because they have a definite answer to give.
type lockDiscovery struct {
	mu   sync.Mutex
	seen map[string]lockStamp
}

type lockStamp struct {
	size int64
	mod  time.Time
}

func (d *lockDiscovery) discover(docPath string) error {
	lock, err := nearestPluginLock(docPath)
	if err != nil || lock == "" {
		return err
	}
	info, err := os.Lstat(lock)
	if err != nil {
		return err
	}
	stamp := lockStamp{size: info.Size(), mod: info.ModTime()}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[lock] == stamp {
		return nil
	}
	if _, err := registerPluginCatalog(lock); err != nil {
		return err
	}
	if d.seen == nil {
		d.seen = map[string]lockStamp{}
	}
	d.seen[lock] = stamp

	return nil
}
