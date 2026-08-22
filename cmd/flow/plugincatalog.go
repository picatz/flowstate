package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
//   - It is opt-in from the command line and from nowhere else. Without
//     --plugin-catalog (and without --plugin-dir) these verbs consult this
//     build's own tasks, a step naming a plugin task still gets
//     unknownTaskMessage's installation question, and nothing on disk is read.
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
// It returns (nil, nil) when no catalog was named, which is every invocation in
// the tree today.
//
// Registration is against [v1.DefaultRegistry] for the reason [startPlugins]
// records: that is the registry every lookup consults, and a registry made for
// the occasion would be a catalog that loaded, parsed, rebuilt every task, and
// answered `unknown task`.
func loadPluginCatalog(cmd *cobra.Command) (*v1.PluginCatalog, error) {
	path := pluginCatalogPath(cmd)
	if path == "" {
		return nil, nil
	}

	// An ambient search path loses to an explicit catalog, and says so once.
	// Silently ignoring configuration a machine already carries is how somebody
	// comes to believe their plugins were launched: the variable is set, the
	// verb printed a plugin's task, and nothing distinguishes the two sources
	// in the output. On the account stream, so a `-o json` consumer's document
	// is untouched.
	if os.Getenv(pluginSearchPathEnv) != "" && !cmd.Flags().Changed("plugin-dir") {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"$%s is set and no plugin was launched: --%s named %s, and this reads it instead.\n",
			pluginSearchPathEnv, pluginCatalogFlag, path)
	}

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
		if err := v1.DefaultRegistry().Register(def); err != nil {
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
	data, err := readBoundedFile(path, maxPluginCatalogBytes)
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

// readBoundedFile reads a file chosen by something other than this process, up
// to max bytes.
//
// The shape [flowfile.readBoundedSource] established and the reasons are the
// same: ask the *descriptor* what it is rather than the path, so there is no
// second lookup for a symlink to land in; refuse anything but a regular file,
// because a device or a pipe has no size a bound could be checked against; and
// read through a limit of max+1 so that "exactly at the bound" and "larger than
// the bound" are distinguishable rather than silently truncated into a document
// nobody wrote.
func readBoundedFile(path string, max int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"%s is not a regular file (%s); a plugin catalog is read as bytes, and a device, pipe "+
				"or directory has no size a bound could be checked against", path, info.Mode().Type())
	}

	data, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > max {
		return nil, fmt.Errorf(
			"%s is larger than the %d byte limit a plugin catalog is read up to; nothing was parsed",
			path, max)
	}

	return data, nil
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
