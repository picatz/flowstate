package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/internal/covbuild"
	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// A worker could discover, launch, supervise and health-check plugins, and then
// answer `unknown task` for every one of their tasks.
//
// Everything upstream of registration was built and tested — discovery, the
// handshake, descriptor reconstruction, dispatch over RPC, restart supervision.
// What nothing did was hand the result to the registry the engine reads, so the
// capability was complete in the way a road is complete when it stops one span
// short of the far bank. This file is the span.
//
// The one line that matters is in [runWorker]: `host.Register` against
// [v1.DefaultRegistry], which is what every lookup in the engine actually
// consults. The rest of this file exists so an operator can say where to look and
// then find out what was found.

// pluginSearchPathEnv names the directories to discover plugins in, separated the
// way $PATH is on the platform.
//
// A flag and an environment variable, because the two audiences differ: a person
// debugging a plugin passes --plugin-dir, and a container image bakes the
// variable into the image rather than into every command line.
const pluginSearchPathEnv = "FLOWSTATE_PLUGIN_DIR"

// pluginPinsEnv names a pins file the same way --plugin-pins does, mirroring
// [taskPolicyEnv] for a container image that bakes configuration into the
// environment rather than into every command line.
const pluginPinsEnv = "FLOWSTATE_PLUGIN_PINS"

// sqlEgressPolicyEnv gives the in-tree SQL plugin an immutable base64 encoding
// of the same operator-owned policy bytes the host parsed for built-in HTTP.
// The plugin process starts with an empty environment, so forwarding the
// snapshot here is an explicit grant, not ambient inheritance. SQL itself
// refuses PostgreSQL when it is absent.
const sqlEgressPolicyEnv = "FLOWSTATE_SQL_EGRESS_POLICY_B64"

// pluginFlags is what a command was told about plugins.
type pluginFlags struct {
	// dirs are the directories to discover in, in precedence order.
	dirs []string

	// only, when non-empty, pins exactly which plugins may launch. A name here
	// with no binary behind it is an error rather than a silent omission, because
	// a deployment that pinned a set expects that set.
	only []string

	// schemes, when non-empty, is the secret schemes a plugin may claim.
	schemes []string

	// allowInsecureDirs permits a plugin directory other users can write to,
	// group-writable as well as world-writable. There is one
	// legitimate use — a container image whose whole filesystem is 0777 and whose
	// only user is root — and no other one.
	allowInsecureDirs bool

	// pinnedDigests feeds [plugin.Config.PinnedDigests] directly: for the names
	// it holds, the digest the binary answering to that name must have. Built
	// by [pluginFlagsOf] from --plugin-pins and --plugin-pin together; see
	// those flags' help for the merge and #1010 for why this surface exists at
	// all.
	pinnedDigests map[string]string

	// egressPolicy is the exact operator policy snapshot already parsed by the
	// host, forwarded to first-party protocol-native plugins that enforce it
	// on their own socket path.
	egressPolicy []byte
}

// pluginFlagsOf reads them off the command being run.
//
// Off the command rather than package variables, for the reason
// [resolveOutputFormat] records: a package variable is written by whichever
// command parsed last, which in a test binary running commands in parallel is
// whichever one got there first.
func pluginFlagsOf(cmd *cobra.Command) (pluginFlags, error) {
	dirs, _ := cmd.Flags().GetStringArray("plugin-dir")
	only, _ := cmd.Flags().GetStringArray("plugin")
	schemes, _ := cmd.Flags().GetStringArray("plugin-scheme")
	allowInsecure, _ := cmd.Flags().GetBool("allow-insecure-plugin-dir")
	pinFlags, _ := cmd.Flags().GetStringArray("plugin-pin")
	pinsFile, _ := cmd.Flags().GetString("plugin-pins")
	egressPolicy := egressPolicySnapshot(cmd)

	// The $FLOWSTATE_PLUGIN_DIR fallback is bound at registration time, in
	// addPluginFlags, as the flag's own default — not here — so that the
	// generated CLI reference's environment-mirror detection (which works by
	// scanning DefValue for a sentinel written through os.Setenv before the
	// tree is rebuilt) can see it. A read-time fallback here would be invisible
	// to that scan the same way it was before #725.

	// A saved catalog and a way to launch plugins are two sources of one fact,
	// and this is the only place that sees all of them — so the refusal is here,
	// before [startPlugins] can bring a single process up behind a catalog the
	// caller asked to be checked against (#710). See
	// [errPluginCatalogAndLaunch] for why only flags given on the command line
	// count, and why an ambient $FLOWSTATE_PLUGIN_DIR loses to an explicit
	// --plugin-catalog rather than refusing the run.
	if pluginCatalogPath(cmd) != "" {
		var named []string
		for _, name := range []string{"plugin-dir", "plugin", "plugin-scheme", "allow-insecure-plugin-dir", "plugin-pin", "plugin-pins"} {
			if cmd.Flags().Changed(name) {
				named = append(named, "--"+name)
			}
		}
		if len(named) > 0 {
			return pluginFlags{}, newUsageError(fmt.Errorf(
				"%w: %s launches plugins and --%s reads what they said without launching anything; "+
					"pass one of them",
				errPluginCatalogAndLaunch, strings.Join(named, " and "), pluginCatalogFlag))
		}

		// Nothing is launched behind a catalog. An ambient search path is
		// dropped here rather than left to [pluginFlags.configured], so that
		// every later question about this invocation — whether to open a host,
		// whether a --plugin pin has somewhere to look — is answered by the
		// source the command line chose.
		return pluginFlags{}, nil
	}

	// Resolved here rather than left to fail inside the host, because the message
	// a person needs names the path they typed and the directory it resolved
	// against, and by the time the host sees it only the first half survives.
	//
	// Except where the working directory is not the command's to trust: see
	// [commandLinePluginsOnly]. There a relative path is refused rather than
	// resolved, because the directory it would resolve against is one the
	// workspace chose.
	editorOnly := commandLinePluginsOnly(cmd)
	absolute := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if editorOnly {
			if !filepath.IsAbs(dir) {
				return pluginFlags{}, newUsageError(fmt.Errorf(
					"--plugin-dir %q is relative, and %s resolves it against the directory the "+
						"editor started this process in — which is the workspace, so a repository "+
						"somebody cloned would choose what their editor executes: pass an absolute "+
						"path such as %q",
					dir, cmd.Name(), filepath.Join(string(filepath.Separator), "opt", "flowstate", "plugins")))
			}
			absolute = append(absolute, dir)

			continue
		}

		abs, err := filepath.Abs(dir)
		if err != nil {
			return pluginFlags{}, fmt.Errorf("resolving plugin directory %q: %w", dir, err)
		}
		absolute = append(absolute, abs)
	}

	// A pin with nowhere to look is refused here, not silently ignored.
	//
	// --plugin is the one plugin flag that makes a *promise*: its own help says a
	// name with no binary behind it is an error rather than a silent omission,
	// because a deployment that pinned a set expects that set. That promise is
	// kept inside the host — which never runs when there is no search path, since
	// [pluginFlags.configured] is a question about directories and
	// [startPlugins] returns early on it. So `--plugin example` with no
	// --plugin-dir and no $FLOWSTATE_PLUGIN_DIR launched nothing and said nothing:
	// the exact "silently half-configured" state every other refusal on this path
	// exists to prevent, and on `flow fix` a rewrite of a plugin's steps as though
	// they were ordinary ones (#835 review).
	//
	// A usage error, because it is one: nothing ran, and the command line asked
	// for two things that do not fit together.
	//
	// Deliberately only --plugin. --plugin-scheme and --allow-insecure-plugin-dir
	// narrow or widen what a search path may do, so with no search path they
	// restrict an empty set, which is vacuous rather than unmet. --plugin names
	// something that must come up.
	//
	// The remedy differs by trust: an editor process does not read
	// $FLOWSTATE_PLUGIN_DIR at all, so offering it would send someone to set a
	// variable this command ignores — a diagnostic that names a fix which does
	// not work is worse than a shorter one.
	if len(absolute) == 0 && len(only) > 0 {
		remedy := "pass --plugin-dir <directory> as well, or set $" + pluginSearchPathEnv
		if editorOnly {
			remedy = "pass --plugin-dir <absolute directory> as well"
		}

		return pluginFlags{}, newUsageError(fmt.Errorf(
			"--plugin %s names a plugin that must launch, and there is nowhere to look for it: "+
				"%s. A pinned plugin is never quietly skipped",
			strings.Join(only, ", "), remedy))
	}

	pins, err := pluginPinsOf(pinsFile, pinFlags)
	if err != nil {
		return pluginFlags{}, err
	}

	// A digest pin with nowhere to look is refused for the identical reason a
	// --plugin pin is, just above: the host that would enforce it never opens,
	// since [pluginFlags.configured] never sees a directory. Silently launching
	// nothing is the "believes it is pinned and is not" state #1010 exists to
	// close.
	if len(absolute) == 0 && len(pins) > 0 {
		remedy := "pass --plugin-dir <directory> as well, or set $" + pluginSearchPathEnv
		if editorOnly {
			remedy = "pass --plugin-dir <absolute directory> as well"
		}

		return pluginFlags{}, newUsageError(fmt.Errorf(
			"a plugin pin names a plugin that must launch, and there is nowhere to look for it: "+
				"%s. A pinned plugin is never quietly skipped", remedy))
	}

	// A pin naming a plugin --plugin does not admit can never be checked,
	// because [Config.Only] refuses the name before [Config.PinnedDigests] is
	// ever consulted — so a pin here reads as protecting a plugin that this
	// same command line already excludes. Refusing it at startup beats an
	// operator believing a name is digest-pinned when it can never launch to
	// be checked.
	if len(only) > 0 {
		permitted := make(map[string]bool, len(only))
		for _, name := range only {
			permitted[name] = true
		}

		for _, name := range slices.Sorted(maps.Keys(pins)) {
			if !permitted[name] {
				return pluginFlags{}, newUsageError(fmt.Errorf(
					"a plugin pin names %q, which --plugin does not admit: %q can never launch to "+
						"be checked against its pin. Add it to --plugin or remove its pin",
					name, name))
			}
		}
	}

	return pluginFlags{
		dirs:              absolute,
		only:              only,
		schemes:           schemes,
		allowInsecureDirs: allowInsecure,
		pinnedDigests:     pins,
		egressPolicy:      egressPolicy,
	}, nil
}

// pluginPinsOf builds [Config.PinnedDigests] from a pins file and repeatable
// --plugin-pin entries together.
//
// The file, when given, is the base; --plugin-pin extends it, for pinning one
// plugin without maintaining a file. A name given by both, or given twice on
// the command line, is refused rather than resolved by which source ran last:
// the effective pin would otherwise depend on an order nothing about the
// command line states, which is the same "one value written down twice"
// hazard CLAUDE.md already catalogues, arriving through two flags instead of
// two code paths.
func pluginPinsOf(pinsFile string, pinFlags []string) (map[string]string, error) {
	var base map[string]string
	if pinsFile != "" {
		data, err := os.ReadFile(pinsFile)
		if err != nil {
			return nil, fmt.Errorf("reading plugin pins %s: %w", pinsFile, err)
		}

		cfg, err := plugin.ParsePinsConfig(data)
		if err != nil {
			return nil, fmt.Errorf("parsing plugin pins %s: %w", pinsFile, err)
		}

		base = cfg.Pins
	}

	if len(base) == 0 && len(pinFlags) == 0 {
		return nil, nil
	}

	out := make(map[string]string, len(base)+len(pinFlags))
	for name, digest := range base {
		out[name] = digest
	}

	for _, entry := range pinFlags {
		name, digest, found := strings.Cut(entry, "=")
		if !found {
			return nil, newUsageError(fmt.Errorf(
				"--plugin-pin %q is not of the form name=sha256:hex", entry))
		}

		if existing, ok := out[name]; ok {
			if _, fromFile := base[name]; !fromFile || pinsFile == "" {
				return nil, newUsageError(fmt.Errorf(
					"--plugin-pin names %q more than once (%s and %s). A name pinned twice is "+
						"ambiguous even when the two digests agree — remove one",
					name, existing, digest))
			}

			return nil, newUsageError(fmt.Errorf(
				"--plugin-pins and --plugin-pin both pin %q (%s and %s). A name pinned twice is "+
					"ambiguous even when the two digests agree — remove one",
				name, existing, digest))
		}

		out[name] = digest
	}

	return out, nil
}

// ambientPluginSearchPath is what the environment says the search path is,
// unsplit and unresolved.
//
// A function rather than an [os.Getenv] call at each site, so this file stays
// the one place that reads $FLOWSTATE_PLUGIN_DIR — which is what the env-var
// reference's `read:` column names, and what
// docsgen's TestEveryDocumentedReadLocationIsWhereItIsRead checks. A second
// reader elsewhere sends anyone following that column to the wrong file.
func ambientPluginSearchPath() string { return os.Getenv(pluginSearchPathEnv) }

// splitSearchPath splits a path-list environment variable, dropping empties.
//
// An empty entry is dropped rather than resolved, because it resolves to the
// working directory — so a trailing separator, which is a typo, would otherwise
// make the process execute whatever happens to be named flowstate-plugin-* in
// the directory it was launched from.
func splitSearchPath(value string) []string {
	var out []string
	for _, part := range strings.Split(value, string(os.PathListSeparator)) {
		if part != "" {
			out = append(out, part)
		}
	}

	return out
}

// configured reports whether anything asked for plugins at all.
//
// Worth a method rather than a length check at each call site: no search path is
// the overwhelmingly common case, and it must cost nothing — a host opened over
// no directories would launch nothing and still be a lifecycle to get wrong.
//
// It is a question about directories only, and that is safe *because*
// [pluginFlagsOf] refuses a --plugin pin with no search path before ever
// building one of these. Without that refusal this method answers "nothing was
// asked for" to a command line that named a plugin, which is how a pin came to
// be silently skipped.
func (f pluginFlags) configured() bool { return len(f.dirs) > 0 }

// host builds a host for these flags. The caller owns closing it.
func (f pluginFlags) host(logger *slog.Logger) (*plugin.Host, error) {
	env := covbuild.Env()
	if len(f.egressPolicy) > 0 {
		env = append(env, sqlEgressPolicyEnv+"="+base64.StdEncoding.EncodeToString(f.egressPolicy))
	}
	return plugin.NewHost(plugin.Config{
		SearchPath:              f.dirs,
		AllowInsecureSearchPath: f.allowInsecureDirs,
		Only:                    f.only,
		PinnedDigests:           f.pinnedDigests,
		PermittedSchemes:        f.schemes,
		HostVersion:             version,
		Logger:                  logger,
		// pluginEnv (pkg/flowstate/v1/plugin/launch.go) deliberately strips a
		// launched plugin down to the protocol variables plus whatever an
		// operator names here — GOCOVERDIR is not ambient by design. Forward
		// it only when it is set on this process, which is only ever true
		// under `make coverage` (see internal/covbuild); an ordinary `flow
		// worker` or `flow mcp` run never sets GOCOVERDIR and this is a no-op.
		// Without it, a coverage-instrumented plugin binary launched through
		// this host writes nothing, and #519's plugin blind spot stays closed
		// only for the tests that build their own Config.Env by hand.
		Env: env,
	})
}

// addPluginFlags declares them on a command.
func addPluginFlags(cmd *cobra.Command) {
	cmd.Flags().StringArray("plugin-dir", splitSearchPath(os.Getenv(pluginSearchPathEnv)),
		"directory to discover plugins in, repeatable, in precedence order "+
			"(default $"+pluginSearchPathEnv+")")
	cmd.Flags().StringArray("plugin", nil,
		"launch only the named plugin, repeatable; a name with no binary is an error")
	cmd.Flags().StringArray("plugin-scheme", nil,
		"secret reference scheme a plugin may claim, repeatable (default: any)")
	cmd.Flags().Bool("allow-insecure-plugin-dir", false,
		"permit a plugin directory other users can write to, which lets them choose what this worker runs")
	cmd.Flags().StringArray("plugin-pin", nil,
		"pin a plugin name to a digest, name=sha256:hex, repeatable; a discovered binary "+
			"answering to that name must match it or is refused before it runs. A name with no "+
			"pin, here or in --plugin-pins, launches exactly as it always has (#1010) — pinning "+
			"is adopted one plugin at a time, not all at once")
	cmd.Flags().String("plugin-pins", os.Getenv(pluginPinsEnv),
		"path to a YAML pins file (default $"+pluginPinsEnv+"), the file form of --plugin-pin "+
			"for a deployment that pins more than a couple of plugins: `pins: {name: sha256:hex}`; "+
			"merged with any --plugin-pin, and a name given by both is refused")
}

// pluginTrustAnnotation marks a command that does not trust its surroundings to
// choose the code it launches, and [pluginTrustCommandLineOnly] is its one
// value. Two questions are settled by it: whether $FLOWSTATE_PLUGIN_DIR is
// bound as the search path's default, and whether a relative --plugin-dir
// resolves or is refused.
//
// It travels on the command rather than being handed to [pluginFlagsOf] as a
// second argument, and that is the point. The narrower help text and the
// narrower behaviour are then one decision written once: the first version of
// this change had them as two, so `flow lsp --help` printed a
// "(default [...])" taken from an environment variable the command had stopped
// reading — a command contradicting its own help, in the one place where the
// help is all an operator has.
const (
	pluginTrustAnnotation      = "flowstate.io/plugin-trust"
	pluginTrustCommandLineOnly = "command-line-only"
)

// commandLinePluginsOnly reports whether cmd was registered with
// [addEditorPluginFlags].
func commandLinePluginsOnly(cmd *cobra.Command) bool {
	return cmd.Annotations[pluginTrustAnnotation] == pluginTrustCommandLineOnly
}

// addEditorPluginFlags declares the same flags [addPluginFlags] does, then
// narrows the things an editor process may not let its surroundings decide.
//
// A narrowing on top of the one registration rather than a second reader beside
// it: `flow lsp` still takes exactly the flags `flow worker` takes, still
// refuses a --plugin pin with nowhere to look, and still reports a search path
// the same way — the review of the first version of this change found a
// parallel `lspPluginFlagsOf` that had quietly dropped that pin refusal, which
// is the "silently half-configured" state #835 exists to prevent, reachable
// again on this one path.
//
// What is narrowed:
//
//   - The environment is not bound as the search path's default, nor as the
//     pins-file default. An editor hands the language server whatever
//     environment the desktop session has, which is not a command line a
//     person wrote for this process.
//   - A relative --plugin-dir is refused rather than resolved, in
//     [pluginFlagsOf], because the working directory an editor starts a
//     language server in is the opened workspace.
//
// Both are the same claim in two places: the operator's command line is the
// whole of the opt-in, and a cloned repository does not get a vote.
func addEditorPluginFlags(cmd *cobra.Command) {
	addPluginFlags(cmd)

	if cmd.Annotations == nil {
		cmd.Annotations = map[string]string{}
	}
	cmd.Annotations[pluginTrustAnnotation] = pluginTrustCommandLineOnly

	dir := cmd.Flags().Lookup("plugin-dir")

	// Cleared through the flag's own value, so `--help`, `GetStringArray` and
	// the generated CLI reference all read one answer. DefValue is what cobra
	// prints as "(default …)", and "[]" is the zero value pflag suppresses.
	if slice, ok := dir.Value.(pflag.SliceValue); ok {
		_ = slice.Replace(nil)
	}
	dir.DefValue = dir.Value.String()

	dir.Usage = "absolute directory to discover plugins in, repeatable, in precedence order; " +
		"a relative path is refused and $" + pluginSearchPathEnv + " is not read, because an " +
		"editor starts this process in the workspace"

	// $FLOWSTATE_PLUGIN_PINS is ambient configuration exactly as $FLOWSTATE_PLUGIN_DIR
	// is, and the same claim applies: the desktop session's environment is not a
	// command line a person wrote for this process, so it is not read as a
	// pins-file default here either.
	pins := cmd.Flags().Lookup("plugin-pins")
	_ = pins.Value.Set("")
	pins.DefValue = pins.Value.String()
	pins.Usage = "path to a YAML pins file; $" + pluginPinsEnv + " is not read, because an " +
		"editor starts this process in the workspace"
}

// runPlugins implements the plugins sub-command.
//
// It opens a host of its own rather than asking a worker, which is a smaller
// claim than it looks: what it reports is what a worker started with this same
// configuration *would* bring up, not what one currently running has. Those
// differ exactly when the two were configured differently or a plugin has since
// crashed past its restart budget — and answering the second question means a
// worker-introspection RPC, which is a bigger change than this command is.
//
// The distinction is in the summary line rather than left to be discovered.
func runPlugins(cmd *cobra.Command, args []string) error {
	surface := newSurface(cmd)

	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	flags, err := pluginFlagsOf(cmd)
	if err != nil {
		return err
	}

	if !flags.configured() {
		// An empty answer with two meanings — nothing installed, or nowhere to
		// look — and only one of them is a mistake. So the machine shape carries
		// the search path and this says which it is.
		if format.Machine() {
			return writeJSON(surface, format, &v1.PluginCatalog{})
		}

		fmt.Fprintf(surface.Out, "%s\n",
			surface.Theme.Muted.Render(
				"No plugin directory is configured. Pass --plugin-dir or set $"+pluginSearchPathEnv+"."))

		return nil
	}

	host, err := flags.host(pluginLogger(surface))
	if err != nil {
		return err
	}
	defer func() { _ = host.Close(cmd.Context()) }()

	if err := host.Open(cmd.Context()); err != nil {
		return err
	}

	catalog := host.Catalog()

	if format.Machine() {
		return writeJSON(surface, format, catalog)
	}

	return writePluginCatalog(surface, catalog)
}

// writePluginCatalog renders the catalog for a person.
func writePluginCatalog(surface *ui.UI, catalog *v1.PluginCatalog) error {
	var (
		out   = surface.Out
		theme = surface.Theme
	)

	if len(catalog.GetPlugins()) == 0 {
		fmt.Fprintf(out, "%s\n", theme.Muted.Render(
			"No plugins on "+strings.Join(catalog.GetSearchPath(), ", ")+
				". A plugin is an executable named "+plugin.BinaryPrefix+"<name>."))

		return nil
	}

	for i, p := range catalog.GetPlugins() {
		if i > 0 {
			fmt.Fprintln(out)
		}

		fmt.Fprintf(out, "%s %s\n", theme.Accent.Render(p.GetName()), theme.Muted.Render(p.GetVersion()))

		if summary := p.GetDescription(); summary != "" {
			fmt.Fprintf(out, "  %s\n", summary)
		}

		fmt.Fprintf(out, "  %s\n", theme.Muted.Render(p.GetPath()))

		if schemes := p.GetSecretSchemes(); len(schemes) > 0 {
			fmt.Fprintf(out, "  secrets: %s\n", strings.Join(schemes, ", "))
		}

		for _, task := range p.GetTasks() {
			fmt.Fprintf(out, "\n  %s\n    %s\n", theme.Accent.Render(task.GetName()), task.GetSummary())

			// The two claims that change what this task can see, in words an
			// operator can act on rather than a field an operator has to already
			// know to go looking for (#712). Printed here, ahead of the
			// input/output tables, because they are trust posture rather than
			// shape: a reviewer deciding whether to trust this task reads these
			// two lines before they read what it takes.
			if secretInputs := task.GetSecretInputs(); len(secretInputs) > 0 {
				fmt.Fprintf(out, "    accepts a secret in: %s\n", strings.Join(secretInputs, ", "))
			}
			fmt.Fprintf(out, "    receives prior step outputs: %s\n", yesNo(task.GetNeedsScope()))

			if err := writeFields(out, theme, surface.Caps.Width, []fieldGroup{
				{label: "inputs", fields: inputFields(task.GetInputs())},
				{label: "outputs", fields: inputFields(task.GetOutputs())},
			}); err != nil {
				return fmt.Errorf("writing the plugin catalog: %w", err)
			}
		}
	}

	// Said rather than implied. This command launched the plugins itself, so a
	// reader could reasonably take the listing for the state of a running worker.
	fmt.Fprintf(out, "\n%s\n", theme.Muted.Render(
		"This is what a worker with the same --plugin-dir would bring up, not the state of one already running."))

	return nil
}

// yesNo renders a bool the way an operator reads a trust-posture line, rather
// than as a Go zero value.
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// pluginLogger sends host events and plugin stderr to the account stream.
//
// To Err rather than Out, per the stream discipline the whole CLI keeps: a
// plugin failing to launch is an account of what happened, and the answer is the
// catalog. Piping this command into jq must not interleave a plugin's log lines
// with the document.
func pluginLogger(surface *ui.UI) *slog.Logger {
	return slog.New(slog.NewTextHandler(surface.Err, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// inputFields converts described fields back into the shape the renderer takes.
//
// The two exist because one is the wire form and the other is what a terminal
// renderer needs, and this direction is only ever travelled here — a plugin's
// tasks arrive already described, since [plugin.Host] renders them through the
// same [v1.DescribeTask] a built-in goes through.
func inputFields(fields []*v1.TaskField) []v1.InputField {
	out := make([]v1.InputField, 0, len(fields))
	for _, field := range fields {
		out = append(out, v1.InputField{
			Name:        field.GetName(),
			Type:        field.GetType(),
			Required:    field.GetRequired(),
			Deferred:    field.GetDeferred(),
			Constraints: field.GetConstraints(),
		})
	}

	return out
}

// startPlugins brings up the plugins this command was configured with and adds
// what they provide to the registry the engine reads.
//
// The returned function must run. It is what kills the plugin processes; nothing
// else does, and a worker that exits without it leaves them behind holding the
// sockets it created.
//
// The catalog is returned with it, and returning it is not a convenience. It is
// what a `plugins:` requirement is resolved against on a server and what a
// worker's admission check compares a run's pins to, and this function is the
// only place in the process that has it: a caller that could not receive it would
// have to build a second host to ask the same question, or, as this returned
// only a stop function for one review cycle, silently answer "no plugins
// installed" on exactly the deployments that have them. It is nil for a command
// configured with no plugins, which is the same fact said the other way.
//
// Registration is against [v1.DefaultRegistry] and could not be against anything
// else. Every lookup the engine makes — dispatching a step, deciding which inputs
// it resolves and which the task does, deciding whether to ship prior outputs,
// and validating a specification at submit — goes through a package-level
// function over that one registry. A host registered into a registry made for the
// occasion is a host whose plugins launched, passed their health checks, served
// nothing, and answered `unknown task`. That was the state of this subsystem
// before this function existed.
//
// It is a one-way door: there is no Unregister. A worker opens one host and holds
// it until the process exits, which is the only lifecycle this supports and the
// only one it needs.
func startPlugins(cmd *cobra.Command, secretProviders *secrets.Registry) (*v1.PluginCatalog, func(), error) {
	flags, err := pluginFlagsOf(cmd)
	if err != nil {
		return nil, func() {}, err
	}
	return startPluginsWithFlags(cmd, secretProviders, flags)
}

func startPluginsWithFlags(cmd *cobra.Command, secretProviders *secrets.Registry, flags pluginFlags) (*v1.PluginCatalog, func(), error) {
	noop := func() {}

	if !flags.configured() {
		return nil, noop, nil
	}

	surface := newSurface(cmd)

	host, err := flags.host(pluginLogger(surface))
	if err != nil {
		return nil, noop, err
	}

	stop := func() {
		// Its own bounded context rather than the command's, which is already
		// cancelled by the time a worker is shutting down — passing it would
		// mean asking every plugin to stop and then refusing to wait for any of
		// them.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(cmd.Context()), pluginShutdownGrace)
		defer cancel()

		if err := host.Close(ctx); err != nil {
			fmt.Fprintf(surface.Err, "stopping plugins: %v\n", err)
		}
	}

	if err := host.Open(cmd.Context()); err != nil {
		stop()

		return nil, noop, err
	}
	if err := checkSQLPluginSecurityContract(host.Plugins()); err != nil {
		stop()

		return nil, noop, err
	}

	if err := host.Register(v1.DefaultRegistry(), secretProviders); err != nil {
		stop()

		return nil, noop, err
	}

	catalog := host.Catalog()
	for _, p := range catalog.GetPlugins() {
		names := make([]string, 0, len(p.GetTasks()))
		for _, task := range p.GetTasks() {
			names = append(names, task.GetName())
		}

		// On the account stream, at startup, naming what each plugin added.
		// A step failing with `unknown task` and a worker that quietly found no
		// plugins look identical from a Flowfile, and this is what tells them
		// apart without a debugger.
		infraLogger().Info("loaded plugin",
			"plugin", p.GetName(),
			"version", p.GetVersion(),
			"path", p.GetPath(),
			"tasks", strings.Join(names, ", "))
	}

	return catalog, stop, nil
}

// checkSQLPluginSecurityContract prevents a partially upgraded deployment from
// pairing this host with the pre-egress-policy SQL binary. Protocol version 3
// remains compatible for every other plugin; SQL is singled out by its own
// manifest name (rather than its renameable executable name) and must assert
// the two claims this host enforces before either task can be registered.
//
// This is deliberately not satisfied by the egress environment grant alone:
// an old SQL process ignores unknown environment and would otherwise retain
// unrestricted PostgreSQL and SQLite access. The current SQL plugin both makes
// these claims and refuses to start without a valid policy snapshot.
func checkSQLPluginSecurityContract(plugins []*plugin.Plugin) error {
	for _, p := range plugins {
		if err := checkSQLManifestSecurityContract(p.Manifest()); err != nil {
			return err
		}
	}

	return nil
}

func checkSQLManifestSecurityContract(manifest *pluginv1.PluginManifest) error {
	if manifest.GetName() != "sql" {
		return nil
	}

	for _, name := range []string{"query", "exec"} {
		var task *pluginv1.TaskManifest
		for _, candidate := range manifest.GetTasks() {
			if candidate.GetName() == name {
				task = candidate
				break
			}
		}
		if task == nil || !slices.Contains(task.GetRequiredSecretInputs(), "dsn") {
			return fmt.Errorf(
				"SQL plugin is not compatible with this host's security contract: task sql.%s must require dsn as a whole secret reference; upgrade flowstate-plugin-sql together with the host",
				name,
			)
		}
	}

	return nil
}

// pluginShutdownGrace bounds how long a worker waits for its plugins to exit.
//
// The host kills what has not gone by then, so this is how long a well-behaved
// plugin has to finish an in-flight call, not how long a wedged one can hold
// shutdown open.
const pluginShutdownGrace = 10 * time.Second
