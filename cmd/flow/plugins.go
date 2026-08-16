package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/internal/covbuild"
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

	if len(dirs) == 0 {
		dirs = splitSearchPath(os.Getenv(pluginSearchPathEnv))
	}

	// Resolved here rather than left to fail inside the host, because the message
	// a person needs names the path they typed and the directory it resolved
	// against, and by the time the host sees it only the first half survives.
	absolute := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return pluginFlags{}, fmt.Errorf("resolving plugin directory %q: %w", dir, err)
		}
		absolute = append(absolute, abs)
	}

	return pluginFlags{
		dirs:              absolute,
		only:              only,
		schemes:           schemes,
		allowInsecureDirs: allowInsecure,
	}, nil
}

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
func (f pluginFlags) configured() bool { return len(f.dirs) > 0 }

// host builds a host for these flags. The caller owns closing it.
func (f pluginFlags) host(logger *slog.Logger) (*plugin.Host, error) {
	return plugin.NewHost(plugin.Config{
		SearchPath:              f.dirs,
		AllowInsecureSearchPath: f.allowInsecureDirs,
		Only:                    f.only,
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
		Env: covbuild.Env(),
	})
}

// addPluginFlags declares them on a command.
func addPluginFlags(cmd *cobra.Command) {
	cmd.Flags().StringArray("plugin-dir", nil,
		"directory to discover plugins in, repeatable, in precedence order "+
			"(default $"+pluginSearchPathEnv+")")
	cmd.Flags().StringArray("plugin", nil,
		"launch only the named plugin, repeatable; a name with no binary is an error")
	cmd.Flags().StringArray("plugin-scheme", nil,
		"secret reference scheme a plugin may claim, repeatable (default: any)")
	cmd.Flags().Bool("allow-insecure-plugin-dir", false,
		"permit a plugin directory other users can write to, which lets them choose what this worker runs")
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
	noop := func() {}

	flags, err := pluginFlagsOf(cmd)
	if err != nil {
		return nil, noop, err
	}

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

// pluginShutdownGrace bounds how long a worker waits for its plugins to exit.
//
// The host kills what has not gone by then, so this is how long a well-behaved
// plugin has to finish an in-flight call, not how long a wedged one can hold
// shutdown open.
const pluginShutdownGrace = 10 * time.Second
