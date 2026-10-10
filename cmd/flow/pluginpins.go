package main

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// readPluginPinsFile is the one place a pins file is read and parsed: the
// bounded read, then [plugin.ParsePinsConfig]. Consuming a file as
// --plugin-pins and comparing against one as --diff-pins both go through it, so
// a file one accepts the other accepts, with the same error.
func readPluginPinsFile(path string) (plugin.PinsConfig, error) {
	data, err := readBoundedFile(path, "a plugin pins file", maxPluginPinsBytes)
	if err != nil {
		return plugin.PinsConfig{}, fmt.Errorf("reading plugin pins %s: %w", path, err)
	}

	cfg, err := plugin.ParsePinsConfig(data)
	if err != nil {
		return plugin.PinsConfig{}, fmt.Errorf("parsing plugin pins %s: %w", path, err)
	}

	return cfg, nil
}

// pluginPinReport is what `flow plugins` was asked to do with the digests it
// measures instead of listing plugins (#1326): write them as a pins file, or
// compare them with one.
type pluginPinReport struct {
	emit     bool
	diffFile string
}

func (r pluginPinReport) active() bool { return r.emit || r.diffFile != "" }

// addPluginPinReportFlags declares the two flags on `flow plugins` alone: the
// verbs that launch plugins to run them have no use for them.
func addPluginPinReportFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("emit-pins", false,
		"write a pins file (`pins: {name: sha256:hex}`, usable as --plugin-pins) for the plugins "+
			"this directory holds, instead of listing them. Nothing is launched: the digests are "+
			"what hashing each binary measured, so emitting from a directory nobody has vetted pins whatever is in it "+
			"(trust on first use): review the file before adopting it")
	cmd.Flags().String("diff-pins", "",
		"compare the digests this directory measures with a pins file and report each plugin "+
			"added (unpinned), changed or missing, exiting 1 on any drift, instead of listing "+
			"plugins. Nothing is launched, so a swapped binary is reported without being run")
}

// pluginPinReportOf reads the two flags off cmd and refuses a combination that
// would have one run answer two questions.
func pluginPinReportOf(cmd *cobra.Command, format OutputFormat) (pluginPinReport, error) {
	emit, _ := cmd.Flags().GetBool("emit-pins")
	diffFile, _ := cmd.Flags().GetString("diff-pins")

	report := pluginPinReport{emit: emit, diffFile: diffFile}

	switch {
	case emit && diffFile != "":
		return report, newUsageError(errors.New("--emit-pins and --diff-pins are two different answers; pass one"))
	case report.active() && (cmd.Flags().Changed("plugin-pins") || cmd.Flags().Changed("plugin-pin")):
		return report, newUsageError(errors.New(
			"--emit-pins and --diff-pins measure with no pins applied; drop --plugin-pins/--plugin-pin, or use --diff-pins FILE to compare against a pins file"))
	case report.active() && (format.Machine() || cmd.Flags().Changed("output")):
		return report, newUsageError(errors.New(
			"--emit-pins and --diff-pins write their own document and take no --output format"))
	}

	return report, nil
}

// write answers the request from the measured catalog.
func (r pluginPinReport) write(surface *ui.UI, measured map[string]string) error {
	if r.emit {
		return writePluginPins(surface, measured)
	}

	cfg, err := readPluginPinsFile(r.diffFile)
	if err != nil {
		return err
	}

	// The same check a pin gets when a host is built, so a malformed name or
	// digest is refused the way --plugin-pins refuses it rather than reported
	// as ordinary drift.
	if err := plugin.ValidatePins(cfg.Pins); err != nil {
		return fmt.Errorf("plugin pins %s: %w", r.diffFile, err)
	}

	drift := diffPluginPins(cfg.Pins, measured)
	drift.write(surface)

	return drift.err()
}

// writePluginPins renders measured as a [plugin.PinsConfig] document, the same
// shape --plugin-pins reads.
func writePluginPins(surface *ui.UI, measured map[string]string) error {
	fmt.Fprint(surface.Out, "# Measured from the plugins found (none launched), not vetted: pinning an\n"+
		"# unvetted directory is trust on first use. Review before adopting.\n")

	if len(measured) == 0 {
		fmt.Fprintln(surface.Out, "pins: {}")

		return nil
	}

	body, err := yaml.Marshal(plugin.PinsConfig{Pins: measured})
	if err != nil {
		return fmt.Errorf("encoding plugin pins: %w", err)
	}

	_, err = surface.Out.Write(body)

	return err
}

// pluginPinDrift is how the measured digests differ from a pins file. Each
// slice is sorted by plugin name.
type pluginPinDrift struct {
	added   []string // measured, not pinned
	changed []pluginPinChange
	missing []string // pinned, not found
	matched int
}

type pluginPinChange struct{ name, pinned, measured string }

// diffPluginPins compares pinned with measured in both directions.
func diffPluginPins(pinned, measured map[string]string) pluginPinDrift {
	var d pluginPinDrift

	for _, name := range slices.Sorted(maps.Keys(measured)) {
		want, ok := pinned[name]
		switch {
		case !ok:
			d.added = append(d.added, name)
		case want != measured[name]:
			d.changed = append(d.changed, pluginPinChange{name, want, measured[name]})
		default:
			d.matched++
		}
	}

	for _, name := range slices.Sorted(maps.Keys(pinned)) {
		if _, ok := measured[name]; !ok {
			d.missing = append(d.missing, name)
		}
	}

	return d
}

func (d pluginPinDrift) drifted() int { return len(d.added) + len(d.changed) + len(d.missing) }

func (d pluginPinDrift) write(surface *ui.UI) {
	out := surface.Out

	for _, c := range d.changed {
		fmt.Fprintf(out, "changed  %s\n  pinned:   %s\n  measured: %s\n", c.name, c.pinned, c.measured)
	}
	for _, name := range d.added {
		fmt.Fprintf(out, "added    %s (found but not pinned; it would launch unpinned)\n", name)
	}
	for _, name := range d.missing {
		fmt.Fprintf(out, "missing  %s (pinned but not found; a worker restricted to it would refuse to start)\n", name)
	}

	if d.drifted() == 0 {
		fmt.Fprintf(out, "plugin pins match: %d plugin(s) checked\n", d.matched)
	}
}

// err is nil when nothing drifted, so the exit status carries the answer for a
// script.
func (d pluginPinDrift) err() error {
	if d.drifted() == 0 {
		return nil
	}

	var parts []string
	for _, n := range []struct {
		count int
		label string
	}{{len(d.changed), "changed"}, {len(d.added), "added"}, {len(d.missing), "missing"}} {
		if n.count > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n.count, n.label))
		}
	}

	return fmt.Errorf("plugin pins have drifted: %s", strings.Join(parts, ", "))
}
