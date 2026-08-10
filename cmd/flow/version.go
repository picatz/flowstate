package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// versionInfo is what `flow version` answers, in text and in -o json alike:
// one struct, both renderings, so a field appearing in one is never a
// question in the other.
//
// A plain struct rather than a proto message: this is not a fact the binary
// tells a peer over the wire, only a fact it tells whoever ran it about
// itself, which puts it beside jwt.go's inspectResult and keys.go's own
// result types rather than beside the schema in proto/flowstate/v1. See
// CLAUDE.md's proto-first section for the boundary this sits on the near
// side of.
type versionInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`

	// CommitDate is vcs.time: the time of the commit the binary was built
	// from, not the moment of compilation. A freshly built binary of an old
	// commit carries an old date here, honestly; nothing in a module-aware
	// build records the compile time itself.
	CommitDate string `json:"commitDate"`
	GoVersion  string `json:"goVersion"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`

	// Modified is "true", "false", or "unknown". A string rather than a bool
	// because absence is a real answer: a binary built with -buildvcs=false
	// has no vcs.modified setting, and a zero-valued bool would assert the
	// tree was clean when nothing recorded whether it was.
	Modified string `json:"modified"`
}

// resolveVersionInfo reads what the toolchain stamped into this binary.
//
// `-ldflags -X main.version=...` (see the doc on the version var above) stays
// optional. runtime/debug.ReadBuildInfo carries vcs.revision and vcs.time for
// any module-aware build without a linker flag in sight, which is what makes
// a plain `go build ./cmd/flow` still answer the "which build" question a bug
// report needs. A binary with neither, one built with module information
// stripped, or outside a module entirely, says "devel" and "unknown" rather
// than leaving a field blank or inventing a number: the honesty the issue
// asked for is in never claiming to know what was not stamped.
func resolveVersionInfo() versionInfo {
	info := versionInfo{
		Version:   version,
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		Modified:  "unknown",
	}

	if info.Version == "" || info.Version == "dev" {
		info.Version = "devel"
	}

	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		info.Commit = "unknown"
		info.CommitDate = "unknown"
		return info
	}

	// Main.Version is a pseudo-version for `go install pkg@version` and
	// "(devel)" for a plain `go build .`; neither is what -ldflags stamps,
	// so this only fills in the version when the linker flag left the
	// package-level default behind.
	if info.Version == "devel" && buildInfo.Main.Version != "" && buildInfo.Main.Version != "(devel)" {
		info.Version = buildInfo.Main.Version
	}

	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			info.Commit = setting.Value
		case "vcs.time":
			info.CommitDate = setting.Value
		case "vcs.modified":
			if setting.Value == "true" {
				info.Modified = "true"
			} else {
				info.Modified = "false"
			}
		}
	}

	if info.Commit == "" {
		info.Commit = "unknown"
	}
	if info.CommitDate == "" {
		info.CommitDate = "unknown"
	}

	return info
}

// newVersionCommand builds `flow version`.
//
// `flow --version` keeps printing cobra's own first line unchanged, nothing
// scripted against it breaks, and this is the answer-shaped form beside it:
// every other verb that produces an answer speaks -o json (output.go's
// addOutputFlag), and until now version was the one that did not.
func newVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version, commit, and commit date",
		Long: "Print what the toolchain stamped into this binary: version, commit, " +
			"the commit's date, the Go version it was compiled with, and the " +
			"platform it was built for. The date is the commit's, because that " +
			"is what a module-aware build records; nothing stamps the moment of " +
			"compilation.\n\n" +
			"Answered entirely from what this binary already carries, no network " +
			"call, so it works the same offline as everything else here. When " +
			"nothing was stamped (a plain `go build` with no -ldflags and no module " +
			"information) it says so honestly: \"devel\" for the version, \"unknown\" " +
			"for the commit and its date, rather than a number invented for the " +
			"occasion.",
		Args: cobra.NoArgs,
		RunE: runVersion,
		Example: `# What build is this:
flow version

# The same answer, addressable by field:
flow version -o json | jq -r .commit

# Gate a script on this being a real build rather than one compiled by hand:
flow version -o json | jq -e '.version != "devel"'`,
	}

	addOutputFlag(cmd)
	// This command's machine output is its own small document, not a server
	// message, and the shared flag help promises protojson of an RPC response.
	// Say what is true instead.
	cmd.Flags().Lookup("output").Usage = "how to render the answer: text, json, jsonl. " +
		"The JSON shape is this command's own documented field set, not a server message"

	return cmd
}

func runVersion(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	info := resolveVersionInfo()

	if format == FormatText {
		return writeVersionText(surface, info)
	}

	return writeVersionJSON(surface, format, info)
}

// writeVersionText writes the plain line docs/CLI_DESIGN.md's text tier
// promises: every field named in the issue, on stdout, since this is the
// answer and not an account of how it was produced.
func writeVersionText(surface *ui.UI, info versionInfo) error {
	_, err := fmt.Fprintf(surface.Out, "%s %s (commit %s, committed %s, %s, %s/%s)\n",
		surface.Theme.Strong.Render("flow"), info.Version,
		info.Commit, info.CommitDate, info.GoVersion, info.OS, info.Arch)
	return err
}

// writeVersionJSON mirrors [writeJSON]'s indent-vs-compact split between the
// two machine formats, without protojson: versionInfo is not a schema type,
// see its own doc comment for why, so this marshals the plain struct
// instead, the same way jwt.go's writeInspectResult does for its own answer.
func writeVersionJSON(surface *ui.UI, format OutputFormat, info versionInfo) error {
	var (
		encoded []byte
		err     error
	)
	if format == FormatJSON {
		encoded, err = json.MarshalIndent(info, "", "  ")
	} else {
		encoded, err = json.Marshal(info)
	}
	if err != nil {
		return fmt.Errorf("rendering version: %w", err)
	}

	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)
	return err
}
