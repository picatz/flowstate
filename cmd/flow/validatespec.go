package main

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const (
	specFlag         = "spec"
	checkSourcesFlag = "check-sources"
)

// addValidateSpecFlags adds `--spec` and `--source-map` to `flow validate`: the
// check for a program that is not a Flowfile on disk (#1568).
func addValidateSpecFlags(cmd *cobra.Command) {
	cmd.Flags().Bool(specFlag, false,
		"treat the argument as a compiled specification (`flow compile --output json`) rather than a Flowfile")
	cmd.Flags().String(sourceMapFlag, "",
		"with --spec, the `path` of the specification's source map (`flow compile --source-map`): a problem is then "+
			"reported at the file, line and column the map records for its step instead of only its address")
	cmd.Flags().Bool(checkSourcesFlag, false,
		"with --source-map, read the source files the map names and withhold the lines of any whose bytes differ from "+
			"the map's digest; without it the map is trusted for what the files hold, and none is read")
}

// validateSpecOptions reads `--spec` and `--source-map` and refuses the
// combinations that cannot mean anything.
func validateSpecOptions(cmd *cobra.Command, args []string) (spec bool, mapPath string, err error) {
	spec, _ = cmd.Flags().GetBool(specFlag)
	if cmd.Flags().Changed(checkSourcesFlag) && !cmd.Flags().Changed(sourceMapFlag) {
		return false, "", newUsageError(fmt.Errorf("--%s compares the files a source map names; add --%s", checkSourcesFlag, sourceMapFlag))
	}
	if !cmd.Flags().Changed(sourceMapFlag) {
		return spec, "", nil
	}
	mapPath, _ = cmd.Flags().GetString(sourceMapFlag)
	switch {
	case !spec:
		return false, "", newUsageError(fmt.Errorf("--%s locates the problems of a compiled specification; add --%s", sourceMapFlag, specFlag))
	case mapPath == "":
		return false, "", newUsageError(fmt.Errorf("--%s needs a file path, not an empty one", sourceMapFlag))
	case len(args) != 1:
		return false, "", newUsageError(fmt.Errorf("--%s describes one specification, and %d were named", sourceMapFlag, len(args)))
	}

	return spec, mapPath, nil
}

// runValidateSpec is `flow validate --spec`: it checks compiled specifications
// with the checks that need no source, and places a problem at its source when
// a map says where its step was written.
//
// The map is bound to the specification by digest. One that describes another
// program is refused outright, with nothing checked, because the line it would
// name does not hold the step; a document the map names that is on disk and no
// longer the bytes the map was made from has its locations withheld, so a
// problem is shown by its address rather than at a shifted line.
func runValidateSpec(cmd *cobra.Command, args []string, mapPath string, format OutputFormat, catalog *v1.PluginCatalog, claims *claimCheck) error {
	if claims != nil {
		return newUsageError(errors.New("--auth-policy checks the rules a Flowfile writes; it does not apply to a compiled specification"))
	}

	check, _ := cmd.Flags().GetBool(checkSourcesFlag)
	var sourceMap *v1.DebugSourceMap
	if mapPath != "" {
		data, err := readBoundedFile(mapPath, "a source map", maxExistingSourceMapBytes)
		if err != nil {
			return fmt.Errorf("--%s: %w", sourceMapFlag, err)
		}
		sourceMap = &v1.DebugSourceMap{}
		if err := protojson.Unmarshal(data, sourceMap); err != nil {
			return fmt.Errorf("--%s: %s is not a source map (`flow compile --source-map` writes one): %w", sourceMapFlag, mapPath, err)
		}
	}

	var failed bool
	surface := newSurface(cmd)
	var reports []*v1.DiagnosticReport
	for _, path := range args {
		diagnostics, err := validateSpecFile(path, sourceMap, check, catalog)
		if err != nil {
			return err
		}

		if format.Machine() {
			reports = append(reports, diagnostics.Reports(path)...)
			failed = failed || len(diagnostics) > 0
			continue
		}
		if len(diagnostics) == 0 {
			fmt.Fprintf(surface.Out, "%s: %s\n", surface.Theme.Muted.Render(path), surface.Theme.Success.Render("ok"))
			continue
		}
		failed = true
		writeDiagnostics(surface.Out, surface.Theme.Muted.Render(path), dedupeCalleeDiagnostics(diagnostics))
	}

	if format.Machine() {
		var err error
		if format == FormatJSONL {
			for _, report := range reports {
				if err = writeJSON(surface, format, report); err != nil {
					return err
				}
			}
		} else if err = writeJSON(surface, format, &v1.ValidationReport{Files: reports}); err != nil {
			return err
		}
	}
	if failed {
		return errValidationFailed
	}

	return nil
}

// validateSpecFile checks one compiled specification, placing its problems
// with sourceMap when there is one.
func validateSpecFile(path string, sourceMap *v1.DebugSourceMap, check bool, catalog *v1.PluginCatalog) (flowfile.Diagnostics, error) {
	data, err := readBoundedFile(path, "a compiled specification", maxCompiledSpecBytes)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var workflow v1.Workflow
	if err := protojson.Unmarshal(data, &workflow); err != nil {
		return nil, fmt.Errorf("%s is not a compiled specification (`flow compile --output json` writes one): %w", path, err)
	}

	// First, as source validation and the server do: a specification over the
	// execution limit is refused at submit whatever else is right with it.
	var diagnostics flowfile.Diagnostics
	if err := v1.CheckSpecSize(&workflow); err != nil {
		diagnostics = append(diagnostics, flowfile.Diagnostic{Message: err.Error()})
	}
	if err := v1.Validate(&workflow); err != nil {
		diagnostics = append(diagnostics, flowfile.Diagnostic{Message: err.Error()})
	}
	diagnostics = append(diagnostics, flowfile.Validate(&workflow)...)
	if catalog != nil && len(workflow.GetPluginRequirements()) > 0 {
		if err := v1.CheckPluginRequirements(&workflow, catalog); err != nil {
			diagnostics = append(diagnostics, flowfile.Diagnostic{Message: err.Error()})
		}
	}

	if sourceMap != nil {
		var changed func(*v1.DebugSourceDocument) bool
		if check {
			changed = documentChanged
		}
		if _, err := diagnostics.Locate(&workflow, sourceMap, changed); err != nil {
			return nil, fmt.Errorf("--%s: %w; compile it again with `flow compile --source-map`", sourceMapFlag, err)
		}
	}

	return diagnostics, nil
}

// documentChanged reports whether the file a source map names is no longer the
// bytes the map was made from, so that a line in it would point at other text.
//
// Only called under --check-sources, because a map is data and does not send
// this process to read a path it chose: the caller authorizes it. Only an
// absolute file name is looked at, and only to hash it; nothing read is shown. A document that is not a file here (a canvas, or a path
// from another machine) cannot be compared and is trusted as the map says. A
// file that is there but cannot be read as a regular file of a Flowfile's size
// is treated as changed, because what it holds cannot be shown to match.
func documentChanged(document *v1.DebugSourceDocument) bool {
	uri := document.GetUri()
	if document.GetDigest() == "" || !filepath.IsAbs(uri) {
		return false
	}
	data, err := readBoundedFile(uri, "a source document", maxFlowfileSourceBytes)
	if err != nil {
		return !errors.Is(err, fs.ErrNotExist)
	}

	return v1.ContentDigest(data) != document.GetDigest()
}
