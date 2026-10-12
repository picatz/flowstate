package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow compile` is the answer to a question `flow validate` deliberately does not
// answer: not "is this file correct" but "what does this file become".
//
// A Flowfile is source, and the thing that runs is a [v1.Workflow] — the same
// message `flow run` submits and the same one a plugin, a review tool or an agent
// reads. Until this command existed the only ways to see it were to run the
// workload, which has side effects, or to drive the Compile RPC over MCP, which
// needs an agent in the loop to look at a file already sitting on disk.
//
// It calls [flowfile.ParseFile] and [flowfile.ValidateSourceFile] directly rather
// than dialing the Compile RPC the way it used to: this command already has the one
// thing the RPC deliberately does not — a real path on the machine it is running
// on — and a `call:` step needs that path to resolve. The RPC and `flowstate_compile`
// stay bytes-only on purpose (see their own docs), because a browser or an agent
// driving them may have no filesystem at all; this command always does, so paying
// for that restriction here would refuse a file this command is the one place able
// to compile in full. All three still call the same [flowfile.Parse] underneath —
// what differs is only whether a path travels with the bytes.

// newCompileCommand builds the `flow compile` command.
func newCompileCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compile <workflow-file>",
		Short: "Print the workflow specification a Flowfile compiles to",
		Long: "Compile a Flowfile and write the resulting workflow specification to standard " +
			"output, executing no workflow step and contacting no server. `--plugin-dir` does " +
			"launch those plugin binaries to read their task descriptors; use `--plugin-catalog` " +
			"to compile against the same descriptors without starting a process.\n\n" +
			"This is the sibling of `flow validate` and the two answer different questions. " +
			"`flow validate` answers whether a file is correct, and its answer is the list of " +
			"problems. This answers what a correct file becomes, and its answer is the " +
			"specification: the same `Workflow` message `flow run` submits, so a reviewer, a " +
			"diff, or a tool reading a step's compiled expressions is reading exactly what " +
			"would have executed.\n\n" +
			"A file with problems is refused: the diagnostics go to standard error in the " +
			"same `file:line:column: message` form `flow validate` writes, standard output " +
			"stays empty, and the exit status is non-zero. A specification handed out beside " +
			"a list of its problems would be an invitation to run it anyway.\n\n" +
			"There is no `--input` or `--input-file` here, and the omission is the design rather " +
			"than a gap. Compilation takes no arguments: a workflow's `inputs:` are bound to " +
			"values when a run is submitted, so the specification is the same document " +
			"whatever it will later be run with. Give arguments to `flow run` or " +
			"`flow run local`.\n\n" +
			"The specification is a protobuf message, and protojson is the only faithful way " +
			"to write one down, so `--output text` writes the same document `--output json` " +
			"does rather than inventing a second rendering nobody could read back. " +
			"`--output jsonl` writes that document on a single line.\n\n" +
			"`--schema inputs` or `--schema outputs` writes the workflow's contract instead: a " +
			"JSON Schema (2020-12) of what a run takes or answers with, record types under " +
			"`$defs`. It is a projection of the same declarations binding a run enforces, for " +
			"an editor form, an agent choosing arguments, or a gateway validating a request " +
			"before it reaches a run; a `must:` rule and a `sensitive:` mark ride along as " +
			"`x-flowstate-` annotations because JSON Schema cannot state them.\n\n" +
			"`--source-map PATH` also writes the step debugger's source map (a `DebugSourceMap`, " +
			"protojson) to PATH: where each step of the specification just written is on its " +
			"line, bound to that specification by its `ir_digest` and to the exact bytes of every " +
			"file it names by content digest, from the same read of the file. The specification " +
			"on standard output is unchanged. PATH is written only after the specification was, " +
			"never when the file is refused, and a map the schema's bounds would cut short is an " +
			"error rather than a partial file. The flag only creates PATH or replaces an earlier " +
			"source map: any other existing file, a Flowfile above all, is refused. It cannot be " +
			"combined with `--schema`.",
		Args:          cobra.ExactArgs(1),
		RunE:          runCompile,
		SilenceErrors: true,
		// A file with a problem in it is not a command someone invoked wrongly, and
		// a usage block after the diagnostics sends the reader to check their flags
		// instead of the line they were just told about — the same reason `validate`
		// and `fix` silence it.
		SilenceUsage: true,
		Example: `# See what a Flowfile compiles to:
flow compile examples/hello-world/workflow.yaml

# Keep the specification for a review or a diff:
flow compile examples/hello-world/workflow.yaml > hello-world.json

# Ask what one step became:
flow compile examples/hello-world/workflow.yaml | jq '.steps[0]'

# Write the JSON Schema of what a run takes:
flow compile examples/record-types/workflow.yaml --schema inputs

# Write the specification and the source map that locates its steps:
flow compile examples/hello-world/workflow.yaml \\
  --source-map hello-world.map.json > hello-world.json`,
	}

	// The specification is a schema message, so `--output json` means here what it
	// means everywhere else in this CLI: the fields are the schema's, addressable
	// by name, with no encoder of this command's own between them and a reader.
	addOutputFlag(cmd)
	addPluginFlags(cmd)
	addPluginCatalogFlag(cmd)

	cmd.Flags().String(schemaFlag, "", "write the JSON Schema of the workflow's inputs or outputs instead of the specification")
	_ = cmd.RegisterFlagCompletionFunc(schemaFlag,
		func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
			return []string{schemaInputs, schemaOutputs}, cobra.ShellCompDirectiveNoFileComp
		})

	cmd.Flags().String(sourceMapFlag, "", "also write the workflow's debug source map (protojson) to this `path`, which must be new or an earlier source map")

	return cmd
}

// sourceMapFlag is `flow compile --source-map`.
const sourceMapFlag = "source-map"

// The values of `flow compile --schema`.
const (
	schemaFlag    = "schema"
	schemaInputs  = "inputs"
	schemaOutputs = "outputs"
)

// errCompileRefused reports that the file did not compile.
//
// It carries a short message rather than none, because unlike `flow validate` — where
// the diagnostics *are* the answer and saying "validation failed" after them adds a
// word — this command promised a document on stdout and is not writing one. The line
// says why stdout is empty.
var errCompileRefused = errors.New("the file has problems, so there is no specification to write")

// runCompile compiles one Flowfile and writes the specification.
func runCompile(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	which, _ := cmd.Flags().GetString(schemaFlag)
	if which != "" && which != schemaInputs && which != schemaOutputs {
		return newUsageError(fmt.Errorf("--%s takes %q or %q, not %q", schemaFlag, schemaInputs, schemaOutputs, which))
	}

	sourceMapPath, err := resolveSourceMapPath(cmd, which, args[0])
	if err != nil {
		return err
	}

	_, closePlugins, err := startPluginsQuietly(cmd, nil)
	if err != nil {
		if isUsageError(err) {
			return err
		}

		return fmt.Errorf("--plugin-dir names what this file is compiled against, and one of those plugins would not start: %w", err)
	}
	defer closePlugins()

	if _, err := loadPluginCatalog(cmd, args...); err != nil {
		return fmt.Errorf("the plugin catalog (--%s, or the %s found next to this file) is what it is compiled against, and it could not be read: %w", pluginCatalogFlag, pluginLockName, err)
	}

	path := args[0]

	// Path-aware, so a `call:` step resolves relative to this file's own
	// directory exactly as `flow validate` and `flow run` resolve it.
	//
	// Compiled and validated in one pass (#1795): the compiler accepts more than
	// the validator does — a parse can succeed on a file validation would still
	// object to — so the full check runs too, on the workflow already in hand
	// rather than on the file a second time.
	//
	// The bytes are read here, once, so the source map is made from the very
	// read the specification was compiled from.
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	data, err := flowfile.ReadSource(path)
	var (
		workflow    *v1.Workflow
		positions   *flowfile.Positions
		diagnostics flowfile.Diagnostics
	)
	if err == nil {
		workflow, positions, diagnostics, err = flowfile.ParseAndValidateSourceAt(data, path)
	}
	surface := newSurface(cmd)
	if err != nil {
		if _, ok := errors.AsType[*os.PathError](err); ok {
			// Not a diagnostic: the path itself cannot be read, which is a fact
			// about the invocation rather than about a workflow, and listing it
			// beside "this step references a step that does not exist" would put
			// a mistake fixed in the shell in among mistakes fixed in the file.
			return fmt.Errorf("error reading %s: %w", path, err)
		}

		// Diagnostics from a file that failed to compile, or — the one shape that
		// is not a [flowfile.Diagnostics] — a document that is not YAML at all.
		// Both are the file's problem rather than the invocation's, so both are
		// reported the same way.
		diagnostics := errDiagnosticsOf(err)
		writeDiagnostics(surface.Err, surface.ErrTheme.Muted.Render(path), diagnostics)
		return errCompileRefused
	}

	// A file with diagnostics answers with them and no specification. A
	// specification handed out beside a list of its problems would be an
	// invitation to run it anyway.
	if len(diagnostics) > 0 {
		// stderr, which is the split this command turns on. `flow validate` writes
		// diagnostics to stdout because they are its answer; here the answer is
		// the specification, so a diagnostic is the account of why there is not
		// one — and a reader piping this into `jq` must never receive one.
		writeDiagnostics(surface.Err, surface.ErrTheme.Muted.Render(path), diagnostics)
		return errCompileRefused
	}

	// The specification carries where each step is written, so a run from it
	// (`flow run --spec`) can still point a failure back at this file.
	flowfile.AttachSources(workflow, positions, path)

	if which != "" {
		render := v1.InputsJSONSchema
		if which == schemaOutputs {
			render = v1.OutputsJSONSchema
		}

		return writeSchema(surface, format, render(workflow))
	}

	// Indented unless the line-per-record form was asked for. There is one document
	// either way, because there is one specification — `jsonl` here is the compact
	// spelling rather than a different answer.
	document := format
	if document == FormatText {
		document = FormatJSON
	}

	if err := writeJSON(surface, document, workflow); err != nil {
		return err
	}
	if sourceMapPath == "" {
		return nil
	}

	// Made from the workflow just printed, after its locations were attached:
	// the digest ignores them, so it names the program rather than the print.
	encoded, err := encodeSourceMap(flowfile.SourceMap(absolute, data, workflow, positions), document == FormatJSON)
	if err != nil {
		return fmt.Errorf("--%s: %w; nothing was written to %s", sourceMapFlag, err, sourceMapPath)
	}
	if err := writeFileAtomic(sourceMapPath, encoded, 0o644); err != nil {
		return fmt.Errorf("--%s: %w", sourceMapFlag, err)
	}

	return nil
}

// resolveSourceMapPath validates `--source-map` before anything is compiled, so
// a refusal creates nothing. It returns "" when the flag was not given.
func resolveSourceMapPath(cmd *cobra.Command, schema, input string) (string, error) {
	if !cmd.Flags().Changed(sourceMapFlag) {
		return "", nil
	}
	path, _ := cmd.Flags().GetString(sourceMapFlag)
	if path == "" {
		return "", newUsageError(fmt.Errorf("--%s needs a file path, not an empty one", sourceMapFlag))
	}
	if schema != "" {
		return "", newUsageError(fmt.Errorf("--%s describes the specification, and --%s writes a JSON Schema instead of it; use one or the other", sourceMapFlag, schemaFlag))
	}
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return "", newUsageError(fmt.Errorf("--%s %s is a directory; name the file to write", sourceMapFlag, path))
	}
	if sameFileAs(path, input) {
		return "", newUsageError(fmt.Errorf("--%s %s is the Flowfile being compiled, and writing the map there would replace it; name another file", sourceMapFlag, path))
	}
	if err := refuseReplacingAnythingButASourceMap(path); err != nil {
		return "", newUsageError(fmt.Errorf("--%s %w", sourceMapFlag, err))
	}
	if info, err := os.Stat(filepath.Dir(path)); err != nil || !info.IsDir() {
		return "", newUsageError(fmt.Errorf("--%s %s: its directory does not exist", sourceMapFlag, path))
	}

	return path, nil
}

// maxExistingSourceMapBytes bounds what is read of a file already at PATH to
// decide whether it is an earlier source map. The schema's own bounds put a
// full map well under it.
const maxExistingSourceMapBytes = 8 << 20

// refuseReplacingAnythingButASourceMap is the rule that makes `--source-map` safe
// to point anywhere: it only creates PATH or replaces an earlier source map.
// Whatever else exists there (a Flowfile, a module it uses, a callee reached
// through a symlink) is refused, so the flag cannot be the way a source file is
// lost, whichever file the compile happened to read.
//
// An absent PATH, an empty file, and a regular file that parses as a
// DebugSourceMap are replaceable. It needs only PATH, so it runs before anything
// is compiled.
func refuseReplacingAnythingButASourceMap(path string) error {
	f, err := flowfile.OpenRegular(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("%s exists and cannot be replaced: %w", path, err)
	}
	defer f.Close()

	existing, err := io.ReadAll(io.LimitReader(f, maxExistingSourceMapBytes+1))
	if err != nil {
		return fmt.Errorf("%s exists and could not be read: %w", path, err)
	}
	if len(existing) == 0 {
		return nil
	}
	if len(existing) <= maxExistingSourceMapBytes {
		var previous v1.DebugSourceMap
		if protojson.Unmarshal(existing, &previous) == nil {
			return nil
		}
	}

	return fmt.Errorf("%s exists and is not a source map, so writing one there would replace it; "+
		"name a new file or an earlier source map", path)
}

// encodeSourceMap renders sourceMap as the protojson written to PATH, with the
// one trailing newline, refusing a map at either bound of the schema:
// [flowfile.Env.SourceMap] stops silently there, so a map at a bound may have
// been cut short and is an error rather than a partial file.
func encodeSourceMap(sourceMap *v1.DebugSourceMap, indent bool) ([]byte, error) {
	if len(sourceMap.GetDocuments()) >= flowfile.MaxSourceMapDocuments || len(sourceMap.GetEntries()) >= flowfile.MaxSourceMapEntries {
		return nil, fmt.Errorf("the source map reaches the schema's bounds (%d documents, %d entries), so it would be partial",
			flowfile.MaxSourceMapDocuments, flowfile.MaxSourceMapEntries)
	}
	encoded, err := v1.MarshalSchemaJSON(sourceMap, indent)
	if err != nil {
		return nil, fmt.Errorf("rendering the source map: %w", err)
	}

	return append(encoded, '\n'), nil
}

// sameFileAs reports whether a and b name one file, through a symlink or a
// hard link as well as by spelling. A name that does not exist is not any file.
func sameFileAs(a, b string) bool {
	infoA, err := os.Stat(a)
	if err != nil {
		return false
	}
	infoB, err := os.Stat(b)

	return err == nil && os.SameFile(infoA, infoB)
}

// maxTempBaseBytes keeps the temporary name [writeFileAtomic] derives from the
// target's under the 255 byte NAME_MAX of common file systems.
const maxTempBaseBytes = 100

// writeFileAtomic writes data to path through a temporary file in the same
// directory and a rename, so a reader never sees a half-written file and a
// failure leaves any previous file intact.
func writeFileAtomic(path string, data []byte, mode os.FileMode) (err error) {
	// A file already there keeps its permission bits: replacing it must not
	// widen who can read it.
	if info, statErr := os.Stat(path); statErr == nil {
		mode = info.Mode().Perm()
	}
	base := filepath.Base(path)
	if len(base) > maxTempBaseBytes {
		base = base[:maxTempBaseBytes]
	}
	temp, err := os.CreateTemp(filepath.Dir(path), "."+base+"-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.Remove(temp.Name()))
		}
	}()
	if _, err = temp.Write(data); err != nil {
		return errors.Join(err, temp.Close())
	}
	if err = temp.Chmod(mode); err != nil {
		return errors.Join(err, temp.Close())
	}
	if err = temp.Close(); err != nil {
		return err
	}

	return os.Rename(temp.Name(), path)
}
