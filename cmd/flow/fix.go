package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` is what makes the language's no-deprecation rule affordable.
//
// Surface syntax here gets no deprecation window: a replaced spelling is gone
// rather than warned about, because carrying two spellings costs the parser, the
// validator, the language server, the marshaller, and every test that crosses
// them, for as long as the window lasts. The trade only works if migrating is a
// command someone runs in a second, which is this one.
//
// Two properties it has to have, and both are about trust rather than
// correctness. It has to be safe to run over a directory — so a file with
// nothing to change comes back byte for byte, and `--check` reports without
// writing. And it has to refuse rather than guess: a shape it cannot rewrite is
// reported with its position and the file it sits in is left alone entirely,
// because a file that looks fixed and is not is worse than one that was never
// touched.
//
// The unit of that refusal is the file, and it is the whole file. `flow fix`
// used to rewrite everything it could and report the rest, which put the current
// edition at the top of a document still holding a step written in the grammar
// the previous edition spoke: a file claiming a form it is not in, refused by
// `flow validate`, and no longer the file whose diagnostics the author is
// reading (issue #382). Other files named in the same invocation still convert
// on their own merits, because that is the unit `fix` reports in.

// fixOptions are the flags `flow fix` takes.
type fixOptions struct {
	// check reports what would change without writing anything, and exits
	// non-zero if anything would. This is the form CI runs.
	check bool

	// stdout writes the result to standard output instead of back to the file,
	// which is how a single file is piped somewhere else.
	stdout bool
}

// newFixCommand builds the `flow fix` command.
func newFixCommand() *cobra.Command {
	var opts fixOptions

	cmd := &cobra.Command{
		Use:   "fix [path...]",
		Short: "Rewrite Flowfiles into the current edition",
		Long: "Rewrite Flowfiles written in an older edition of the language into the current one, " +
			"preserving comments, formatting, and everything the change does not touch. " +
			"A directory is walked for .yaml and .yml files. A file with nothing to change is left " +
			"byte for byte as it was.\n\n" +
			"Shapes that cannot be rewritten without guessing (a task written in flow style, or one " +
			"standing behind a YAML alias) are reported with their position, and the file holding " +
			"one is not written at all: a file converts entirely or it is left exactly as it was, so " +
			"nobody is handed half a migration.\n\n" +
			"A run that rewrites a file another one pins with `digest:` reports every pin it " +
			"invalidated, naming the digest to adopt, and exits non-zero. It never re-stamps one: " +
			"a pin is the caller saying it read those bytes, and only a person can say that.\n\n" +
			"`--output json` or `--output jsonl` turns `--check` into a report a program reads " +
			"instead of scrapes: what changed or would change, what was refused, and what pins the " +
			"run invalidated, per file. CI that wants structured data rather than stderr text asks " +
			"for one of those.\n\n" +
			"--plugin-dir launches the plugins there first, and a file whose steps name a " +
			"plugin's tasks wants it: what this rewriter may do to a step depends on what the " +
			"task declares — which of its inputs it evaluates itself, and whether it shapes its " +
			"own outputs — and for a plugin's task those facts arrive with the plugin. Without " +
			"it a plugin task is rewritten as an ordinary one, which is right for most of them " +
			"and a guess for the rest. A plugin that will not start fails the command before " +
			"any file is touched.",
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: true,
		// A file that needs fixing is not a command someone invoked wrongly, and
		// printing the usage block after the diagnostics reads as though it were.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFix(cmd, args, opts)
		},
		Example: `# Rewrite one file in place:
flow fix workflow.yaml

# Rewrite a whole directory:
flow fix examples/

# Report what would change without writing, for CI:
flow fix --check examples/

# The same, as a report CI can parse instead of scraping stderr:
flow fix --check -o jsonl examples/*/workflow.yaml

# Write the result somewhere else:
flow fix --stdout old.yaml > new.yaml`,
	}

	cmd.Flags().BoolVar(&opts.check, "check", false,
		"report what would change and exit non-zero if anything would, without writing")
	cmd.Flags().BoolVar(&opts.stdout, "stdout", false,
		"write the result to standard output instead of back to the file")

	// Diagnostics are a schema message, so `-o json`/`-o jsonl` mean here what they
	// mean on `validate`: the fields are the schema's and addressable by name.
	addOutputFlag(cmd)

	// And the same plugin flags `validate` takes (#710), for a reason particular
	// to a rewriter rather than for symmetry. CLAUDE.md's own account of the two
	// times `flow fix` corrupted a valid file is that "the rewriter knew less
	// about scope than the language does" — and the walk in
	// flowfile/fix.go:StepTaskKeys and its caller branch on [v1.LookupTask]: a
	// task that shapes its own outputs has its `outputs:` kept as a map, and a
	// task's deferred inputs are left for the task to evaluate rather than being
	// treated as ordinary references. For a plugin's task those two facts arrive
	// with the plugin and are unavailable without it.
	//
	// Opt-in, as everywhere else, and the default is unchanged: with no
	// --plugin-dir a plugin task is rewritten the way it is today.
	addPluginFlags(cmd)

	return cmd
}

// errFixIncomplete reports that some file could not be fully rewritten, or that
// --check found work to do. It carries no message because the detail has already
// been printed.
var errFixIncomplete = errors.New("fix did not finish")

// errFixStalePin reports that the run's own rewrite invalidated a `digest:` pin
// somewhere else in the files it was given. Its own sentinel rather than
// [errFixIncomplete], because the fix did finish: what is left is a check the
// author has to re-authorize by hand, named line by line above it.
var errFixStalePin = errors.New("fix invalidated a digest: pin, which a person has to read the callee and re-adopt")

// runFix rewrites each path given.
func runFix(cmd *cobra.Command, paths []string, opts fixOptions) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	// Every refusal from here down is about the flags asking for two different
	// things, decided before a single file is touched — an invocation mistake
	// rather than a finding about any file named, so each is marked with
	// newUsageError the same way resolveOutputFormat marks its own.
	if opts.stdout && opts.check {
		return newUsageError(errors.New("--stdout and --check ask for different things: one writes the result, the other only reports"))
	}
	if opts.stdout && format.Machine() {
		// Both want stdout for something different — the rewritten document, or the
		// report — and only one document belongs on a stream a pipe reads.
		return newUsageError(fmt.Errorf("--stdout and --output %s both want stdout: one is the rewritten document, the other the report", format))
	}

	// Before a single file is read, let alone written: a rewriter that had
	// already changed half a directory when its plugin failed to come up would
	// have rewritten those files against a task set the author did not ask for.
	_, closePlugins, err := startPlugins(cmd, nil)
	if err != nil {
		return fmt.Errorf("--plugin-dir names what these files are rewritten against, "+
			"and one of those plugins would not start, so nothing was written: %w", err)
	}
	defer closePlugins()

	files, err := collectFlowfiles(paths)
	if err != nil {
		return err
	}
	if opts.stdout && len(files) != 1 {
		return newUsageError(fmt.Errorf("--stdout writes one document, but %d files were named", len(files)))
	}

	// Reports go to stderr and the rewritten document to stdout, so that
	// `flow fix --stdout old.yaml > new.yaml` cannot produce a new.yaml whose first
	// line is a diagnostic about old.yaml. A tool that writes its own complaints
	// into its output is a tool that cannot be piped.
	// Through the surface rather than the raw writers, and carrying the theme that
	// belongs to whichever stream the reports land on — they go to stderr only when
	// stdout is carrying a document, so the two cases have different palettes for
	// the same reason `flow get x | jq` does.
	surface := newSurface(cmd)

	out := surface.Out
	reports, reportTheme := surface.Err, surface.ErrTheme
	if !opts.stdout {
		reports, reportTheme = surface.Out, surface.Theme
	}

	var (
		refused    bool
		pending    bool
		machine    = format.Machine()
		fixReports []*v1.FixReport
		outcomes   = make(map[string]fixOutcome, len(files))
	)
	for _, path := range files {
		result, err := fixOne(out, reports, reportTheme, path, opts, machine)
		if err != nil {
			return err
		}
		refused = refused || result.refused
		pending = pending || (result.changed && opts.check)
		outcomes[path] = result
		if machine {
			fixReports = append(fixReports, result.report)
		}
	}

	// After every file, because this is the one question that is about the run
	// rather than about a file: a rewrite here makes a pin over there stale, and
	// which files were rewritten is not known until the last one is done. See
	// fixstale.go.
	stale := findStalePins(files, outcomes)
	for _, pin := range stale {
		diagnostic := pin.diagnostic(!opts.check)
		if !machine {
			fmt.Fprintln(reports, diagnosticLine(reportTheme.Muted.Render(pin.caller),
				flowfile.Diagnostic{
					Line:    diagnostic.Line,
					Column:  diagnostic.Column,
					Message: reportTheme.Danger.Render(diagnostic.Message),
				}))
		}
		if outcome, ok := outcomes[pin.caller]; ok && outcome.report != nil {
			outcome.report.StalePins = append(outcome.report.StalePins, diagnostic.Proto())
		}
	}

	if machine {
		// Projected from the same outcome the text form prints, never recomputed —
		// two readings of one rewrite that could otherwise drift.
		if format == FormatJSONL {
			// One line per file, so a consumer reads the first report without
			// waiting for the last.
			for _, report := range fixReports {
				if err := writeJSON(surface, format, report); err != nil {
					return err
				}
			}
		} else if err := writeJSON(surface, format, &v1.FixReports{Files: fixReports}); err != nil {
			// One document per invocation, the same as everywhere else `json` means
			// that in this CLI: fixing three files is still one answer.
			return err
		}
	}

	// Non-zero for either. `--check` finding work is the CI case, and a refusal is
	// the case that matters more: `flow fix . && git commit` must not succeed while
	// steps are still in a spelling that does not compile.
	if refused || pending {
		return errFixIncomplete
	}
	// And non-zero for a pin this run invalidated, for the same reason and with
	// its own sentence: the rewrite landed, so nothing was refused and nothing is
	// pending, but a call in this tree no longer compiles until somebody reads
	// the callee and adopts the digest the lines above name.
	if len(stale) > 0 {
		return errFixStalePin
	}
	return nil
}

// A fixOutcome is what one file's rewrite amounted to.
type fixOutcome struct {
	// changed reports that the file was rewritten, or would be under --check.
	//
	// False when anything was refused, because then nothing was written: the
	// edits that were available are still listed, and none of them landed.
	changed bool

	// refused reports that some part of the file could not be rewritten safely,
	// which leaves the file exactly as it was found.
	refused bool

	// report is the same outcome as a schema message, built whether or not a
	// machine format asked for it — the cost of building the struct is nothing next
	// to building a rewriter twice, once for a person and once for a program.
	report *v1.FixReport

	// beforeDigest and afterDigest name the file's bytes as they were read and
	// as this run rewrites them — the latter being what `--check` *would* write,
	// since a rewrite it only reports would invalidate exactly the pins the same
	// rewrite applied does.
	//
	// Digests rather than the bytes themselves, and that is a bound rather than
	// a tidiness: one of these is held per file for the whole invocation, and
	// while a Flowfile is capped at a mebibyte the *number* of them in a
	// directory is the user's tree to decide, so holding two bodies each would
	// scale a `flow fix` over a large generated tree with the whole tree (#833).
	// Two 71-byte strings per file is the whole of what the staleness scan needs
	// from a callee; the pins themselves are read one file at a time in
	// [findStalePins].
	//
	// Carried at all because a pin is over bytes: the only thing that can answer
	// "did this run invalidate a pin" is the pair — the digest a caller wrote has
	// to have named beforeDigest and to no longer name afterDigest.
	beforeDigest, afterDigest string
}

// fixOne rewrites a single file.
//
// machine suppresses the human-readable lines this would otherwise write to
// reports: a machine format renders [fixOutcome.report] itself, and printing both
// would be the same fact said twice on the same stream.
func fixOne(out, reports io.Writer, theme ui.Theme, path string, opts fixOptions, machine bool) (fixOutcome, error) {
	report := &v1.FixReport{File: path}

	data, err := os.ReadFile(path)
	if err != nil {
		return fixOutcome{}, fmt.Errorf("error reading %s: %w", path, err)
	}

	result, err := flowfile.Fix(data)
	if err != nil {
		// Not YAML at all, or [flowfile.Fix] refused before a first pass for a
		// reason of its own (too large, stuck in a rewrite cycle) — either way
		// reported rather than returned, so one unparseable file does not stop
		// the rest of a directory, but counted as a refusal, because the file is
		// certainly not in the current edition. Routed through the one
		// formatter every text surface shares (#384): err may itself carry
		// [flowfile.Diagnostics] with real positions, and rendering it with
		// %v instead is exactly what gave `flow validate` two other spellings
		// of the same fact.
		if !machine {
			writeErrDiagnostics(reports, theme, path, err)
		}
		report.Refusals = errDiagnosticsProto(err)
		digest := v1.ContentDigest(data)
		return fixOutcome{refused: true, report: report, beforeDigest: digest, afterDigest: digest}, nil
	}

	for _, refusal := range result.Refusals {
		if !machine {
			fmt.Fprintln(reports, positionLine(theme.Muted.Render(path), theme.Danger.Render(refusal.Error())))
		}
		report.Refusals = append(report.Refusals, refusal.Proto())
	}
	// Notes do not affect the outcome. They are places worth a reader's eye, not
	// work left undone, and failing on one would let a comment nobody has to change
	// stop `flow fix . && git commit`.
	for _, note := range result.Notes {
		if !machine {
			fmt.Fprintln(reports, diagnosticLine(theme.Muted.Render(path), note))
		}
		report.Notes = append(report.Notes, note.Proto())
	}
	// Whether the bytes this run produces are a rewrite at all. A refusal means
	// they are not, because [flowfile.Fix] hands back the document as it was when a
	// file cannot convert entirely, and neither the write below nor the lines
	// printed above it may pretend otherwise.
	applied := result.Changed() && !opts.check

	outcome := fixOutcome{
		changed:      result.Changed(),
		refused:      !result.Complete(),
		report:       report,
		beforeDigest: v1.ContentDigest(data),
	}
	// The bytes this run produces for the file, whether or not they were
	// written: `--check` finding a rewrite that would invalidate a pin has the
	// same thing to say about it as a run that made one, in a different tense.
	// The document itself when nothing was produced, which is what
	// [flowfile.Fix] hands back for a refusal too.
	outcome.afterDigest = outcome.beforeDigest
	if outcome.changed {
		outcome.afterDigest = v1.ContentDigest(result.Source)
	}
	report.Changed = outcome.changed

	if len(result.Changes) == 0 {
		if !outcome.refused && !machine {
			fmt.Fprintf(reports, "%s: %s\n",
				theme.Muted.Render(path), theme.Success.Render("already current"))
		}
		if opts.stdout {
			_, err := out.Write(result.Source)
			return outcome, err
		}
		return outcome, nil
	}

	for _, change := range result.Changes {
		if !machine {
			// The tense follows the bytes. Saying "updated" about an edit that was
			// not written is the same dishonesty as writing half a migration, told
			// rather than done.
			message := change.Message
			if !applied {
				message = change.Pending
			}
			fmt.Fprintln(reports, diagnosticLine(theme.Muted.Render(path),
				flowfile.Diagnostic{Line: change.Line, Message: theme.Warning.Render(message)}))
		}
		report.Changes = append(report.Changes, &v1.FixChange{
			Line:    uint32(max(change.Line, 0)),
			Message: change.Message,
		})
	}

	// Said after the changes rather than beside the refusals, because it is the
	// answer to the list the author has just read: these are the edits, and none of
	// them happened. Without it the report names every edit in a tense that says it
	// did not happen and never says why, which reads as a tool that lost its nerve.
	//
	// The path sits inside the sentence rather than in front of it, because every
	// line here that begins `<path>:` continues with a position, and this fact has
	// none: it is about the file rather than about somewhere in it.
	if outcome.refused && !machine {
		fmt.Fprintln(reports, theme.Danger.Render(fmt.Sprintf(
			"not rewritten: %s is exactly as it was, because a file converts in full or not at all; "+
				"the %d place(s) above have to be fixed by hand first",
			path, len(result.Refusals))))
	}

	if opts.stdout {
		// The original document when nothing was applied, which is what
		// [flowfile.Fix] returns for a refusal: `flow fix --stdout old.yaml > new.yaml`
		// must not put a half-migrated document in new.yaml either. The stream is
		// this invocation's copy of the file, and the file is the unit.
		_, err := out.Write(result.Source)
		return outcome, err
	}

	if !applied {
		return outcome, nil
	}

	// Written through the file's own mode, so fixing a file does not change who
	// can read it. A rewriter that widens permissions is a rewriter nobody should
	// run over a repository.
	info, err := os.Stat(path)
	if err != nil {
		return outcome, fmt.Errorf("error reading mode of %s: %w", path, err)
	}
	if err := os.WriteFile(path, result.Source, info.Mode().Perm()); err != nil {
		return outcome, fmt.Errorf("error writing %s: %w", path, err)
	}
	return outcome, nil
}

// collectFlowfiles expands the paths given into the files to rewrite.
//
// A named file is taken as given, whatever it is called: someone naming a file
// explicitly has said what they mean, and it reaches [flowfile.Fix] directly —
// which is what refuses it, with a diagnostic, if it turns out not to be a
// Flowfile or a Flowfile test. That is the one place this refusal belongs: a
// path typed on the command line is a claim, and the claim gets checked.
//
// A directory is walked for more than its file extensions. Filtering by
// `.yaml`/`.yml` alone was once enough, but this tree keeps an egress policy, two
// auth/trust policies, and unrelated YAML (docker-compose.yaml, Grafana
// provisioning) beside its Flowfiles, all with the same extensions and none of
// them a Flowfile — so a sweep also reads each file's shape and selects only
// what [flowfile.LooksLikeFlowfile] recognizes. This is the same allowlist
// [flowfile.Fix] enforces from the inside, read here so a walk never hands a
// policy file to the rewriter to begin with, and a sweep over a mixed directory
// does not surface a refusal for every file in it that was never a Flowfile.
//
// [flowfile.LooksLikeFlowfile] answers false for two different reasons, and a
// sweep must not treat them alike: a document that parses fine into some other
// recognized shape (a policy file, docker-compose.yaml) is correctly passed
// over in silence, but a document that does not parse as YAML *at all* is not
// "recognizably something else" — it is broken, and broken is exactly what a
// sweep must surface rather than swallow, the same way a named path already
// does through [flowfile.Fix]'s own refusal. So a file the shape check rejects
// gets one more look, through [flowfile.IsMalformedYAML], before it is dropped;
// a file that fails to parse is added anyway; [fixOne] then hands it to
// [flowfile.Fix], whose own refusal reports the parse error with the file's
// name attached.
//
// # One file, once
//
// Arguments overlap: `flow fix ./tree ./tree/callee.yaml` names the same file
// twice, and so does a directory given twice under two spellings. Each occurrence
// is dropped after the first, compared by [canonicalPath] so that `./tree/x.yaml`
// and an absolute path to it are one file rather than two.
//
// Not a tidiness. The second pass over a file this run has already rewritten
// reads the *rewritten* document, finds nothing to do, and reports `changed:
// false` — and that answer used to overwrite the first pass's, which is what
// [findStalePins] reads to know which callees moved. A caller pinning that
// callee then went unreported and the command exited zero on a tree whose call
// no longer compiles (#833). Rewriting a file twice in one invocation is also
// simply not what "fix these paths" means, so the fix is to process it once
// rather than to make two passes agree.
func collectFlowfiles(paths []string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	add := func(path string) {
		key := canonicalPath(path)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, path)
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("error reading %s: %w", path, err)
		}
		if !info.IsDir() {
			add(path)
			continue
		}
		err = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			switch filepath.Ext(p) {
			case ".yaml", ".yml":
				// Bounded exactly as [collectValidateTargets]'s walk is, and
				// for the reason written there: a directory can hold a file of
				// any size somebody else chose, and reading one whole merely to
				// classify it defeats every bound applied afterward. This walk
				// had the unbounded read that one was fixed to avoid, which is
				// how a bound that lives in one of two sibling walks tends to
				// end up. A file past the bound classifies as neither a
				// Flowfile nor a test, which is what
				// [flowfile.LooksLikeFlowfile] answers for one that size
				// anyway.
				data, truncated, err := readFileBounded(p)
				if err != nil {
					return fmt.Errorf("error reading %s: %w", p, err)
				}
				if truncated {
					return nil
				}
				if flowfile.LooksLikeFlowfile(data) || flowfile.IsMalformedYAML(data) {
					add(p)
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error walking %s: %w", path, err)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no Flowfiles found in the paths given")
	}
	return out, nil
}
