package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow init` is the first command somebody runs, and the only one whose reader
// has not read anything yet.
//
// Every peer tool has one — `cargo new`, `npm init`, `terraform init` — and what
// they have in common is not the files they write but the question they answer:
// what does a correct one of these look like, before I know enough to write it.
// Without it the entry point to this tool is a documentation page, and the first
// Flowfile anybody writes is a transcription of an example they hope is current.
//
// Two properties, and they are the whole of the design.
//
// It never overwrites. A scaffold command is run in directories that already
// contain work — often by somebody who typed it in the wrong terminal — so both
// paths are checked before either is written, and a file already there stops the
// command with its name in the message rather than being replaced by a greeting.
// Nothing is partially written: two files land or neither does.
//
// And the file it writes cannot rot. The edition comes from
// [flowfile.CurrentEdition] rather than a string typed here, and
// TestInitProducesAFlowfileTheToolAccepts runs the real `validate`, `test` and
// `fix --check` against what this produces — the same mechanism `buf generate`
// plus `git diff --exit-code` uses for generated code, pointed at a template.
// A starter file that the tool itself rejects is worse than no starter file,
// because the person reading it has no way to tell which of the two is wrong.

// initOptions are the flags `flow init` takes.
type initOptions struct {
	// name is the workflow's name, when the author states it rather than
	// letting the directory decide.
	name string
}

// scaffoldFile is one file `flow init` writes: the name it takes in the target
// directory, and what goes in it.
type scaffoldFile struct {
	name     string
	contents string
}

// The two names a scaffolded directory holds, named once so the no-clobber
// check, the writes and the report cannot disagree about what was created.
const (
	scaffoldWorkflow = "workflow.yaml"
	scaffoldTest     = "workflow.test.yaml"
)

// fallbackName is the workflow name used when the target directory's own name
// yields nothing legal. Said out loud when it happens, rather than silently
// substituted — an author who ends up with a workflow called something they did
// not choose deserves to know which of the two decided it.
const fallbackName = "workflow"

// newInitCommand builds the `flow init` command.
func newInitCommand() *cobra.Command {
	var opts initOptions

	cmd := &cobra.Command{
		Use:   "init [dir]",
		Short: "Scaffold a workflow and its tests in a directory",
		Long: "Write a starter Flowfile and the test file that goes with it into a directory, " +
			"creating the directory if it does not exist. With no argument the files land in the " +
			"current directory, and the workflow is named after the directory holding it.\n\n" +
			"Nothing is ever overwritten: if either file exists already the command refuses and " +
			"writes neither, naming the file that stopped it.\n\n" +
			"What it writes is in the edition this build speaks, and passes `flow validate`, " +
			"`flow test` and `flow fix --check` the moment it lands.",
		Args:          cobra.MaximumNArgs(1),
		SilenceErrors: true,
		// A directory that already holds a workflow is a finding about the
		// directory, not a command someone typed wrongly, and a usage block
		// under that refusal sends the reader to check their flags instead of
		// the file they were just told about.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}
			return runInit(cmd, dir, opts)
		},
		Example: `# Scaffold in the current directory, named after it:
flow init

# Scaffold a new directory, creating it:
flow init deploy-frontend

# Choose the workflow's name rather than taking the directory's:
flow init . --name nightly-report

# What the scaffold is for. Run it, then run its tests:
flow run local ./workflow.yaml
flow test .`,
	}

	cmd.Flags().StringVar(&opts.name, "name", "",
		"the workflow's name; unset takes the target directory's name")

	return cmd
}

// runInit writes the scaffold into dir.
func runInit(cmd *cobra.Command, dir string, opts initOptions) error {
	name, note, err := scaffoldName(dir, opts.name)
	if err != nil {
		return err
	}

	files := []scaffoldFile{
		{name: scaffoldWorkflow, contents: starterWorkflow(name)},
		{name: scaffoldTest, contents: starterTest()},
	}

	// Fail closed, and fail before writing anything. Both paths are checked
	// first so that a directory holding a workflow.test.yaml and no
	// workflow.yaml does not end up with a workflow.yaml written beside a test
	// that describes something else — a half-scaffolded directory is a state
	// nobody asked for and nothing else in this tool would produce.
	for _, f := range files {
		path := filepath.Join(dir, f.name)
		switch _, err := os.Lstat(path); {
		case err == nil:
			return errScaffoldExists(path)
		case !os.IsNotExist(err):
			return fmt.Errorf("error reading %s: %w", path, err)
		}
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("error creating %s: %w", dir, err)
	}

	reserved, err := reserveScaffoldFiles(dir, files)
	if err != nil {
		return err
	}

	if err := fillScaffoldFiles(reserved, files); err != nil {
		return err
	}

	surface := newSurface(cmd)
	printScaffold(surface, dir, files, note)

	return nil
}

// errScaffoldExists is the refusal this command gives for a path it will not
// overwrite, and it is one sentence because two spellings of one refusal drift.
// runInit's preflight says it about a file that was already there; the
// exclusive create says it about a file that appeared in between.
func errScaffoldExists(path string) error {
	return fmt.Errorf("%s exists already, and `flow init` never overwrites: "+
		"scaffold into an empty directory, or move that file aside first", path)
}

// scaffoldWrite is a destination this invocation created and therefore owns.
// The handle is held rather than the path reopened, so nothing appearing
// between the create and the write can substitute the file the bytes land in.
type scaffoldWrite struct {
	path string
	file *os.File
}

// reserveScaffoldFiles creates every destination before any of them is written.
//
// O_EXCL is what makes the no-overwrite promise authoritative: runInit's
// preflight observes the directory and then writes, and a directory entry can
// appear in between — a symlink there would otherwise be followed and its
// target truncated. Reserving all the destinations first keeps the other half
// of that promise, the one the preflight was written for: a collision on the
// second file must not leave the first one behind.
func reserveScaffoldFiles(dir string, files []scaffoldFile) ([]scaffoldWrite, error) {
	reserved := make([]scaffoldWrite, 0, len(files))
	for _, f := range files {
		path := filepath.Join(dir, f.name)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			discardScaffoldFiles(reserved)
			if errors.Is(err, fs.ErrExist) {
				return nil, errScaffoldExists(path)
			}
			return nil, fmt.Errorf("error writing %s: %w", path, err)
		}
		reserved = append(reserved, scaffoldWrite{path: path, file: file})
	}
	return reserved, nil
}

// fillScaffoldFiles writes the reserved destinations, and removes all of them
// if any write or close fails, so that an error arriving mid-scaffold leaves
// the directory as it found it rather than half-written.
func fillScaffoldFiles(reserved []scaffoldWrite, files []scaffoldFile) error {
	for i, f := range files {
		if _, err := reserved[i].file.Write([]byte(f.contents)); err != nil {
			discardScaffoldFiles(reserved)
			return fmt.Errorf("error writing %s: %w", reserved[i].path, err)
		}
	}

	// Close is checked rather than deferred and dropped: on a networked or
	// full filesystem this is where the write actually fails, and a scaffold
	// reported as created has to be one that reached the disk.
	for _, r := range reserved {
		if err := r.file.Close(); err != nil {
			discardScaffoldFiles(reserved)
			return fmt.Errorf("error writing %s: %w", r.path, err)
		}
	}
	return nil
}

// discardScaffoldFiles removes the destinations this invocation exclusively
// created, and only those: O_EXCL is what makes the removal safe to do without
// a second look, since nothing else can own a path this reservation opened.
func discardScaffoldFiles(reserved []scaffoldWrite) {
	for _, r := range reserved {
		_ = r.file.Close()
		_ = os.Remove(r.path)
	}
}

// printScaffold reports what was written and what to type next.
//
// To the answer stream with the answer stream's theme, the way `flow fix` and
// `flow fmt` report a rewrite: this command produces no document a pipe reads,
// so there is nothing for the account to get out of the way of.
func printScaffold(surface *ui.UI, dir string, files []scaffoldFile, note string) {
	out, theme := surface.Out, surface.Theme
	mark := surface.Caps.Symbols().Success

	if note != "" {
		fmt.Fprintf(out, "%s\n\n", theme.Muted.Render(note))
	}

	// A glyph beside a word that already carries the meaning, per the CLI
	// design language: this is a list being scanned, not a summary with one
	// outcome worth landing on, so no pill and no colour doing the work alone.
	for _, f := range files {
		fmt.Fprintf(out, "%s created %s\n",
			theme.Success.Render(mark), theme.Strong.Render(filepath.Join(dir, f.name)))
	}

	// Through [writeNextCommands], which is the CLI's one way of suggesting next
	// commands: the same element the unreachable-server report draws, so a reader
	// who has met one recognizes the other. The durable venue is the second block
	// and not the first because rehearsing locally is still the faster loop and
	// should stay the first thing tried; it is named at all because this is the
	// moment somebody has a workflow and no idea that running it for real is two
	// commands, one that assembles the stack and the same `flow run` without
	// `local`.
	var next strings.Builder
	writeNextCommands(&next, theme, []commandBlock{
		{commands: []string{
			"flow run local " + filepath.Join(dir, scaffoldWorkflow),
			"flow test " + dir,
		}},
		{
			lead: "then, durably, in two commands:",
			commands: []string{
				"flow server dev",
				"flow run " + filepath.Join(dir, scaffoldWorkflow),
			},
		},
	})

	fmt.Fprint(out, next.String())
}

// scaffoldName decides what the workflow is called, and returns a note to print
// when that decision was not the author's.
//
// A `--name` that the grammar refuses is an error rather than something quietly
// corrected: the author typed it, so telling them what a name may hold and what
// theirs would have to become is the diagnostic, and rewriting it behind their
// back is not. A *derived* name is the opposite case — nobody typed it, the
// directory did — so it is sanitized and the substitution is said out loud.
func scaffoldName(dir, given string) (name, note string, err error) {
	if given != "" {
		if bad := firstIllegalNameRune(given); bad != "" {
			return "", "", newUsageError(fmt.Errorf(
				"--name %q may not contain %s; a workflow name is used as an identifier, "+
					"so it takes letters, digits, - and _ (try %q)",
				given, bad, sanitizeName(given)))
		}
		if len(given) > maxWorkflowNameLen {
			return "", "", newUsageError(fmt.Errorf(
				"--name is %d characters, and a workflow name may be at most %d",
				len(given), maxWorkflowNameLen))
		}
		return given, "", nil
	}

	base := ""
	if abs, err := filepath.Abs(dir); err == nil {
		base = filepath.Base(abs)
	}

	derived := sanitizeName(base)
	if len(derived) > maxWorkflowNameLen {
		derived = derived[:maxWorkflowNameLen]
	}

	switch {
	case derived == "":
		return fallbackName, fmt.Sprintf(
			"%q gave no legal workflow name, so this one is called %q; rename it in the file, "+
				"or scaffold again with --name", base, fallbackName), nil
	case derived != base:
		return derived, fmt.Sprintf(
			"named %q after the directory, with what a name may not hold replaced: "+
				"a workflow name takes letters, digits, - and _", derived), nil
	}

	return derived, "", nil
}

// maxWorkflowNameLen is the schema's own bound on Workflow.name.
const maxWorkflowNameLen = 128

// sanitizeName replaces every character a workflow name may not hold, and drops
// the dashes that leaves at either end so a directory called ".config" does not
// become a workflow called "-config".
func sanitizeName(s string) string {
	mapped := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		}
		return '-'
	}, s)

	return strings.Trim(mapped, "-")
}

// firstIllegalNameRune describes the first character a workflow name may not
// hold, or empty when every one of them is legal.
//
// The same rule flowfile's validator states, applied one surface earlier so that
// `flow init --name 'my workflow'` is refused when it is typed rather than by
// the validate run that follows it. Both read the pattern the schema declares on
// Workflow.name; neither is the other's source, and if that pattern ever widens
// this one is a place to look.
func firstIllegalNameRune(name string) string {
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			continue
		case r == ' ':
			return "spaces"
		default:
			return fmt.Sprintf("%q", string(r))
		}
	}
	return ""
}

// yamlName renders a workflow name as a YAML scalar that reads back as the
// string it is.
//
// Found by the gate rather than by reading, which is the argument for the gate:
// scaffolding into a directory called `001` — a temporary directory, a numbered
// fixture, a release directory — wrote `name: 001`, which YAML reads as a number
// and the parser refuses with "must be a string, but a number was written here".
// Every legal name is spelled with letters, digits, `-` and `_`, so the only
// values needing quotes are the ones a plain scalar would resolve as something
// other than a string: a leading digit or dash, and the words YAML reads as a
// boolean or a null.
func yamlName(name string) string {
	if plainYAMLString(name) {
		return name
	}
	// Every character is [A-Za-z0-9-_], so a double-quoted scalar needs no
	// escaping — there is nothing in the set a quote could interrupt.
	return `"` + name + `"`
}

// plainYAMLString reports whether name can be written unquoted and still read
// back as a string.
func plainYAMLString(name string) bool {
	if name == "" {
		return false
	}

	switch strings.ToLower(name) {
	case "y", "n", "yes", "no", "true", "false", "on", "off", "null":
		return false
	}

	first := rune(name[0])
	return (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_'
}

// starterWorkflow is the Flowfile `flow init` writes.
//
// The edition is [flowfile.CurrentEdition] rather than the string it happens to
// be today, which is the whole of what keeps this from becoming the stalest file
// in the repository: an edition bump moves what this writes on the same commit,
// and the test that runs `fix --check` over the result fails if it does not.
//
// The comments are the point of the file as much as the steps are. A scaffold
// whose every line is obvious teaches nothing; these name the three things an
// author has to know before they can write the second step: that a step has an
// id, that `${...}` is CEL, and where a run's arguments come from.
//
// It is written in the formatter's own canonical shape, so `flow fmt` on a fresh
// scaffold is a byte-for-byte no-op (#451): the description is one unfolded line,
// list entries sit indented under the key that holds them (#850), the CEL string
// is double-quoted,
// and the blank lines are the ones Marshal keeps. A scaffold the CLI's own
// formatter would rewrite is the CLI disagreeing with itself about canonical form,
// and it teaches the shape `flow fmt` was about to undo. TestFmtOnTheScaffoldIsANoOp
// holds this to the byte.
func starterWorkflow(name string) string {
	return `edition: ` + flowfile.CurrentEdition + `
name: ` + yamlName(name) + `
description: A starter workflow. Replace these steps with the work you actually want done.

# What a run is given. ` + "`flow run local workflow.yaml --input name=you`" + ` overrides
# the default; a run that names nothing gets it.
inputs:
  name:
    type: string
    default: world
    description: who to greet
steps:
  # Every step declares an id. It is how a later step, a test case, and
  # ` + "`flow get`" + ` all refer to this one.
  - id: greet
    log:
      # ${...} is CEL, and an expression is the whole value rather than a
      # fragment spliced into text, so a greeting is built in CEL. A run's
      # inputs, earlier steps' outputs, and anything enclosing control flow
      # bound are all in scope.
      message: ${"hello, " + inputs.name}
`
}

// starterTest is the test file `flow init` writes beside the workflow.
//
// It stubs the one task the workflow calls and asserts on the *invocation*
// rather than only on the step having run, because `ran:` alone passes for a
// workflow that logged something else entirely — the same point
// examples/hello-world/workflow.test.yaml makes, and the habit worth copying
// into the first test anybody writes here.
func starterTest() string {
	return `# A workflow ships with its own tests. ` + "`flow test .`" + ` runs them with no server,
# no Temporal, and no network: every task is answered by a stub, so a case
# asserts what the workflow *did* rather than what the world did back.
edition: ` + flowfile.CurrentEdition + `
tests:
  - name: the greeting uses the input it was given
    workflow: ./` + scaffoldWorkflow + `
    inputs:
      name: flowstate
    stubs:
      # ` + "`where:`" + ` is the assertion. A ` + "`log`" + ` invocation whose message is anything
      # else matches no stub and fails the case, which ` + "`ran:`" + ` alone could not
      # notice.
      - task: log
        where: inputs.message == 'hello, flowstate'
        returns: {}
    expect:
      ran: [greet]
`
}
