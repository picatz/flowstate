package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Transcripts: multi-command scenarios as data, over the one harness in
// runflow_test.go (#404).
//
// The bugs a CLI probe finds are almost never about one verb. #381, #382 and
// #392 all came out of *sequences* — scaffold something, format it, validate
// what the formatter produced — and the join is where they lived: `flow fmt`
// was correct about a file and `flow init` was correct about a template, and
// running one on the other lost every comment in it. A Go table test can hold a
// sequence, but it holds it as code, so each one gets written slightly
// differently and none of them reads like the bug report it came from.
//
// A transcript reads like the session that found it:
//
//	# scaffold, then the formatter must accept its own output
//	$ flow init demo
//	-- exit --
//	0
//
//	$ flow fmt demo
//	-- stdout --
//	$WORK/demo/workflow.test.yaml: already formatted
//	$WORK/demo/workflow.yaml: already formatted
//
// Every scenario in testdata/script is run by [TestTranscripts], in a temporary
// directory of its own, through [runFlow] — the same in-process tier every
// other test in this package uses, so a transcript costs about what a function
// call costs and its lines are visible to `go test -cover` without any
// subprocess at all.
//
// What a transcript is for, and what it is not (the boundary #404 draws):
// behaviour contracts — streams, exit codes, file effects — in the plain,
// non-TTY mode where output is stable by design. Styled bytes, colour, width
// and TTY detection belong to #402's goldens and the ui capability matrix, and
// a transcript must never grow an assertion about them. Single-verb behaviour
// stays in Go, where require and go-cmp can reach into a parsed structure; a
// transcript earns its keep on the *walk*, per CLAUDE.md's traversal rule.
//
// Not [testscript]. It is the mature prior art and #404 asked for it to be
// evaluated, so: what it does that this does not is a shell — its own command
// table, `cmp`, `exists`, conditions, env manipulation — and what it wants in
// exchange is the process model. Its commands are subprocesses or a
// `testscript.Main` that owns the test binary's entry point, and this package's
// TestMain already owns that (it boots one Temporal dev server for the whole
// package). Going through a subprocess would also mean building `flow` before
// any transcript could run, and taking coverage of those lines back out through
// covbuild's GOCOVERDIR path rather than getting it for free. The scenarios
// #404 names need none of the shell: they run `flow`, and they read what it
// wrote. So the runner here is a hundred lines over the harness that already
// exists, and the format below is deliberately smaller than testscript's — if a
// transcript ever needs conditions or a second binary, that is the moment to
// take the dependency rather than grow this into it.
//
// [testscript]: https://pkg.go.dev/github.com/rogpeppe/go-internal/testscript

// The format, in full:
//
//   - `# ...` is a comment, and a blank line is nothing. Both are preserved by
//     -update.
//   - `$ flow ...` runs a command. The whole line is split on spaces; there is
//     no quoting, because nothing needs it yet.
//   - `-- write path --` seeds a file *before* the next command runs. Its
//     content is the section body.
//   - `-- stdout --`, `-- stderr --` and `-- exit --` are assertions about the
//     command above them: exactly these bytes, exactly this status. A section
//     that is not written is not asserted — declaring only stdout says nothing
//     about stderr — and a section written empty asserts emptiness.
//   - `-- file path --` asserts the contents of a file the command left behind.
//
// A section's body runs to the next `-- ... --` marker or the next `$ ` line,
// whichever comes first, so an expected line may not itself begin with `$ ` at
// column 0. Nothing this CLI prints does.
//
// The one other restriction the format pays for readability: a run of blank and
// `#` lines directly above a command introduces that command, even where it
// follows a section — which is where an author naturally writes the sentence
// saying what the next command is for. A declared body therefore may not end
// with a comment line, since that comment would read as the next command's.
//
// Trailing newlines are trimmed from both a declared body and what a command
// wrote, on both sides of every comparison: a section in a text file cannot
// distinguish "ends with a newline" from "does not", and pretending otherwise
// would make every transcript's last line a coin flip. A claim about a final
// newline belongs in Go, where the bytes can be compared as bytes.
//
// $WORK stands for the scenario's directory in every expectation and in every
// command line, since a temporary directory's real path is different on every
// run and on every machine.

const transcriptWorkDir = "$WORK"

// TestTranscripts runs every scenario under testdata/script.
//
// Regenerate the expectations of one with:
//
//	go test ./cmd/flow -run 'TestTranscripts/name' -update
//
// after reading what changed, on #402's convention — the `-update` flag is the
// one charmbracelet's golden package registers, deliberately shared rather than
// a second flag spelled the same way. Scoped to `./cmd/flow` rather than
// `./cmd/flow/...`, because that flag is not registered in sibling packages'
// test binaries and they fail on being passed it.
func TestTranscripts(t *testing.T) {
	scripts, err := filepath.Glob(filepath.Join("testdata", "script", "*.txt"))
	require.NoError(t, err)
	require.NotEmpty(t, scripts, "no transcripts found under testdata/script")

	for _, script := range scripts {
		t.Run(strings.TrimSuffix(filepath.Base(script), ".txt"), func(t *testing.T) {
			// Not parallel, and this is the one place in the package where
			// that is structural rather than incidental: a scenario runs its
			// commands from its own directory, and a working directory is
			// process-wide. t.Chdir says so itself by failing a test that has
			// called t.Parallel.
			runTranscript(t, script)
		})
	}
}

// runTranscript executes one scenario and asserts everything it declares.
func runTranscript(t *testing.T, script string) {
	t.Helper()

	// Absolute before the chdir below, since -update writes the scenario back
	// out from inside the scenario's own directory.
	script, err := filepath.Abs(script)
	require.NoError(t, err)

	source, err := os.ReadFile(script)
	require.NoError(t, err)

	steps, err := parseTranscript(string(source))
	require.NoErrorf(t, err, "%s is not a transcript this runner can read", script)

	work := t.TempDir()

	// Symlinks under /var on macOS and /tmp elsewhere mean the path a command
	// reports is not always the path handed to it. Resolving once here is what
	// keeps $WORK substitution working on both.
	if resolved, err := filepath.EvalSymlinks(work); err == nil {
		work = resolved
	}

	t.Chdir(work)

	updating, updated := transcriptUpdating(), false

	for i := range steps {
		step := &steps[i]

		for _, seed := range step.writes {
			path := filepath.Join(work, filepath.FromSlash(seed.name))
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, []byte(seed.body), 0o600))
		}

		res := runFlow(t, expandWork(step.args, work)...)

		observed := map[string]string{
			"stdout": collapseWork(res.Stdout, work),
			"stderr": collapseWork(res.Stderr, work),
			"exit":   strconv.Itoa(res.ExitCode),
		}

		for j := range step.claims {
			claim := &step.claims[j]

			want, got := claim.body, ""
			switch claim.kind {
			case "stdout", "stderr", "exit":
				got = observed[claim.kind]
			case "file":
				data, err := os.ReadFile(filepath.Join(work, filepath.FromSlash(claim.name)))
				require.NoErrorf(t, err, "%s\n$ %s\nleft no %s behind",
					script, strings.Join(step.args, " "), claim.name)
				got = collapseWork(string(data), work)
			}

			if updating {
				if claim.body != got {
					claim.body, updated = got, true
				}
				continue
			}

			assert.Equalf(t, want, got,
				"%s\n$ %s\n%s does not match. Re-read what the command does, and "+
					"regenerate with `go test ./cmd/flow -run 'TestTranscripts/%s' -update` "+
					"only once the new bytes are the ones you meant.\nstdout:\n%s\nstderr:\n%s",
				script, strings.Join(step.args, " "), claim.label(),
				strings.TrimSuffix(filepath.Base(script), ".txt"),
				observed["stdout"], observed["stderr"])
		}
	}

	if updating && updated {
		require.NoError(t, os.WriteFile(script, []byte(formatTranscript(steps)), 0o600))
		t.Logf("%s regenerated", script)
	}
}

// transcriptUpdating reports whether this run was asked to regenerate
// expectations, by reading #402's `-update` rather than declaring a second flag
// spelled the same way: the golden package registers one in this test binary
// already, and two registrations of one name is a panic at init. Read through
// flag.Lookup at assertion time, because flags are not parsed when this
// package's variables are initialized.
//
// False when nothing registered it — a transcript that cannot be regenerated
// still runs and still asserts, which is the safe direction.
func transcriptUpdating() bool {
	declared := flag.Lookup("update")
	if declared == nil {
		return false
	}

	getter, ok := declared.Value.(flag.Getter)
	if !ok {
		return false
	}

	on, _ := getter.Get().(bool)

	return on
}

// transcriptStep is one command and everything declared about it.
type transcriptStep struct {
	args   []string
	writes []transcriptSection
	claims []transcriptSection
	lead   []string // comments and blank lines written above the command
}

// transcriptSection is one `-- kind name --` block.
type transcriptSection struct {
	kind string
	name string
	body string
}

func (s transcriptSection) label() string {
	if s.name != "" {
		return s.kind + " " + s.name
	}
	return s.kind
}

// leadLines marks the lines that introduce a command rather than belong to a
// section: a maximal run of blank and `#` lines ending at a `$ ` line, plus the
// same run at the top of the file before the first command.
//
// Needed because a section body is arbitrary text — a Flowfile's own YAML
// comments live inside one — so `#` cannot mean "comment" on its own. Position
// is what distinguishes them, and position is knowable in one backwards pass.
func leadLines(lines []string) []bool {
	lead := make([]bool, len(lines))

	introducing := false
	for n := len(lines) - 1; n >= 0; n-- {
		line := lines[n]

		switch {
		case strings.HasPrefix(line, "$ "):
			introducing = true
		case strings.HasPrefix(line, "-- write "):
			// A seeded file belongs to the command below it, so a comment
			// above the seed introduces that command just as one directly
			// above the `$` line does. (Its body, read a moment earlier in
			// this backwards walk, has already cleared the flag.)
			introducing = true
		case strings.HasPrefix(line, "-- ") && strings.HasSuffix(line, " --"):
			introducing = false
		case introducing && (strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#")):
			lead[n] = true
		default:
			introducing = false
		}
	}

	return lead
}

// parseTranscript reads a scenario into steps.
func parseTranscript(source string) ([]transcriptStep, error) {
	var (
		steps       []transcriptStep
		pendingLead []string
		pending     []transcriptSection
		current     *transcriptStep
		section     *transcriptSection
		body        []string
	)

	closeSection := func() {
		if section == nil {
			return
		}

		section.body = strings.TrimRight(strings.Join(body, "\n"), "\n")

		// A `write` seeds the file the *next* command reads, so it is held
		// until that command arrives; everything else is a claim about the
		// command already above it.
		if section.kind == "write" {
			pending = append(pending, *section)
		} else {
			current.claims = append(current.claims, *section)
		}

		section, body = nil, nil
	}

	lines := strings.Split(source, "\n")
	lead := leadLines(lines)

	for n, line := range lines {
		switch {
		case strings.HasPrefix(line, "$ "):
			closeSection()

			args := strings.Fields(strings.TrimPrefix(line, "$ "))
			if len(args) == 0 || args[0] != "flow" {
				return nil, fmt.Errorf("line %d: a command line runs `flow`, not %q", n+1, line)
			}

			steps = append(steps, transcriptStep{args: args[1:], writes: pending, lead: pendingLead})
			current, pendingLead, pending = &steps[len(steps)-1], nil, nil

		case strings.HasPrefix(line, "-- ") && strings.HasSuffix(line, " --"):
			closeSection()

			fields := strings.Fields(strings.TrimSuffix(strings.TrimPrefix(line, "-- "), " --"))
			next := transcriptSection{kind: fields[0]}
			switch next.kind {
			case "stdout", "stderr", "exit":
				if len(fields) != 1 {
					return nil, fmt.Errorf("line %d: `-- %s --` takes no name", n+1, next.kind)
				}
			case "file":
				if len(fields) != 2 {
					return nil, fmt.Errorf("line %d: `-- %s --` needs a path", n+1, next.kind)
				}
				next.name = fields[1]
			case "write":
				if len(fields) != 2 {
					return nil, fmt.Errorf("line %d: `-- write --` needs a path", n+1)
				}
				next.name = fields[1]
			default:
				return nil, fmt.Errorf("line %d: no section is spelled %q", n+1, next.kind)
			}

			if next.kind != "write" && current == nil {
				return nil, fmt.Errorf("line %d: `-- %s --` claims something about a command, and none has run yet",
					n+1, next.kind)
			}

			section = &next

		case lead[n]:
			// A run of blank and comment lines directly above a command
			// belongs to the command, wherever it sits — including where it
			// follows a section, which is where an author naturally writes the
			// sentence saying what the next command is for. The cost is stated
			// in the format's doc above: a declared body may not end with one.
			closeSection()

			pendingLead = append(pendingLead, line)

		case section != nil:
			body = append(body, line)

		case strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#"):
			// Only reachable above no command at all: everywhere else a
			// comment is either introducing the command below it or part of a
			// declared body. Said plainly, because the alternative reading —
			// "my comment is not a comment" — sends an author looking in the
			// wrong place.
			return nil, fmt.Errorf("line %d: %q introduces no command; the transcript runs no commands after it", n+1, line)

		default:
			return nil, fmt.Errorf("line %d: %q is neither a command, a section, nor a comment", n+1, line)
		}
	}

	closeSection()

	if len(steps) == 0 {
		return nil, fmt.Errorf("the transcript runs no commands")
	}

	return steps, nil
}

// formatTranscript writes steps back out in the format parseTranscript reads,
// which is what makes -update a round trip rather than a rewrite: comments and
// spacing an author wrote survive it.
func formatTranscript(steps []transcriptStep) string {
	var out strings.Builder

	for _, step := range steps {
		for _, line := range step.lead {
			out.WriteString(line + "\n")
		}

		for _, section := range step.writes {
			out.WriteString("-- write " + section.name + " --\n")
			out.WriteString(section.body + "\n")
		}

		out.WriteString("$ flow " + strings.Join(step.args, " ") + "\n")

		for _, section := range step.claims {
			out.WriteString("-- " + section.label() + " --\n")
			if section.body != "" {
				out.WriteString(section.body + "\n")
			}
		}
	}

	return out.String()
}

// expandWork puts the scenario's directory back into a command line.
func expandWork(args []string, work string) []string {
	expanded := make([]string, len(args))
	for i, arg := range args {
		expanded[i] = strings.ReplaceAll(arg, transcriptWorkDir, work)
	}
	return expanded
}

// collapseWork takes it back out of what the command wrote, so an expectation
// is about the CLI rather than about where a test run happened to put its
// files.
func collapseWork(text, work string) string {
	return strings.TrimRight(strings.ReplaceAll(text, work, transcriptWorkDir), "\n")
}

// TestTranscriptFormatRoundTrips is what makes `-update` safe to run on a file
// somebody wrote: regenerating a transcript whose expectations already match
// must give back the same bytes, comments and spacing included. Without this,
// the first `-update` after an unrelated change would quietly reformat every
// scenario and bury the one line that actually moved.
func TestTranscriptFormatRoundTrips(t *testing.T) {
	t.Parallel()

	scripts, err := filepath.Glob(filepath.Join("testdata", "script", "*.txt"))
	require.NoError(t, err)
	require.NotEmpty(t, scripts)

	for _, script := range scripts {
		t.Run(filepath.Base(script), func(t *testing.T) {
			t.Parallel()

			source, err := os.ReadFile(script)
			require.NoError(t, err)

			steps, err := parseTranscript(string(source))
			require.NoError(t, err)

			assert.Equal(t, string(source), formatTranscript(steps),
				"parsing and writing this transcript back out did not reproduce it, "+
					"so `-update` would rewrite parts of it nobody changed")
		})
	}
}

// TestTranscriptParserRefusesWhatItCannotRun covers the diagnostics half: a
// transcript is authored by hand, so a mistake in one has to say what is wrong
// rather than silently assert nothing. Silently asserting nothing is the shape
// that matters — a scenario that parses into no claims is a test that passes
// without looking.
func TestTranscriptParserRefusesWhatItCannotRun(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		script string
		want   string
	}{
		{"nothing runs", "# only a comment\n", "introduces no command"},
		{"not flow", "$ ls\n", "runs `flow`"},
		{"a claim with no command", "-- stdout --\nx\n$ flow validate x\n", "none has run yet"},
		{"an invented section", "$ flow validate x\n-- shout --\nx\n", `no section is spelled "shout"`},
		{"a named stream", "$ flow validate x\n-- stdout x --\n", "takes no name"},
		{"an unnamed file", "$ flow validate x\n-- file --\n", "needs a path"},
		{"loose text", "$ flow validate x\nnot a section\n", "neither a command"},
		{"an empty transcript", "", "runs no commands"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseTranscript(test.script)
			require.Error(t, err, "a transcript this runner cannot run was accepted")
			assert.Contains(t, err.Error(), test.want)
		})
	}
}
