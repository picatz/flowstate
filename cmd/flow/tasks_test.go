package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// tasksCommand builds a command carrying the flags `flow tasks` declares.
//
// The flags are declared the way [newRootCommand] declares them rather than
// asserted into a struct, so a test invokes the same flag set the CLI does: a
// command missing one answers its default, which is how a test can pass against a
// surface nobody can actually reach.
func tasksCommand(t *testing.T, out *bytes.Buffer, format string) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{}
	addOutputFlag(cmd)
	cmd.Flags().Bool(expressionsFlag, false, "")

	if err := cmd.Flags().Set("output", format); err != nil {
		t.Fatal(err)
	}

	cmd.SetOut(out)
	cmd.SetErr(&bytes.Buffer{})

	return cmd
}

// runTasksInto executes `flow tasks` with a format and captures stdout.
func runTasksInto(t *testing.T, format string, args ...string) string {
	t.Helper()

	var out bytes.Buffer

	if err := runTasks(tasksCommand(t, &out, format), args); err != nil {
		t.Fatalf("runTasks(%s, %v): %v", format, args, err)
	}

	return out.String()
}

// runTasksFlagged executes `flow tasks` with a flag set, for the surfaces an
// argument does not reach.
func runTasksFlagged(t *testing.T, format, flag string) string {
	t.Helper()

	var out bytes.Buffer

	cmd := tasksCommand(t, &out, format)
	if err := cmd.Flags().Set(flag, "true"); err != nil {
		t.Fatal(err)
	}

	if err := runTasks(cmd, nil); err != nil {
		t.Fatalf("runTasks(--%s): %v", flag, err)
	}

	return out.String()
}

// TestTasksJSONIsOneDocumentAConsumerCanIndex is the whole point of the machine
// form.
//
// A program has to address a value rather than recognize one. Before this, the
// only way out of the registry was a table meant for a terminal, so anything
// automating the CLI had to parse columns — and column positions are not a
// contract, which is why the first thing to change the layout would have broken
// them silently.
func TestTasksJSONIsOneDocumentAConsumerCanIndex(t *testing.T) {
	var catalog struct {
		Tasks []struct {
			Name    string `json:"name"`
			Summary string `json:"summary"`
			Inputs  []struct {
				Name     string `json:"name"`
				Type     string `json:"type"`
				Required bool   `json:"required"`
				Deferred bool   `json:"deferred"`
			} `json:"inputs"`
			Outputs []struct {
				Name string `json:"name"`
			} `json:"outputs"`
		} `json:"tasks"`
		CELLibraries []string `json:"celLibraries"`
		CELFunctions []struct {
			Name    string `json:"name"`
			Library string `json:"library"`
			Macro   bool   `json:"macro"`
		} `json:"celFunctions"`
		DurationUnits []string `json:"durationUnits"`
		NowIdentifier string   `json:"nowIdentifier"`
	}

	rendered := runTasksInto(t, "json")
	if err := json.Unmarshal([]byte(rendered), &catalog); err != nil {
		t.Fatalf("the catalog is not one JSON document: %v\n%s", err, rendered)
	}

	if len(catalog.Tasks) != len(v1.TaskNames()) {
		t.Fatalf("catalog has %d tasks, the registry has %d", len(catalog.Tasks), len(v1.TaskNames()))
	}

	// Indexed the way a consumer actually would, rather than by position.
	var http *struct {
		Name    string `json:"name"`
		Summary string `json:"summary"`
		Inputs  []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Required bool   `json:"required"`
			Deferred bool   `json:"deferred"`
		} `json:"inputs"`
		Outputs []struct {
			Name string `json:"name"`
		} `json:"outputs"`
	}
	for i := range catalog.Tasks {
		if catalog.Tasks[i].Name == "http" {
			http = &catalog.Tasks[i]
		}
	}
	if http == nil {
		t.Fatal("no http task in the catalog")
	}

	required := map[string]string{}
	deferred := map[string]bool{}
	for _, in := range http.Inputs {
		if in.Required {
			required[in.Name] = in.Type
		}
		deferred[in.Name] = in.Deferred
	}

	if required["url"] != "string" {
		t.Errorf("http url is not reported required and string: %v", required)
	}
	if _, isRequired := required["method"]; isRequired {
		t.Error("http method is optional and is reported required")
	}
	if !deferred["outputs"] {
		t.Error("http outputs is evaluated by the task and is not marked deferred")
	}

	if len(catalog.CELLibraries) == 0 || len(catalog.DurationUnits) == 0 || catalog.NowIdentifier == "" {
		t.Errorf("the catalog omits what an expression can say: %+v", catalog)
	}

	// The names, and not only the libraries they are grouped under. A consumer
	// reading `celLibraries` to find out how to sort a list learns that `lists` is
	// enabled and still cannot write the call.
	//
	// Checked here rather than only against the Go value because this is the form an
	// agent actually receives: a field that exists on the message and never reaches
	// the pipe is not a contract, it is an intention.
	if len(catalog.CELFunctions) == 0 {
		t.Error("the catalog names the libraries and not what is in them")
	}

	byName := make(map[string]string, len(catalog.CELFunctions))
	for _, fn := range catalog.CELFunctions {
		byName[fn.Name] = fn.Library
	}
	for name, library := range map[string]string{
		"sortBy":        "lists",
		"upperAscii":    "strings",
		"regex.replace": "regex",
	} {
		if byName[name] != library {
			t.Errorf("the catalog puts %q in %q rather than %q", name, byName[name], library)
		}
	}
}

// TestTasksTextShowsWhatATaskTakes keeps the human form honest.
//
// It listed a name and a summary, which sent a reader to the README's
// hand-maintained table — the drift the registry exists to prevent. It is the
// per-task view that owes that answer now (#379): the index is a name and a
// purpose, and `flow tasks http` is where the schema is.
func TestTasksTextShowsWhatATaskTakes(t *testing.T) {
	rendered := runTasksInto(t, "text", "http")

	for _, want := range []string{"http", "url*", "status_code", "inputs", "outputs"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("`flow tasks http` does not mention %q:\n%s", want, rendered)
		}
	}

	// A machine-shaped document must not appear on the human path, or a person
	// piping to a pager gets JSON they did not ask for.
	if strings.HasPrefix(strings.TrimSpace(rendered), "{") {
		t.Error("the text form rendered a JSON document")
	}
}

// TestTasksRefusesAFormatItDoesNotHave is the same refusal the listing makes.
//
// A caller who wrote --output yaml wants YAML, and quietly handing them a table
// is a worse answer than saying no.
func TestTasksRefusesAFormatItDoesNotHave(t *testing.T) {
	var out bytes.Buffer
	cmd := &cobra.Command{}
	addOutputFlag(cmd)
	if err := cmd.Flags().Set("output", "yaml"); err != nil {
		t.Fatal(err)
	}
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})

	err := runTasks(cmd, nil)
	if err == nil {
		t.Fatal("--output yaml was accepted")
	}
	if out.Len() != 0 {
		t.Errorf("a refused format still wrote to stdout: %q", out.String())
	}
}

// TestTasksColumnsAlignOnATerminal is the bug styling introduced, which a piped
// test cannot see.
//
// `flow tasks` was the one reference surface with no theme while help, errors, get
// and list were all styled. Adding one broke the layout: it was built with
// text/tabwriter, which measures the *bytes* it is given, and a styled cell is
// mostly escape sequences — so every column after the first shifted by however long
// the colour codes were. Piped output stayed correct, because piped output is
// unstyled, which is exactly why this asserts on a styled render.
//
// Measured with lipgloss, which counts displayed columns rather than bytes, the way
// the help page's own two-column lists are.
func TestTasksColumnsAlignOnATerminal(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	surface := ui.ForCapabilities(&out, &out,
		ui.Capabilities{Profile: colorprofile.TrueColor, TTY: true, Width: 120},
		ui.Capabilities{Profile: colorprofile.TrueColor, TTY: true, Width: 120})

	writeFields(&out, surface.Theme, surface.Caps.Width, []fieldGroup{
		{label: "inputs", fields: []v1.InputField{
			{Name: "url", Type: "string", Required: true},
			{Name: "retry_on_unknown_outcome", Type: "bool"},
		}},
		{label: "outputs", fields: []v1.InputField{
			{Name: "status_code", Type: "int"},
		}},
	})

	// The type column starts in the same place on every row, which is the whole
	// point of a column. Measured on the rendered text with the styling stripped,
	// since that is what a reader sees laid out.
	var starts []int
	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		plain := stripANSI(line)
		for _, kind := range []string{"string", "bool", "int"} {
			if i := strings.LastIndex(plain, kind); i > 0 {
				starts = append(starts, i)

				break
			}
		}
	}

	require.Len(t, starts, 3, "not every row was rendered:\n%s", out.String())
	for _, start := range starts[1:] {
		assert.Equal(t, starts[0], start,
			"the type column starts in a different place on each row, so the styling "+
				"was counted as width:\n%s", out.String())
	}
}

// stripANSI removes escape sequences, so a rendered line can be measured the way it
// is seen rather than the way it is stored.
func stripANSI(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1b {
			for i < len(s) && s[i] != 'm' {
				i++
			}

			continue
		}
		b.WriteByte(s[i])
	}

	return b.String()
}

// TestTasksDegradesToWhatTheStreamCarries is the half of styling that is not about
// choosing a colour.
//
// A theme resolves to the palette's own values, which are 24-bit. It is the
// colorprofile writer that turns those into what the stream actually supports — 256
// colours, then 16, then none. `runTasks` built a profile-aware surface and then
// wrote past it to cmd.OutOrStdout(), so a terminal that had told us it has 256
// colours received truecolor sequences it cannot render.
//
// Invisible through a pipe, like the alignment bug: an unstyled stream degrades to
// nothing either way.
func TestTasksDegradesToWhatTheStreamCarries(t *testing.T) {
	t.Parallel()

	// The profile is set rather than detected. colorprofile.NewWriter asks the
	// writer what it is, and a strings.Builder is not a terminal — so detection
	// answers NoTTY and the writer strips every sequence, which made the first
	// version of this test pass while proving nothing. Its own guard caught that.
	var raw strings.Builder
	styled := &colorprofile.Writer{Forward: &raw, Profile: colorprofile.ANSI256}

	surface := ui.ForCapabilities(styled, styled,
		ui.Capabilities{Profile: colorprofile.ANSI256, TTY: true, Width: 120},
		ui.Capabilities{Profile: colorprofile.ANSI256, TTY: true, Width: 120})

	require.NoError(t, writeFields(surface.Out, surface.Theme, surface.Caps.Width, []fieldGroup{
		{label: "inputs", fields: []v1.InputField{{Name: "url", Type: "string", Required: true}}},
	}))

	rendered := raw.String()
	require.Contains(t, rendered, "\x1b[", "nothing was styled, so this proves nothing")
	assert.NotContains(t, rendered, "38;2;",
		"a 24-bit colour reached a stream that carries 256, so the degrading writer was bypassed")
}

// TestTasksReportsAWriteItCouldNotFinish keeps a truncated listing from reporting
// success.
//
// The tabwriter this replaced returned the error from Flush. Writing directly makes
// every Fprintf a place the failure can be dropped instead — a full disk, or a pipe
// that has gone away — and a listing that stopped halfway while exiting zero is
// worse than one that says it could not finish.
func TestTasksReportsAWriteItCouldNotFinish(t *testing.T) {
	t.Parallel()

	err := writeFields(failingWriter{}, ui.Plain(io.Discard, io.Discard).Theme, 80, []fieldGroup{
		{label: "inputs", fields: []v1.InputField{{Name: "url", Type: "string"}}},
	})

	require.Error(t, err, "a listing that could not be written reported success")
}

// failingWriter is a stdout that has gone away.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// renderTasksAt draws a text surface at a stated terminal width.
//
// The width is asserted rather than detected, because the whole question these
// tests ask is about a terminal narrower than the one the test runs on. A piped
// buffer reports the fallback 80 either way, which is what made the overflow this
// fixes invisible to every test that existed.
func renderTasksAt(t *testing.T, width int, draw func(*ui.UI) error) string {
	t.Helper()

	var out strings.Builder

	caps := ui.Capabilities{Width: width}
	surface := ui.ForCapabilities(&out, &out, caps, caps)

	require.NoError(t, draw(surface))

	return out.String()
}

// widestLine is the display width of the longest line in a rendered surface.
func widestLine(rendered string) (int, string) {
	widest, worst := 0, ""
	for _, line := range strings.Split(rendered, "\n") {
		if w := lipgloss.Width(stripANSI(line)); w > widest {
			widest, worst = w, line
		}
	}

	return widest, worst
}

// collapse makes a rendered paragraph comparable to the phrase it was built from.
func collapse(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

// TestTasksIndexFitsANarrowTerminal is the symptom that named #379, asserted where
// it happens rather than where it was noticed.
//
// The listing joined a library's function names into one line and printed it, so a
// terminal decided where the line broke: `math.sign` came out as `math.si` on one
// line and `gn,` on the next, splitting an identifier the wrapper in help.go exists
// to keep whole. Every text surface of this command is measured here, at the
// narrowest width the ui package will ever report, because the index is not the
// only page that grew past its columns.
//
// A width of 80 rather than a smaller one: [ui.Capabilities] reports 80 for a pipe
// and for a terminal it could not measure, which makes it the width most of this
// command's output is actually read at.
func TestTasksIndexFitsANarrowTerminal(t *testing.T) {
	t.Parallel()

	const width = 80

	for _, surface := range []struct {
		name string
		draw func(*ui.UI) error
	}{
		{"index", func(surface *ui.UI) error { return writeTaskIndex(surface, nil) }},
		{"expressions", writeExpressionReference},
	} {
		t.Run(surface.name, func(t *testing.T) {
			t.Parallel()

			rendered := renderTasksAt(t, width, surface.draw)
			widest, worst := widestLine(rendered)

			assert.LessOrEqual(t, widest, width,
				"a line ran past the terminal:\n%q\nin:\n%s", worst, rendered)
		})
	}

	for _, def := range v1.DefaultRegistry().All() {
		t.Run("task/"+def.Name, func(t *testing.T) {
			t.Parallel()

			rendered := renderTasksAt(t, width, func(surface *ui.UI) error {
				return writeTask(surface, def, nil)
			})
			widest, worst := widestLine(rendered)

			// A single token wider than the terminal is left whole rather than cut,
			// which is help.go's own rule and the reason a pattern or a URL in this
			// view is still one thing to copy. So what is asserted is that nothing
			// *composed* overflows: a line over the width has to be one token.
			if widest > width {
				assert.Len(t, strings.Fields(strings.TrimSpace(stripANSI(worst))), 1,
					"a line of several words ran past the terminal:\n%q\nin:\n%s", worst, rendered)
			}
		})
	}
}

// TestTaskViewCarriesEveryDeclaredInputAndItsBounds is the answer the old listing
// could not give.
//
// `method` is three to six characters matching a pattern and `fields` holds at most
// thirty-two entries with bounded keys and values, and none of that was printed
// anywhere: an author found out by running a step that failed. The expectations are
// read out of the descriptors rather than written down here, so a bound tightened in
// the schema is a bound this test demands the view state.
func TestTaskViewCarriesEveryDeclaredInputAndItsBounds(t *testing.T) {
	t.Parallel()

	// Wide, because what is under test is whether the phrase is present, not where
	// it wrapped. Whitespace is collapsed for the same reason.
	const width = 200

	for _, def := range v1.DefaultRegistry().All() {
		t.Run(def.Name, func(t *testing.T) {
			t.Parallel()

			rendered := collapse(stripANSI(renderTasksAt(t, width, func(surface *ui.UI) error {
				return writeTask(surface, def, nil)
			})))

			for _, field := range v1.Inputs(def) {
				assert.Contains(t, rendered, field.Name,
					"the view of %s leaves out its %q input", def.Name, field.Name)

				for _, constraint := range field.Constraints {
					assert.Contains(t, rendered, collapse(constraint),
						"the view of %s does not say that %q is %s",
						def.Name, field.Name, constraint)
				}
			}

			for _, field := range v1.Outputs(def) {
				assert.Contains(t, rendered, field.Name,
					"the view of %s leaves out its %q output", def.Name, field.Name)
			}

			// The names, and not only the sentence explaining what deferred means.
			// An author deciding what to write in `outputs` needs to know that this
			// is one of them.
			for _, name := range def.DeferredInputs {
				assert.Contains(t, rendered, name,
					"the view of %s does not name %q among the inputs it evaluates itself",
					def.Name, name)
			}
		})
	}
}

// TestHTTPBoundsReachTheView pins the two length bounds by hand.
//
// The test above derives its expectations, which is what keeps it honest as the
// schema changes and also what would let it pass against a build where every
// constraint had quietly become empty. This one names two of them, so that
// deriving nothing fails somewhere.
func TestHTTPBoundsReachTheView(t *testing.T) {
	t.Parallel()

	def, found := v1.LookupTask("http")
	require.True(t, found, "this build has no http task")

	rendered := collapse(stripANSI(renderTasksAt(t, 200, func(surface *ui.UI) error {
		return writeTask(surface, def, nil)
	})))

	assert.Contains(t, rendered, "3 to 6 characters", "the method input's length bounds are not shown")
	assert.Contains(t, rendered, "1 to 128 characters", "the credential input's length bounds are not shown")
}

// TestTasksIndexIsOneLinePerTask is the shape the split was for.
//
// A catalog that plugins extend has to stay scannable, and the way it stops being
// scannable is a block per task. So the index is measured: every task named, on one
// line, with the pointer at the view that has the rest.
func TestTasksIndexIsOneLinePerTask(t *testing.T) {
	t.Parallel()

	rendered := stripANSI(renderTasksAt(t, 100, func(surface *ui.UI) error { return writeTaskIndex(surface, nil) }))

	for _, def := range v1.DefaultRegistry().All() {
		var found int
		for _, line := range strings.Split(rendered, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), def.Name+" ") {
				found++
			}
		}

		assert.Equal(t, 1, found, "%q is named on %d lines of the index:\n%s", def.Name, found, rendered)
	}

	// The index no longer carries a task's schema, which is the whole point, so an
	// author has to be told where it went.
	assert.Contains(t, collapse(rendered), "flow tasks <name>",
		"the index does not say where the rest of a task's description is:\n%s", rendered)
	assert.Contains(t, collapse(rendered), "flow tasks --expressions",
		"the index does not say where the expression reference went:\n%s", rendered)
}

// TestTasksUnknownNameSuggestsTheNearest is the diagnostic standard applied to an
// argument.
//
// The registry knows every name it has, so refusing one without saying which was
// meant withholds an answer it is holding. Marked as an invocation mistake, since
// nothing ran.
func TestTasksUnknownNameSuggestsTheNearest(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	err := runTasks(tasksCommand(t, &out, "text"), []string{"htp"})

	require.Error(t, err, "an unknown task name was accepted")
	assert.Contains(t, err.Error(), `did you mean "http"?`,
		"the refusal does not name the task that was probably meant")
	assert.True(t, isUsageError(err), "an unknown task name is a mistake about the command line")
	assert.Zero(t, out.Len(), "a refused name still wrote an answer to stdout: %q", out.String())
}

// TestTasksJSONNarrowsToTheNamedTask keeps the machine surface in step with the
// text one.
//
// The argument means "describe this task" on both, so answering it with the whole
// catalog would leave every consumer doing the selecting the argument already did.
// The document is the schema's own TaskDescription, which is what a plugin's tasks
// are described by too.
func TestTasksJSONNarrowsToTheNamedTask(t *testing.T) {
	t.Parallel()

	var task struct {
		Name   string `json:"name"`
		Inputs []struct {
			Name        string   `json:"name"`
			Type        string   `json:"type"`
			Required    bool     `json:"required"`
			Constraints []string `json:"constraints"`
		} `json:"inputs"`
		Outputs []struct {
			Name string `json:"name"`
		} `json:"outputs"`
	}

	rendered := runTasksInto(t, "json", "http")
	require.NoError(t, json.Unmarshal([]byte(rendered), &task),
		"the per-task answer is not one JSON document:\n%s", rendered)

	assert.Equal(t, "http", task.Name)
	require.NotEmpty(t, task.Inputs)

	bounds := map[string][]string{}
	for _, in := range task.Inputs {
		bounds[in.Name] = in.Constraints
	}

	assert.Contains(t, bounds["method"], "3 to 6 characters",
		"the machine form drops the bounds the text form prints: %v", bounds)
	assert.Contains(t, bounds["url"], "a URI")
}

// TestTasksExpressionsIsItsOwnPage checks that the reference moved rather than
// being deleted.
//
// It was an appendix to a task listing that had nothing to do with tasks, and it is
// the only place in the terminal that prints what an `if:` may say. Moving it out
// of the index only helps if it is still reachable.
func TestTasksExpressionsIsItsOwnPage(t *testing.T) {
	t.Parallel()

	rendered := collapse(stripANSI(runTasksFlagged(t, "text", expressionsFlag)))

	for _, want := range []string{"sortBy", "math.sign", "days", v1.NowIdentifier, v1.VarsRoot} {
		assert.Contains(t, rendered, want, "the expression reference leaves out %q", want)
	}

	// And it is not the task list wearing a flag.
	assert.NotContains(t, rendered, "Perform an HTTP request",
		"the expression reference printed the task catalog too")
}

// The mirror test for the worked example itself — TestBuildValidates — moved
// to cmd/flow/internal/taskexample, alongside the code it tests, when the
// task reference generator (docs/reference/tasks.md) started needing the
// same worked example `flow tasks <name>` already built. See that package's
// doc comment for why the two share one source rather than two.
