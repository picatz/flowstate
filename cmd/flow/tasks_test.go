package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// runTasksInto executes `flow tasks` with a format and captures stdout.
func runTasksInto(t *testing.T, format string) string {
	t.Helper()

	var out bytes.Buffer
	cmd := &cobra.Command{}
	addOutputFlag(cmd)
	if err := cmd.Flags().Set("output", format); err != nil {
		t.Fatal(err)
	}
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})

	if err := runTasks(cmd, nil); err != nil {
		t.Fatalf("runTasks(%s): %v", format, err)
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
		CELLibraries  []string `json:"celLibraries"`
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
}

// TestTasksTextShowsWhatATaskTakes keeps the human form honest.
//
// It listed a name and a summary, which sent a reader to the README's
// hand-maintained table — the drift the registry exists to prevent.
func TestTasksTextShowsWhatATaskTakes(t *testing.T) {
	rendered := runTasksInto(t, "text")

	for _, want := range []string{"http", "url*", "status_code", "inputs", "outputs"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("`flow tasks` does not mention %q:\n%s", want, rendered)
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

	writeFields(&out, surface.Theme, []fieldGroup{
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

	require.NoError(t, writeFields(surface.Out, surface.Theme, []fieldGroup{
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

	err := writeFields(failingWriter{}, ui.Plain(io.Discard, io.Discard).Theme, []fieldGroup{
		{label: "inputs", fields: []v1.InputField{{Name: "url", Type: "string"}}},
	})

	require.Error(t, err, "a listing that could not be written reported success")
}

// failingWriter is a stdout that has gone away.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
