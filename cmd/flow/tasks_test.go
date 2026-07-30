package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"

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
