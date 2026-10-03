package flowdap_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
)

// jsonFields is the set of JSON keys a struct reads, skipping the fields that
// opt out with `json:"-"`.
func jsonFields(t *testing.T, v any) []string {
	t.Helper()
	var names []string
	for field := range reflect.TypeOf(v).Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}

// TestVSCodeDebugTypeNamesTheFieldsTheAdapterReads holds the extension's
// `flowstate` debug type to the launch and attach arguments the adapter
// decodes. The manifest is hand-written TypeScript-side JSON and the adapter
// is Go, so nothing else can notice one saying `workflowID` while the other
// reads `workflowId`: the editor would accept the configuration and the
// adapter would silently ignore the field.
func TestVSCodeDebugTypeNamesTheFieldsTheAdapterReads(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "editors", "vscode", "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Contributes struct {
			Debuggers []struct {
				Type                    string `json:"type"`
				ConfigurationAttributes map[string]struct {
					Properties map[string]json.RawMessage `json:"properties"`
				} `json:"configurationAttributes"`
			} `json:"debuggers"`
		} `json:"contributes"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	// The anti-vacuity guard: a renamed contribution would compare nothing.
	if len(manifest.Contributes.Debuggers) != 1 || manifest.Contributes.Debuggers[0].Type != "flowstate" {
		t.Fatalf("expected one flowstate debug type, got %+v", manifest.Contributes.Debuggers)
	}
	attributes := manifest.Contributes.Debuggers[0].ConfigurationAttributes

	for request, args := range map[string]any{
		"launch": flowdap.LaunchArguments{},
		"attach": flowdap.AttachArguments{},
	} {
		var declared []string
		for name := range attributes[request].Properties {
			declared = append(declared, name)
		}
		slices.Sort(declared)

		if want := jsonFields(t, args); !slices.Equal(declared, want) {
			t.Errorf("%s: manifest declares %v, the adapter reads %v", request, declared, want)
		}
	}
}
