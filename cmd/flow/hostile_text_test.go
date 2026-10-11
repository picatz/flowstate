package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// hostile is text a plugin's descriptor could carry to drive a terminal: an
// escape sequence, a bell, a bidi override, and a newline that fabricates a
// row. Each field under test gets its own tag, so one field's escaped copy
// cannot satisfy the check for another.
func hostile(tag string) string {
	return tag + "\x1b[2J\a\u202e\nforged"
}

// hostilePlain is unstyled, so any ESC in the output is the plugin's and the
// bytes can be inspected without stripping anything.
func hostilePlain(out *strings.Builder) *ui.UI {
	return ui.Plain(out, out)
}

// requireEscaped fails on any raw control byte, and requires the tagged
// payload to appear in its escaped spelling.
func requireEscaped(t *testing.T, rendered, tag string) {
	t.Helper()

	for _, raw := range []string{"\x1b", "\a", "\u202e", "\nforged"} {
		assert.NotContains(t, rendered, raw, "raw %q reached the terminal", raw)
	}
	assert.Contains(t, rendered, tag+`\x1b[2J`, "the %s payload did not render escaped, so this proves nothing", tag)
}

// TestTasksEscapesPluginTextInEveryField is #2677: a task's name, summary and
// every field's name, type and constraints are supplied by a plugin.
func TestTasksEscapesPluginTextInEveryField(t *testing.T) {
	t.Parallel()

	fieldCases := map[string]v1.InputField{
		"fname":       {Name: hostile("fname")},
		"ftype":       {Name: "x", Type: hostile("ftype")},
		"fconstraint": {Name: "x", Type: "string", Constraints: []string{hostile("fconstraint")}},
	}
	for tag, field := range fieldCases {
		t.Run(tag, func(t *testing.T) {
			t.Parallel()

			var out strings.Builder
			require.NoError(t, writeFields(&out, hostilePlain(&out).Theme, 120,
				[]fieldGroup{{label: "inputs", fields: []v1.InputField{field}}}))
			requireEscaped(t, out.String(), tag)
		})
	}

	taskCases := map[string]v1.TaskDef{
		"tname":    {Name: "p." + hostile("tname"), Summary: "ok"},
		"Tsummary": {Name: "p.t", Summary: hostile("Tsummary")},
	}
	for tag, def := range taskCases {
		t.Run(tag, func(t *testing.T) {
			t.Parallel()

			var out strings.Builder
			// The example builder may refuse a descriptor-less task; the
			// header is written before it, which is what is under test.
			_ = writeTask(hostilePlain(&out), def, nil)
			requireEscaped(t, out.String(), tag)
		})
	}

	provenanceCases := map[string]*v1.PluginDescription{
		"pversion": {Name: "p", Version: hostile("pversion"), Path: "/x", Tasks: []*v1.TaskDescription{{Name: "p.t"}}},
		"ppath":    {Name: "p", Version: "1", Path: hostile("ppath"), Tasks: []*v1.TaskDescription{{Name: "p.t"}}},
		"ptask":    {Name: "p", Version: "1", Path: "/x", Tasks: []*v1.TaskDescription{{Name: "p." + hostile("ptask")}}},
	}
	for tag, plugin := range provenanceCases {
		t.Run(tag, func(t *testing.T) {
			t.Parallel()

			var out strings.Builder
			require.NoError(t, writeTaskIndex(hostilePlain(&out), &v1.PluginCatalog{Plugins: []*v1.PluginDescription{plugin}}))
			requireEscaped(t, out.String(), tag)
		})
	}

	t.Run("task page provenance", func(t *testing.T) {
		t.Parallel()

		catalog := &v1.PluginCatalog{Plugins: []*v1.PluginDescription{{
			Name: "p", Version: hostile("tpv"), Path: hostile("tpp"),
		}}}
		line := taskProvenance(v1.TaskDef{Name: "p.t"}, catalog)
		requireEscaped(t, line, "tpv")
		assert.Contains(t, line, `tpp\x1b[2J`)
	})

	t.Run("marker", func(t *testing.T) {
		t.Parallel()

		markers := strings.Join(taskMarkers(v1.TaskDef{Name: hostile("mk") + ".t"}), " ")
		requireEscaped(t, markers, "mk")
	})
}

// TestPluginsEscapesPluginTextInEveryField is the same hole in `flow plugins`,
// one field at a time.
func TestPluginsEscapesPluginTextInEveryField(t *testing.T) {
	t.Parallel()

	field := func(tag string) *v1.TaskField {
		return &v1.TaskField{Name: "f", Type: "string", Constraints: []string{tag}}
	}

	cases := map[string]func(tag string, p *v1.PluginDescription){
		"name":     func(tag string, p *v1.PluginDescription) { p.Name = tag },
		"version":  func(tag string, p *v1.PluginDescription) { p.Version = tag },
		"desc":     func(tag string, p *v1.PluginDescription) { p.Description = tag },
		"path":     func(tag string, p *v1.PluginDescription) { p.Path = tag },
		"digest":   func(tag string, p *v1.PluginDescription) { p.DistributionDigest = tag },
		"scheme":   func(tag string, p *v1.PluginDescription) { p.SecretSchemes = []string{tag} },
		"credname": func(tag string, p *v1.PluginDescription) { p.Credentials = []*v1.CredentialDeclaration{{Name: tag}} },
		"creddesc": func(tag string, p *v1.PluginDescription) {
			p.Credentials = []*v1.CredentialDeclaration{{Name: "c", Description: tag}}
		},
		"taskname": func(tag string, p *v1.PluginDescription) { p.Tasks = []*v1.TaskDescription{{Name: tag}} },
		"tasksum":  func(tag string, p *v1.PluginDescription) { p.Tasks = []*v1.TaskDescription{{Name: "t", Summary: tag}} },
		"secretin": func(tag string, p *v1.PluginDescription) {
			p.Tasks = []*v1.TaskDescription{{Name: "t", SecretInputs: []string{tag}}}
		},
		"credinkey": func(tag string, p *v1.PluginDescription) {
			p.Tasks = []*v1.TaskDescription{{Name: "t", CredentialInputs: map[string]string{tag: "c"}}}
		},
		"credinval": func(tag string, p *v1.PluginDescription) {
			p.Tasks = []*v1.TaskDescription{{Name: "t", CredentialInputs: map[string]string{"in": tag}}}
		},
		"fieldconstraint": func(tag string, p *v1.PluginDescription) {
			p.Tasks = []*v1.TaskDescription{{Name: "t", Inputs: []*v1.TaskField{field(tag)}}}
		},
	}

	for tag, set := range cases {
		t.Run(tag, func(t *testing.T) {
			t.Parallel()

			p := &v1.PluginDescription{Name: "p", Version: "1", Path: "/x"}
			set(hostile(tag), p)

			var out strings.Builder
			require.NoError(t, writePluginCatalog(hostilePlain(&out), &v1.PluginCatalog{Plugins: []*v1.PluginDescription{p}}))
			requireEscaped(t, out.String(), tag)
		})
	}
}
