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
// escape sequence, a bell, a bidi override, and a newline that fabricates a row.
const hostile = "x\x1b[2J\a\u202e\nforged"

// requireNoRawControl fails when any rune the terminal would act on survived.
// Styling escapes the theme itself writes are removed first.
func requireNoRawControl(t *testing.T, rendered, where string) {
	t.Helper()

	plain := stripANSI(rendered)
	for _, raw := range []string{"\x1b", "\a", "\u202e"} {
		assert.NotContains(t, plain, raw, "%s: raw %q reached the terminal", where, raw)
	}
	assert.Contains(t, plain, `\x1b[2J`, "%s: the hostile text did not render escaped, so this proves nothing", where)
}

// TestTasksEscapesPluginTextInEveryField is #2677: a task's name, summary and
// every field's name, type and constraints are supplied by a plugin.
func TestTasksEscapesPluginTextInEveryField(t *testing.T) {
	t.Parallel()

	field := v1.InputField{Name: hostile, Type: hostile, Constraints: []string{hostile}, Required: true}

	t.Run("fields", func(t *testing.T) {
		t.Parallel()

		requireNoRawControl(t, renderProse(t, 120, []v1.InputField{field}, nil), "writeFields")
	})

	t.Run("task page", func(t *testing.T) {
		t.Parallel()

		def := v1.TaskDef{Name: "p." + hostile, Summary: hostile}
		rendered := renderTasksAt(t, 120, func(surface *ui.UI) error {
			// The example builder may refuse a descriptor-less task; the
			// header is written before it, which is what is under test.
			_ = writeTask(surface, def, nil)

			return nil
		})
		requireNoRawControl(t, rendered, "writeTask")
	})

	t.Run("provenance", func(t *testing.T) {
		t.Parallel()

		catalog := &v1.PluginCatalog{Plugins: []*v1.PluginDescription{{
			Name: "p", Version: hostile, Path: hostile,
			Tasks: []*v1.TaskDescription{{Name: "p." + hostile}},
		}}}
		rendered := renderTasksAt(t, 120, func(surface *ui.UI) error { return writeTaskIndex(surface, catalog) })
		requireNoRawControl(t, rendered, "writeTaskIndex provenance")

		def := v1.TaskDef{Name: "p.t"}
		assert.NotContains(t, taskProvenance(def, catalog), "\x1b")
		assert.NotContains(t, strings.Join(taskMarkers(v1.TaskDef{Name: hostile + ".t"}), " "), "\x1b")
	})
}

// TestPluginsEscapesPluginTextInEveryField is the same hole in `flow plugins`.
func TestPluginsEscapesPluginTextInEveryField(t *testing.T) {
	t.Parallel()

	field := &v1.TaskField{Name: hostile, Type: hostile, Constraints: []string{hostile}}
	catalog := &v1.PluginCatalog{Plugins: []*v1.PluginDescription{{
		Name: hostile, Version: hostile, Description: hostile, Path: hostile,
		DistributionDigest: hostile,
		SecretSchemes:      []string{hostile},
		Credentials:        []*v1.CredentialDeclaration{{Name: hostile, Description: hostile}},
		Tasks: []*v1.TaskDescription{{
			Name: hostile, Summary: hostile,
			SecretInputs:     []string{hostile},
			CredentialInputs: map[string]string{hostile: hostile},
			Inputs:           []*v1.TaskField{field},
			Outputs:          []*v1.TaskField{field},
		}},
	}}}

	var out strings.Builder
	surface := ui.ForCapabilities(&out, &out, ui.Capabilities{Width: 120}, ui.Capabilities{Width: 120})
	require.NoError(t, writePluginCatalog(surface, catalog))

	requireNoRawControl(t, out.String(), "writePluginCatalog")
}
