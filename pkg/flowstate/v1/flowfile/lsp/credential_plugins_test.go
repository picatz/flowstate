package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// registerCredentialPlugins installs the two fixture plugins the way `flow lsp
// --plugin-dir` does, in the process-wide registry the compiler reads, and removes
// them afterwards.
func registerCredentialPlugins(t *testing.T) {
	t.Helper()

	require.NoError(t, v1.DefaultRegistry().Register(conformance.BoundCredentialTaskDef()))
	require.NoError(t, v1.DefaultRegistry().Register(conformance.FederatedCredentialTaskDef()))
	t.Cleanup(func() {
		v1.DefaultRegistry().Unregister(conformance.BoundCredentialTaskName)
		v1.DefaultRegistry().Unregister(conformance.FederatedCredentialTaskName)
	})
}

func TestDiagnosticsExplainAnUnboundOrWrongKindCredential(t *testing.T) {
	registerCredentialPlugins(t)

	c := newClient(t)
	c.initialize()

	const header = "edition: v2026.4\nname: bound\n"
	cases := []struct {
		name, src, want string
	}{
		{"bound once is clean", header + `plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:T')}
steps:
  - id: a
    bound.use: {}
`, ""},
		{"neither bound nor written", header + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use: {}
`, "api_token"},
		{"a credential the plugin does not declare", header + `plugins:
  bound:
    version: v0.1.0
    credentials:
      nope: ${secret('env:T')}
steps:
  - id: a
    bound.use:
      token: ${secret('env:T')}
`, "nope"},
		{"the wrong kind of reference", header + `plugins:
  federated:
    version: v0.1.0
    credentials:
      partner_token: ${secret('env:T')}
steps:
  - id: a
    federated.use: {}
`, "partner_token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := messages(c.open("file:///"+tc.name+".yaml", tc.src).Diagnostics)
			if tc.want == "" {
				assert.Empty(t, got)

				return
			}
			require.NotEmpty(t, got, "the editor shows nothing for a file `flow validate` refuses")
			assert.Contains(t, got[0], tc.want)
		})
	}
}

func TestCompletionOffersThePluginsEntryKeysAndCredentialNames(t *testing.T) {
	registerCredentialPlugins(t)

	c := newClient(t)
	c.initialize()

	complete := func(name, src string) []string {
		t.Helper()

		text, pos := splitCursor(t, src)
		uri := "file:///" + name + ".yaml"
		c.open(uri, text)

		return labels(c.complete(uri, pos.Line, pos.Character).Items)
	}

	assert.ElementsMatch(t, []string{"version", "credentials"},
		complete("entry", "edition: v2026.4\nname: x\nplugins:\n  bound:\n    |\n"))
	assert.Equal(t, []string{"api_token"},
		complete("names", "edition: v2026.4\nname: x\nplugins:\n  bound:\n    version: v0.1.0\n    credentials:\n      |\n"),
		"the credentials the plugin's registered tasks claim")
	assert.Equal(t, []string{"partner_token"},
		complete("federated", "edition: v2026.4\nname: x\nplugins:\n  federated:\n    credentials:\n      |\n"))
	assert.Empty(t,
		complete("unloaded", "edition: v2026.4\nname: x\nplugins:\n  unloaded:\n    credentials:\n      |\n"),
		"a plugin this process has not loaded declares nothing it can name")
}

func TestHoverDescribesAPluginCredentialAndTheInputsItReaches(t *testing.T) {
	registerCredentialPlugins(t)

	c := newClient(t)
	c.initialize()

	const src = `edition: v2026.4
name: bound
plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:T')}
steps:
  - id: a
    bound.use:
      note: hi
  - id: b
    bound.use:
      token: ${secret('env:OTHER')}
`
	const uri = "file:///hover-credential.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics))

	pos := positionOf(t, src, "api_token", 1)
	got := c.hover(uri, pos.Line, pos.Character)
	require.NotNil(t, got, "hovering a credential binding produced nothing")
	text := hoverText(got)
	assert.Contains(t, text, "credential of the `bound` plugin")
	assert.Contains(t, text, "bound.use.token", "the hover does not name the input the credential reaches")
	assert.Contains(t, text, "${secret('env:NAME')}")
	assert.NotContains(t, text, "env:T", "a hover describes the declaration, not the reference the file wrote")

	pos = positionOf(t, src, "token: ${secret('env:OTHER", 1)
	text = hoverText(c.hover(uri, pos.Line, pos.Character))
	assert.Contains(t, text, "`bound` plugin's `api_token` credential", "an input's hover does not say it takes a credential")
	assert.Contains(t, text, "bind that once under `plugins:`")

	pos = positionOf(t, src, "note", 1)
	assert.NotContains(t, hoverText(c.hover(uri, pos.Line, pos.Character)), "credential",
		"an input that claims no credential is described as one")
}

func TestInputHoverRecommendsTheReferenceKindTheCredentialTakes(t *testing.T) {
	registerCredentialPlugins(t)

	c := newClient(t)
	c.initialize()

	hoverFor := func(plugin, override string) string {
		src := "edition: v2026.4\nname: x\nplugins:\n  " + plugin + ": v0.1.0\nsteps:\n  - id: a\n    " + plugin + ".use:\n      token: " + override + "\n"
		uri := "file:///hover-kind-" + plugin + ".yaml"
		c.open(uri, src)
		pos := positionOf(t, src, "token:", 1)

		return hoverText(c.hover(uri, pos.Line, pos.Character))
	}

	stored := hoverFor("bound", "${secret('env:T')}")
	assert.Contains(t, stored, "${secret('...')}")
	assert.NotContains(t, stored, "${credential('...')}")

	federated := hoverFor("federated", "${credential('partner')}")
	assert.Contains(t, federated, "${credential('...')}", "a federated credential is overridden by a credential reference")
	assert.NotContains(t, federated, "${secret('...')}", "the hover recommends a reference the validator refuses")
}
