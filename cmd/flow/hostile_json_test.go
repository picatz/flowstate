package main

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/internal/termsafe"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// hostileJSON is text a plugin could carry to a terminal through `-o json`: a
// C1 control (U+009B is the single-byte CSI), a bidi override, an isolate, an
// Arabic letter mark, the two line separators and DEL. Written as escapes so
// this file holds none of them raw. Each field gets its own tag.
func hostileJSON(tag string) string {
	return tag + "\u009b31m\u202e\u2066\u061c\u2028\u2029\u007fend"
}

// rawTerminalRune reports a rune a terminal would act on that a JSON encoder
// has no reason to have written raw: everything termsafe names except the
// whitespace the encoder itself writes between tokens.
func rawTerminalRune(r rune) bool {
	return termsafe.Unsafe(r) && r != '\n' && r != '\r' && r != '\t'
}

// requireTerminalSafeJSON asserts on the written bytes (no raw rune reaches the
// terminal), that the document still parses, and that the payload is there in
// its \u spelling so the check cannot pass on a document that dropped the field.
func requireTerminalSafeJSON(t *testing.T, written, tag string) {
	t.Helper()

	for i, r := range written {
		require.Falsef(t, rawTerminalRune(r), "raw %U at byte %d reached the terminal: %q", r, i, written)
	}
	assert.True(t, json.Valid([]byte(written)), "the document no longer parses:\n%s", written)
	assert.Contains(t, written, tag+`\u009b31m\u202e\u2066\u061c\u2028\u2029\u007fend`,
		"the %s payload is missing or not escaped as \\uXXXX", tag)
}

// TestJSONEscapesPluginTextInEveryField is #2693: `flow plugins -o json` and
// `flow tasks -o json` print plugin text through writeJSON, and the encoder
// writes the C1, bidi and separator runes raw.
func TestJSONEscapesPluginTextInEveryField(t *testing.T) {
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
		"fieldname": func(tag string, p *v1.PluginDescription) {
			p.Tasks = []*v1.TaskDescription{{Name: "t", Inputs: []*v1.TaskField{{Name: tag, Type: "string"}}}}
		},
	}

	for tag, set := range cases {
		for _, format := range []OutputFormat{FormatJSON, FormatJSONL} {
			t.Run(tag+"/"+string(format), func(t *testing.T) {
				t.Parallel()

				plugin := &v1.PluginDescription{Name: "p", Version: "1", Path: "/x"}
				set(hostileJSON(tag), plugin)
				catalog := &v1.PluginCatalog{Plugins: []*v1.PluginDescription{plugin}}

				var out strings.Builder
				require.NoError(t, writeJSON(hostilePlain(&out), format, catalog))
				requireTerminalSafeJSON(t, out.String(), tag)

				var decoded v1.PluginCatalog
				require.NoError(t, protojson.Unmarshal([]byte(out.String()), &decoded))
				assert.True(t, proto.Equal(catalog, &decoded), "decoding the escaped document did not give back the original")
			})
		}
	}

	t.Run("tasks page", func(t *testing.T) {
		t.Parallel()

		description := &v1.TaskDescription{
			Name:    "p.t",
			Summary: hostileJSON("tsummary"),
			Inputs:  []*v1.TaskField{{Name: "x", Type: "string", Constraints: []string{hostileJSON("tfield")}}},
		}

		var out strings.Builder
		require.NoError(t, writeJSON(hostilePlain(&out), FormatJSON, description))
		for _, tag := range []string{"tsummary", "tfield"} {
			requireTerminalSafeJSON(t, out.String(), tag)
		}

		var decoded v1.TaskDescription
		require.NoError(t, protojson.Unmarshal([]byte(out.String()), &decoded))
		assert.True(t, proto.Equal(description, &decoded))
	})
}

// TestJSONEscapesEveryOtherEncoderPath covers the `-o json` paths that do not go
// through writeJSON: the run document, the lint report, the JSON Schema.
func TestJSONEscapesEveryOtherEncoderPath(t *testing.T) {
	t.Parallel()

	t.Run("run document", func(t *testing.T) {
		t.Parallel()

		for _, raw := range []bool{false, true} {
			response := &v1.GetResponse{WorkflowId: hostileJSON("wid"), RunId: hostileJSON("rid")}

			var out strings.Builder
			rendering := runRendering{format: FormatJSON, raw: raw}
			require.NoError(t, writeRunJSON(hostilePlain(&out), rendering, response))
			requireTerminalSafeJSON(t, out.String(), "wid")
			requireTerminalSafeJSON(t, out.String(), "rid")
		}
	})

	t.Run("lint report", func(t *testing.T) {
		t.Parallel()

		var out strings.Builder
		require.NoError(t, writeLintJSON(hostilePlain(&out), FormatJSON, lintReport{Skipped: []string{hostileJSON("lint")}}))
		requireTerminalSafeJSON(t, out.String(), "lint")
	})

	t.Run("json schema", func(t *testing.T) {
		t.Parallel()

		var out strings.Builder
		require.NoError(t, writeSchema(hostilePlain(&out), FormatJSON, map[string]any{"title": hostileJSON("schema")}))
		requireTerminalSafeJSON(t, out.String(), "schema")
	})
}

// TestJSONLeavesOrdinaryTextAlone pins that the rewrite is narrow: text in any
// script, emoji, accents and a literal backslash-u sequence come out as they
// went in.
func TestJSONLeavesOrdinaryTextAlone(t *testing.T) {
	t.Parallel()

	ordinary := "日本語 \U0001F680 café ñandú Ελληνικά שלום \\u202e"
	catalog := &v1.PluginCatalog{Plugins: []*v1.PluginDescription{{Name: ordinary, Version: "1", Path: "/x"}}}

	var out strings.Builder
	require.NoError(t, writeJSON(hostilePlain(&out), FormatJSON, catalog))

	var decoded v1.PluginCatalog
	require.NoError(t, protojson.Unmarshal([]byte(out.String()), &decoded))
	assert.Equal(t, ordinary, decoded.GetPlugins()[0].GetName())
	for _, kept := range []string{"日本語", "\U0001F680", "café", "ñandú", "Ελληνικά", "שלום"} {
		assert.Contains(t, out.String(), kept, "%q was escaped but is ordinary text", kept)
	}
	assert.Contains(t, out.String(), `\\u202e`, "a literal backslash-u sequence must stay a literal, not be unescaped or doubled")
}

// TestJSONCheckDetectsUnescapedOutput is the negative direction: the same
// payload through a bare protojson encoder, which is what writeJSON was, trips
// the raw-rune check, so the tests above fail without the change.
func TestJSONCheckDetectsUnescapedOutput(t *testing.T) {
	t.Parallel()

	catalog := &v1.PluginCatalog{Plugins: []*v1.PluginDescription{{Name: hostileJSON("bare")}}}
	bare, err := protojson.Marshal(catalog)
	require.NoError(t, err)

	var found bool
	for _, r := range string(bare) {
		found = found || rawTerminalRune(r)
	}
	require.True(t, found, "protojson no longer writes these runes raw; the guard is protecting nothing")

	// And the one shared rewrite is what closes it.
	for _, r := range string(termsafe.EscapeJSON(bare)) {
		require.False(t, rawTerminalRune(r), "%U survived EscapeJSON", r)
	}
}

// TestJSONEscapeSetMatchesTextViews is the drift test: a rune the text views
// escape is escaped in JSON and the reverse, because both ask termsafe.Unsafe.
// ASCII is compared only where the JSON encoder does not already own it.
func TestJSONEscapeSetMatchesTextViews(t *testing.T) {
	t.Parallel()

	for r := rune(0x7f); r <= unicode.MaxRune; r++ {
		if r >= 0xD800 && r <= 0xDFFF {
			continue
		}

		textEscaped := ui.EscapeControl(string(r)) != string(r)
		jsonEscaped := string(termsafe.EscapeJSON([]byte(string(r)))) != string(r)
		if textEscaped != jsonEscaped {
			t.Fatalf("U+%04X: text views escape=%v, JSON escape=%v", r, textEscaped, jsonEscaped)
		}
	}
}
