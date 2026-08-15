package main

import (
	"encoding/json"
	"testing"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	"github.com/picatz/flowstate/cmd/flow/internal/reference"
)

// TestResourcesMatchTheInventory is the resource half of the pin the tool list
// has had since it existed.
//
// The reasoning is the same and the failure mode is worse. A tool is chosen
// deliberately by a model; a resource is *listed* to it, so a resource added
// without review is context every agent silently receives, from a surface with
// no argument to constrain it. The inventory is therefore written out here and
// held to what a client is served, in both directions — something registered
// without a line fails, and a line naming something no longer registered fails.
//
// The examples are the one part not written out, and that is deliberate rather
// than a loosening: they are *derived* from the embedded set, which
// TestTheMirrorMatchesTheRepository holds to examples/ on disk. Writing 23 names
// here would pin them to a third place, and the reviewable question — "may
// examples/ appear on this surface at all" — is answered by this line, not by
// the names.
func TestResourcesMatchTheInventory(t *testing.T) {
	t.Parallel()

	inventory := map[string]string{
		flowmcp.DSLResourceURI: flowmcp.MarkdownMIME,
		// The one resource on this surface that is not reference material and
		// not addressed to a model at all: the MCP Apps view a host renders for
		// flowstate_get. It is listed because the extension requires a view to be
		// predeclared and enumerable, which is what lets a host prefetch it and a
		// reviewer inspect it, and its description says plainly that a model has
		// no reason to read its bytes.
		flowmcp.ApprovalCardURI:    flowmcp.UIAppMIME,
		flowmcp.CatalogResourceURI: flowmcp.JSONMIME,
	}
	for _, name := range reference.ExampleNames() {
		inventory[flowmcp.ExamplePrefix+name] = flowmcp.YAMLMIME
	}
	require.Greater(t, len(inventory), 3, "no examples are embedded; the mirror is empty")

	session := connectMCP(t, defaultLocalRunPosture())

	served := map[string]string{}
	result, err := session.ListResources(t.Context(), &mcp.ListResourcesParams{})
	require.NoError(t, err)

	for _, resource := range result.Resources {
		assert.NotEmpty(t, resource.Name, "resource %s has no name", resource.URI)
		assert.NotEmpty(t, resource.Description,
			"resource %s has no description; a mute resource is one a model cannot choose", resource.URI)

		served[resource.URI] = resource.MIMEType
	}

	for uri, mime := range inventory {
		if assert.Contains(t, served, uri, "%s is in the inventory and nothing serves it", uri) {
			assert.Equal(t, mime, served[uri], "%s is served as the wrong media type", uri)
		}
	}
	for uri := range served {
		assert.Contains(t, inventory, uri,
			"`flow mcp` serves the resource %s, which the inventory does not name; add it "+
				"deliberately, with the reason an agent should be handed it", uri)
	}

	// Templates are a second list on the protocol, and a second way to add a
	// surface without touching the first.
	templates, err := session.ListResourceTemplates(t.Context(), &mcp.ListResourceTemplatesParams{})
	require.NoError(t, err)

	got := make([]string, 0, len(templates.ResourceTemplates))
	for _, template := range templates.ResourceTemplates {
		assert.NotEmpty(t, template.Description, "template %s has no description", template.URITemplate)

		got = append(got, template.URITemplate)
	}
	assert.Equal(t, []string{flowmcp.ExampleTemplate}, got)
}

// TestEveryListedResourceIsReadable walks the listing and reads each entry.
//
// Over the protocol, on the listed URI, because a resource that lists and does
// not read is the shape this fails as: the registration and the handler are two
// statements and only one of them is checked by declaring it.
func TestEveryListedResourceIsReadable(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	listed, err := session.ListResources(t.Context(), &mcp.ListResourcesParams{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Resources)

	for _, resource := range listed.Resources {
		result, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: resource.URI})
		require.NoError(t, err, "reading %s", resource.URI)
		require.Len(t, result.Contents, 1, "reading %s answered with %d parts", resource.URI, len(result.Contents))

		contents := result.Contents[0]
		assert.Equal(t, resource.URI, contents.URI)
		assert.Equal(t, resource.MIMEType, contents.MIMEType,
			"%s is declared as one media type and served as another", resource.URI)
		assert.NotEmpty(t, contents.Text, "%s reads as an empty document", resource.URI)

		if resource.Size > 0 {
			assert.Equal(t, resource.Size, int64(len(contents.Text)),
				"%s declares a size the read does not match", resource.URI)
		}
	}
}

// TestTheDSLResourceServesTheWholeReference.
//
// Whole is the decision recorded in mcpDSLResourceHandler, so it is the thing
// asserted: not that the read answered, but that what came back is the entire
// document. A reference truncated somewhere in the middle would still pass every
// test that only checks for a heading, and would leave an agent authoring
// against however much of the grammar happened to fit.
func TestTheDSLResourceServesTheWholeReference(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: flowmcp.DSLResourceURI})
	require.NoError(t, err)
	require.Len(t, result.Contents, 1)

	assert.Equal(t, reference.DSL(), result.Contents[0].Text)
	assert.Equal(t, flowmcp.MarkdownMIME, result.Contents[0].MIMEType)
}

// TestTheCatalogResourceIsTheCatalogTheToolAnswers.
//
// The join, not the halves: the resource is only worth serving if reading it is
// the same as calling flowstate_get_catalog, and "the same" means byte for byte
// through one encoder rather than two renderings that agree today. This is the
// `--output json` rule — one encoder, no second dialect — asserted on the
// surface where a hand-written second copy would have been easiest.
func TestTheCatalogResourceIsTheCatalogTheToolAnswers(t *testing.T) {
	t.Parallel()

	response, err := server.New(nil).GetCatalog(t.Context(), connect.NewRequest(&v1.GetCatalogRequest{}))
	require.NoError(t, err)

	want, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(response.Msg)
	require.NoError(t, err)

	session := connectMCP(t, defaultLocalRunPosture())

	read, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: flowmcp.CatalogResourceURI})
	require.NoError(t, err)
	require.Len(t, read.Contents, 1)
	assert.Equal(t, string(want), read.Contents[0].Text)

	// And against the tool, over the same session, since that is the comparison
	// an agent can actually make.
	called, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("GetCatalog"),
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	require.False(t, called.IsError, "flowstate_get_catalog answered with an error")
	require.Len(t, called.Content, 1)

	text, ok := called.Content[0].(*mcp.TextContent)
	require.True(t, ok, "the tool answered with %T", called.Content[0])
	assert.Equal(t, text.Text, read.Contents[0].Text,
		"the catalog resource and flowstate_get_catalog describe two different engines")

	// It is JSON an agent can address, not prose about tasks.
	var decoded struct {
		Catalog struct {
			Tasks []struct {
				Name string `json:"name"`
			} `json:"tasks"`
			CELLibraries []string `json:"celLibraries"`
		} `json:"catalog"`
	}
	require.NoError(t, json.Unmarshal([]byte(read.Contents[0].Text), &decoded))
	assert.NotEmpty(t, decoded.Catalog.Tasks, "the catalog resource names no tasks")
	assert.NotEmpty(t, decoded.Catalog.CELLibraries, "the catalog resource names no CEL libraries")
}

// TestEveryExampleResourceIsAValidFlowfile.
//
// The example resources are described to a model as working references, which is
// a claim about their content rather than about their delivery. An example that
// no longer validates would be a resource teaching a form `flow validate`
// refuses — the most expensive kind of stale documentation, because the agent
// reading it trusts it more than its own draft.
// examplesNeedingAFile names an example whose `workflow.yaml` alone cannot
// compile, because it names a sibling file by a relative path and this
// resource serves exactly one document's bytes.
//
// A `call:` step is resolved at compile time against the directory of the file
// that names it — see `v1.Call`'s doc on why filesystem access stays at the
// client compiling a Flowfile — and an MCP resource has no such directory: it
// is embedded content read as bytes, the same boundary [parseFlowfileSource]
// documents for submitted source. `call-a-workflow` is the first example this
// applies to, and it is exercised in full — both files, compiled together —
// by the flowfile and examples test suites, which read it from the real
// filesystem the resource embedding is built from.
var examplesNeedingAFile = map[string]bool{
	"call-a-workflow":                true,
	"enterprise-customer-onboarding": true,
	"fan-out-calls":                  true,
	"pinned-call":                    true,
	"progressive-rollout":            true,
}

func TestEveryExampleResourceIsAValidFlowfile(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	names := reference.ExampleNames()
	require.NotEmpty(t, names)

	for _, name := range names {
		if examplesNeedingAFile[name] {
			continue
		}

		result, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{
			URI: flowmcp.ExamplePrefix + name,
		})
		require.NoError(t, err, "reading the %s example", name)
		require.Len(t, result.Contents, 1)

		source := []byte(result.Contents[0].Text)

		_, err = flowfile.Unmarshal(source)
		require.NoError(t, err, "the %s example does not parse", name)

		diagnostics, err := flowfile.ValidateSource(source)
		require.NoError(t, err, "validating the %s example", name)
		assert.Empty(t, diagnostics, "the %s example has diagnostics", name)
	}
}

// TestAnUnknownExampleIsNotFound.
//
// The template matches every name, so the handler is what decides which of them
// exist — and the negative direction is the one that matters here, as it is
// everywhere else in this repository: an agent composing a URI from a guess must
// be told there is no such example, rather than handed an empty document it will
// read as an example that does nothing.
func TestAnUnknownExampleIsNotFound(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	for _, uri := range []string{
		flowmcp.ExamplePrefix + "no-such-example",
		flowmcp.ExamplePrefix + "hello-world.yaml",
		flowmcp.ExamplePrefix + "../DSL.md",
		flowmcp.ExamplePrefix,
	} {
		_, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: uri})
		assert.Error(t, err, "reading %s answered with something", uri)
	}
}

// TestTheResourcesNeverDial.
//
// connectMCP fails the test if anything reaches for the remote client, so this
// asserts the property by reading every resource through that session: the read
// half of the surface answers from this build, with no server, no address and no
// tenant — which is what lets an agent orient itself before anything is stood
// up. The tool half's local three are covered the same way.
func TestTheResourcesNeverDial(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	listed, err := session.ListResources(t.Context(), &mcp.ListResourcesParams{})
	require.NoError(t, err)

	for _, resource := range listed.Resources {
		_, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: resource.URI})
		require.NoError(t, err, "reading %s", resource.URI)
	}
}
