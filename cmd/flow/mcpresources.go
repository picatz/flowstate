package main

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"

	"github.com/picatz/flowstate/cmd/flow/internal/reference"
)

// What an agent needs to read, as opposed to call.
//
// The tools on this surface are verbs — validate this, compile that, run the
// other — and a verb is the wrong shape for "what is the language". An agent
// that has to spend a tool call, a round trip, and a slice of its context window
// to learn the vocabulary will guess instead, and a guessed Flowfile is a
// diagnostic loop the model pays for three times.
//
// So the reference material is resources: read-only, addressed by URI, listed
// before anything is called, and cacheable by the client. Nothing here mutates,
// nothing here dials a server, and nothing here is affected by the flags the
// process was started with — a resource is a fact about this *build*, which is
// exactly why the answers are compiled in rather than read off a disk that may
// hold a different checkout, or no checkout at all. See the reference package
// for that tradeoff written out.
//
// The three are one triple deliberately: the language (docs/dsl), the vocabulary
// this build can actually execute (catalog/tasks), and working files that use
// both (docs/examples/<name>). Prose alone lets an agent write a task this binary
// does not have; a catalog alone gives it the names and not the grammar.

const (
	// mcpDSLResourceURI is the Flowfile language reference.
	mcpDSLResourceURI = "flowstate://docs/dsl"

	// mcpCatalogResourceURI is what this build can execute.
	mcpCatalogResourceURI = "flowstate://catalog/tasks"

	// mcpExamplePrefix is where a single example is addressed, and
	// mcpExampleTemplate is the RFC 6570 template a client expands to reach one.
	mcpExamplePrefix   = "flowstate://docs/examples/"
	mcpExampleTemplate = mcpExamplePrefix + "{name}"
)

// MIME types, named once so a resource's declaration and its contents cannot
// disagree about what was served.
const (
	mcpMarkdownMIME = "text/markdown"
	mcpJSONMIME     = "application/json"
	mcpYAMLMIME     = "application/yaml"
)

// addMCPResources registers the read-only half of the surface.
//
// local is the same in-process server the Validate, Compile and GetCatalog tools
// answer from, passed in rather than constructed here for the reason this
// repository states most often: two constructions are two catalogs, and the one
// an agent reads would eventually stop being the one it is validated against.
func addMCPResources(srv *mcp.Server, local *server.FlowstateServer) {
	srv.AddResource(&mcp.Resource{
		URI:      mcpDSLResourceURI,
		Name:     "flowfile-dsl-reference",
		Title:    "Flowfile DSL reference",
		MIMEType: mcpMarkdownMIME,
		Size:     int64(len(reference.DSL())),
		Description: "The complete Flowfile language reference: the grammar, every step kind, " +
			"expression scoping and the CEL roots in scope where, retries, timeouts, loops, " +
			"parallel blocks, waits, secrets, and the reasoning behind each rule. Read this before " +
			"authoring a workflow. It is compiled into this binary, so it describes the engine you " +
			"are about to call rather than whatever is checked out nearby.",
	}, mcpDSLResourceHandler())

	srv.AddResource(&mcp.Resource{
		URI:      mcpCatalogResourceURI,
		Name:     "task-catalog",
		Title:    "Task and function catalog",
		MIMEType: mcpJSONMIME,
		Description: "What this build can execute, as the JSON of a GetCatalogResponse: every task " +
			"with its typed inputs and outputs, and every CEL function an expression may call. The " +
			"same answer flowstate_get_catalog gives, without spending a tool call. Read it as a " +
			"resource when you are about to author, and call the tool when you need it mid-reasoning.",
	}, mcpCatalogResourceHandler(local))

	// The template is what tells a client the *shape* of an example URI; the
	// concrete resources below are what let it discover the names without
	// guessing. Both, because a template alone enumerates nothing and a listing
	// alone says nothing about how it was addressed.
	srv.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: mcpExampleTemplate,
		Name:        "flowfile-example",
		Title:       "Example Flowfile",
		MIMEType:    mcpYAMLMIME,
		Description: "One example workflow from the repository's examples/ directory, by its " +
			"directory name: flowstate://docs/examples/hello-world, flowstate://docs/examples/" +
			"http-json. Each is a complete Flowfile that CI runs, so it is a working reference " +
			"rather than a fragment. The names are listed as resources of their own.",
	}, mcpExampleResourceHandler())

	for _, name := range reference.ExampleNames() {
		content, ok := reference.Example(name)
		if !ok {
			// Unreachable: the name came from the same embedded filesystem.
			continue
		}

		srv.AddResource(&mcp.Resource{
			URI:      mcpExamplePrefix + name,
			Name:     "example-" + name,
			Title:    "Example: " + name,
			MIMEType: mcpYAMLMIME,
			Size:     int64(len(content)),
			Description: fmt.Sprintf("The %s example: a complete, CI-run Flowfile you can read as a "+
				"reference or adapt. Execute it as-is with flowstate_run_local to see what it does.", name),
		}, mcpExampleResourceHandler())
	}
}

// mcpDSLResourceHandler serves the language reference, whole.
//
// Whole because there is nowhere else for it to go. MCP pages a resource *list*
// — the SDK's listResources runs through paginateList — and has no notion of a
// partial read: ReadResource takes a URI and returns contents, with no offset,
// no range, and no cursor. A resource template is addressing, not chunking; it
// expands a URI into a resource, and each of those still arrives entire.
//
// So the choices were to serve the document, to serve it cut into sections
// behind a template, or to serve a summary. Sections would mean inventing an
// index no client asked for and no heading structure guarantees — and an agent
// that fetched the wrong three sections would author against two-thirds of the
// grammar, which is the failure mode the reference exists to prevent. It is
// ~110 KB, read once, and clients cache resources. Serving it whole is the
// pragmatic answer and this is it being said plainly.
func mcpDSLResourceHandler() mcp.ResourceHandler {
	return func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      req.Params.URI,
				MIMEType: mcpMarkdownMIME,
				Text:     reference.DSL(),
			}},
		}, nil
	}
}

// mcpCatalogResourceHandler serves the catalog as the RPC's own answer.
//
// Through the same handler and the same encoder flowstate_get_catalog uses, so
// the resource and the tool cannot describe two different engines: this is the
// `--output json` rule — one encoder, no second dialect — applied to a surface
// where the second copy would be the more tempting one to hand-write.
//
// Computed per read rather than at registration, because a build's catalog is
// not fixed until it is asked for: a plugin registers tasks at start-up, and a
// snapshot taken while wiring resources would be the catalog from before that.
func mcpCatalogResourceHandler(local *server.FlowstateServer) mcp.ResourceHandler {
	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		resp, err := local.GetCatalog(ctx, connect.NewRequest(&v1.GetCatalogRequest{}))
		if err != nil {
			return nil, fmt.Errorf("reading the task catalog: %w", err)
		}

		encoded, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(resp.Msg)
		if err != nil {
			return nil, fmt.Errorf("rendering the task catalog: %w", err)
		}

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      req.Params.URI,
				MIMEType: mcpJSONMIME,
				Text:     string(encoded),
			}},
		}, nil
	}
}

// mcpExampleResourceHandler serves one example, whichever way it was addressed.
//
// One handler for the template and for every concrete resource, because they are
// the same read: the URI is the argument either way, and two handlers would be
// two answers for one URI the day one of them was edited.
//
// A name that is not an example answers with the protocol's own not-found rather
// than an empty document, since a client that cannot tell "no such example" from
// "an example that is empty" will report the second.
func mcpExampleResourceHandler() mcp.ResourceHandler {
	return func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		uri := req.Params.URI

		name, ok := strings.CutPrefix(uri, mcpExamplePrefix)
		if !ok {
			return nil, mcp.ResourceNotFoundError(uri)
		}

		content, ok := reference.Example(name)
		if !ok {
			return nil, mcp.ResourceNotFoundError(uri)
		}

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      uri,
				MIMEType: mcpYAMLMIME,
				Text:     content,
			}},
		}, nil
	}
}
