package main

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"

	"github.com/picatz/flowstate/cmd/flow/internal/fragments"
)

// The MCP Apps surface: one UI resource, declared on one tool, carrying no
// authority of its own.
//
// # What is served, and to whom
//
// A host that has negotiated the io.modelcontextprotocol/ui extension reads the
// resource named by a tool's `_meta.ui.resourceUri` and renders it in a sandboxed
// frame, handing it the tool's result over postMessage. A host that has not
// negotiated it sees a `_meta` key it does not know, ignores it, and renders the
// text result. Both are served the same thing, because registration is not
// conditional on what a client said at initialize.
//
// That is a deliberate reading of the specification's advice, which is that a
// server SHOULD check client capabilities before registering UI-enabled tools.
// Registering two variants of one tool would mean the answer an agent gets
// depends on a capability negotiation it never sees, and the failure it produces
// is the worst kind: a model told a tool does not exist because the host it
// happens to be behind did not ask for HTML. The `_meta` key is additive and
// ignorable by construction, so the honest move is to serve one tool.
//
// # The card is not a permission
//
// The specification does not require a host to obtain a user's consent before
// forwarding a tools/call a view makes. So the card is a *renderer* of a decision
// and never the check on it. `flowstate_signal` authorizes a delivery exactly as
// it does for `flow signal` from a terminal: against the run's declared signal
// policy, on the identity the connection carries. There is no tool on this
// surface a card can call and a caller cannot, and nothing here uses
// `visibility: ["app"]`, which would hide a tool from the model while leaving it
// callable from a frame - a card-only capability under another name.
//
// The consequence a v1 has to state out loud is whose identity a signal travels
// as. Over stdio, `flow mcp` is a process the operator started, and the server
// sees that process. It does not see the human who clicked. The tool description
// says so; an attested approver waits on the remote MCP surface.

const (
	// mcpUIExtension is the MCP Apps extension identifier, as the specification
	// defines it. It is the key both a client's and a server's capabilities use.
	mcpUIExtension = "io.modelcontextprotocol/ui"

	// mcpUIAppMIME is the media type an MCP Apps resource is served as. It is a
	// profile of text/html, not a media type of its own, which is what lets a
	// host that does not know the profile still recognise the document.
	mcpUIAppMIME = "text/html;profile=mcp-app"

	// mcpApprovalCardURI is the card's identity. The URI never changes; the
	// content digest below is what says which revision of it a host is holding.
	mcpApprovalCardURI = "ui://flowstate/approval-card"

	// mcpUIToolMetaKey is the `_meta` member a tool declares its view under, and
	// mcpUIResourceURIKey the member inside it naming the resource.
	mcpUIToolMetaKey     = "ui"
	mcpUIResourceURIKey  = "resourceUri"
	mcpUIMIMETypesKey    = "mimeTypes"
	mcpUIContentHashKey  = "picatz.github.io/flowstate.contentDigest"
	mcpUICardResourceKey = "approval-card"
)

// mcpUIServerCapabilities is what `flow mcp` declares at initialize.
//
// A non-nil ServerCapabilities replaces the SDK's default, which is the deprecated
// logging capability and nothing else; the tools and resources capabilities are
// still inferred from what was registered, because the SDK only fills a field this
// leaves nil. Nothing on this surface emits a log message, so dropping that default
// removes a claim rather than a feature.
func mcpUIServerCapabilities() *mcp.ServerCapabilities {
	caps := &mcp.ServerCapabilities{}
	caps.AddExtension(mcpUIExtension, map[string]any{
		mcpUIMIMETypesKey: []string{mcpUIAppMIME},
	})

	return caps
}

// mcpApprovalCardDigest names the exact bytes served, in the one spelling this
// tree uses for that: see [v1.ContentDigest], which is the same function the
// `digest:` pin on a `call:` step is compared against.
//
// The URI is the identity and this is the version. A host caches a resource by
// URI, so "which card am I holding" is a question only the content can answer,
// and answering it with a hash rather than a hand-maintained number means the
// answer cannot be forgotten in a diff that changes the card.
func mcpApprovalCardDigest() string {
	return v1.ContentDigest([]byte(fragments.ApprovalCard()))
}

// mcpApprovalCardResourceMeta is the `_meta` served both on the resource's
// declaration and on its contents, so a host comparing the two cannot find them
// disagreeing about which revision it has.
//
// No `ui.csp` and no `ui.permissions`. The restrictive default is the point: the
// card's data arrives on the connection it is already on, its actions leave as
// tool calls on that same connection, and it needs no origin, no camera, no
// clipboard and no network of its own. A relaxation asked for "just in case" is a
// relaxation a host grants.
func mcpApprovalCardResourceMeta() mcp.Meta {
	return mcp.Meta{mcpUIContentHashKey: mcpApprovalCardDigest()}
}

// mcpUIToolMeta is the `_meta` a tool carries to say which view renders it.
func mcpUIToolMeta(resourceURI string) mcp.Meta {
	return mcp.Meta{mcpUIToolMetaKey: map[string]any{
		mcpUIResourceURIKey: resourceURI,
		// Deliberately no `visibility`. The default leaves the tool visible to
		// the model, which is what keeps the plain result the primary answer and
		// the card a second rendering of it.
	}}
}

// addMCPUIResources registers the UI half of the resource surface.
//
// Separate from addMCPResources because the two are different kinds of thing.
// Those are documents an agent reads to decide what to do; this is a document a
// *host* renders, and no model should ever be handed its bytes. It is registered
// as a resource because that is how the extension addresses a view, not because
// it is reference material.
func addMCPUIResources(srv *mcp.Server) {
	card := fragments.ApprovalCard()

	srv.AddResource(&mcp.Resource{
		URI:      mcpApprovalCardURI,
		Name:     mcpUICardResourceKey,
		Title:    "Approval card",
		MIMEType: mcpUIAppMIME,
		Size:     int64(len(card)),
		Meta:     mcpApprovalCardResourceMeta(),
		Description: "The interactive approval card an MCP Apps host renders for flowstate_get: the " +
			"gates a run is parked on, each with the question it is asking, and an approve and a " +
			"reject that travel as a flowstate_signal call the server authorizes exactly as it " +
			"authorizes `flow signal`. It is a rendering of the tool's own result and carries no " +
			"authority of its own. Not reference material: a model has no reason to read its bytes.",
	}, mcpApprovalCardHandler())
}

// mcpApprovalCardHandler serves the card.
//
// Whole, and from the embedded bytes rather than from disk, for the reason the
// fragments package doc gives: what this binary serves has to be what this binary
// was built from.
func mcpApprovalCardHandler() mcp.ResourceHandler {
	return func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		if req.Params.URI != mcpApprovalCardURI {
			return nil, mcp.ResourceNotFoundError(req.Params.URI)
		}

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      mcpApprovalCardURI,
				MIMEType: mcpUIAppMIME,
				Text:     fragments.ApprovalCard(),
				Meta:     mcpApprovalCardResourceMeta(),
			}},
		}, nil
	}
}
