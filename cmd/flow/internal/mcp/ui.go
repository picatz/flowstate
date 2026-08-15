package mcp

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
	// UIExtension is the MCP Apps extension identifier, as the specification
	// defines it. It is the key both a client's and a server's capabilities use.
	UIExtension = "io.modelcontextprotocol/ui"

	// UIAppMIME is the media type an MCP Apps resource is served as. It is a
	// profile of text/html, not a media type of its own, which is what lets a
	// host that does not know the profile still recognise the document.
	UIAppMIME = "text/html;profile=mcp-app"

	// ApprovalCardURI is the card's identity. The URI never changes; the
	// content digest below is what says which revision of it a host is holding.
	ApprovalCardURI = "ui://flowstate/approval-card"

	// UIToolMetaKey is the `_meta` member a tool declares its view under, and
	// UIResourceURIKey the member inside it naming the resource.
	UIToolMetaKey     = "ui"
	UIResourceURIKey  = "resourceUri"
	UIMIMETypesKey    = "mimeTypes"
	UIContentHashKey  = "picatz.github.io/flowstate.contentDigest"
	UICardResourceKey = "approval-card"
)

// uiServerCapabilities is what `flow mcp` declares at initialize.
//
// A non-nil ServerCapabilities replaces the SDK's default, which is the deprecated
// logging capability and nothing else; the tools and resources capabilities are
// still inferred from what was registered, because the SDK only fills a field this
// leaves nil. Nothing on this surface emits a log message, so dropping that default
// removes a claim rather than a feature.
func uiServerCapabilities() *mcp.ServerCapabilities {
	caps := &mcp.ServerCapabilities{}
	caps.AddExtension(UIExtension, map[string]any{
		UIMIMETypesKey: []string{UIAppMIME},
	})

	return caps
}

// ApprovalCardDigest names the exact bytes served, in the one spelling this
// tree uses for that: see [v1.ContentDigest], which is the same function the
// `digest:` pin on a `call:` step is compared against.
//
// The URI is the identity and this is the version. A host caches a resource by
// URI, so "which card am I holding" is a question only the content can answer,
// and answering it with a hash rather than a hand-maintained number means the
// answer cannot be forgotten in a diff that changes the card.
func ApprovalCardDigest() string {
	return v1.ContentDigest([]byte(fragments.ApprovalCard()))
}

// ApprovalCardResourceMeta is the `_meta` served both on the resource's
// declaration and on its contents, so a host comparing the two cannot find them
// disagreeing about which revision it has.
//
// No `ui.csp` and no `ui.permissions`. The restrictive default is the point: the
// card's data arrives on the connection it is already on, its actions leave as
// tool calls on that same connection, and it needs no origin, no camera, no
// clipboard and no network of its own. A relaxation asked for "just in case" is a
// relaxation a host grants.
func ApprovalCardResourceMeta() mcp.Meta {
	return mcp.Meta{UIContentHashKey: ApprovalCardDigest()}
}

// uiToolMeta is the `_meta` a tool carries to say which view renders it.
func uiToolMeta(resourceURI string) mcp.Meta {
	return mcp.Meta{UIToolMetaKey: map[string]any{
		UIResourceURIKey: resourceURI,
		// Deliberately no `visibility`. The default leaves the tool visible to
		// the model, which is what keeps the plain result the primary answer and
		// the card a second rendering of it.
	}}
}

// addUIResources registers the UI half of the resource surface.
//
// Separate from addResources because the two are different kinds of thing.
// Those are documents an agent reads to decide what to do; this is a document a
// *host* renders, and no model should ever be handed its bytes. It is registered
// as a resource because that is how the extension addresses a view, not because
// it is reference material.
func addUIResources(srv *mcp.Server) {
	card := fragments.ApprovalCard()

	srv.AddResource(&mcp.Resource{
		URI:      ApprovalCardURI,
		Name:     UICardResourceKey,
		Title:    "Approval card",
		MIMEType: UIAppMIME,
		Size:     int64(len(card)),
		Meta:     ApprovalCardResourceMeta(),
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
		if req.Params.URI != ApprovalCardURI {
			return nil, mcp.ResourceNotFoundError(req.Params.URI)
		}

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      ApprovalCardURI,
				MIMEType: UIAppMIME,
				Text:     fragments.ApprovalCard(),
				Meta:     ApprovalCardResourceMeta(),
			}},
		}, nil
	}
}
