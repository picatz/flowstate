package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"

	"github.com/picatz/flowstate/cmd/flow/internal/fragments"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// What these tests can and cannot see.
//
// Everything here runs in Go, with no browser and no host, so the line worth
// stating up front is where the assertions stop. They cover the declaration (the
// card is offered on the tool that carries the data, under the URI actually
// registered, as the media type the extension defines), the document (nothing in
// it reaches an origin, and nothing in it writes markup), the bridge (a decision
// becomes a flowstate_signal whose coordinates are read off the connection), and
// the negotiation (a client that declares the extension is told the server
// supports it, and can read the card).
//
// They cannot cover: that a host really sandboxes the frame, that the restrictive
// default Content-Security-Policy is really applied, or that a host asks a human
// before forwarding a tools/call a view makes. Those are host behaviours. They
// are checked by hand against the reference host and two real hosts, and recorded
// in the pull request; nothing in CI claims them.

// TestTheApprovalCardIsDeclaredOnTheToolThatReportsGates holds the declaration to
// the registration site, in both directions.
//
// The failure this exists for is a `_meta` naming a resource nobody serves: a URI
// is a string, the SDK does not check it against the resource table, and a host
// would fetch it, get a not-found, and render nothing with no error anywhere near
// the mistake. So the URI on the tool is compared against the URI the resource is
// registered under, and the media type against the one the extension defines.
func TestTheApprovalCardIsDeclaredOnTheToolThatReportsGates(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	tools, err := session.ListTools(t.Context(), &mcp.ListToolsParams{})
	require.NoError(t, err)

	declared := map[string]string{}
	for _, tool := range tools.Tools {
		meta, ok := tool.Meta[mcpUIToolMetaKey]
		if !ok {
			continue
		}

		fields, ok := meta.(map[string]any)
		require.True(t, ok, "%s declares a `ui` _meta member that is not an object", tool.Name)

		// Never `visibility`, and specifically never ["app"]. A tool hidden from
		// the model but callable from a frame is a card-only capability by
		// another spelling, which is the one thing this surface refuses.
		assert.NotContains(t, fields, "visibility",
			"%s declares a visibility for its view; a card must not be able to reach a tool a "+
				"direct caller cannot", tool.Name)

		uri, ok := fields[mcpUIResourceURIKey].(string)
		require.True(t, ok, "%s declares a view with no %s string", tool.Name, mcpUIResourceURIKey)
		declared[tool.Name] = uri
	}

	// The set, not just the entry: a view added to a second tool without a line
	// in mcpToolViews is a surface nobody reviewed.
	want := map[string]string{}
	for method, uri := range mcpToolViews {
		want[mcpToolName(method)] = uri
	}
	assert.Equal(t, want, declared,
		"the tools carrying a view are not the ones mcpToolViews names")

	require.Equal(t, mcpApprovalCardURI, declared[mcpToolName("Get")],
		"the approval card is declared on some other tool than the one that reports a run and its "+
			"pending gates")

	resources, err := session.ListResources(t.Context(), &mcp.ListResourcesParams{})
	require.NoError(t, err)

	var card *mcp.Resource
	for _, resource := range resources.Resources {
		if resource.URI == mcpApprovalCardURI {
			card = resource
		}
	}
	require.NotNil(t, card,
		"flowstate_get points at %s and no resource is registered under it, so a host would fetch "+
			"a view that does not exist", mcpApprovalCardURI)

	assert.True(t, strings.HasPrefix(card.URI, "ui://"),
		"a view must be addressed under the ui:// scheme the extension reserves")
	assert.Equal(t, mcpUIAppMIME, card.MIMEType,
		"the card is served as a media type no MCP Apps host recognises as a view")
}

// TestTheCardIsVersionedByItsContentDigest.
//
// The URI is the identity and the hash is the version, which only works if the
// hash is of the bytes actually served. Read over the protocol, hashed here, and
// compared to what both the declaration and the contents claim.
func TestTheCardIsVersionedByItsContentDigest(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: mcpApprovalCardURI})
	require.NoError(t, err)
	require.Len(t, result.Contents, 1)

	contents := result.Contents[0]
	digest := v1.ContentDigest([]byte(contents.Text))

	assert.Equal(t, digest, contents.Meta[mcpUIContentHashKey],
		"the digest served beside the card is not the digest of the card")
	assert.True(t, strings.HasPrefix(digest, v1.ContentDigestPrefix),
		"the card is versioned by some other spelling than the one the rest of the tree uses")

	listed, err := session.ListResources(t.Context(), &mcp.ListResourcesParams{})
	require.NoError(t, err)

	for _, resource := range listed.Resources {
		if resource.URI != mcpApprovalCardURI {
			continue
		}

		assert.Equal(t, digest, resource.Meta[mcpUIContentHashKey],
			"the declaration and the contents disagree about which revision of the card this is")
	}
}

// TestAHostNegotiatingTheExtensionIsToldTheServerSupportsIt is the full walk: a
// real client, over an in-memory transport, declaring the extension at initialize
// and then doing what a host does with the answer.
func TestAHostNegotiatingTheExtensionIsToldTheServerSupportsIt(t *testing.T) {
	t.Parallel()

	session := connectMCPAsUIHost(t)

	caps := session.InitializeResult().Capabilities
	require.NotNil(t, caps)

	settings, ok := caps.Extensions[mcpUIExtension].(map[string]any)
	require.True(t, ok,
		"a host that declared %s was not told this server serves views at all, so it would never "+
			"look for one", mcpUIExtension)

	mimes, ok := settings[mcpUIMIMETypesKey].([]any)
	require.True(t, ok, "the extension is declared without the required %s setting", mcpUIMIMETypesKey)
	assert.Equal(t, []any{mcpUIAppMIME}, mimes)

	// Registering the tools and resources is not conditional on any of that: the
	// same walk has to work for a host that never mentioned the extension.
	tools, err := session.ListTools(t.Context(), &mcp.ListToolsParams{})
	require.NoError(t, err)

	var get *mcp.Tool
	for _, tool := range tools.Tools {
		if tool.Name == mcpToolName("Get") {
			get = tool
		}
	}
	require.NotNil(t, get)

	view, ok := get.Meta[mcpUIToolMetaKey].(map[string]any)
	require.True(t, ok)
	uri, ok := view[mcpUIResourceURIKey].(string)
	require.True(t, ok)

	read, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: uri})
	require.NoError(t, err, "the URI a host reads off the tool could not be fetched")
	require.Len(t, read.Contents, 1)
	assert.Equal(t, mcpUIAppMIME, read.Contents[0].MIMEType)
	assert.Equal(t, fragments.ApprovalCard(), read.Contents[0].Text,
		"the bytes served are not the bytes compiled in")
}

// TestTheToolResultStandsAloneWithoutTheExtension is the doctrine, asserted.
//
// A host without MCP Apps, and a model reading the result, must both get the run,
// its gates, and the exact flowstate_signal arguments that would approve one. The
// first two are the response document; the third is the tool's own description,
// which is where a model with no card looks.
func TestTheToolResultStandsAloneWithoutTheExtension(t *testing.T) {
	t.Parallel()

	description := mcpDescriptions["Get"]

	for _, needed := range []string{
		"flowstate_signal",
		"signalName",
		"workflowId",
		"payload.namedValues.approved",
		"boolValue",
	} {
		assert.Contains(t, description, needed,
			"flowstate_get's description does not say how to approve a gate without a card, so a "+
				"host that ignores the view leaves a model guessing")
	}

	// The identity a signal actually travels as, said out loud, because the card
	// makes it look like a person clicked.
	assert.Contains(t, description, "process's own identity",
		"the tool does not say that over stdio a signal is delivered as this process rather than "+
			"as whoever asked for it")
}

// TestTheCardHasNoExternalOrigin parses the document and refuses every way one
// could acquire one.
//
// Parsed rather than grepped for the tags, because a tag is a tree node and a
// regular expression over HTML finds what it was told to look for rather than
// what a browser will load. The textual half below covers what parsing cannot
// see, which is the script's own contents.
func TestTheCardHasNoExternalOrigin(t *testing.T) {
	t.Parallel()

	source := fragments.ApprovalCard()

	doc, err := html.Parse(strings.NewReader(source))
	require.NoError(t, err, "the embedded card does not parse as HTML")

	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode {
			attrs := map[string]string{}
			for _, attr := range node.Attr {
				attrs[attr.Key] = attr.Val
			}

			switch node.Data {
			case "script":
				assert.NotContains(t, attrs, "src",
					"the card loads a script from somewhere; it is meant to be readable in the diff")
			case "link":
				assert.NotContains(t, attrs, "href",
					"the card loads a stylesheet from somewhere")
			case "iframe", "frame", "object", "embed", "applet":
				assert.Fail(t, "the card embeds a nested browsing context",
					"a <%s> in a view is a second document nobody reviewed", node.Data)
			case "img", "image", "source", "video", "audio", "track":
				for _, key := range []string{"src", "srcset", "poster"} {
					if value, ok := attrs[key]; ok {
						assert.False(t, looksAbsolute(value),
							"<%s %s> names an absolute origin", node.Data, key)
					}
				}
			case "form":
				assert.Fail(t, "the card contains a form",
					"a form navigates; every action here is a tool call")
			case "base":
				assert.Fail(t, "the card sets a <base>",
					"a base URI changes what every relative reference resolves to")
			}

			for key, value := range attrs {
				if strings.HasPrefix(key, "on") {
					assert.Fail(t, "the card carries an inline event handler",
						"%s=%q is script in an attribute, which is the one place a "+
							"Content-Security-Policy cannot be reasoned about from the diff",
						key, value)
				}
			}
		}

		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)

	// What the tree cannot show: the script body. Every one of these is a way to
	// reach an origin or to hand a string to a parser that treats it as code.
	code := cardCode(t)
	for _, forbidden := range []string{
		"fetch(",
		"XMLHttpRequest",
		"WebSocket",
		"EventSource",
		"navigator.sendBeacon",
		"import(",
		"importScripts",
		"eval(",
		"new Function",
		"document.write",
		"http://",
		"https://",
		"//cdn.",
	} {
		assert.NotContains(t, code, forbidden,
			"the card contains %q, so it can reach or execute something that is not in this diff",
			forbidden)
	}
}

// TestTheCardNeverWritesMarkup is the injection half.
//
// A run's prompt, its step ids and its starter are values a workflow and its
// callers chose. Rendering one through innerHTML would make markup one of the
// things they get to choose, inside a frame that is holding an approve button.
// The rule is absolute rather than careful: no API that parses a string as HTML
// appears in the document at all.
func TestTheCardNeverWritesMarkup(t *testing.T) {
	t.Parallel()

	code := cardCode(t)

	for _, forbidden := range []string{
		"innerHTML",
		"outerHTML",
		"insertAdjacentHTML",
		"createContextualFragment",
		"srcdoc",
	} {
		assert.NotContains(t, code, forbidden,
			"the card uses %q; untrusted run data reaches the document through textContent only",
			forbidden)
	}

	assert.Contains(t, code, "textContent",
		"nothing in the card writes text, so either it renders nothing or it found another way")
}

// TestTheCardDeclaresNoRelaxationAndNoPermission.
//
// The restrictive default is the point. The card's data arrives on the connection
// it is already on and its actions leave as tool calls on that same connection,
// so it needs no origin, no camera and no clipboard. A relaxation asked for "just
// in case" is a relaxation a host grants.
func TestTheCardDeclaresNoRelaxationAndNoPermission(t *testing.T) {
	t.Parallel()

	meta := mcpApprovalCardResourceMeta()

	assert.NotContains(t, meta, "ui",
		"the card's resource _meta carries a `ui` member; v1 declares neither csp nor permissions")

	encoded, err := json.Marshal(meta)
	require.NoError(t, err)

	for _, forbidden := range []string{"csp", "permissions", "connectDomains", "resourceDomains"} {
		assert.NotContains(t, string(encoded), forbidden,
			"the card asks a host for %q", forbidden)
	}
}

// The bridge: the one function where a card action becomes a tool call.

// bridgeRegion returns the source of that function, delimited in the document so
// a test can hold exactly it rather than the whole script.
func bridgeRegion(t *testing.T) string {
	t.Helper()

	return regionOf(t, fragments.ApprovalCard(), "BRIDGE")
}

func regionOf(t *testing.T, source, name string) string {
	t.Helper()

	_, after, ok := strings.Cut(source, "// "+name+"-BEGIN")
	require.True(t, ok, "the card has no %s-BEGIN marker", name)

	body, _, ok := strings.Cut(after, "// "+name+"-END")
	require.True(t, ok, "the card has no %s-END marker", name)

	return body
}

// withoutComments strips the line comments from a region, so a scan for string
// literals reads the code rather than the prose explaining it.
//
// Only a comment that is the whole of its line, which is how every comment in the
// card is written. Cutting at the first `//` anywhere would cut inside a string
// literal, and the literals are what several of these tests are looking at.
func withoutComments(region string) string {
	var kept []string
	for _, line := range strings.Split(region, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}

		kept = append(kept, line)
	}

	return strings.Join(kept, "\n")
}

// cardCode is the card with its prose removed: no HTML comments, no whole-line
// script comments.
//
// The scans below forbid the *names* of dangerous APIs, and the document explains
// itself by naming several of them in the comment that says it does not use them.
// Reading the code is the honest scan; a document that had to avoid saying
// "innerHTML" in order to pass would be a worse document.
func cardCode(t *testing.T) string {
	t.Helper()

	source := fragments.ApprovalCard()
	for {
		open := strings.Index(source, "<!--")
		if open < 0 {
			break
		}

		end := strings.Index(source[open:], "-->")
		require.Positive(t, end, "the card has an unterminated HTML comment")
		source = source[:open] + source[open+end+len("-->"):]
	}

	return withoutComments(source)
}

var jsStringLiteral = regexp.MustCompile(`"([^"\\]|\\.)*"`)

// TestTheCardTakesItsCoordinatesFromTheConnection is the assertion the whole
// design turns on, and the one a mutation has to be able to fail.
//
// A card that carried a run id of its own would sign whatever run it was built
// with rather than the run being displayed, and it would do it convincingly: the
// message is well-formed, the server accepts it, and the wrong workload gets
// approved. So the coordinates in the outgoing call are required to be member
// reads off the gate the binding produced, and the region is required to contain
// no string literal beyond the three constants the dialect needs.
func TestTheCardTakesItsCoordinatesFromTheConnection(t *testing.T) {
	t.Parallel()

	code := withoutComments(bridgeRegion(t))

	for field, expression := range map[string]string{
		"workflowId": "gate.workflowId",
		"name":       "gate.signalName",
	} {
		bound := regexp.MustCompile(field + `:\s*` + regexp.QuoteMeta(expression) + `\b`)
		assert.Regexp(t, bound, code,
			"the outgoing signal's %s is not read off the gate the tool result bound; a coordinate "+
				"the card supplies itself addresses whatever run the card was built with", field)
	}

	// Anything else quoted in here is a candidate coordinate. Three constants are
	// legitimate: the JSON-RPC version, the method, and the name of the tool the
	// server already exposes to the model.
	allowed := map[string]bool{
		`"2.0"`:              true,
		`"tools/call"`:       true,
		`"flowstate_signal"`: true,
	}
	for _, literal := range jsStringLiteral.FindAllString(code, -1) {
		assert.True(t, allowed[literal],
			"the bridge contains the string literal %s; a coordinate written down in this file is "+
				"one the connection did not supply", literal)
	}
}

// TestTheCardBindsItsGatesFromTheToolResultNotification is the other half: the
// coordinates the bridge reads have to come from somewhere, and the only
// permitted somewhere is the notification the host delivers on this connection.
func TestTheCardBindsItsGatesFromTheToolResultNotification(t *testing.T) {
	t.Parallel()

	code := cardCode(t)
	binding := withoutComments(regionOf(t, fragments.ApprovalCard(), "BINDING"))

	assert.Contains(t, code, `"ui/notifications/tool-result"`,
		"the card never names the notification that carries the tool's result, so nothing on this "+
			"connection can reach it")
	assert.Regexp(t, `data\.method === TOOL_RESULT`, code,
		"the card does not dispatch the tool-result notification")
	assert.Regexp(t, `applyToolResult\(data\.params\)`, code,
		"the tool-result notification is received and not bound; the card would render, and sign, "+
			"whatever it was holding")

	// The binding reads the response document, and specifically the fields a gate
	// is made of. A binding that stopped reading pendingWaits would leave the card
	// with a run and no gates, which renders as "nothing to approve" rather than
	// as an error.
	for _, field := range []string{"workflowId", "runId", "pendingWaits", "signalName", "prompt", "policed", "deadline"} {
		assert.Contains(t, binding, field,
			"the binding does not read %s off the tool result", field)
	}

	assert.Regexp(t, `workflowId:\s*run\.workflowId`, binding,
		"a gate's workflow is not the one the tool result reported for the run it belongs to")
}

// TestTheCardsCallsAreValidSignalRequests takes the golden messages the card
// builds and puts them through the schema the server enforces.
//
// This is what keeps the golden honest. A JSON file beside a test proves nothing
// on its own; unmarshalled strictly into [v1.SignalRequest] and validated, it
// proves the shape the card sends is a request this service accepts, and it fails
// the day a field is renamed in the proto.
func TestTheCardsCallsAreValidSignalRequests(t *testing.T) {
	t.Parallel()

	for _, testcase := range []struct {
		file     string
		approved bool
	}{
		{file: "approve.json", approved: true},
		{file: "reject.json", approved: false},
	} {
		t.Run(testcase.file, func(t *testing.T) {
			t.Parallel()

			raw, err := os.ReadFile(filepath.Join("testdata", "approval-card", testcase.file))
			require.NoError(t, err)

			var message struct {
				JSONRPC string `json:"jsonrpc"`
				Method  string `json:"method"`
				Params  struct {
					Name      string          `json:"name"`
					Arguments json.RawMessage `json:"arguments"`
				} `json:"params"`
			}
			require.NoError(t, json.Unmarshal(raw, &message))

			assert.Equal(t, "2.0", message.JSONRPC)
			assert.Equal(t, "tools/call", message.Method)
			assert.Equal(t, mcpToolName("Signal"), message.Params.Name,
				"the card calls a tool other than the one that delivers a signal")

			// DiscardUnknown stays false, exactly as [mcpHandler] leaves it: the
			// tool refuses a field the schema does not have, so a golden carrying
			// one would be a message the server rejects.
			var request v1.SignalRequest
			require.NoError(t, protojson.Unmarshal(message.Params.Arguments, &request),
				"the card's arguments are not a SignalRequest this surface would accept")
			require.NoError(t, v1.Validate(&request),
				"the card's arguments do not satisfy the schema's own rules")

			assert.NotEmpty(t, request.GetWorkflowId())
			assert.NotEmpty(t, request.GetName())
			assert.Empty(t, request.GetRunId(),
				"the card pins its signal to one run; a gate answered after a Continue-As-New would "+
					"be refused, and the schema says an approver is approving the workload")

			approved := request.GetPayload().GetNamedValues()["approved"]
			require.NotNil(t, approved, "the payload does not say whether this was an approval")
			assert.Equal(t, testcase.approved, approved.GetLiteral().GetBoolValue(),
				"approve and reject do not differ in the payload they send")
		})
	}
}

// TestTheGoldenCallsAndTheCardAgree keeps the two from drifting apart.
//
// The goldens are read by Go and the message is built by JavaScript, and nothing
// executes both. What can be checked is that every name and constant the golden
// depends on is present in the one function that builds it, so a rename on either
// side is a failure rather than a silent divergence.
func TestTheGoldenCallsAndTheCardAgree(t *testing.T) {
	t.Parallel()

	code := withoutComments(bridgeRegion(t))

	raw, err := os.ReadFile(filepath.Join("testdata", "approval-card", "approve.json"))
	require.NoError(t, err)

	var message any
	require.NoError(t, json.Unmarshal(raw, &message))

	var names []string
	var collect func(any)
	collect = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			for key, child := range value {
				names = append(names, key)
				collect(child)
			}
		case []any:
			for _, child := range value {
				collect(child)
			}
		}
	}
	collect(message)

	require.NotEmpty(t, names)
	for _, name := range names {
		assert.Contains(t, code, name,
			"the golden message has a %q the card's bridge never writes", name)
	}

	for _, constant := range []string{`"2.0"`, `"tools/call"`, `"flowstate_signal"`} {
		assert.Contains(t, code, constant,
			"the golden message carries %s and the bridge does not", constant)
	}
}

// TestTheCardSpeaksTheHandshake.
//
// The lifecycle is three messages and getting any of them wrong means a host that
// never sends the data: ui/initialize goes out, ui/notifications/initialized
// follows the reply, and the tool-result listener is installed before either, so
// a host that sends the result promptly does not lose it.
func TestTheCardSpeaksTheHandshake(t *testing.T) {
	t.Parallel()

	code := cardCode(t)

	for _, method := range []string{
		"ui/initialize",
		"ui/notifications/initialized",
		"ui/notifications/tool-result",
	} {
		assert.Contains(t, code, `"`+method+`"`,
			"the card does not speak %s, so a host and it never agree that it is ready", method)
	}

	listener := strings.Index(code, `window.addEventListener("message"`)
	handshake := strings.Index(code, "request(INITIALIZE")
	require.Positive(t, listener, "the card installs no message listener")
	require.Positive(t, handshake, "the card never initializes")
	assert.Less(t, listener, handshake,
		"the card initializes before it listens, so a host that answers immediately is answering "+
			"nobody")

	assert.Contains(t, code, "event.source !== window.parent",
		"the card accepts messages from senders that are not its host")
}

// connectMCPAsUIHost is [connectMCP] for a client that declares the extension.
//
// Separate rather than folded in, because the point of having both is that the
// surface must be identical either way: every other test in this package connects
// without declaring anything and reads the same tools and the same resources.
func connectMCPAsUIHost(t *testing.T) *mcp.ClientSession {
	t.Helper()

	srv := newMCPServer("test")

	addMCPCapabilities(srv, server.New(nil), func() flowstatev1connect.WorkflowServiceClient {
		t.Error("a local tool dialed the server")

		return nil
	}, defaultLocalRunPosture())

	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	go func() { _ = srv.Run(t.Context(), serverTransport) }()

	capabilities := &mcp.ClientCapabilities{}
	capabilities.AddExtension(mcpUIExtension, map[string]any{
		mcpUIMIMETypesKey: []string{mcpUIAppMIME},
	})

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-ui-host", Version: "test"},
		&mcp.ClientOptions{Capabilities: capabilities},
	)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	return session
}

// looksAbsolute reports whether a URL reference names an origin of its own.
func looksAbsolute(reference string) bool {
	trimmed := strings.TrimSpace(reference)

	return strings.HasPrefix(trimmed, "//") || strings.Contains(trimmed, "://")
}
