package lsp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A FlowfileServer answers Language Server Protocol requests about Flowfiles.
//
// The zero value is ready to use and safe for concurrent use, which matters
// because the connection serves this handler through [NewHandler], one
// goroutine per message: an editor will have a hover in flight while a
// keystroke is being processed.
type FlowfileServer struct {
	// Logger receives protocol-level messages. When nil, [slog.Default] is used,
	// which the command routes to standard error — standard output carries the
	// JSON-RPC stream and must not be written to by anything else.
	//
	// Document contents are never logged. A Flowfile input can hold a bearer
	// token or a webhook URL, and a language server's log is not a place those
	// should end up. Positions, URIs, and counts are logged instead.
	Logger *slog.Logger

	// Tasks is the registry every answer is derived from — task names in
	// completion, signatures on hover, which step keys the outline treats as
	// work. When nil, [v1.DefaultRegistry] is used, which is the built-in task
	// set plus whatever this process registered into it at start-up.
	//
	// It is a field rather than a package-level lookup because what a server
	// knows about is a property of how it was launched. `flow lsp --plugin-dir`
	// opens a plugin host, registers what it found, and hands the registry here;
	// without the flag this is the built-in set and a plugin's task is unknown,
	// which is the same answer `flow validate` gives in a process that launched
	// nothing.
	//
	// The registry is read, never written. It is set before the first request
	// and shared with whatever else reads it, so a server must not register into
	// it.
	Tasks *v1.Registry

	docs documentStore

	// testDiagnosticsBySource retains each open test document's contribution
	// to every URI it diagnoses. A suite may diagnose its included
	// testdefaults.yaml; aggregation prevents one clean suite from clearing a
	// defaults problem another open suite still reports, and lets closing the
	// source retract only its own contribution.
	testDiagnosticsMu       sync.Mutex
	testDiagnosticsBySource map[lsp.DocumentURI]map[lsp.DocumentURI][]lsp.Diagnostic
	testSourcesByTarget     map[lsp.DocumentURI]map[lsp.DocumentURI]bool
	testDefaultsBySuite     map[lsp.DocumentURI]lsp.DocumentURI
	testSuitesByDefaults    map[lsp.DocumentURI]map[lsp.DocumentURI]bool
	testOverflowsByDefaults map[lsp.DocumentURI]map[lsp.DocumentURI]bool
	testOverflowBySuite     map[lsp.DocumentURI]lsp.DocumentURI

	// initialized and shuttingDown track the protocol lifecycle. The spec
	// requires rejecting requests before initialize and after shutdown, and an
	// editor that reuses a connection depends on that being enforced.
	initialized  atomic.Bool
	shuttingDown atomic.Bool
}

// Handle implements [jsonrpc2.Handler].
//
// It replies to requests, never to notifications — a notification carries no id,
// and replying to one is a protocol violation that some clients treat as a fatal
// error — and it converts a panic into a JSON-RPC error so that one malformed
// document cannot take the server down with it.
func (s *FlowfileServer) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// The stack is logged rather than returned: it names internal symbols
		// that are no use to an editor, and the request must still be answered
		// so the client is not left waiting for a reply that never comes.
		s.logger().Error("recovered from panic handling request",
			"method", req.Method, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		if !req.Notif {
			_ = conn.ReplyWithError(ctx, req.ID, &jsonrpc2.Error{
				Code:    jsonrpc2.CodeInternalError,
				Message: fmt.Sprintf("internal error handling %s", req.Method),
			})
		}
	}()

	result, err := s.dispatch(ctx, conn, req)
	if req.Notif {
		if err != nil {
			s.logger().Warn("notification failed", "method", req.Method, "error", err)
		}
		return
	}
	if err != nil {
		_ = conn.ReplyWithError(ctx, req.ID, asRPCError(err))
		return
	}
	if err := conn.Reply(ctx, req.ID, result); err != nil {
		s.logger().Warn("reply failed", "method", req.Method, "error", err)
	}
}

// codeServerNotInitialized is the LSP-specific JSON-RPC error code for a request
// that arrives before initialize. It is not in the JSON-RPC spec, so jsonrpc2 does
// not define it.
const codeServerNotInitialized int64 = -32002

// dispatch routes one message and returns the result to reply with, if any.
func (s *FlowfileServer) dispatch(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (any, error) {
	// The lifecycle is enforced rather than assumed. An editor that reuses a
	// connection — or a misconfigured client that starts asking for completions
	// before handshaking — should get a clear protocol error instead of results
	// computed from a state the server was never told about.
	switch req.Method {
	case "initialize", "exit":
	default:
		switch {
		case !s.initialized.Load():
			if req.Notif {
				return nil, nil
			}
			return nil, &jsonrpc2.Error{
				Code:    codeServerNotInitialized,
				Message: "server has not been initialized",
			}
		case s.shuttingDown.Load() && req.Method != "shutdown":
			if req.Notif {
				return nil, nil
			}
			return nil, &jsonrpc2.Error{
				Code:    jsonrpc2.CodeInvalidRequest,
				Message: "server is shutting down",
			}
		}
	}

	switch req.Method {
	case "initialize":
		s.initialized.Store(true)
		return &initializeResult{Capabilities: capabilities()}, nil

	case "initialized":
		return nil, nil

	case "shutdown":
		// The spec is explicit: shutdown stops work but does not exit, and the
		// reply must be null.
		s.shuttingDown.Store(true)
		return nil, nil

	case "exit":
		if err := conn.Close(); err != nil && !errors.Is(err, jsonrpc2.ErrClosed) {
			s.logger().Warn("closing connection on exit", "error", err)
		}
		return nil, nil

	case "textDocument/didOpen":
		var params lsp.DidOpenTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc := s.docs.open(params.TextDocument.URI, params.TextDocument.Version, params.TextDocument.Text, s.tasks())
		s.publish(ctx, conn, doc)
		return nil, nil

	case "textDocument/didChange":
		var params lsp.DidChangeTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc := s.docs.change(params.TextDocument.URI, params.TextDocument.Version, params.ContentChanges, s.tasks())
		if doc == nil {
			// A stale edit, already superseded. Publishing diagnostics computed
			// from it would replace the newer document's report with an older one.
			s.logger().Debug("ignored stale change",
				"uri", params.TextDocument.URI, "version", params.TextDocument.Version)
			return nil, nil
		}
		s.publish(ctx, conn, doc)
		return nil, nil

	case "textDocument/didSave":
		// A save carries the text only when the client honors the advertised
		// includeText; either way the open document is the authority, so a save
		// simply re-publishes.
		var params lsp.DidSaveTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		if doc, ok := s.docs.get(params.TextDocument.URI); ok {
			s.publish(ctx, conn, doc)
		}
		return nil, nil

	case "textDocument/didClose":
		var params lsp.DidCloseTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, wasOpen := s.docs.get(params.TextDocument.URI)
		s.docs.close(params.TextDocument.URI)
		if wasOpen && doc.isTestDocument() {
			promoted := s.clearTestDiagnostics(ctx, conn, params.TextDocument.URI)
			if suite, ok := s.docs.get(promoted); ok {
				s.publish(ctx, conn, suite)
			}
			// Closing an unsaved defaults buffer returns its dependent suites to
			// the saved file. Re-run them now; retaining the live-buffer answer
			// until a suite happens to change would leave stale diagnostics.
			if doc.kind == docTestDefaults {
				for _, source := range s.testDiagnosticSourcesFor(testDefaultsDependencyURI(doc)) {
					suite, ok := s.docs.get(source)
					if !ok || suite.kind != docTestFile {
						continue
					}
					s.publishTestDiagnostics(ctx, conn, source, diagnoseTestPublications(suite, nil), suite)
				}
			}
			return nil, nil
		}
		// Diagnostics belong to the editor's problem list until the server
		// clears them, and a closed document's problems are no longer actionable.
		s.notify(ctx, conn, lsp.PublishDiagnosticsParams{
			URI:         params.TextDocument.URI,
			Diagnostics: []lsp.Diagnostic{},
		})
		return nil, nil

	case "textDocument/hover":
		var params lsp.TextDocumentPositionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return nil, nil
		}
		return hoverAt(doc, params.Position), nil

	case "textDocument/completion":
		var params lsp.CompletionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return &lsp.CompletionList{Items: []lsp.CompletionItem{}}, nil
		}
		return completeAt(doc, params.Position), nil

	case "textDocument/definition":
		var params lsp.TextDocumentPositionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return []lsp.Location{}, nil
		}
		locations := definitionAt(doc, params.Position)
		if locations == nil {
			locations = []lsp.Location{}
		}
		return locations, nil

	case "textDocument/documentSymbol":
		var params lsp.DocumentSymbolParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return []lsp.SymbolInformation{}, nil
		}
		return documentSymbols(doc), nil

	case "textDocument/formatting":
		var params lsp.DocumentFormattingParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return []lsp.TextEdit{}, nil
		}
		edits := formatEdits(doc)
		if edits == nil {
			// A document that does not compile draws no edits — never a partial
			// or guessed one — so this is the empty list rather than nil: the
			// same "nothing to do" an already-formatted document returns, which
			// is the honest answer either way.
			edits = []lsp.TextEdit{}
		}
		return edits, nil

	case "textDocument/codeAction":
		var params codeActionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return []codeAction{}, nil
		}
		actions := codeActions(doc, params)
		if actions == nil {
			// Nothing to migrate, nothing that could be migrated safely, or a
			// document that does not parse. The empty list rather than nil, for
			// the reason formatting returns one: "there is nothing to do here" is
			// an answer, and a client should not have to tell it from a failure.
			actions = []codeAction{}
		}
		return actions, nil

	case "$/cancelRequest", "$/setTrace", "$/logTrace", "workspace/didChangeConfiguration",
		"workspace/didChangeWatchedFiles":
		// Accepted and ignored. Per-request cancellation is not routed: the
		// analysis a request runs is bounded by the document size and finishes
		// well inside what an editor would wait. The one place a request can
		// block instead of compute is the wait for a document's build in
		// [FlowfileServer.awaitDoc], and that is bounded twice over and unblocks
		// on the connection's context and on the connection dropping, so a
		// cancelled request costs a bounded wait rather than a stuck one.
		return nil, nil
	}

	if req.Notif {
		// An unknown notification is not an error: the protocol grows, and a
		// server that complains about a message it does not need is noise.
		s.logger().Debug("ignoring unknown notification", "method", req.Method)
		return nil, nil
	}
	return nil, &jsonrpc2.Error{
		Code:    jsonrpc2.CodeMethodNotFound,
		Message: fmt.Sprintf("method not supported: %s", req.Method),
	}
}

// An initializeResult is [lsp.InitializeResult] over the capability set this
// server actually advertises.
//
// It exists for one field. See [serverCapabilities].
type initializeResult struct {
	Capabilities serverCapabilities `json:"capabilities"`
}

// serverCapabilities is [lsp.ServerCapabilities] with `codeActionProvider` in the
// options form.
//
// The vendored go-lsp models that field as a bool, which is one of the two
// spellings the protocol allows and the one that cannot say *which* kinds are on
// offer. Kinds are what a client filters providers by when a user binds fix-on-save
// to `source.fixAll`, so answering with `true` leaves an editor to discover by
// asking — and leaves a user who reads the handshake unable to tell that this
// server has a fixAll at all.
//
// Declaring the field again at the outer level replaces the embedded one: encoding
// and decoding both take the shallower of two fields with the same JSON name, so
// there is exactly one `codeActionProvider` on the wire and it is this one. The
// embedded bool is therefore never set — [capabilities] leaves it alone, and the
// capability pin test reads the field below instead.
type serverCapabilities struct {
	lsp.ServerCapabilities

	CodeActionProvider *codeActionOptions `json:"codeActionProvider,omitempty"`
}

// codeActionOptions says which kinds of action the server can return.
//
// go-lsp has no such type — its code action support predates the kind — so this is
// the protocol's shape written out here.
type codeActionOptions struct {
	CodeActionKinds []lsp.CodeActionKind `json:"codeActionKinds,omitempty"`
}

// capabilities describes exactly what this server implements.
//
// Advertising anything more makes an editor route requests here that get an empty
// answer, which reads to a user as the feature being broken rather than absent.
func capabilities() serverCapabilities {
	return serverCapabilities{
		CodeActionProvider: &codeActionOptions{
			// Both spellings of the same migration: the quickfix an author reaches
			// from the diagnostic under their cursor, and the whole-file action an
			// editor can bind to a command or to save.
			CodeActionKinds: []lsp.CodeActionKind{lsp.CAKQuickFix, codeActionKindSourceFixAll},
		},
		ServerCapabilities: lsp.ServerCapabilities{
			TextDocumentSync: &lsp.TextDocumentSyncOptionsOrKind{
				// Only Options is set: the marshaler prefers Kind when both are
				// present, and the kind form cannot express that the server wants
				// save notifications.
				Options: &lsp.TextDocumentSyncOptions{
					OpenClose: true,
					Change:    lsp.TDSKFull,
					Save:      &lsp.SaveOptions{IncludeText: true},
				},
			},
			HoverProvider: true,
			CompletionProvider: &lsp.CompletionOptions{
				// Enough to open completion at each place a Flowfile has something
				// to offer: after a key's colon, inside ${...}, after a step id's
				// dot, and within a libs list.
				TriggerCharacters: []string{":", " ", ".", "{", "[", ",", "-"},
			},
			DefinitionProvider:         true,
			DocumentSymbolProvider:     true,
			DocumentFormattingProvider: true,
		},
	}
}

// awaitDoc returns the document a request asks about, waiting for a build that
// has arrived and not yet landed.
//
// Every request that reads a document goes through this rather than through
// [documentStore.get], because the connection serves this handler through
// [NewHandler]: a goroutine is started per message from the read loop, so
// messages begin in arrival order and then race. A client is entitled to send
// didOpen and a hover for the cursor's position without waiting in between, and
// it is not entitled to be told null for the document it just opened. Reading
// the store directly answered from whether didOpen's goroutine happened to have
// got there first; waiting here, on a build [NewHandler] announced in arrival
// order, is what answers from the document instead.
//
// The wait is bounded and cancellable, and both matter. The request's context
// carries the client's cancellation and the process's shutdown, and the
// connection's disconnect channel covers an editor that exits without saying so.
// A request that outlives either answers with whatever the store holds, which is
// the empty answer for a document that was never opened.
func (s *FlowfileServer) awaitDoc(ctx context.Context, conn *jsonrpc2.Conn, uri lsp.DocumentURI) (*document, bool) {
	return s.docs.await(ctx, conn.DisconnectNotify(), uri)
}

// publish sends the diagnostics for a document, including when there are none.
//
// An empty array is meaningful: it is how a language server retracts problems it
// reported earlier. Skipping the notification when a document becomes clean leaves
// the editor showing errors the author has already fixed.
func (s *FlowfileServer) publish(ctx context.Context, conn *jsonrpc2.Conn, doc *document) {
	if doc.isTestDocument() {
		var included *document
		var openDefaults *document
		tracked := true
		if doc.kind == docTestFile {
			if path, ok := doc.filesystemPath(); ok {
				defaultsPath := filepath.Join(filepath.Dir(path), flowtest.DirDefaultsName)
				defaultsURI := fileURI(defaultsPath)
				var current bool
				tracked, current = s.rememberTestDefaults(doc, defaultsURI)
				if !current {
					return
				}
				openDefaults, _ = s.docs.getByFilesystemPath(defaultsPath)
				if tracked {
					included = openDefaults
				}
			}
		}
		publications := diagnoseTestPublications(doc, included)
		if !tracked {
			publications[0].diagnostics = append(publications[0].diagnostics, lsp.Diagnostic{
				Range: documentStart, Severity: lsp.Warning, Source: diagnosticSource, Code: codeTestDefaultsDependents,
				Message: fmt.Sprintf("this suite is beyond the %d open-suite limit for live testdefaults.yaml revalidation; saved defaults are checked instead", maxTestDefaultsDependents),
			})
		}
		guards := []*document{doc}
		if included != nil {
			guards = append(guards, included)
		}
		s.publishTestDiagnostics(ctx, conn, doc.uri, publications, guards...)
		// A live defaults edit changes every open suite that includes it. Re-run
		// those suites through the same loader with this buffer; otherwise the
		// editor would keep diagnostics from the saved defaults until each suite
		// happened to change too.
		if doc.kind == docTestDefaults {
			for _, source := range s.testDiagnosticSourcesFor(testDefaultsDependencyURI(doc)) {
				suite, ok := s.docs.get(source)
				if !ok || suite.kind != docTestFile {
					continue
				}
				s.publishTestDiagnostics(ctx, conn, source, diagnoseTestPublications(suite, doc), suite, doc)
			}
		}
		return
	}
	diagnostics := diagnose(doc)
	s.logger().Debug("published diagnostics",
		"uri", doc.uri, "version", doc.version, "count", len(diagnostics))
	s.notify(ctx, conn, lsp.PublishDiagnosticsParams{
		URI:         doc.uri,
		Diagnostics: diagnostics,
	})
}

// siblingDocumentURI retains the client's spelling of a local file URI while
// replacing its basename. In particular, file://localhost and Windows-drive
// authorities must keep matching the URI under which the editor opened the
// sibling buffer; filesystemPath normalization is for loader I/O, not identity.
func siblingDocumentURI(uri lsp.DocumentURI, name string) lsp.DocumentURI {
	u, err := url.Parse(string(uri))
	if err != nil {
		return uri
	}
	u.Path = filepath.ToSlash(filepath.Join(filepath.Dir(u.Path), name))
	u.RawPath = ""
	return lsp.DocumentURI(u.String())
}

func testDefaultsDependencyURI(doc *document) lsp.DocumentURI {
	if path, ok := doc.filesystemPath(); ok {
		return fileURI(path)
	}
	return doc.uri
}

const (
	maxTestDefaultsDependents      = 32
	maxTestDefaultsOverflowMembers = 32
	codeTestDefaultsDependents     = "testdefaults-dependent-limit"
)

func (s *FlowfileServer) testDiagnosticSourcesFor(target lsp.DocumentURI) []lsp.DocumentURI {
	s.testDiagnosticsMu.Lock()
	defer s.testDiagnosticsMu.Unlock()
	sources := make([]lsp.DocumentURI, 0, len(s.testSuitesByDefaults[target]))
	for source := range s.testSuitesByDefaults[target] {
		sources = append(sources, source)
	}
	slices.Sort(sources)
	return sources
}

// rememberTestDefaults records a suite for live-buffer revalidation. The bound
// limits one defaults keystroke to a fixed number of suite parses; callers that
// do not fit use the saved defaults and publish an explicit warning.
func (s *FlowfileServer) rememberTestDefaults(suite *document, defaults lsp.DocumentURI) (tracked, current bool) {
	s.testDiagnosticsMu.Lock()
	defer s.testDiagnosticsMu.Unlock()
	open, ok := s.docs.get(suite.uri)
	if !ok || open != suite {
		return false, false
	}
	if s.testDefaultsBySuite == nil {
		s.testDefaultsBySuite = make(map[lsp.DocumentURI]lsp.DocumentURI)
		s.testSuitesByDefaults = make(map[lsp.DocumentURI]map[lsp.DocumentURI]bool)
		s.testOverflowsByDefaults = make(map[lsp.DocumentURI]map[lsp.DocumentURI]bool)
		s.testOverflowBySuite = make(map[lsp.DocumentURI]lsp.DocumentURI)
	}
	if previous, ok := s.testDefaultsBySuite[suite.uri]; ok {
		return previous == defaults, true
	}
	dependents := s.testSuitesByDefaults[defaults]
	if len(dependents) >= maxTestDefaultsDependents {
		if len(s.testOverflowsByDefaults[defaults]) < maxTestDefaultsOverflowMembers {
			if s.testOverflowsByDefaults[defaults] == nil {
				s.testOverflowsByDefaults[defaults] = make(map[lsp.DocumentURI]bool)
			}
			s.testOverflowsByDefaults[defaults][suite.uri] = true
			s.testOverflowBySuite[suite.uri] = defaults
		}
		return false, true
	}
	if dependents == nil {
		dependents = make(map[lsp.DocumentURI]bool)
		s.testSuitesByDefaults[defaults] = dependents
	}
	s.testDefaultsBySuite[suite.uri] = defaults
	dependents[suite.uri] = true
	return true, true
}

// publishTestDiagnostics replaces source's cached contributions and publishes
// every URI whose aggregate may have changed. The store guards reject stale
// analyses; publishing while the diagnostics lock is held keeps notification
// order identical to cache-update order under concurrent document changes.
func (s *FlowfileServer) publishTestDiagnostics(ctx context.Context, conn *jsonrpc2.Conn, source lsp.DocumentURI, publications []diagnosticPublication, guards ...*document) {
	s.testDiagnosticsMu.Lock()
	defer s.testDiagnosticsMu.Unlock()
	for _, guard := range guards {
		current, ok := s.docs.get(guard.uri)
		if !ok || current != guard {
			return
		}
	}
	if defaultsURI, tracked := s.testDefaultsBySuite[source]; tracked {
		defaultsPath, hasPath := (&document{uri: defaultsURI}).filesystemPath()
		if openDefaults, ok := s.docs.getByFilesystemPath(defaultsPath); hasPath && ok {
			usesOpenBuffer := false
			for _, guard := range guards {
				usesOpenBuffer = usesOpenBuffer || guard == openDefaults
			}
			if !usesOpenBuffer {
				return
			}
		}
	}
	if s.testDiagnosticsBySource == nil {
		s.testDiagnosticsBySource = make(map[lsp.DocumentURI]map[lsp.DocumentURI][]lsp.Diagnostic)
	}
	if s.testSourcesByTarget == nil {
		s.testSourcesByTarget = make(map[lsp.DocumentURI]map[lsp.DocumentURI]bool)
	}
	touched := map[lsp.DocumentURI]bool{source: true}
	for uri := range s.testDiagnosticsBySource[source] {
		touched[uri] = true
		delete(s.testSourcesByTarget[uri], source)
		if len(s.testSourcesByTarget[uri]) == 0 {
			delete(s.testSourcesByTarget, uri)
		}
	}
	next := make(map[lsp.DocumentURI][]lsp.Diagnostic, len(publications))
	var overflowDiagnostics []lsp.Diagnostic
	for _, publication := range publications {
		if source != publication.uri {
			_, tracked := s.testDefaultsBySuite[source]
			if !tracked {
				// Overflow membership is retained for bounded promotion, but its
				// saved defaults result is not a target contributor. Preserve any
				// suite-specific refusal as an explicitly fallback-owned suite error
				// without making target aggregation grow with N.
				for _, diagnostic := range publication.diagnostics {
					diagnostic.Range = documentStart
					diagnostic.Message = "saved testdefaults.yaml fallback: " + diagnostic.Message
					overflowDiagnostics = append(overflowDiagnostics, diagnostic)
				}
				continue
			}
		}
		next[publication.uri] = publication.diagnostics
		touched[publication.uri] = true
		if s.testSourcesByTarget[publication.uri] == nil {
			s.testSourcesByTarget[publication.uri] = make(map[lsp.DocumentURI]bool)
		}
		s.testSourcesByTarget[publication.uri][source] = true
	}
	if len(overflowDiagnostics) > 0 {
		next[source] = append(next[source], overflowDiagnostics...)
	}
	s.testDiagnosticsBySource[source] = next
	for _, publication := range sourceFirst(source, s.aggregateTestDiagnostics(touched)) {
		s.notify(ctx, conn, publication)
	}
}

// clearTestDiagnostics removes a closed document's contributions and publishes
// the aggregates needed to retract them without erasing another suite's.
func (s *FlowfileServer) clearTestDiagnostics(ctx context.Context, conn *jsonrpc2.Conn, source lsp.DocumentURI) lsp.DocumentURI {
	s.testDiagnosticsMu.Lock()
	defer s.testDiagnosticsMu.Unlock()
	// A later didOpen may already have installed and published a new generation
	// for this URI while the old didClose handler was waiting for this lock.
	// Its cache and dependency state belong to the reopened document.
	if _, reopened := s.docs.get(source); reopened {
		return ""
	}
	touched := map[lsp.DocumentURI]bool{source: true}
	for uri := range s.testDiagnosticsBySource[source] {
		touched[uri] = true
		delete(s.testSourcesByTarget[uri], source)
		if len(s.testSourcesByTarget[uri]) == 0 {
			delete(s.testSourcesByTarget, uri)
		}
	}
	delete(s.testDiagnosticsBySource, source)
	var promoted lsp.DocumentURI
	var promotionDefaults lsp.DocumentURI
	if defaults, ok := s.testDefaultsBySuite[source]; ok {
		delete(s.testDefaultsBySuite, source)
		delete(s.testSuitesByDefaults[defaults], source)
		var candidate lsp.DocumentURI
		var candidateDoc *document
		for overflow := range s.testOverflowsByDefaults[defaults] {
			delete(s.testOverflowsByDefaults[defaults], overflow)
			delete(s.testOverflowBySuite, overflow)
			if suite, open := s.docs.get(overflow); open && suite.kind == docTestFile {
				candidate = overflow
				candidateDoc = suite
				break
			}
		}
		if len(s.testOverflowsByDefaults[defaults]) == 0 {
			delete(s.testOverflowsByDefaults, defaults)
		}
		if candidateDoc != nil {
			promoted = candidate
			promotionDefaults = defaults
		}
		if len(s.testSuitesByDefaults[defaults]) == 0 && promoted == "" {
			delete(s.testSuitesByDefaults, defaults)
		}
	}
	if defaults, ok := s.testOverflowBySuite[source]; ok {
		delete(s.testOverflowBySuite, source)
		delete(s.testOverflowsByDefaults[defaults], source)
		if len(s.testOverflowsByDefaults[defaults]) == 0 {
			delete(s.testOverflowsByDefaults, defaults)
		}
	}
	for _, publication := range sourceFirst(source, s.aggregateTestDiagnostics(touched)) {
		s.notify(ctx, conn, publication)
	}
	if promoted != "" {
		s.testDefaultsBySuite[promoted] = promotionDefaults
		s.testSuitesByDefaults[promotionDefaults][promoted] = true
	}
	return promoted
}

func sourceFirst(source lsp.DocumentURI, publications []lsp.PublishDiagnosticsParams) []lsp.PublishDiagnosticsParams {
	for i := range publications {
		if publications[i].URI == source {
			publications[0], publications[i] = publications[i], publications[0]
			break
		}
	}
	return publications
}

func (s *FlowfileServer) aggregateTestDiagnostics(touched map[lsp.DocumentURI]bool) []lsp.PublishDiagnosticsParams {
	uris := make([]lsp.DocumentURI, 0, len(touched))
	for uri := range touched {
		uris = append(uris, uri)
	}
	slices.Sort(uris)
	out := make([]lsp.PublishDiagnosticsParams, 0, len(uris))
	for _, uri := range uris {
		var diagnostics []lsp.Diagnostic
		seen := map[string]bool{}
		ordered := make([]lsp.DocumentURI, 0, len(s.testSourcesByTarget[uri]))
		for source := range s.testSourcesByTarget[uri] {
			ordered = append(ordered, source)
		}
		slices.Sort(ordered)
		// A directly open document owns the code for an otherwise identical
		// diagnostic an including suite also reports on its URI.
		if own := slices.Index(ordered, uri); own >= 0 {
			ordered[0], ordered[own] = ordered[own], ordered[0]
		}
		for _, source := range ordered {
			// A target contribution from an untracked suite is a saved-file
			// fallback, not a diagnosis of the newer open defaults buffer.
			if open, ok := s.docs.get(uri); source != uri && ok && open.kind == docTestDefaults && s.testDefaultsBySuite[source] != testDefaultsDependencyURI(open) {
				continue
			}
			byURI := s.testDiagnosticsBySource[source]
			for _, d := range byURI[uri] {
				key := fmt.Sprintf("%d:%d:%d:%d:%s", d.Range.Start.Line, d.Range.Start.Character,
					d.Range.End.Line, d.Range.End.Character, d.Message)
				if !seen[key] {
					seen[key] = true
					diagnostics = append(diagnostics, d)
				}
			}
		}
		if diagnostics == nil {
			diagnostics = []lsp.Diagnostic{}
		}
		slices.SortStableFunc(diagnostics, func(a, b lsp.Diagnostic) int {
			if a.Range.Start.Line != b.Range.Start.Line {
				return a.Range.Start.Line - b.Range.Start.Line
			}
			if a.Range.Start.Character != b.Range.Start.Character {
				return a.Range.Start.Character - b.Range.Start.Character
			}
			return strings.Compare(a.Message, b.Message)
		})
		out = append(out, lsp.PublishDiagnosticsParams{URI: uri, Diagnostics: diagnostics})
	}
	return out
}

// notify sends a notification, logging a failure rather than propagating it: there
// is no caller to return an error to.
func (s *FlowfileServer) notify(ctx context.Context, conn *jsonrpc2.Conn, params lsp.PublishDiagnosticsParams) {
	if err := conn.Notify(ctx, "textDocument/publishDiagnostics", params); err != nil {
		s.logger().Warn("publishing diagnostics failed", "uri", params.URI, "error", err)
	}
}

// logger returns the configured logger, or the default one.
// tasks returns the registry this server answers from.
//
// The nil case is the zero value's, which is a usable server over the built-in
// task set — the harness and every test that does not care about plugins builds
// one that way.
func (s *FlowfileServer) tasks() *v1.Registry {
	if s.Tasks != nil {
		return s.Tasks
	}
	return v1.DefaultRegistry()
}

func (s *FlowfileServer) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// decode unmarshals a request's parameters.
//
// Absent parameters are not an error: several notifications carry none, and a
// client that omits an empty object is within the spec.
func decode(req *jsonrpc2.Request, into any) error {
	if req.Params == nil {
		return nil
	}
	if err := json.Unmarshal(*req.Params, into); err != nil {
		return &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidParams,
			Message: fmt.Sprintf("invalid params for %s: %v", req.Method, err),
		}
	}
	return nil
}

// asRPCError converts an error into the JSON-RPC form, preserving a code that was
// chosen deliberately.
func asRPCError(err error) *jsonrpc2.Error {
	var rpcErr *jsonrpc2.Error
	if errors.As(err, &rpcErr) {
		return rpcErr
	}
	return &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: err.Error()}
}
