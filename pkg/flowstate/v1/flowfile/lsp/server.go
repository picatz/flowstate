package lsp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"path/filepath"
	"runtime/debug"
	"slices"
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
	// The server never writes this registry. Only the host does, at start-up; a
	// lock's tasks found by [FlowfileServer.PluginCatalog] go to a registry of
	// that document's own, built on this one.
	Tasks *v1.Registry

	// PluginCatalog, when set, is asked about the file behind each document as
	// it is opened, before the document is built, for the plugin catalog found
	// next to the file (plugins.lock.json, as `flow validate` finds it). It is
	// given the file's filesystem path, reads descriptors only, and launches
	// nothing; a nil lock means there is none. A document with no filesystem
	// path is not asked.
	//
	// The lock's tasks are visible to that document alone: it is answered from
	// [FlowfileServer.Tasks] plus the lock's tasks, in a registry that is shared
	// only with other open documents under the same lock version and released
	// when the last of them closes. At most [MaxPluginLocks] are retained.
	//
	// A returned error, or a lock past that bound, is shown to the person as a
	// window message and logged, and the document is built from
	// [FlowfileServer.Tasks] alone, where its plugin tasks read as unknown: a
	// lock that is present but unusable is reported, never silently ignored.
	PluginCatalog func(path string) (*PluginLock, error)

	scopes pluginScopes

	docs documentStore

	// interfaces is the interface each module had when its users were last
	// published for; see recheck.go.
	interfaces interfaceMemo

	// diagnosticsBySource retains each open document's contribution to every URI
	// it diagnoses, and sourcesByTarget is the same relation read the other way.
	// A suite may diagnose its included testdefaults.yaml, and a workflow the
	// file it calls; aggregation prevents one clean source from clearing a problem
	// another open source still reports, and lets closing a source retract only
	// its own contribution. calleeReadsBySource is the canonical path of every file
	// each open workflow's last compile read for its `call:` steps, clean or not,
	// which is what a save of a callee is matched against. See contributions.go.
	diagnosticsMu           sync.Mutex
	diagnosticsBySource     map[lsp.DocumentURI]map[lsp.DocumentURI][]publishedDiagnostic
	sourcesByTarget         map[lsp.DocumentURI]map[lsp.DocumentURI]bool
	calleeReadsBySource     map[lsp.DocumentURI]map[string]bool
	testDefaultsBySuite     map[lsp.DocumentURI]lsp.DocumentURI
	testSuitesByDefaults    map[lsp.DocumentURI]map[lsp.DocumentURI]bool
	testOverflowsByDefaults map[lsp.DocumentURI]map[lsp.DocumentURI]bool
	testOverflowBySuite     map[lsp.DocumentURI]lsp.DocumentURI

	// initialized and shuttingDown track the protocol lifecycle. The spec
	// requires rejecting requests before initialize and after shutdown, and an
	// editor that reuses a connection depends on that being enforced.
	initialized atomic.Bool

	// snippets records that the client said at initialize it can expand snippet
	// completions. A server serves one client over one stream, so this is a
	// property of the session, and a client that does not say so gets plain text.
	snippets     atomic.Bool
	shuttingDown atomic.Bool

	// roots are the workspace folders the client opened, as filesystem paths. A
	// request that has to find other files (a rename across importers, the
	// workspace symbol list) reads only inside them; none known means none read.
	roots atomic.Pointer[[]string]
}

// workspaceFolders is the part of initialize that names the folders the client
// opened; go-lsp's InitializeParams predates `workspaceFolders`.
type workspaceFolders struct {
	RootURI          lsp.DocumentURI `json:"rootUri"`
	WorkspaceFolders []struct {
		URI lsp.DocumentURI `json:"uri"`
	} `json:"workspaceFolders"`
}

// uris lists the folders named, the root first.
func (f workspaceFolders) uris() []lsp.DocumentURI {
	out := []lsp.DocumentURI{f.RootURI}
	for _, folder := range f.WorkspaceFolders {
		out = append(out, folder.URI)
	}

	return out
}

// workspace is what a request that reads beyond its own document may read.
func (s *FlowfileServer) workspace() workspace {
	w := workspace{open: s.docs.getByFilesystemPath}
	if roots := s.roots.Load(); roots != nil {
		w.roots = *roots
	}

	return w
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
		var params lsp.InitializeParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		s.snippets.Store(params.Capabilities.TextDocument.Completion.CompletionItem.SnippetSupport)
		var folders workspaceFolders
		if err := decode(req, &folders); err == nil {
			var roots []string
			for _, uri := range folders.uris() {
				if path, ok := (&document{uri: uri}).filesystemPath(); ok && !slices.Contains(roots, path) {
					roots = append(roots, path)
				}
			}
			s.roots.Store(&roots)
		}
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
		tasks := s.discoverPluginCatalog(ctx, conn, params.TextDocument.URI)
		doc := s.docs.open(params.TextDocument.URI, params.TextDocument.Version, params.TextDocument.Text, tasks)
		s.publish(ctx, conn, doc)
		s.rememberInterface(doc)
		return nil, nil

	case "textDocument/didChange":
		var params lsp.DidChangeTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc := s.docs.change(params.TextDocument.URI, params.TextDocument.Version, params.ContentChanges, s.tasksFor(params.TextDocument.URI))
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
		// simply re-publishes. A saved module also re-publishes the open files
		// whose meaning its save changed; see recheck.go.
		var params lsp.DidSaveTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		if doc, ok := s.docs.get(params.TextDocument.URI); ok {
			s.publish(ctx, conn, doc)
			for _, dependent := range s.dependentsToRecheck(doc) {
				s.afterQueued(ctx, dependent.uri, func(current *document) { s.publish(ctx, conn, current) })
			}
			// A saved file may be the callee another open document reports
			// problems in; those were computed from what the file held before.
			for _, source := range s.calleeSourcesFor(doc) {
				s.afterQueued(ctx, source, func(current *document) { s.publish(ctx, conn, current) })
			}
		}
		return nil, nil

	case "textDocument/didClose":
		var params lsp.DidCloseTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, wasOpen := s.docs.get(params.TextDocument.URI)
		s.docs.close(params.TextDocument.URI)
		s.scopes.release(params.TextDocument.URI)
		if wasOpen && doc.isTestDocument() {
			promoted := s.clearTestDiagnostics(ctx, conn, params.TextDocument.URI)
			if suite, ok := s.docs.get(promoted); ok {
				s.publish(ctx, conn, suite)
			}
			// Closing an unsaved defaults buffer returns its dependent suites to
			// the saved file. Re-run them now; retaining the live-buffer answer
			// until a suite happens to change would leave stale diagnostics.
			//
			// The suites past the dependent bound too: opening the buffer retracted
			// their saved-file contributions from the defaults URI, and nothing but
			// this brings them back (#1273).
			if doc.kind == docTestDefaults {
				target := testDefaultsDependencyURI(doc)
				for _, source := range append(s.testDiagnosticSourcesFor(target), s.testOverflowSourcesFor(target)...) {
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
		s.clearWorkflowDiagnostics(ctx, conn, params.TextDocument.URI)
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

	case "textDocument/signatureHelp":
		var params lsp.TextDocumentPositionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return nil, nil
		}
		return signatureHelpAt(doc, params.Position), nil

	case "textDocument/completion":
		var params lsp.CompletionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return &lsp.CompletionList{Items: []lsp.CompletionItem{}}, nil
		}
		list := completeAt(doc, params.Position)
		if s.snippets.Load() {
			snippetizeTasks(doc, list, doc.tasks)
			return withAdjustedIndentation(list), nil
		}
		return list, nil

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

	case "textDocument/references":
		var params lsp.ReferenceParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return []lsp.Location{}, nil
		}
		locations := referencesAt(doc, params.Position, params.Context.IncludeDeclaration)
		if locations == nil {
			locations = []lsp.Location{}
		}
		return locations, nil

	case "textDocument/documentHighlight":
		var params lsp.TextDocumentPositionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return []lsp.DocumentHighlight{}, nil
		}
		highlights := highlightsAt(doc, params.Position)
		if highlights == nil {
			highlights = []lsp.DocumentHighlight{}
		}
		return highlights, nil

	case "textDocument/prepareRename":
		var params lsp.TextDocumentPositionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return nil, nil
		}
		rng, placeholder, ok := prepareRenameQualified(doc, params.Position)
		if !ok {
			rng, placeholder, ok = prepareRenameAt(doc, params.Position)
		}
		if !ok {
			return nil, nil
		}
		return prepareRenameResult{Range: rng, Placeholder: placeholder}, nil

	case "textDocument/rename":
		var params lsp.RenameParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return nil, nil
		}
		edit, handled, err := renameQualified(doc, s.workspace(), params.Position, params.NewName)
		if !handled {
			edit, err = renameAt(doc, params.Position, params.NewName)
		}
		if err != nil {
			// A refusal the editor shows as the reason; -32803 is the
			// protocol's RequestFailed.
			return nil, &jsonrpc2.Error{Code: requestFailed, Message: err.Error()}
		}
		return edit, nil

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

	case "workspace/symbol":
		var params lsp.WorkspaceSymbolParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		return workspaceSymbols(s.workspace(), params.Query), nil

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

	case "textDocument/semanticTokens/full":
		var params semanticTokensParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return semanticTokensResult{Data: []uint32{}}, nil
		}
		return semanticTokens(doc), nil

	case "textDocument/inlayHint":
		var params inlayHintParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.awaitDoc(ctx, conn, params.TextDocument.URI)
		if !ok {
			return []inlayHint{}, nil
		}
		return inlayHints(doc, params.Range), nil

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

	// SemanticTokensProvider is absent from go-lsp, which predates the feature.
	SemanticTokensProvider *semanticTokensProvider `json:"semanticTokensProvider,omitempty"`

	// InlayHintProvider is absent from go-lsp, which predates the feature.
	InlayHintProvider bool `json:"inlayHintProvider,omitempty"`

	// RenameProvider is the options form for the reason CodeActionProvider is:
	// go-lsp models it as a bool, which cannot say that prepareRename is served.
	RenameProvider *renameOptions `json:"renameProvider,omitempty"`
}

// renameOptions says rename is served with a prepare step, so an editor asks
// whether the cursor is on a renamable id before it shows a prompt.
type renameOptions struct {
	PrepareProvider bool `json:"prepareProvider"`
}

// prepareRenameResult is the `{range, placeholder}` form of the prepareRename
// answer: the characters the editor selects, and the name it offers to edit.
type prepareRenameResult struct {
	Range       lsp.Range `json:"range"`
	Placeholder string    `json:"placeholder"`
}

// requestFailed is the LSP error code for a request that was understood and
// could not be done, which a refused rename is.
const requestFailed = -32803

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
		SemanticTokensProvider: &semanticTokensProvider{
			Legend: semanticTokensLegend{
				TokenTypes:     semanticTokenTypes,
				TokenModifiers: semanticTokenModifiers,
			},
			Full: true,
		},
		InlayHintProvider: true,
		CodeActionProvider: &codeActionOptions{
			// Both spellings of the same migration: the quickfix an author reaches
			// from the diagnostic under their cursor, and the whole-file action an
			// editor can bind to a command or to save.
			CodeActionKinds: []lsp.CodeActionKind{lsp.CAKQuickFix, codeActionKindSourceFixAll},
		},
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
		HoverProvider:         true,
		SignatureHelpProvider: &lsp.SignatureHelpOptions{TriggerCharacters: []string{"(", ","}},
		CompletionProvider: &lsp.CompletionOptions{
			// Enough to open completion at each place a Flowfile has something
			// to offer: after a key's colon, inside ${...}, after a step id's
			// dot, and within a libs list.
			TriggerCharacters: []string{":", " ", ".", "{", "[", ",", "-"},
		},
		DefinitionProvider:         true,
		ReferencesProvider:         true,
		DocumentHighlightProvider:  true,
		RenameProvider:             &renameOptions{PrepareProvider: true},
		DocumentSymbolProvider:     true,
		WorkspaceSymbolProvider:    true,
		DocumentFormattingProvider: true,
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
		// A live defaults edit changes every open suite that includes it. Re-run
		// those suites through the same loader with this buffer; otherwise the
		// editor would keep diagnostics from the saved defaults until each suite
		// happened to change too.
		//
		// Before the buffer's own publication, not after: that publication
		// aggregates the tracked suites' cached contributions, which until they
		// are re-run were computed from the saved file, and a range from the saved
		// file does not exist in a buffer that has since been corrected (#1273).
		if doc.kind == docTestDefaults {
			for _, source := range s.testDiagnosticSourcesFor(testDefaultsDependencyURI(doc)) {
				suite, ok := s.docs.get(source)
				if !ok || suite.kind != docTestFile {
					continue
				}
				s.publishTestDiagnosticsWithholding(ctx, conn, source, diagnoseTestPublications(suite, doc),
					[]lsp.DocumentURI{doc.uri, testDefaultsDependencyURI(doc)}, suite, doc)
			}
		}
		s.publishTestDiagnostics(ctx, conn, doc.uri, publications, guards...)
		return
	}
	s.publishWorkflowDiagnostics(ctx, conn, doc)
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
	s.diagnosticsMu.Lock()
	defer s.diagnosticsMu.Unlock()
	sources := make([]lsp.DocumentURI, 0, len(s.testSuitesByDefaults[target]))
	for source := range s.testSuitesByDefaults[target] {
		sources = append(sources, source)
	}
	slices.Sort(sources)
	return sources
}

// testOverflowSourcesFor is [FlowfileServer.testDiagnosticSourcesFor] for the
// suites waiting past the dependent bound.
func (s *FlowfileServer) testOverflowSourcesFor(target lsp.DocumentURI) []lsp.DocumentURI {
	s.diagnosticsMu.Lock()
	defer s.diagnosticsMu.Unlock()
	sources := slices.Collect(maps.Keys(s.testOverflowsByDefaults[target]))
	slices.Sort(sources)
	return sources
}

// rememberTestDefaults records a suite for live-buffer revalidation. The bound
// limits one defaults keystroke to a fixed number of suite parses; callers that
// do not fit use the saved defaults and publish an explicit warning.
func (s *FlowfileServer) rememberTestDefaults(suite *document, defaults lsp.DocumentURI) (tracked, current bool) {
	s.diagnosticsMu.Lock()
	defer s.diagnosticsMu.Unlock()
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
	s.publishTestDiagnosticsWithholding(ctx, conn, source, publications, nil, guards...)
}

// publishTestDiagnosticsWithholding is [FlowfileServer.publishTestDiagnostics]
// that records everything but does not notify the URIs in withheld.
//
// A live defaults buffer re-runs each tracked suite against itself before it
// publishes its own aggregate. Every one of those re-runs would otherwise
// notify the defaults URI with the suites not yet re-run still contributing their
// saved-file positions, so the buffer is withheld until the last one is current
// and then published once (#1273).
func (s *FlowfileServer) publishTestDiagnosticsWithholding(ctx context.Context, conn *jsonrpc2.Conn, source lsp.DocumentURI, publications []diagnosticPublication, withheld []lsp.DocumentURI, guards ...*document) {
	s.diagnosticsMu.Lock()
	defer s.diagnosticsMu.Unlock()
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
	next := make(map[lsp.DocumentURI][]publishedDiagnostic, len(publications))
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
		next[publication.uri] = wrapDiagnostics(publication.diagnostics)
	}
	if len(overflowDiagnostics) > 0 {
		next[source] = append(next[source], wrapDiagnostics(overflowDiagnostics)...)
	}
	touched := s.replaceContributionsLocked(source, next)
	for _, publication := range sourceFirst(source, s.aggregateDiagnostics(touched)) {
		if slices.Contains(withheld, publication.URI) {
			continue
		}
		s.notify(ctx, conn, publication)
	}
}

// clearTestDiagnostics removes a closed document's contributions and publishes
// the aggregates needed to retract them without erasing another suite's.
func (s *FlowfileServer) clearTestDiagnostics(ctx context.Context, conn *jsonrpc2.Conn, source lsp.DocumentURI) lsp.DocumentURI {
	s.diagnosticsMu.Lock()
	defer s.diagnosticsMu.Unlock()
	// A later didOpen may already have installed and published a new generation
	// for this URI while the old didClose handler was waiting for this lock.
	// Its cache and dependency state belong to the reopened document.
	if _, reopened := s.docs.get(source); reopened {
		return ""
	}
	touched := s.dropContributionsLocked(source)
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
	for _, publication := range sourceFirst(source, s.aggregateDiagnostics(touched)) {
		s.notify(ctx, conn, publication)
	}
	if promoted != "" {
		s.testDefaultsBySuite[promoted] = promotionDefaults
		s.testSuitesByDefaults[promotionDefaults][promoted] = true
	}
	return promoted
}

// notify sends a notification, logging a failure rather than propagating it: there
// is no caller to return an error to.
func (s *FlowfileServer) notify(ctx context.Context, conn *jsonrpc2.Conn, params publishDiagnosticsParams) {
	if err := conn.Notify(ctx, "textDocument/publishDiagnostics", params); err != nil {
		s.logger().Warn("publishing diagnostics failed", "uri", params.URI, "error", err)
	}
}

// tasks returns the registry this server answers from.
//
// The nil case is the zero value's, which is a usable server over the built-in
// task set — the harness and every test that does not care about plugins builds
// one that way.
func (s *FlowfileServer) tasks() *v1.Registry {
	if s.Tasks != nil {
		return s.Tasks
	}
	return defaultTasks()
}

// logger returns the configured logger, or the default one.
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
	if rpcErr, ok := errors.AsType[*jsonrpc2.Error](err); ok {
		return rpcErr
	}
	return &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: err.Error()}
}
