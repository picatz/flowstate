package lsp

import (
	"context"
	"encoding/json"
	"log"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
)

/* WARNING: VERY EXPIERIMENTAL, WORK IN PROGRESS */

type FlowfileServer struct{}

func (s *FlowfileServer) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	switch req.Method {
	case "initialize":
		result := &lsp.InitializeResult{
			Capabilities: lsp.ServerCapabilities{
				TextDocumentSync: &lsp.TextDocumentSyncOptionsOrKind{
					Kind: func() *lsp.TextDocumentSyncKind { k := lsp.TDSKFull; return &k }(),
				},
			},
		}
		conn.Reply(ctx, req.ID, result)
	case "textDocument/didOpen":
		var params lsp.DidOpenTextDocumentParams
		if req.Params != nil {
			if err := json.Unmarshal(*req.Params, &params); err != nil {
				conn.ReplyWithError(ctx, req.ID, &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: err.Error()})
				return
			}
		}
		err := s.validate(ctx, params.TextDocument.URI, params.TextDocument.Text)
		if err != nil {
			conn.ReplyWithError(ctx, req.ID, &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: err.Error()})
			return
		}
		conn.Reply(ctx, req.ID, nil)
	case "textDocument/didChange":
		var params lsp.DidChangeTextDocumentParams
		if req.Params != nil {
			if err := json.Unmarshal(*req.Params, &params); err != nil {
				conn.ReplyWithError(ctx, req.ID, &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: err.Error()})
				return
			}
		}
		if len(params.ContentChanges) > 0 {
			err := s.validate(ctx, params.TextDocument.URI, params.ContentChanges[0].Text)
			if err != nil {
				conn.ReplyWithError(ctx, req.ID, &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: err.Error()})
				return
			}
		}
		conn.Reply(ctx, req.ID, nil)
	default:
		conn.Reply(ctx, req.ID, nil)
	}
}

func (s *FlowfileServer) validate(_ context.Context, _ lsp.DocumentURI, text string) error {
	_, err := flowfile.Unmarshal([]byte(text))
	if err != nil {
		log.Printf("flowfile validation error: %v", err)
		// TODO(kent):send diagnostics here
	}
	return nil
}
