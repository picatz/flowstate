package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The check an author could only run where the files were.
//
// `flow validate` is offline on purpose and stays that way — invariant 8 says a
// first run needs nothing, and a validator that needs a server stops working on
// an aeroplane. What was missing was the same check for a caller with no
// filesystem to point the command at: a CI service holding a diff, a browser, an
// agent checking a file it has not written anywhere. They had two options, both
// wrong — ship the file somewhere a CLI could see it, or submit it and find out
// by running it, which is exactly what a validator exists to make unnecessary.
//
// Both handlers here are thin by design and that thinness is the property worth
// keeping: each one calls the same function its CLI verb calls and returns the
// same message that verb prints. There is no server-side validator to drift from
// the offline one, because there is no server-side validator — there is one
// validator with two callers.

// Validate checks the submitted Flowfiles and reports their diagnostics,
// executing nothing.
func (s *FlowstateServer) Validate(
	ctx context.Context,
	req *connect.Request[v1.ValidateRequest],
) (*connect.Response[v1.ValidateResponse], error) {
	// Authentication happened before this ran — the authenticator middleware
	// wraps every RPC route, and an unauthenticated deployment is a choice made
	// with --insecure-no-auth rather than a path through here. What this method
	// deliberately does not do is derive a tenant: checking a file touches no
	// run and reads no tenant's anything, so the caller's identity has nothing
	// to scope.
	//
	// The bounds, though, are enforced here and not left to an interceptor. The
	// schema declares them — sixty-four files, a megabyte each — and the CLI
	// installs a protovalidate interceptor that enforces them, but an embedder
	// mounting this handler directly gets no interceptor, and a bound enforced
	// by a caller's configuration fails open for whoever wired it up without
	// one. Run learned this exact lesson (see the comment there); a handler that
	// parses attacker-sized input checks its own request.
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	reports := make([]*v1.DiagnosticReport, 0, len(req.Msg.GetFiles()))

	for _, file := range req.Msg.GetFiles() {
		diagnostics, err := flowfile.ValidateSource(file.GetSource())
		if err != nil {
			var parsed flowfile.Diagnostics
			if !errors.As(err, &parsed) {
				// Not a shape the parser could position — a document that is not
				// YAML at all. Still the file's problem rather than the caller's,
				// so it is a diagnostic rather than an RPC error: an RPC error
				// here would fail the whole request over one bad file, and a
				// caller checking sixty-four files wants sixty-four answers.
				parsed = flowfile.Diagnostics{{Message: err.Error()}}
			}
			diagnostics = parsed
		}

		reports = append(reports, diagnostics.Report(file.GetName()))
	}

	return connect.NewResponse(&v1.ValidateResponse{
		Report: &v1.ValidationReport{Files: reports},
	}), nil
}

// GetCatalog reports what this deployment can execute.
func (s *FlowstateServer) GetCatalog(
	ctx context.Context,
	req *connect.Request[v1.GetCatalogRequest],
) (*connect.Response[v1.GetCatalogResponse], error) {
	// Nothing to check today — the request is empty — and checked anyway, so the
	// habit holds when a filter field arrives and so this handler reads like its
	// siblings.
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// The process-wide catalog, deliberately. It reads the default registry, so
	// a deployment that extended this process — a worker's plugins, a build with
	// more built-ins — answers with what it can actually do, and a stock one
	// answers with the built-ins. What a *different* process can do is not this
	// server's to claim: a plugin loaded by a worker somewhere else is real and
	// invisible here, which is the same split `flow validate` has with plugin
	// tasks, and closing it is the worker-introspection problem rather than a
	// bigger version of this handler.
	return connect.NewResponse(&v1.GetCatalogResponse{Catalog: v1.Catalog()}), nil
}
