package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRunServerResolvesTheTemporalConfigurationOnce forbids the defect rather than
// checking for its absence today.
//
// `flow server` dials one Temporal client and then, much later, needs that
// client's namespace named: `server.WithTemporalNamespace` records it, and
// `AddSearchAttributes` carries it as a request field. The obvious way to get it
// is `cfg.Options().Namespace`, and that is wrong — [temporalclient.Config.Options]
// reads the process environment and a TOML file on every call, so a second
// resolution is a fresh question rather than a copy of the first answer.
// TestASecondResolutionCanDisagreeWithTheDial, in the temporalclient package,
// makes the two disagree to prove the window is real; between them in this
// function sit plugin subprocesses starting, secret providers loading and a
// webhook receiver compiling Flowfiles.
//
// What a disagreement produces is the failure this whole seam exists to prevent: a
// namespace recorded beside a client that is not connected to it, used to address
// a raw request that then succeeds against another tenant's namespace, with
// nothing anywhere saying so. It is unreachable by an ordinary test — asserting
// what `runServer` passes needs a whole server stood up, and reproducing the
// divergence needs the configuration to change mid-startup — so the claim is made
// against the source, the way TestDevBannerSaysWhatTheReplacedFlagsSay makes its
// own (Codex, #1139).
//
// One assertion is enough, and deliberately: reverting the dial to
// [temporalclient.Dial] without also re-resolving does not compile, because
// nothing else in this function supplies the namespace. So every way back to the
// defect passes through a second Options() call, and counting those catches all
// of them. Adding a second assertion about *how* the client is dialed would fire
// on an ordinary refactor that extracts the dial into a helper and carries the
// value correctly, which is a check crying wolf rather than a check.
func TestRunServerResolvesTheTemporalConfigurationOnce(t *testing.T) {
	t.Parallel()

	body := functionBody(t, "main.go", "runServer")

	var resolutions []token.Pos
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Options" {
			resolutions = append(resolutions, selector.Sel.Pos())
		}
		return true
	})

	require.Empty(t, resolutions,
		"runServer resolves a configuration it already resolved at the dial. Options() reads the "+
			"environment and a TOML file on every call, so this answer can differ from the namespace "+
			"the client beside it is connected to — carry the value DialWithNamespace returned "+
			"instead of asking again")
}

// functionBody parses one Go file and returns the named top-level function's body.
//
// It fails the test when the function is absent, so a rename is a failure that
// names the function rather than a walk over nothing quietly asserting that no
// defect was found — which is the vacuous shape a source-reading test falls into
// most easily.
func functionBody(t *testing.T, path, name string) *ast.BlockStmt {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	require.NoError(t, err, "parsing %s", path)

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name || fn.Recv != nil {
			continue
		}
		require.NotNil(t, fn.Body, "%s in %s has no body", name, path)
		return fn.Body
	}

	t.Fatalf("no func %s in %s; if it was renamed, this test has to be pointed at the new name "+
		"rather than deleted — the claim it makes still applies", name, path)
	return nil
}
