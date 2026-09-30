package envelope_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
)

// TestEveryDecodeErrorIsClassified: workflow code fails a run on a payload
// this process cannot read, and treats every other decode error as the
// payload's own, so a sentinel left unclassified is a refusal that drops a
// signal on one worker and replays on another. Twice a worker-local refusal
// shipped unclassified (ErrKeyDenied, then ErrSuiteRefused, both Codex on
// #2167), so every exported Err sentinel here must be named below with the
// class it belongs to, and a new one fails this test until it is.
func TestEveryDecodeErrorIsClassified(t *testing.T) {
	t.Parallel()

	const (
		notReadableHere = "not readable here" // this process's refusal; another worker may read it
		unavailable     = "unavailable"       // this process cannot ask right now
		payloadsFault   = "the payload's"     // the payload itself is wrong
		notADecode      = "not a decode"      // raised by something other than Decode
	)
	classified := map[string]struct {
		err   error
		class string
	}{
		"ErrUnknownKey":          {envelope.ErrUnknownKey, notReadableHere},
		"ErrUnknownVersion":      {envelope.ErrUnknownVersion, notReadableHere},
		"ErrKeyDenied":           {envelope.ErrKeyDenied, notReadableHere},
		"ErrSuiteRefused":        {envelope.ErrSuiteRefused, notReadableHere},
		"ErrUnencrypted":         {envelope.ErrUnencrypted, notReadableHere},
		"ErrProviderUnavailable": {envelope.ErrProviderUnavailable, unavailable},
		"ErrMalformed":           {envelope.ErrMalformed, payloadsFault},
		"ErrAuthentication":      {envelope.ErrAuthentication, payloadsFault},
		"ErrReaderCannotEncode":  {envelope.ErrReaderCannotEncode, notADecode},
	}

	for name, c := range classified {
		switch c.class {
		case notReadableHere:
			require.ErrorIs(t, c.err, payloadcodec.ErrNotReadableHere, name)
		case unavailable:
			require.ErrorIs(t, c.err, payloadcodec.ErrUnavailable, name)
		default:
			require.NotErrorIs(t, c.err, payloadcodec.ErrNotReadableHere, name)
			require.NotErrorIs(t, c.err, payloadcodec.ErrUnavailable, name)
		}
	}

	// Every exported Err sentinel the package declares is in the table.
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	declared := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				for _, name := range spec.(*ast.ValueSpec).Names {
					if !strings.HasPrefix(name.Name, "Err") {
						continue
					}
					declared++
					_, ok := classified[name.Name]
					require.True(t, ok, "%s: %s is not classified in TestEveryDecodeErrorIsClassified",
						fset.Position(name.Pos()), name.Name)
				}
			}
		}
	}
	require.Equal(t, len(classified), declared, "the table names a sentinel the package no longer declares")
}
