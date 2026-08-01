package server_test

import (
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// Neither handler touches Temporal, which the nil client is the proof of: a
// regression that made checking a file read a run's state would panic here
// before it confused anybody in production.
func validateServer(t *testing.T) *server.FlowstateServer {
	t.Helper()

	return server.New(nil)
}

const aValidFile = `edition: v2026.2
name: remote-check
steps:
  - id: only
    log:
      message: checked without a filesystem
`

// TestValidateAnswersWithTheSameReportTheCommandPrints is the contract: one
// validator, two callers, zero drift.
func TestValidateAnswersWithTheSameReportTheCommandPrints(t *testing.T) {
	t.Parallel()

	s := validateServer(t)

	resp, err := s.Validate(t.Context(), connect.NewRequest(&v1.ValidateRequest{
		Files: []*v1.SourceFile{
			{Name: "clean.yaml", Source: []byte(aValidFile)},
			{Name: "broken.yaml", Source: []byte(strings.Replace(aValidFile, "log:", "lg:", 1))},
		},
	}))
	require.NoError(t, err)

	files := resp.Msg.GetReport().GetFiles()
	require.Len(t, files, 2,
		"a clean file was dropped from the report; 'checked and clean' and 'not checked' "+
			"are different facts and a consumer cannot tell them apart without the entry")

	assert.Equal(t, "clean.yaml", files[0].GetFile())
	assert.Empty(t, files[0].GetDiagnostics())

	require.NotEmpty(t, files[1].GetDiagnostics(),
		"a file with an unknown step key came back clean")

	d := files[1].GetDiagnostics()[0]
	assert.NotZero(t, d.GetLine(),
		"the diagnostic has no position, so a caller cannot point an editor at it")
	assert.Contains(t, d.GetMessage(), "lg",
		"the diagnostic does not name what the author wrote")
}

// TestAFileThatIsNotYAMLIsADiagnosticNotAnRPCError pins the failure boundary.
//
// A caller checking sixty-four files wants sixty-four answers. Failing the
// request over one unparseable file would take the other sixty-three answers
// with it — so a bad file is a report entry, and an RPC error is reserved for
// requests the schema refuses.
func TestAFileThatIsNotYAMLIsADiagnosticNotAnRPCError(t *testing.T) {
	t.Parallel()

	s := validateServer(t)

	resp, err := s.Validate(t.Context(), connect.NewRequest(&v1.ValidateRequest{
		Files: []*v1.SourceFile{
			{Name: "not-yaml.yaml", Source: []byte("\t{{{")},
			{Name: "clean.yaml", Source: []byte(aValidFile)},
		},
	}))
	require.NoError(t, err,
		"one unparseable file failed the whole request, taking the other file's answer with it")

	files := resp.Msg.GetReport().GetFiles()
	require.Len(t, files, 2)
	assert.NotEmpty(t, files[0].GetDiagnostics(), "an unparseable file came back clean")
	assert.Empty(t, files[1].GetDiagnostics(),
		"a clean file after a broken one picked up the broken one's diagnostics")
}

// TestGetCatalogAnswersWithTheCatalog.
//
// Thin because the handler is thin, and the assertions are on the properties a
// remote consumer depends on rather than on the catalog's content, which
// catalog_test.go already holds to the registry.
func TestGetCatalogAnswersWithTheCatalog(t *testing.T) {
	t.Parallel()

	s := validateServer(t)

	resp, err := s.GetCatalog(t.Context(), connect.NewRequest(&v1.GetCatalogRequest{}))
	require.NoError(t, err)

	catalog := resp.Msg.GetCatalog()
	require.NotNil(t, catalog)

	names := make([]string, 0, len(catalog.GetTasks()))
	for _, task := range catalog.GetTasks() {
		names = append(names, task.GetName())
	}

	assert.Contains(t, names, "log")
	assert.Contains(t, names, "http")
	assert.NotEmpty(t, catalog.GetCelFunctions(),
		"the catalog names no functions, so an agent reading it would conclude "+
			"expressions can call nothing")
}

// TestTheHandlerEnforcesItsOwnBounds is here because review caught it missing.
//
// The CLI installs a protovalidate interceptor, so through `flow server` the
// schema's bounds held — and an embedder mounting the handler directly got no
// interceptor and no bounds, which is a bound enforced by a caller's
// configuration: it fails open for whoever wires it up without one. These tests
// call the handler with no interceptor in front, which is exactly that embedder.
func TestTheHandlerEnforcesItsOwnBounds(t *testing.T) {
	t.Parallel()

	s := validateServer(t)

	t.Run("too many files", func(t *testing.T) {
		t.Parallel()

		files := make([]*v1.SourceFile, 65)
		for i := range files {
			files[i] = &v1.SourceFile{Name: "f.yaml", Source: []byte(aValidFile)}
		}

		_, err := s.Validate(t.Context(), connect.NewRequest(&v1.ValidateRequest{Files: files}))
		require.Error(t, err,
			"sixty-five files were parsed; the schema's bound holds only behind an "+
				"interceptor an embedder does not have")
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("no files", func(t *testing.T) {
		t.Parallel()

		_, err := s.Validate(t.Context(), connect.NewRequest(&v1.ValidateRequest{}))
		require.Error(t, err, "an empty request was accepted; the schema requires a file")
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("an oversized file", func(t *testing.T) {
		t.Parallel()

		_, err := s.Validate(t.Context(), connect.NewRequest(&v1.ValidateRequest{
			Files: []*v1.SourceFile{{Name: "big.yaml", Source: make([]byte, 1<<20+1)}},
		}))
		require.Error(t, err,
			"a file over the schema's megabyte bound reached the parser")
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

// TestCompileAnswersWithWhatRunTakes closes the loop review found open: an
// agent could check a file and could submit a specification, with nothing on
// the wire connecting the two.
func TestCompileAnswersWithWhatRunTakes(t *testing.T) {
	t.Parallel()

	s := validateServer(t)

	t.Run("a clean file compiles to a specification", func(t *testing.T) {
		t.Parallel()

		resp, err := s.Compile(t.Context(), connect.NewRequest(&v1.CompileRequest{
			File: &v1.SourceFile{Name: "clean.yaml", Source: []byte(aValidFile)},
		}))
		require.NoError(t, err)

		workflow := resp.Msg.GetWorkflow()
		require.NotNil(t, workflow, "a clean file compiled to nothing")
		assert.Equal(t, "remote-check", workflow.GetName())
		require.Len(t, workflow.GetSteps(), 1)

		// The artifact is what Run submits, so it must pass the same check Run
		// applies at submit — otherwise this hands out specifications the next
		// call refuses.
		assert.NoError(t, v1.Validate(workflow))

		// Present and empty rather than absent: "compiled clean" is stated.
		require.NotNil(t, resp.Msg.GetReport())
		assert.Empty(t, resp.Msg.GetReport().GetDiagnostics())
	})

	t.Run("a broken file answers with diagnostics and no specification", func(t *testing.T) {
		t.Parallel()

		resp, err := s.Compile(t.Context(), connect.NewRequest(&v1.CompileRequest{
			File: &v1.SourceFile{
				Name:   "broken.yaml",
				Source: []byte(strings.Replace(aValidFile, "log:", "lg:", 1)),
			},
		}))
		require.NoError(t, err,
			"a file with a problem failed the RPC; the diagnostics are the answer")

		assert.Nil(t, resp.Msg.GetWorkflow(),
			"a specification was handed out beside a list of its problems")
		assert.NotEmpty(t, resp.Msg.GetReport().GetDiagnostics())
	})

	t.Run("the bounds hold here too", func(t *testing.T) {
		t.Parallel()

		_, err := s.Compile(t.Context(), connect.NewRequest(&v1.CompileRequest{}))
		require.Error(t, err, "a request with no file was accepted")
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

func TestCompileRejectsUnknownDeploymentCredentialTarget(t *testing.T) {
	s := server.New(nil, server.WithCredentialTargets("partner-api"))
	source := []byte(`edition: v2026.2
name: federated
steps:
  - id: call
    http:
      url: https://api.example.com
      credential: aws-prod
`)
	response, err := s.Compile(t.Context(), connect.NewRequest(&v1.CompileRequest{
		File: &v1.SourceFile{Name: "workflow.yaml", Source: source},
	}))
	require.NoError(t, err)
	require.Nil(t, response.Msg.GetWorkflow())
	require.Len(t, response.Msg.GetReport().GetDiagnostics(), 1)
	require.Contains(t, response.Msg.GetReport().GetDiagnostics()[0].GetMessage(),
		`credential target "aws-prod" is not configured`)
}
