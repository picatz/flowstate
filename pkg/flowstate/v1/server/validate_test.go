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
