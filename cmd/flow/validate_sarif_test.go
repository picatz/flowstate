package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sarifLog struct {
	Version string `json:"version"`
	Runs    []struct {
		Results []struct {
			RuleID    string `json:"ruleId"`
			Locations []struct {
				PhysicalLocation struct {
					ArtifactLocation struct {
						URI string `json:"uri"`
					} `json:"artifactLocation"`
					Region *struct {
						StartLine uint32 `json:"startLine"`
					} `json:"region"`
				} `json:"physicalLocation"`
			} `json:"locations"`
		} `json:"results"`
	} `json:"runs"`
}

func decodeSARIF(t *testing.T, out string) sarifLog {
	t.Helper()

	var log sarifLog
	require.NoError(t, json.Unmarshal([]byte(out), &log), out)
	require.Equal(t, "2.1.0", log.Version)
	require.Len(t, log.Runs, 1)

	return log
}

func TestValidateWritesSARIFForAFileWithAProblem(t *testing.T) {
	path := writeWorkflow(t, "broken.yaml", brokenWorkflow)

	res := runFlow(t, "validate", path, "-o", "sarif")

	require.Error(t, res.Err, "a file with problems still fails the command")
	log := decodeSARIF(t, res.Stdout)
	require.NotEmpty(t, log.Runs[0].Results)
	result := log.Runs[0].Results[0]
	assert.NotEmpty(t, result.RuleID)
	require.NotEmpty(t, result.Locations)
	assert.Equal(t, "file://"+path, result.Locations[0].PhysicalLocation.ArtifactLocation.URI)
	require.NotNil(t, result.Locations[0].PhysicalLocation.Region)
	assert.Positive(t, result.Locations[0].PhysicalLocation.Region.StartLine)
}

func TestValidateWritesAnEmptySARIFRunForACleanFile(t *testing.T) {
	path := writeWorkflow(t, "clean.yaml", cleanWorkflow)

	res := runFlow(t, "validate", path, "-o", "sarif")

	require.NoError(t, res.Err, res.Output())
	assert.JSONEq(t, `[]`, mustMarshal(t, decodeSARIF(t, res.Stdout).Runs[0].Results), "checked and clean is an empty run")
}

func TestValidateSpecWritesSARIFWithTheMappedLocation(t *testing.T) {
	spec, mapPath, source := brokenSpec(t)

	res := runFlow(t, "validate", "--spec", spec, "--source-map", mapPath, "-o", "sarif")

	require.Error(t, res.Err)
	results := decodeSARIF(t, res.Stdout).Runs[0].Results
	require.NotEmpty(t, results)
	var line uint32
	for _, r := range results {
		for _, l := range r.Locations {
			if l.PhysicalLocation.ArtifactLocation.URI == "file://"+source && l.PhysicalLocation.Region != nil {
				line = l.PhysicalLocation.Region.StartLine
			}
		}
	}
	assert.EqualValues(t, 7, line, "the mapped step's line in the Flowfile")
}

// SARIF is offered where diagnostics are the answer, not everywhere `-o` is.
func TestOtherVerbsDoNotAcceptSARIF(t *testing.T) {
	path := writeWorkflow(t, "clean.yaml", cleanWorkflow)

	res := runFlow(t, "compile", path, "-o", "sarif")

	require.Error(t, res.Err)
	assert.Contains(t, res.Output(), "not a format")
	assert.NotContains(t, res.Output(), "sarif,", "its help does not list a format it refuses")
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()

	data, err := json.Marshal(v)
	require.NoError(t, err)

	return string(data)
}
