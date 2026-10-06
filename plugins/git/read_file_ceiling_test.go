package main

import (
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	gitv1 "github.com/picatz/flowstate/plugins/git/gen/git/v1"
)

// TestReadFileCeilingFitsATaskOutput pins #2168: the largest file read_file
// admits, encoded as the step output the host measures, is within
// flowstatev1.MaxTaskOutputBytes, and one byte more than the ceiling is
// refused here rather than later by the host.
func TestReadFileCeilingFitsATaskOutput(t *testing.T) {
	// Non-zero bytes so a compact encoding cannot hide the size.
	content := make([]byte, maxReadFileBytes)
	for i := range content {
		content[i] = 0xff
	}

	out, err := sdk.EncodeOutputs(&gitv1.ReadFileOutputs{
		Content: content,
		Size:    int64(len(content)),
		Mode:    "100755",
		Binary:  true,
	})
	if err != nil {
		t.Fatalf("EncodeOutputs: %v", err)
	}
	if err := flowstatev1.CheckTaskOutputSize(out); err != nil {
		t.Fatalf("a read_file at its own ceiling (%d bytes) is refused by the host: %v", maxReadFileBytes, err)
	}
}
