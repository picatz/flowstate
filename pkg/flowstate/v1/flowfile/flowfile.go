package flowfile

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Unmarshal compiles a Flowfile into the workflow it describes.
//
// It is [Parse] without the source positions, for a caller that only needs the
// workflow. Problems are returned as [Diagnostics], one per problem, each carrying
// the line and column it was found on.
func Unmarshal(data []byte) (*v1.Workflow, error) {
	workflow, _, err := Parse(data)
	if err != nil {
		return nil, err
	}
	return workflow, nil
}
