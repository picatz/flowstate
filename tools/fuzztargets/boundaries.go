package fuzztargets

import (
	_ "embed"
	"fmt"
	"strings"
)

//go:embed boundaries.txt
var boundariesFile string

// A Boundary is one parser that takes bytes across a trust boundary and the
// fuzz target that covers it.
type Boundary struct {
	// Dir is the parser's package directory relative to the module root.
	Dir string

	// Parser is the function's name, or Type.Method for a method.
	Parser string

	// Target is the fuzz target's name, as targets.txt lists it.
	Target string
}

// Boundaries reads boundaries.txt.
func Boundaries() ([]Boundary, error) {
	var boundaries []Boundary
	for i, line := range strings.Split(boundariesFile, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("boundaries.txt:%d: want three columns (directory, parser, target), got %d: %q", i+1, len(fields), line)
		}
		boundaries = append(boundaries, Boundary{Dir: fields[0], Parser: fields[1], Target: fields[2]})
	}
	return boundaries, nil
}
