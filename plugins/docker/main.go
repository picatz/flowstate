package main

import (
	"context"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	dockerv1 "github.com/picatz/flowstate/plugins/docker/gen/docker/v1"
)

// operatorGrants is the authority this plugin was configured with, read once at
// startup. Nil means the file was missing or unusable and [grantsRefusal] says
// which.
var (
	operatorGrants *grants
	grantsRefusal  error
)

func main() {
	loadOperatorGrants()

	sdk.Main(sdk.Plugin{
		Name:        "docker",
		Version:     "0.1.0",
		Description: "Runs one operator-granted container to completion: digest-pinned image, explicit argv, no network, read-only root, bounded and removed.",
		Tasks: []sdk.Task{{
			Name:    "run",
			Summary: "Run one container grant to completion, filling the placeholders it declares, and return its bounded output.",
			Input:   &dockerv1.RunInputs{},
			Output:  &dockerv1.RunOutputs{},
			Fn:      dockerRun,
		}},
		Health: checkHealth,
	})
}

// loadOperatorGrants reads the grants file, keeping the reason it could not.
//
// A missing or invalid file does not stop the process: discovery, validation
// and the catalog are what a worker needs from a plugin it has not configured
// yet, and they need no authority at all.
func loadOperatorGrants() {
	parsed, err := loadGrants()
	if err != nil {
		grantsRefusal = err
		fmt.Fprintf(os.Stderr, "docker: no usable grants: %v\n", err)
		return
	}
	operatorGrants = parsed
}

// checkHealth reports whether this plugin could serve a call at all. Without
// grants there is no daemon and no run, and an operator who fixes the file and
// restarts should see that in the plugin's health rather than in the first
// workflow that fails.
func checkHealth(_ context.Context) error {
	if operatorGrants == nil {
		return fmt.Errorf("no usable grants: %v", grantsRefusal)
	}
	return nil
}
