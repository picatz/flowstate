package main

import (
	"context"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	sshv1 "github.com/picatz/flowstate/plugins/ssh/gen/ssh/v1"
)

// operatorGrants is the authority this plugin was configured with, read once at
// startup. Nil means the file was missing or unusable, and [grantsRefusal] says
// which - every call is refused with that reason rather than with a denial of
// this plugin's invention.
var (
	operatorGrants *grants
	grantsRefusal  error
)

func main() {
	installEgressPolicy()
	loadOperatorGrants()

	sdk.Main(sdk.Plugin{
		Name:        "ssh",
		Version:     "0.1.0",
		Description: "Runs operator-granted commands on operator-granted hosts over SSH: pinned host keys, no shell, no arbitrary command line.",
		Tasks: []sdk.Task{{
			Name:    "run",
			Summary: "Run one command grant on one host grant, filling the placeholders the grant declares.",
			Input:   &sshv1.RunInputs{},
			Output:  &sshv1.RunOutputs{},
			Fn:      sshRun,
		}},
		Health: checkHealth,
	})
}

// loadOperatorGrants reads the grants file, keeping the reason it could not.
//
// A missing or invalid file does not stop the process. Discovery, validation
// and the catalog are what a worker needs from a plugin it has not configured
// yet, and they need no authority at all; ssh.run refuses at its own boundary
// with the reason recorded here.
func loadOperatorGrants() {
	parsed, err := loadGrants()
	if err != nil {
		grantsRefusal = err
		fmt.Fprintf(os.Stderr, "ssh: no usable grants: %v\n", err)
		return
	}
	operatorGrants = parsed
}

// checkHealth reports whether this plugin could serve a call at all.
//
// Unlike the registry and directory plugins, this one has something local and
// honest to answer with: without grants there is no host it could ever reach,
// and an operator restarting a worker after fixing the file should see that in
// the health of the plugin rather than in the first runbook that fails.
func checkHealth(_ context.Context) error {
	if operatorGrants == nil {
		return fmt.Errorf("no usable grants: %v", grantsRefusal)
	}
	if egressPolicy == nil {
		return fmt.Errorf("no usable egress policy: %v", egressRefusalReason())
	}
	return nil
}
