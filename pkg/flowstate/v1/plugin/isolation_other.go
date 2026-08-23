//go:build !linux

package plugin

import (
	"fmt"
	"os/exec"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func configureIsolation(_ *exec.Cmd, p *flowstatev1.PluginIsolationProfile) error {
	if p == nil {
		return nil
	}
	return fmt.Errorf("%w: profile %q is required but this platform launcher provides no isolation controls", ErrIsolationProfile, p.GetName())
}
