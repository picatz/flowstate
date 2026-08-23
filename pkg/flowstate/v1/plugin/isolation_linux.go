//go:build linux

package plugin

import (
	"fmt"
	"os/exec"
	"syscall"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// configureIsolation applies controls os/exec can install atomically in the
// child before exec. Controls requiring a supervised mount/cgroup/seccomp
// helper are refused until that helper exists; pretending they were applied is
// a security failure.
func configureIsolation(cmd *exec.Cmd, p *flowstatev1.PluginIsolationProfile) error {
	if p == nil {
		return nil
	}
	if p.GetReadOnlyRoot() || len(p.GetMounts()) != 0 || p.GetTemporaryStorageBytes() != 0 ||
		p.GetMaxProcesses() != 0 || p.GetCpuTimeSeconds() != 0 || p.GetMemoryBytes() != 0 ||
		p.GetMaxOpenFiles() != 0 || len(p.GetAllowedSyscalls()) != 0 {
		return fmt.Errorf("%w: profile %q requests mount, storage, resource, or syscall controls not atomically supported by this launcher", ErrIsolationProfile, p.GetName())
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	if p.GetDropPrivileges() {
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: p.GetUid(), Gid: p.GetGid(), NoSetGroups: true}
	}
	switch p.GetNetwork() {
	case flowstatev1.PluginNetworkIsolation_PLUGIN_NETWORK_ISOLATION_UNSPECIFIED,
		flowstatev1.PluginNetworkIsolation_PLUGIN_NETWORK_ISOLATION_HOST:
	case flowstatev1.PluginNetworkIsolation_PLUGIN_NETWORK_ISOLATION_NONE:
		cmd.SysProcAttr.Cloneflags |= syscall.CLONE_NEWNET
	case flowstatev1.PluginNetworkIsolation_PLUGIN_NETWORK_ISOLATION_NETPOLICY:
		return fmt.Errorf("%w: profile %q requires the netpolicy proxy, but none is configured", ErrIsolationProfile, p.GetName())
	default:
		return fmt.Errorf("%w: profile %q has unknown network isolation %d", ErrIsolationProfile, p.GetName(), p.GetNetwork())
	}
	return nil
}
