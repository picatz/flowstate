package flowstatev1

import (
	"fmt"
	"strconv"
	"strings"
)

// ValidPluginVersion reports whether v is the deliberately small Flowfile
// semver grammar: an explicit v prefix and three non-negative numeric parts.
func ValidPluginVersion(v string) bool {
	_, ok := parsePluginVersion(v)
	return ok
}

func parsePluginVersion(v string) ([3]uint64, bool) {
	var out [3]uint64
	parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
	if !strings.HasPrefix(v, "v") || len(parts) != 3 {
		return out, false
	}
	for i, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') {
			return out, false
		}
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return out, false
		}
		out[i] = n
	}
	return out, true
}

// ResolvePlugins selects and records the deployment plugins required by wf.
// It mutates wf only after every requirement has resolved, so refusal cannot
// leave a partly pinned specification behind.
func ResolvePlugins(wf *Workflow, catalog *PluginCatalog) error {
	available := make(map[string]*PluginDescription)
	if catalog != nil {
		for _, p := range catalog.GetPlugins() {
			available[p.GetName()] = p
		}
	}
	resolved := make([]*ResolvedPlugin, 0, len(wf.GetPluginRequirements()))
	for _, requirement := range wf.GetPluginRequirements() {
		want, ok := parsePluginVersion(requirement.GetMinimumVersion())
		if !ok {
			return fmt.Errorf("plugin %q has invalid minimum version %q", requirement.GetName(), requirement.GetMinimumVersion())
		}
		p := available[requirement.GetName()]
		if p == nil {
			return fmt.Errorf("required plugin %q is not installed", requirement.GetName())
		}
		have, ok := parsePluginVersion(p.GetVersion())
		if !ok {
			return fmt.Errorf("plugin %q advertises invalid version %q", p.GetName(), p.GetVersion())
		}
		if have[0] != want[0] {
			return fmt.Errorf("plugin %q major version is v%d, need v%d", p.GetName(), have[0], want[0])
		}
		if versionLess(have, want) {
			return fmt.Errorf("plugin %q version %s is below required minimum %s", p.GetName(), p.GetVersion(), requirement.GetMinimumVersion())
		}
		if p.GetProtocolVersion() == 0 || p.GetTaskSchemaDigest() == "" || p.GetDistributionDigest() == "" {
			return fmt.Errorf("plugin %q catalog entry is incomplete", p.GetName())
		}
		resolved = append(resolved, &ResolvedPlugin{Name: p.GetName(), Version: p.GetVersion(), ProtocolVersion: p.GetProtocolVersion(), TaskSchemaDigest: p.GetTaskSchemaDigest(), DistributionDigest: p.GetDistributionDigest()})
	}
	wf.ResolvedPlugins = resolved
	return nil
}

func versionLess(a, b [3]uint64) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// CheckResolvedPlugins is the worker-side replay guard. A rolling deployment
// may move a run only to a worker whose catalog resolves to the identical
// behavior tuple selected at submission.
func CheckResolvedPlugins(wf *Workflow, catalog *PluginCatalog) error {
	want := wf.GetResolvedPlugins()
	clone := &Workflow{PluginRequirements: wf.GetPluginRequirements()}
	if err := ResolvePlugins(clone, catalog); err != nil {
		return err
	}
	if len(want) != len(clone.GetResolvedPlugins()) {
		return fmt.Errorf("resolved plugin contract is incomplete")
	}
	for i, expected := range want {
		actual := clone.GetResolvedPlugins()[i]
		if expected.GetName() != actual.GetName() || expected.GetVersion() != actual.GetVersion() || expected.GetProtocolVersion() != actual.GetProtocolVersion() || expected.GetTaskSchemaDigest() != actual.GetTaskSchemaDigest() || expected.GetDistributionDigest() != actual.GetDistributionDigest() {
			return fmt.Errorf("plugin %q does not match the run's replay contract", expected.GetName())
		}
	}
	return nil
}
