package plugin

import (
	"context"
	"fmt"
	"maps"
	"slices"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// SignatureVerifier is the trust boundary for artifact signatures. Implementations
// must verify digest binding as well as signer identity.
type SignatureVerifier interface {
	VerifyPlugin(context.Context, string, string, *flowstatev1.PluginArtifactProvenance, []string) error
}

// ProfileSelection binds one plugin name to deployment-owned admission and
// isolation profiles. This boundary-only routing value is deliberately not a
// protobuf: unlike the profiles, it never travels or enters workflow history.
type ProfileSelection struct {
	Admission string
	Isolation string
}

func (c Config) validateProfiles() error {
	for _, name := range slices.Sorted(maps.Keys(c.AdmissionProfiles)) {
		p := c.AdmissionProfiles[name]
		if p == nil || p.GetName() != name || !validPluginName(name) {
			return fmt.Errorf("%w: invalid admission profile %q", ErrAdmissionProfile, name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(c.IsolationProfiles)) {
		p := c.IsolationProfiles[name]
		if p == nil || p.GetName() != name || !validPluginName(name) {
			return fmt.Errorf("%w: invalid isolation profile %q", ErrIsolationProfile, name)
		}
		if p.GetDropPrivileges() && (p.Uid == nil || p.Gid == nil) {
			return fmt.Errorf("%w: profile %q drops privileges but does not declare both uid and gid", ErrIsolationProfile, name)
		}
	}
	for _, plugin := range slices.Sorted(maps.Keys(c.PluginProfiles)) {
		if !validPluginName(plugin) {
			return fmt.Errorf("%w: invalid plugin name %q", ErrAdmissionProfile, plugin)
		}
		sel := c.PluginProfiles[plugin]
		if _, ok := c.AdmissionProfiles[sel.Admission]; !ok {
			return fmt.Errorf("%w: plugin %q selects unknown admission profile %q", ErrAdmissionProfile, plugin, sel.Admission)
		}
		if _, ok := c.IsolationProfiles[sel.Isolation]; !ok {
			return fmt.Errorf("%w: plugin %q selects unknown isolation profile %q", ErrIsolationProfile, plugin, sel.Isolation)
		}
	}
	return nil
}

func (c Config) isolationFor(name string) *flowstatev1.PluginIsolationProfile {
	return c.IsolationProfiles[c.PluginProfiles[name].Isolation]
}
