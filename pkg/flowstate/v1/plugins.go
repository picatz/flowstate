package flowstatev1

import (
	"fmt"
	"strconv"
	"strings"
)

// ValidPluginVersion reports whether v is the deliberately small Flowfile
// semver grammar: an explicit v prefix and three non-negative numeric parts.
//
// The prefix is required *here* and optional in [parsePluginVersion], and the
// difference is deliberate. This function answers a question about a Flowfile,
// where one spelling keeps `plugins:` blocks comparable by eye. A plugin's
// advertised version arrives from a manifest written by somebody else, and every
// plugin in this tree writes it bare ("0.1.0"), so requiring the prefix on that
// side would refuse every real plugin at the very boundary this feature exists
// to cross.
func ValidPluginVersion(v string) bool {
	if !strings.HasPrefix(v, "v") {
		return false
	}
	_, ok := parsePluginVersion(v)

	return ok
}

// parsePluginVersion reads MAJOR.MINOR.PATCH, with or without a leading v.
//
// Leading zeros are refused because they make two spellings of one version:
// "v1.02.0" and "v1.2.0" would compare equal as numbers and unequal as strings,
// and a replay contract compares strings.
func parsePluginVersion(v string) ([3]uint64, bool) {
	var out [3]uint64
	parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
	if len(parts) != 3 {
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

// maxPluginScanDepth bounds how deeply the plugin walks descend, through loop
// bodies, parallel branches, and inlined callees.
//
// The same bound the other whole-specification walks use, for the same reason
// ([maxVarScanDepth]): a specification built in process is not depth-limited by
// the protobuf runtime, and a recursive walk whose only bound is its caller's
// good manners has no bound at all. Breadth is bounded by [CheckSpecSize],
// which weighs the whole message. A call tree that fans out multiplies bytes at
// every level exactly as it multiplies work here, so the byte bound is what
// keeps a diamond of calls from multiplying this walk unboundedly.
const maxPluginScanDepth = maxVarScanDepth

// walkPluginWorkflows visits wf and every workflow embedded in it by a `call:`.
//
// A callee is compiled into its caller's specification (see flowfile/call.go), so
// its `plugins:` block travels with the submission and is as much a part of what a
// run needs as the top level's. A walk that stopped at the call would pin the half
// of a specification that happens to be spelled at the top.
func walkPluginWorkflows(wf *Workflow, depth int, visit func(wf *Workflow) error) error {
	if wf == nil {
		return nil
	}
	if depth > maxPluginScanDepth {
		// Fail closed: past this the walk cannot say which plugins a specification
		// needs, and a check that cannot decide must not allow.
		return fmt.Errorf("steps nest more than %d deep, past what a specification is checked to; "+
			"nothing this deep can be confirmed to declare the plugins it uses", maxPluginScanDepth)
	}
	if err := visit(wf); err != nil {
		return err
	}

	return walkPluginNodes(wf.GetSteps(), depth, visit)
}

func walkPluginNodes(nodes []*Node, depth int, visit func(wf *Workflow) error) error {
	if depth > maxPluginScanDepth {
		return fmt.Errorf("steps nest more than %d deep, past what a specification is checked to; "+
			"nothing this deep can be confirmed to declare the plugins it uses", maxPluginScanDepth)
	}

	for _, node := range nodes {
		if loop := node.GetForEach(); loop != nil {
			if err := walkPluginNodes(loop.GetBody(), depth+1, visit); err != nil {
				return err
			}
		}
		if loop := node.GetLoop(); loop != nil {
			if err := walkPluginNodes(loop.GetBody(), depth+1, visit); err != nil {
				return err
			}
		}
		if parallel := node.GetParallel(); parallel != nil {
			for _, branch := range parallel.GetBranches() {
				if err := walkPluginNodes(branch.GetSteps(), depth+1, visit); err != nil {
					return err
				}
			}
		}
		if sw := node.GetSwitch(); sw != nil {
			for _, body := range SwitchBodies(sw) {
				if err := walkPluginNodes(body, depth+1, visit); err != nil {
					return err
				}
			}
		}
		if callee := node.GetCall().GetWorkflow(); callee != nil {
			if err := walkPluginWorkflows(callee, depth+1, visit); err != nil {
				return fmt.Errorf("step %q calls workflow %q: %w", node.GetId(), callee.GetName(), err)
			}
		}
	}

	return nil
}

// ResolvePlugins selects and records the deployment plugins required by wf and
// by every workflow it calls.
//
// It runs unconditionally, including for a specification declaring no
// requirements at all, and it always overwrites `resolved_plugins`. That field is
// the control plane's own selection rather than anything a caller may state: a
// submission arriving with one already filled in is describing a deployment it
// does not get to describe, and leaving it in place would persist a replay
// contract no worker's catalog ever produced. So a caller-supplied value is
// discarded here rather than merged, and an empty selection is written as
// emphatically as a full one.
//
// Each workflow in the call tree is pinned against its own requirements, so a
// callee that needs a plugin its caller does not is pinned where it is declared.
//
// A refusal leaves the specification untouched at the workflow that could not
// resolve, and pins nothing further, so no partly pinned specification is left
// behind by a submission that was refused.
func ResolvePlugins(wf *Workflow, catalog *PluginCatalog) error {
	available := make(map[string]*PluginDescription)
	for _, p := range catalog.GetPlugins() {
		available[p.GetName()] = p
	}

	return walkPluginWorkflows(wf, 0, func(wf *Workflow) error {
		resolved, err := resolveOne(wf, available, catalog.GetClaimsSchemaVersion())
		if err != nil {
			return err
		}
		wf.ResolvedPlugins = resolved

		return nil
	})
}

// resolveOne selects the plugins one workflow requires, or says why it cannot.
//
// The refusals name the plugin, what the file asked for, and what the deployment
// has, because that triple is the whole of what somebody needs to act: install
// it, raise the deployment's version, or lower the file's floor.
//
// claimsSchemaVersion is the catalog's own [PluginCatalog.ClaimsSchemaVersion]
// rather than anything per-plugin — one build computes every plugin's claim
// fields under the same schema version, so it is read once here and pinned
// onto every resolution from this catalog.
func resolveOne(wf *Workflow, available map[string]*PluginDescription, claimsSchemaVersion uint32) ([]*ResolvedPlugin, error) {
	resolved := make([]*ResolvedPlugin, 0, len(wf.GetPluginRequirements()))
	for _, requirement := range wf.GetPluginRequirements() {
		want, ok := parsePluginVersion(requirement.GetMinimumVersion())
		if !ok {
			return nil, fmt.Errorf("plugin %q has invalid minimum version %q; write it as vMAJOR.MINOR.PATCH",
				requirement.GetName(), requirement.GetMinimumVersion())
		}
		p := available[requirement.GetName()]
		if p == nil {
			return nil, fmt.Errorf("required plugin %q is not installed on this deployment; "+
				"install it where the workers look for plugins, or drop it from `plugins:`", requirement.GetName())
		}
		have, ok := parsePluginVersion(p.GetVersion())
		if !ok {
			return nil, fmt.Errorf("plugin %q advertises invalid version %q", p.GetName(), p.GetVersion())
		}
		if have[0] != want[0] {
			return nil, fmt.Errorf("plugin %q is v%d.%d.%d on this deployment and the file requires v%d.x; "+
				"a major version is a different contract, so neither side can stand in for the other",
				p.GetName(), have[0], have[1], have[2], want[0])
		}
		if versionLess(have, want) {
			return nil, fmt.Errorf("plugin %q is %s on this deployment, below the %s the file requires",
				p.GetName(), p.GetVersion(), requirement.GetMinimumVersion())
		}
		if p.GetProtocolVersion() == 0 || p.GetTaskSchemaDigest() == "" || p.GetDistributionDigest() == "" || p.GetClaimsDigest() == "" {
			return nil, fmt.Errorf("plugin %q catalog entry is incomplete, so there is nothing to pin: "+
				"a run is pinned to a protocol version, a task schema digest, a claims digest and a "+
				"distribution digest, and this deployment reported %q at protocol %d",
				p.GetName(), p.GetVersion(), p.GetProtocolVersion())
		}
		resolved = append(resolved, pinOf(p, claimsSchemaVersion))
	}

	return resolved, nil
}

// pinOf is the one place a catalog entry becomes a replay contract.
//
// One function because the selection made at submit and the tuple a worker
// compares against have to be the same fields read the same way: two copies
// of this is how a field added to the contract ends up checked on one side only.
//
// A submission always resolves against a *live* catalog — the deployment it is
// submitted to, right now — so ClaimsDigest and claimsSchemaVersion are always
// populated here even though [sameResolvedPlugin] treats either as zero-valued
// as "not asserted": that leniency exists only for a pin already durable when
// the field was added, never for a new one.
//
// claimsSchemaVersion comes from the catalog rather than from p, because it
// is [PluginCatalog.ClaimsSchemaVersion] — one value for the whole catalog,
// not a per-plugin field on [PluginDescription].
func pinOf(p *PluginDescription, claimsSchemaVersion uint32) *ResolvedPlugin {
	return &ResolvedPlugin{
		Name:                p.GetName(),
		Version:             p.GetVersion(),
		ProtocolVersion:     p.GetProtocolVersion(),
		TaskSchemaDigest:    p.GetTaskSchemaDigest(),
		DistributionDigest:  p.GetDistributionDigest(),
		ClaimsDigest:        p.GetClaimsDigest(),
		ClaimsSchemaVersion: claimsSchemaVersion,
	}
}

func versionLess(a, b [3]uint64) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}

	return false
}

// PinnedPlugins is a run's replay contract, flattened out of its specification.
//
// It is a pure function of the specification and reads nothing about the process
// it runs in, which is what lets workflow code call it: the interpreter derives
// the contract from what it is executing and hands it to an activity, and only
// the activity, where process state may honestly be read, compares it against
// a worker's catalog. See engine/plugins.go.
//
// The whole call tree, because a callee's requirement is the run's requirement:
// its steps execute on the same worker as its caller's. Names are unique in the
// result and the order is the order the requirements are declared in, so two
// workers deriving the contract from one specification derive the same list.
//
// It also answers the question a catalog cannot: whether the specification was
// pinned at all. A workflow that declares requirements and carries no selection
// for them never went through [ResolvePlugins], which means it was not submitted
// through a control plane that knows about plugins, and there is no contract to
// check.
func PinnedPlugins(wf *Workflow) ([]*ResolvedPlugin, error) {
	var (
		pins   []*ResolvedPlugin
		byName = make(map[string]*ResolvedPlugin)
	)

	err := walkPluginWorkflows(wf, 0, func(wf *Workflow) error {
		requirements, resolved := wf.GetPluginRequirements(), wf.GetResolvedPlugins()
		if len(resolved) != len(requirements) {
			return fmt.Errorf("workflow %q requires %d plugins and is pinned to %d; "+
				"a specification reaches a worker with the control plane's selection recorded on it, "+
				"so this one was never resolved against a deployment",
				wf.GetName(), len(requirements), len(resolved))
		}
		for i, requirement := range requirements {
			pin := resolved[i]
			if pin.GetName() != requirement.GetName() {
				return fmt.Errorf("workflow %q requires plugin %q and is pinned to %q in its place",
					wf.GetName(), requirement.GetName(), pin.GetName())
			}
			if seen, ok := byName[pin.GetName()]; ok {
				// One name, two contracts, in one specification: the two callees
				// cannot both be satisfied by one worker, and picking either would
				// be picking one of them silently.
				if err := sameResolvedPlugin(seen, pin); err != nil {
					return fmt.Errorf("the specification pins plugin %q two different ways: %w", pin.GetName(), err)
				}
				continue
			}
			byName[pin.GetName()] = pin
			pins = append(pins, pin)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return pins, nil
}

// CheckPluginsAvailable is the worker-side replay guard: it reports whether this
// worker's catalog reproduces the behavior tuple a run was pinned to.
//
// A rolling deployment may move a run only to a worker that does. The refusal
// names the plugin, the field, what the run expects and what this worker has,
// because an operator reading it is deciding whether a rollout is half done or a
// binary was replaced underneath one.
func CheckPluginsAvailable(pins []*ResolvedPlugin, catalog *PluginCatalog) error {
	available := make(map[string]*PluginDescription, len(catalog.GetPlugins()))
	for _, p := range catalog.GetPlugins() {
		available[p.GetName()] = p
	}

	for _, pin := range pins {
		p, ok := available[pin.GetName()]
		if !ok {
			return fmt.Errorf("the run is pinned to plugin %q and this worker has no such plugin installed; "+
				"it cannot execute this run", pin.GetName())
		}
		if err := sameResolvedPlugin(pin, pinOf(p, catalog.GetClaimsSchemaVersion())); err != nil {
			return err
		}
	}

	return nil
}

// CheckResolvedPlugins is [PinnedPlugins] and [CheckPluginsAvailable] together:
// the whole worker-side admission decision for one specification.
//
// The two halves are separately exported because the interpreter has to run them
// in different places (the first is deterministic and runs in workflow code, the
// second reads the worker it is on and runs in an activity), and this is the
// spelling for every caller that is not the interpreter, including a host
// embedding the engine and the tests.
func CheckResolvedPlugins(wf *Workflow, catalog *PluginCatalog) error {
	pins, err := PinnedPlugins(wf)
	if err != nil {
		return err
	}

	return CheckPluginsAvailable(pins, catalog)
}

// sameResolvedPlugin compares one pinned tuple against the one this worker
// resolves, naming the field that differs.
//
// Naming plugin, field, want and have rather than reporting "does not match" is
// the difference between an operator knowing a rollout is half done and an
// operator reading a worker log with a debugger open.
func sameResolvedPlugin(want, have *ResolvedPlugin) error {
	if want.GetName() != have.GetName() {
		return fmt.Errorf("the run is pinned to plugin %q where this worker has %q", want.GetName(), have.GetName())
	}
	for _, field := range []struct {
		name string
		want string
		have string
	}{
		{"version", want.GetVersion(), have.GetVersion()},
		{"protocol version", strconv.FormatUint(uint64(want.GetProtocolVersion()), 10), strconv.FormatUint(uint64(have.GetProtocolVersion()), 10)},
		{"task schema digest", want.GetTaskSchemaDigest(), have.GetTaskSchemaDigest()},
		{"distribution digest", want.GetDistributionDigest(), have.GetDistributionDigest()},
	} {
		if field.want == field.have {
			continue
		}

		return fmt.Errorf("plugin %q does not match the run's replay contract: %s is %s here and the run is pinned to %s",
			want.GetName(), field.name, field.have, field.want)
	}

	// ClaimsDigest is checked only when the pin asserts one. Empty means the
	// run was resolved before this field existed — there is nothing recorded
	// to compare a worker's claims digest against, and treating that as a
	// mismatch would turn a routine worker upgrade into a non-retryable
	// failure for every already-durable run touching a plugin with a
	// non-default claim, for a property that pin never promised to track
	// (#763 review). A pin resolved after this field existed always carries
	// one — see [pinOf] — so this is a one-way door: once a run is pinned
	// with a claims digest, it is enforced for that run for good.
	if want.GetClaimsDigest() != "" && want.GetClaimsDigest() != have.GetClaimsDigest() {
		return fmt.Errorf("plugin %q does not match the run's replay contract: claims digest is %s here and the run is pinned to %s",
			want.GetName(), have.GetClaimsDigest(), want.GetClaimsDigest())
	}

	// ClaimsSchemaVersion is checked only when the pin asserts one, for the
	// same reason and by the same one-way door as ClaimsDigest just above:
	// zero means the run was resolved before this field existed, and there is
	// nothing recorded to compare a worker's version against. Checked
	// separately from claims_digest, rather than folded into what it hashes,
	// because a schema-version bump is allowed to redefine what an existing
	// claim field means without changing any claim's serialized value — two
	// byte-identical claims_digests computed under different schema versions
	// are not the same security posture, and only comparing the version
	// itself catches that a claims_digest match alone would miss.
	if want.GetClaimsSchemaVersion() != 0 && want.GetClaimsSchemaVersion() != have.GetClaimsSchemaVersion() {
		return fmt.Errorf("plugin %q does not match the run's replay contract: claims schema version is %d here and the run is pinned to %d",
			want.GetName(), have.GetClaimsSchemaVersion(), want.GetClaimsSchemaVersion())
	}

	return nil
}
