package plugintest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// Check names one conformance check. The names are stable: a finding is
// identified by plugin and check, so a test can assert that a known problem is
// reported, and a check is skipped by ignoring its name rather than by
// anything position-dependent.
type Check string

// The checks [Session.Audit] runs. Each is described at [Session.Conform].
const (
	CheckIdentity         Check = "identity"
	CheckSummaries        Check = "summaries"
	CheckDeclaredOutputs  Check = "declared outputs"
	CheckDocumentedFields Check = "documented fields"
	CheckHealth           Check = "health"
	CheckStableDigests    Check = "stable digests"
)

// Checks lists every check in the order Audit runs them.
func Checks() []Check {
	return []Check{
		CheckIdentity, CheckSummaries, CheckDeclaredOutputs,
		CheckDocumentedFields, CheckHealth, CheckStableDigests,
	}
}

// Finding is one way a plugin falls short of what it should declare.
type Finding struct {
	// Plugin is the plugin's name.
	Plugin string

	// Check is which check found it.
	Check Check

	// Message says what is wrong and what to change.
	Message string
}

func (f Finding) String() string { return fmt.Sprintf("%s: %s: %s", f.Plugin, f.Check, f.Message) }

// Conform fails the test for every finding [Session.Audit] reports, one subtest
// per plugin and check so a run lists them all and a passing check is visible
// as passing.
//
// The checks are about what a plugin declares, are the ones whose failure a
// worker would not notice until an author did, and call no task — so a plugin
// whose tasks reach a network or mutate state is as safe to conform as a pure
// one:
//
//   - identity: a version and a description, which `flow plugins` prints and an
//     operator reads before approving a pin.
//   - summaries: every task has a one-line summary, which is what `flow tasks`
//     and an editor's completion list show beside its name.
//   - declared outputs: a task declares the outputs it returns, because a step
//     reading `${steps.x.name}` is checked against them and an undeclared task
//     leaves the validator, the language server, and the docs with nothing to
//     say. A task that shapes its outputs from an `outputs:` input is exempt.
//   - documented fields: every input and output field carries a comment, which
//     is the hover text and the reference page, and the one piece of
//     documentation a generated schema cannot fall out of sync with.
//   - health: a launched plugin answers its health poll. Not serving is an
//     answer, and the right one for a plugin nobody configured, provided it says
//     why; a plugin that cannot be reached, or declines without a reason, is
//     what an operator cannot diagnose.
//   - stable digests: launching the same binary twice yields the same
//     task-schema and claims digests. A plugin whose descriptors depend on map
//     iteration or a clock cannot be pinned, because the pin it was given would
//     fail on its own next launch.
func (s *Session) Conform(t *testing.T, ignore ...Check) {
	t.Helper()

	findings := s.Audit(t.Context(), t)
	for _, p := range s.host.Plugins() {
		t.Run(p.Name(), func(t *testing.T) {
			for _, check := range Checks() {
				if slices.Contains(ignore, check) {
					continue
				}
				t.Run(string(check), func(t *testing.T) {
					for _, f := range findings {
						if f.Plugin == p.Name() && f.Check == check {
							t.Error(f.Message)
						}
					}
				})
			}
		})
	}
}

// Audit runs every conformance check and returns what it found, without
// failing anything. It is [Session.Conform] for a caller that wants the
// findings as data — to assert that a known problem is reported, or to print
// them from something that is not a test.
//
// The stable-digests check launches the plugins a second time and needs a
// testing.TB to own that launch's cleanup.
func (s *Session) Audit(ctx context.Context, t testing.TB) []Finding {
	t.Helper()

	catalog := s.host.Catalog()
	described := map[string]*flowstatev1.TaskDescription{}
	for _, p := range catalog.GetPlugins() {
		for _, task := range p.GetTasks() {
			described[task.GetName()] = task
		}
	}
	defs := map[string]flowstatev1.TaskDef{}
	for _, def := range s.host.TaskDefs() {
		defs[def.Name] = def
	}

	var findings []Finding
	add := func(plugin string, check Check, format string, args ...any) {
		findings = append(findings, Finding{Plugin: plugin, Check: check, Message: fmt.Sprintf(format, args...)})
	}

	for _, p := range s.host.Plugins() {
		manifest := p.Manifest()

		if manifest.GetVersion() == "" {
			add(p.Name(), CheckIdentity, "the plugin declares no version; set Plugin.Version so an operator can tell two builds apart")
		}
		if strings.TrimSpace(manifest.GetDescription()) == "" {
			add(p.Name(), CheckIdentity, "the plugin declares no description; set Plugin.Description")
		}

		for _, task := range manifest.GetTasks() {
			name := p.Name() + "." + task.GetName()

			switch summary := task.GetSummary(); {
			case strings.TrimSpace(summary) == "":
				add(p.Name(), CheckSummaries, "%s has no summary; set Task.Summary", name)
			case strings.ContainsAny(summary, "\r\n"):
				add(p.Name(), CheckSummaries, "%s summary spans lines; it is one line in `flow tasks`", name)
			}

			if !task.GetShapesOutputs() && len(described[name].GetOutputs()) == 0 {
				add(p.Name(), CheckDeclaredOutputs, "%s declares no outputs; set Task.Output to a message so a workflow "+
					"can read ${steps.<step>.<name>} and have it checked", name)
			}

			def := defs[name]
			for _, side := range []struct {
				label string
				desc  protoreflect.MessageDescriptor
			}{{"input", def.Inputs}, {"output", def.Outputs}} {
				for _, field := range undocumented(side.desc) {
					add(p.Name(), CheckDocumentedFields, "%s %s field %q has no comment; it is the hover text and "+
						"the reference page. Comment it in the .proto and regenerate", name, side.label, field)
				}
			}
		}

		switch h := p.CheckHealth(ctx); {
		case h.Status == plugin.HealthServing:
		case h.Status == plugin.HealthNotServing && strings.TrimSpace(h.Message) != "":
			// Declining with a reason is the contract for a plugin that depends
			// on configuration this session did not supply.
		default:
			add(p.Name(), CheckHealth, "health = %v (%q, %v); a plugin answers serving, or not serving with a reason",
				h.Status, h.Message, h.Err)
		}
	}

	again := Launch(t, s.dir, s.opts...)
	got := digests(again.host.Catalog())
	for name, want := range digests(catalog) {
		switch g, ok := got[name]; {
		case !ok:
			add(name, CheckStableDigests, "the plugin was not launched the second time")
		case g != want:
			add(name, CheckStableDigests, "digests differ between two launches of one binary (%+v, then %+v); "+
				"its descriptors are not deterministic, so no pin could hold", want, g)
		}
	}

	return findings
}

type digestPair struct{ Schema, Claims string }

func digests(c *flowstatev1.PluginCatalog) map[string]digestPair {
	out := map[string]digestPair{}
	for _, p := range c.GetPlugins() {
		out[p.GetName()] = digestPair{Schema: p.GetTaskSchemaDigest(), Claims: p.GetClaimsDigest()}
	}
	return out
}

// undocumented names the fields of a message that have no leading comment in
// the descriptor the host reconstructed from what the plugin sent.
func undocumented(msg protoreflect.MessageDescriptor) []string {
	if msg == nil {
		return nil
	}
	var missing []string
	fields := msg.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if strings.TrimSpace(msg.ParentFile().SourceLocations().ByDescriptor(field).LeadingComments) == "" {
			missing = append(missing, string(field.Name()))
		}
	}
	return missing
}
