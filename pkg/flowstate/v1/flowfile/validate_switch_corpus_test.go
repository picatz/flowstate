package flowfile

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// #540 asks for every example in the corpus to be swept onto `switch:`,
// `value:` and optionals. This file is not that sweep — it converts nothing
// and changes no YAML. It is the instrument the sweep needs.
//
// [switchDomain] gates five diagnostics — a typo'd case, an unhandled value,
// an unreachable `default:`, a type mismatch, a duplicate case — behind one
// boolean, and every one of them is silent when the answer is no. A sweep
// that rewrites a discriminant's shaping expression into a nicer spelling can
// therefore turn checking off for a switch and leave every existing test
// green, because "the file still validates" is exactly as true with the
// domain open as with it closed (#578, #590). Without a pin naming which
// switches are closed today, nobody reviewing a conversion PR can tell
// checking survived it; with one, losing it is a one-line diff against a
// table, and a new switch nobody triaged is a build failure with a name
// rather than a silent gap.
//
// switchCorpusTable is the pin: every `switch:` step this repository ships,
// keyed by "<example-dir>/<step-id>", naming either its exact closed domain
// (in first-appearance order — the order the diagnostics themselves print)
// or "open", with a comment saying why. Written by observing what
// switchDomain reports today, not by deciding what it should report — this
// slice records the status quo.
var switchCorpusTable = map[string]switchCorpusEntry{
	// The domain diagnostics' own demonstration fixture (#357, #578, #590):
	// `outcome` is shaped by conditionals over string literals all the way
	// down, which is exactly the form switchDomain reads.
	"approval-gate/decision": closedDomain("deployed", "rejected", "undecided"),

	// `status` is a `value:` step shaped by a ternary whose *conditions* are
	// list comprehensions (`all`/`exists`) — comprehensions sit in the
	// conditional's condition argument, which literalStringLeaves never
	// walks, so only the two string-literal branches are read.
	"list-comprehensions/report": closedDomain("healthy", "degraded", "down"),

	// `outcome` is a `value:` step naming the value-step tier switchDomain
	// added alongside the wait tier: conditionals over string literals,
	// reached through `${steps.outcome.value}` rather than a wait's shaped
	// output.
	"optional-dispatch/report": closedDomain("no_response", "approved", "rejected"),

	// `settle` dispatches on `outcome`, a `value:` step combining two wait
	// gates' own shaped outcomes into the report's four endings. Both of
	// `outcome`'s branches are `steps.<id>.value` leaves rather than string
	// literals, and one of them — `escalation_outcome` — has a leaf of its own
	// again. That is the decomposition #674 is about, and since #646's corpus
	// slice it is *two* hops deep rather than one, so this pin exercises the
	// recursive case switchDomain's leaf walk added at more than its first
	// step. The order is the order the ternaries read, outermost first.
	"expense-approval/settle": closedDomain("denied_no_response", "approved_after_escalation",
		"denied", "approved_by_manager"),

	// `report` dispatches on `outcome`, a `value:` step over `steps.decision`'s
	// own `.payload.?accepted` — the identical `optMap`/`orValue` chain
	// approval-gate's own gate uses, reached through a `value:` step rather
	// than a wait's shaped `outputs:` because the wait itself carries no
	// `outputs:` block to shape.
	"callback-address/report": closedDomain("accepted", "rejected", "abandoned"),

	// Pinned already by TestWebhookRoutingDomainStaysOpen
	// (validate_switch_domain_internal_test.go, #590) — restated here rather
	// than duplicated, so the corpus table has one entry per shipped switch.
	//
	// Open, and correctly so. This entry used to say enum-typed inputs (#332)
	// would close it, and that was wrong in a way worth recording rather than
	// quietly deleting: `action` is a webhook provider's field, and the
	// example's own prose says so — "the provider can add an action type
	// tomorrow, and no validator on this machine can know the set — which is
	// exactly where `default:` earns its slot."
	//
	// Declaring it `type: enum` would not improve a diagnostic, it would
	// change runtime behaviour: a delivery carrying a novel action would be
	// refused at BindRunInputs instead of landing in `default:` and being
	// recorded, which is the drift detector this example exists to teach. An
	// open set the far side owns is a string, and `default:` is how a file
	// handles it.
	//
	// The closed-domain-from-an-input pin belongs to an example whose set the
	// *author* owns — the `must: 'this in [...]'` sites are the candidates.
	"webhook-routing/on_event": open("the discriminant is `${inputs.action}`, a set the webhook provider " +
		"owns rather than this file; correctly declared `type: string`, with `default:` handling what it " +
		"has not learned"),
}

// switchCorpusEntry is one table row: a switch's domain, if closed, and why
// it is open when it is not.
type switchCorpusEntry struct {
	domain []string // nil when open
	known  bool
	reason string // required when !known
}

func closedDomain(values ...string) switchCorpusEntry {
	return switchCorpusEntry{domain: values, known: true}
}

func open(reason string) switchCorpusEntry {
	return switchCorpusEntry{known: false, reason: reason}
}

// corpusSwitch is one `switch:` step found walking examples/, addressed the
// way the table keys it.
type corpusSwitch struct {
	exampleDir string
	stepID     string
	sw         *v1.Switch
	wf         *v1.Workflow
}

func (c corpusSwitch) key() string { return c.exampleDir + "/" + c.stepID }

// walkExampleSwitches parses every workflow.yaml under examples/ and returns
// every `switch:` step found, including ones nested inside `for_each`,
// `loop`, `parallel` and other `switch:` bodies — the same node kinds
// nodeWithID (validate.go) recurses into, because those are the only kinds
// the schema lets carry a body.
func walkExampleSwitches(t *testing.T) []corpusSwitch {
	t.Helper()

	root := repoRoot()
	examplesDir := filepath.Join(root, "examples")

	var files []string
	err := filepath.WalkDir(examplesDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "workflow.yaml" {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, files, "found no examples/**/workflow.yaml — the walk itself is broken")
	sort.Strings(files)

	var out []corpusSwitch
	for _, path := range files {
		rel, err := filepath.Rel(examplesDir, path)
		require.NoError(t, err)
		exampleDir := filepath.ToSlash(filepath.Dir(rel))

		data, err := os.ReadFile(path)
		require.NoError(t, err)

		// ParseAt, not Unmarshal: this file's own location is needed to
		// resolve a `call:` step's relative path, which several examples
		// under examples/ use (see call.go).
		wf, _, err := ParseAt(data, path)
		require.NoError(t, err, "examples/%s/workflow.yaml failed to parse", exampleDir)

		var walk func(nodes []*v1.Node)
		walk = func(nodes []*v1.Node) {
			for _, node := range nodes {
				switch kind := node.GetKind().(type) {
				case *v1.Node_Switch:
					out = append(out, corpusSwitch{
						exampleDir: exampleDir,
						stepID:     node.GetId(),
						sw:         kind.Switch,
						wf:         wf,
					})
					for _, body := range v1.SwitchBodies(kind.Switch) {
						walk(body)
					}
				case *v1.Node_ForEach:
					walk(kind.ForEach.GetBody())
				case *v1.Node_Loop:
					walk(kind.Loop.GetBody())
				case *v1.Node_Parallel:
					for _, branch := range kind.Parallel.GetBranches() {
						walk(branch.GetSteps())
					}
				}
			}
		}
		walk(wf.GetSteps())
	}

	return out
}

// TestSwitchDomainCorpusPin is the instrument #540 needs before any example
// is swept onto the modern idioms: every `switch:` step under examples/,
// matched one-to-one against switchCorpusTable.
//
// Two failure directions, both load-bearing:
//
//   - a switch found in the corpus but absent from the table means a switch
//     landed (or a sweep converted one to a fresh step id) without anyone
//     deciding, and recording, whether checking survived;
//   - a table entry naming a file or step absent from the corpus means an
//     example moved, was renamed, or was deleted without its claim following
//     it.
//
// A closed entry is compared on the full domain slice, in order — the order
// the diagnostics themselves enumerate — not merely on the boolean, for the
// identical reason validate_switch_domain_internal_test.go does: silence is
// also what an open domain produces, so asserting only "known" would pass
// just as well with the wrong values.
func TestSwitchDomainCorpusPin(t *testing.T) {
	t.Parallel()

	found := walkExampleSwitches(t)

	// The anti-vacuity guard: a walk that silently stopped finding switches
	// (a changed node-kind name, a body field renamed, a Node_Switch case
	// falling out of the recursion) would otherwise make every assertion
	// below pass on an empty set — the exact failure mode #540 exists to
	// prevent, just moved into the instrument meant to catch it.
	require.NotZero(t, len(switchCorpusTable), "the table itself must not be empty")
	require.GreaterOrEqual(t, len(found), len(switchCorpusTable),
		"the walk found fewer switch steps than the table has entries — "+
			"either the table has a stale entry or the walk stopped finding switches")

	seen := make(map[string]bool, len(found))
	for _, cs := range found {
		key := cs.key()
		if seen[key] {
			t.Fatalf("two switch steps in the corpus share the key %q; the table cannot address them separately", key)
		}
		seen[key] = true

		entry, ok := switchCorpusTable[key]
		if !ok {
			t.Errorf("examples/%s/workflow.yaml step %q is a `switch:` with no entry in switchCorpusTable — "+
				"a new dispatch was added without deciding, and recording, whether its domain is checked",
				cs.exampleDir, cs.stepID)
			continue
		}

		domain, known := switchDomain(cs.sw.GetValue(), domainScope(cs.wf))

		if entry.known {
			require.NotEmpty(t, entry.domain, "table entry %q claims a closed domain with no values listed", key)
			assert.True(t, known, "table says %q has a closed domain %v, but switchDomain now reports it open", key, entry.domain)
			if known {
				assert.Equal(t, entry.domain, domain, "table's closed domain for %q no longer matches what switchDomain reports", key)
			}
		} else {
			require.NotEmpty(t, strings.TrimSpace(entry.reason), "table entry %q is marked open with no reason recorded", key)
			assert.False(t, known, "table says %q is open (%s), but switchDomain now reports a closed domain %v — "+
				"update the table (and say why the inference widened) rather than leaving the claim stale", key, entry.reason, domain)
		}
	}

	for key := range switchCorpusTable {
		if !seen[key] {
			t.Errorf("switchCorpusTable has an entry %q naming a switch step that no longer exists in examples/ — "+
				"the example moved, was renamed, or was deleted, and the table's claim did not follow it", key)
		}
	}
}
