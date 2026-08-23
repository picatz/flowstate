package main

import (
	"strings"
	"testing"
)

// Three legs read the forcing that decides what CI runs, and they do not all
// read it the same way. staticcheck and vet follow it — where a change to the
// harness or the module graph widens the required job, those legs widen too.
// test deliberately does not, because it is the one leg whose wide form costs
// the better part of ten minutes, and a gate people stop running protects
// nothing (#887).
//
// Both halves of that are decisions rather than accidents, so both are pinned
// here: the diffs where a leg and the job it predicts could disagree are one
// table, and each leg's answer over that table is asserted rather than
// remembered.

// forcingCase is one diff, the packages it reaches, and whether the harness or
// the module graph forces CI's jobs wide for it.
type forcingCase struct {
	name     string
	changed  []string
	affected []string
	wide     bool // CI runs the module, not this diff's packages
}

// forcingCases are the diff shapes where the tiers could disagree.
//
// The last four are the ones the first version of the staticcheck leg got
// wrong, and they are here because a reviewer found them rather than a test: a
// change to a workflow, the Makefile, this gate's own source or the fuzz target
// list sets p.ciWide (plan.go), ciForceReason turns that into a force (ci.go),
// and the force sets every decision's Run — so CI's jobs run over ./... on a
// diff that may affect no Go package whatsoever.
var forcingCases = []forcingCase{
	{"a markdown-only diff reaches no package", []string{"CLAUDE.md"}, nil, false},
	{"an ordinary Go change", []string{"pkg/flowstate/v1/engine/policy.go"}, []string{modulePath + "/pkg/flowstate/v1/engine"}, false},
	{"a diff whose only Go reach is through the import graph", []string{"proto/flowstate/v1/signal.proto"}, []string{modulePath + "/pkg/flowstate/v1"}, false},
	{"an examples-only change that still seeds a package", []string{"examples/http/flow.yaml"}, []string{modulePath + "/pkg/flowstate/v1/flowfile"}, false},
	{"an examples-only change reaching no package at all", []string{"examples/README.md"}, nil, false},

	// The module graph moved, so every package is affected on both tiers.
	{"a go.mod change", []string{"go.mod"}, []string{modulePath + "/pkg/flowstate/v1"}, true},

	// The harness moved. Note the affected sets: a workflow and the Makefile
	// are not Go files and reach nothing, which is precisely why a leg
	// reading only `affected` skipped them.
	{"a workflow-only change, e.g. bumping STATICCHECK_VERSION", []string{".github/workflows/ci.yml"}, nil, true},
	{"a Makefile-only change", []string{"Makefile"}, nil, true},
	{"a change to the gate itself", []string{"tools/gate/plan.go"}, []string{modulePath + "/tools/gate"}, true},
	{"a change to the fuzz target list", []string{"tools/fuzztargets/targets.txt"}, nil, true},
}

// TestTheVetLegFollowsCIsForcing is the vet half of #887, and the same
// assertion TestAForcedStaticcheckLegAnalysesTheWholeModule makes one leg over.
//
// CI's test job builds and vets the module, and the harness forcing runs that
// job on a diff that may touch no Go file at all. A vet leg reading `affected`
// alone therefore skipped, or vetted four packages, exactly where the job vets
// the module — the same "local pass over a commit CI rejects" hole, in the
// cheapest leg the gate has. So: forced wide, the leg's argv ends in ./...;
// not forced, it must not, because vetting the module on every ordinary diff
// is the cost this tier exists to avoid.
func TestTheVetLegFollowsCIsForcing(t *testing.T) {
	for _, tc := range forcingCases {
		t.Run(tc.name, func(t *testing.T) {
			p := buildPlan(tc.changed)

			if forced := forcedWide(p) != ""; forced != tc.wide {
				t.Fatalf("forcedWide says forced=%t, but CI runs %s for this diff",
					forced, pick(tc.wide, "the whole module", "only what the diff reaches"))
			}

			if !scopedLegRuns(p, tc.affected) {
				if tc.wide {
					t.Fatalf("the vet leg skips, but CI's test job vets ./... for this diff")
				}
				return
			}

			scope := vetScope(p, tc.affected)
			if tc.wide && (len(scope) != 1 || scope[0] != "./...") {
				t.Errorf("forced wide, but the leg vets %v; CI's test job vets ./..., so a finding anywhere else passes here and fails there", scope)
			}
			if !tc.wide && len(scope) == 1 && scope[0] == "./..." {
				t.Errorf("not forced, but the leg vets ./...; this tier is diff-scoped by design")
			}
			if got := vet(scope...).display(); !strings.HasPrefix(got, "go vet ") {
				t.Errorf("the vet leg runs %q, which is not go vet", got)
			}
		})
	}
}

// TestTheTestLegStaysNarrowAndSaysWhy pins the other half — the one that is a
// gap left open on purpose.
//
// On a harness diff CI's test job runs the whole module and this leg runs the
// affected set, which is exactly the shape every other leg here treats as a
// defect. What makes it a decision instead of a bug is that the leg's own
// printed line says so, so this asserts the line: the residual clause appears
// on a harness diff (whether the leg runs or skips, since a workflow-only diff
// affects no package at all) and never on a diff where the two tiers agree.
// Delete the clause and this fails, which is the point: the residual is allowed
// to exist and is not allowed to be silent.
func TestTheTestLegStaysNarrowAndSaysWhy(t *testing.T) {
	for _, tc := range forcingCases {
		t.Run(tc.name, func(t *testing.T) {
			p := buildPlan(tc.changed)

			// The module-graph forcing has no residual: every package is
			// affected, so the leg runs the whole module anyway.
			if p.moduleWide {
				if strings.Contains(withTestResidual(p, "why"), testResidual) {
					t.Error("go.mod moved, so the leg runs ./... and there is no residual to report")
				}
				return
			}

			line := withTestResidual(p, "why")
			if got := strings.Contains(line, testResidual); got != p.ciWide {
				t.Fatalf("the test leg's line %s the residual, but the harness %s moved:\n  %s",
					pick(got, "names", "does not name"), pick(p.ciWide, "did", "did not"), line)
			}
			if !p.ciWide {
				return
			}
			if !strings.Contains(line, "#887") {
				t.Errorf("the residual clause does not cite the issue that priced it, so a reader cannot tell a decision from a gap:\n  %s", line)
			}

			// And it stays narrow: this is the disagreement being
			// priced, not one being closed by accident later.
			ds := decide(t, tc.changed, tc.affected, "pull_request")
			if !ds["test"].Run {
				t.Fatalf("CI's test job is skipped for a harness diff (%s); the residual this leg reports would not exist", ds["test"].Why)
			}
		})
	}
}
