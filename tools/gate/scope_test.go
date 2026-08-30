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
	{"an unvalidated markdown-only diff reaches no package", []string{"SECURITY.md"}, nil, false},
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

			if forced := forcedWide(resolvedBase, p) != ""; forced != tc.wide {
				t.Fatalf("forcedWide says forced=%t, but CI runs %s for this diff",
					forced, pick(tc.wide, "the whole module", "only what the diff reaches"))
			}

			if !scopedLegRuns(resolvedBase, p, tc.affected) {
				if tc.wide {
					t.Fatalf("the vet leg skips, but CI's test job vets ./... for this diff")
				}
				return
			}

			scope := vetScope(resolvedBase, p, tc.affected)
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

// TestAnUnresolvableBaseSelectsEverything is the safety property of the
// fallback in [resolveBase]: when no merge-base can be established, the run is
// wide rather than absent.
//
// It matters because the alternative the gate used to have was to exit with an
// error telling the caller to fetch origin/main. Every one of the seven pull
// requests in one wave reported exactly that refusal, and every one of them
// was then opened unvalidated — a gate that refuses to start protects nothing.
//
// The fix is only safe in one direction. Running everything costs time; running
// a *subset* of what the diff can reach passes commits the checks reject. So
// what is asserted is that no narrowing survives an unmeasurable diff, and it
// is asserted by calling the functions the two tiers actually call —
// [changedFiles], [forcedWide], [scopedLegRuns], [vetScope], [ciDecisions] —
// rather than by rebuilding what they are expected to answer. The first draft
// of this test did the latter, and three mutations of the production code
// walked straight past it.
func TestAnUnresolvableBaseSelectsEverything(t *testing.T) {
	// The empty base is what resolveBase returns when it has exhausted the ref,
	// the fetch and the deepen. changedFiles turns that into the whole tree.
	changed, err := changedFiles("")
	if err != nil {
		t.Fatalf("changedFiles with no base: %v", err)
	}
	if len(changed) < 100 {
		t.Fatalf("changedFiles with no base answered %d file(s), which is not the tree: "+
			"a narrow answer here is the whole hazard, since every leg would then be scoped to it", len(changed))
	}

	p := buildPlan(changed)

	// The local tier: nothing may be scoped to the affected set, and the two
	// legs that follow CI's scope must go wide with an empty affected set —
	// which is the state a caller reaches when no package resolved.
	if why := forcedWide("", p); why == "" {
		t.Error("forcedWide answers \"\" with no merge-base, so vet and staticcheck would narrow")
	}
	if !scopedLegRuns("", p, nil) {
		t.Error("scopedLegRuns is false with no merge-base and nothing affected, so the leg would be skipped")
	}
	if got := vetScope("", p, nil); len(got) != 1 || got[0] != "./..." {
		t.Errorf("vetScope with no merge-base is %v, want [./...]", got)
	}

	// And every conditional leg, so none is skipped on a run that cannot tell
	// whether its inputs moved.
	for name, on := range map[string]bool{
		"proto":        p.proto,
		"docs":         p.docs,
		"examples":     p.examples,
		"repoTestData": p.repoTestData,
	} {
		if !on {
			t.Errorf("the %s leg is not selected with no merge-base, so it would be skipped on an unmeasurable diff", name)
		}
	}

	// The CI tier, through the same call the plan job makes.
	decisions := ciDecisions(p, nil, ciForceReason("pull_request", "", p))
	if len(decisions) == 0 {
		t.Fatal("no decisions")
	}
	for _, d := range decisions {
		if !d.Run {
			t.Errorf("job %q is skipped on a run with no merge-base: %s", d.Job, d.Why)
		}
	}
}

// TestAResolvableBaseIsStillNarrow is the other direction, and it is the one
// that keeps the fallback honest: a gate that ran wide always would satisfy
// every assertion above and destroy the whole point of this tool.
func TestAResolvableBaseIsStillNarrow(t *testing.T) {
	p := buildPlan([]string{"pkg/flowstate/v1/auth/issuer.go"})

	if why := forcedWide(resolvedBase, p); why != "" {
		t.Errorf("one .go file forces a wide run: %s", why)
	}
	if got := vetScope(resolvedBase, p, []string{"example.com/x"}); len(got) == 1 && got[0] == "./..." {
		t.Error("one .go file vets the whole module")
	}
	if p.proto || p.examples {
		t.Error("one .go file selected a conditional leg whose inputs did not move")
	}
}

// TestResolveBaseFallsBackRatherThanRefusing is the give-up branch, exercised
// where it actually happens: a repository with no origin at all, which is what
// a --single-branch clone with no network looks like from in here.
//
// The assertion is the shape of the answer, not the wording: an empty base,
// because that is what [changedFiles] reads as "take the whole tree", and a
// non-empty reason, because a wide run that does not say why it is wide is
// indistinguishable from a diff that happened to touch everything.
func TestResolveBaseFallsBackRatherThanRefusing(t *testing.T) {
	dir := t.TempDir()
	git(t, dir, "init", "--initial-branch=main")
	git(t, dir, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "--allow-empty", "-m", "root")

	t.Chdir(dir)

	base, why := resolveBase()
	if base != "" {
		t.Errorf("resolveBase invented a base %q where there is no origin, so the diff would be taken against it", base)
	}
	if why == "" {
		t.Error("resolveBase gave up silently, so a wide run reads as a diff that touched everything")
	}
}

// TestResolveBaseFindsTheBaseWhenItIsThere is the happy path, and it is what
// stops the fallback from becoming the only path: this repository has
// origin/main, so the gate must answer with a real merge-base and say nothing.
func TestResolveBaseFindsTheBaseWhenItIsThere(t *testing.T) {
	if _, err := gitOutput("rev-parse", "--verify", "--quiet", "origin/main"); err != nil {
		t.Skip("no origin/main in this checkout, which is the case the other test covers")
	}

	base, why := resolveBase()
	if len(base) != 40 {
		t.Errorf("resolveBase answered %q, want a commit id", base)
	}
	if why != "" {
		t.Errorf("resolveBase had to do something to find a base that was already here: %s", why)
	}
}

// TestAnUnmeasurableDiffSaysSoRatherThanBlamingTheDiff isolates the one thing
// the base arm of [ciForceReason] is for.
//
// It is not scope: an unresolvable base makes [changedFiles] answer with the
// whole tree, which sets ciWide and moduleWide, so every job would run through
// those arms whether this one existed or not. What it is for is the *reason*,
// and a wrong reason is a real defect here — docs/CI.md's whole argument is
// that a skip, or a wide run, has to be a decision somebody can read. Told that
// the workflows changed, a reader goes looking for a workflow diff that is not
// there.
//
// So the plan is deliberately a narrow one, where the other arms cannot fire.
func TestAnUnmeasurableDiffSaysSoRatherThanBlamingTheDiff(t *testing.T) {
	narrow := buildPlan([]string{"pkg/flowstate/v1/auth/issuer.go"})

	if why := ciForceReason("pull_request", resolvedBase, narrow); why != "" {
		t.Fatalf("this plan is not narrow, so the test proves nothing: %s", why)
	}

	why := ciForceReason("pull_request", "", narrow)
	if why == "" {
		t.Fatal("an unmeasurable diff does not force a wide run")
	}
	if !strings.Contains(why, "merge-base") {
		t.Errorf("the reason for a wide run is %q, which does not name the merge-base as the cause", why)
	}
}

// TestResolveBaseFetchesWhatTheCheckoutIsMissing is the case that motivated all
// of this, reproduced: a clone that has the branch under review and not the
// branch it will merge into.
//
// Both shapes are covered because they fail differently and are fixed
// differently. A --single-branch clone has full history and no origin/main ref
// at all, so fetching the branch is enough. A --depth=1 clone gets the ref and
// still has no common ancestor with it, because the shared history is exactly
// what the depth cut off — so it needs deepening as well, and a gate that only
// did the first would still refuse on the second.
func TestResolveBaseFetchesWhatTheCheckoutIsMissing(t *testing.T) {
	for _, test := range []struct {
		name  string
		clone []string
		want  string
	}{
		{name: "the ref is absent", clone: []string{"--single-branch", "--branch", "feature"}, want: "fetched"},
		{name: "the history is cut off", clone: []string{"--depth=1", "--single-branch", "--branch", "feature"}, want: "deepened"},
	} {
		t.Run(test.name, func(t *testing.T) {
			src := t.TempDir()
			git(t, src, "init", "--initial-branch=main")
			git(t, src, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "--allow-empty", "-m", "shared history")
			git(t, src, "checkout", "-b", "feature")
			git(t, src, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "--allow-empty", "-m", "the change under review")

			dst := t.TempDir() + "/clone"
			git(t, t.TempDir(), append(append([]string{"clone"}, test.clone...), "file://"+src, dst)...)

			t.Chdir(dst)

			base, why := resolveBase()
			if base == "" {
				t.Fatalf("resolveBase gave up on a checkout it could have repaired, so the gate runs wide "+
					"on every pull request from a clone like this: %s", why)
			}
			if !strings.Contains(why, test.want) {
				t.Errorf("resolveBase found a base but reports %q, which does not say it %s anything", why, test.want)
			}
		})
	}
}
