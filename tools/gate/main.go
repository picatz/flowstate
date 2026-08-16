// Command gate is the diff-scoped local tier of flowstate's verification
// gate (#482). Run it from anywhere in the repository before pushing a PR
// branch:
//
//	go run ./tools/gate
//
// It computes the files changed against the merge-base with origin/main,
// maps them to Go packages, expands to every package whose build or tests
// can see a changed one, and runs the always tier: build, vet and bounded
// -race tests for the affected set, plus gofmt on the changed files. A
// change under examples/ additionally seeds the affected set with whichever
// packages' test files actually read example workflows off disk at runtime
// (see exampleDataDepPackages) — a data dependency the import graph alone
// cannot see (#589). The conditional legs fire only when their inputs
// changed: the buf trio and the descriptorset pin on proto/, the docs mirror
// and reference drift checks on docs/DSL.md and the registry/cobra/MCP
// surfaces, example fix and coverage checks on examples/, and the -cpu=1
// ordering line when the flowtest package is affected. Every leg prints one
// line saying it ran or why it was skipped. PR CI remains the full gate;
// `make check` remains the full local rehearsal. See CLAUDE.md's gate
// section.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// bufVersion pins buf to the same release the Makefile and CI run, so this
// tier cannot pass a schema the required jobs reject.
const bufVersion = "v1.72.0"

func main() {
	// One flag, because there is one thing to choose: who is asking. The
	// local tier runs the legs; CI asks only which of its jobs this diff can
	// reach and decides the rest itself. Both answers come from the same
	// analyse() call below, which is the point — see ci.go.
	ci := flag.Bool("ci", false, "print the CI job plan for this diff and write it to $GITHUB_OUTPUT, rather than running the local legs")
	event := flag.String("event", os.Getenv("GITHUB_EVENT_NAME"), "the GitHub event name; anything other than pull_request forces every job to run")
	flag.Parse()

	var err error
	if *ci {
		err = runCI(*event)
	} else {
		err = run()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "gate:", err)
		os.Exit(1)
	}
}

// analysis is what both tiers need before they can decide anything: the plan
// the changed paths alone imply, and the package set the import graph expands
// it to.
type analysis struct {
	plan            plan
	affected        []string
	exampleDataDeps []string
	base            string
	changed         []string
}

// analyse is the one computation. The local tier turns it into legs; the CI
// tier turns it into jobs. Nothing else in this repository is allowed a second
// opinion about which packages a diff reaches.
func analyse() (analysis, error) {
	// Root the whole run at the repository top level so the gate behaves
	// identically from any working directory.
	root, err := gitOutput("rev-parse", "--show-toplevel")
	if err != nil {
		return analysis{}, fmt.Errorf("not inside a git repository: %w", err)
	}
	if err := os.Chdir(strings.TrimSpace(root)); err != nil {
		return analysis{}, err
	}

	base, err := gitOutput("merge-base", "HEAD", "origin/main")
	if err != nil {
		return analysis{}, fmt.Errorf("cannot find the merge-base with origin/main (run `git fetch origin main` first): %w", err)
	}
	base = strings.TrimSpace(base)

	changed, err := changedFiles(base)
	if err != nil {
		return analysis{}, err
	}

	p := buildPlan(changed)

	pkgs, err := goList()
	if err != nil {
		return analysis{}, err
	}
	byDir := map[string]string{}
	for _, m := range pkgs {
		if d, ok := repoRelDir(m.Dir); ok {
			byDir[d] = m.ImportPath
		}
	}

	// A changed .go file whose own directory is not a key in byDir means `go
	// list` cannot find a package there on the tree as checked out — almost
	// always because this diff deleted the last source file a package had,
	// so the directory holds none any more. resolveDirs cannot see that: it
	// walks upward looking for the nearest package ancestor, which finds
	// whatever package happens to sit above an emptied directory (or nothing
	// at all) and silently drops the file rather than reporting that its own
	// package vanished. The risk is not the deleted package itself — it is
	// gone, so it has nothing left to test — it is whatever else in the tree
	// still imports it: that importer is now broken, but nothing changed in
	// *its* files, so affectedPackages alone would never flag it. Treated the
	// same conservative way p.moduleWide already is: every package affected,
	// so the widest job it could possibly reach still runs.
	unresolvedGoDir := hasUnresolvedGoDir(p.goFiles, byDir)

	var affected []string
	var exampleDataDeps []string
	if p.moduleWide || unresolvedGoDir {
		for _, m := range pkgs {
			affected = append(affected, m.ImportPath)
		}
	} else {
		changedSet := map[string]bool{}
		for _, ip := range resolveDirs(p.fileDirs, byDir) {
			changedSet[ip] = true
		}

		// A change under examples/ is a data dependency the import graph
		// is blind to: several packages read example workflows off disk
		// with os.ReadFile at test time rather than importing them (#589).
		// Seed the changed set with whichever packages' test files
		// actually reach into examples/, so affectedPackages' existing
		// test-import expansion carries them the rest of the way.
		if p.examples {
			exampleDataDeps = exampleDataDepPackages(readTestSources(pkgs))
			for _, ip := range exampleDataDeps {
				changedSet[ip] = true
			}
		}

		affected = affectedPackages(pkgs, changedSet)
	}

	return analysis{plan: p, affected: affected, exampleDataDeps: exampleDataDeps, base: base, changed: changed}, nil
}

// runCI is the plan job's whole body: analyse the diff, decide which of
// ci.yml's jobs it can reach, and publish that. It runs no checks itself and
// never fails on a finding — a wrong answer here is a job that did not run, and
// the verdict job is what turns that into a red required check.
func runCI(event string) error {
	a, err := analyse()
	if err != nil {
		return err
	}
	fmt.Printf("plan: %d changed file(s) vs merge-base %s, %d affected package(s)\n",
		len(a.changed), a.base[:12], len(a.affected))
	return writeCIDecisions(ciDecisions(a.plan, a.affected, ciForceReason(event, a.plan)))
}

func run() error {
	a, err := analyse()
	if err != nil {
		return err
	}
	p, affected, exampleDataDeps := a.plan, a.affected, a.exampleDataDeps
	fmt.Printf("gate: %d changed file(s) vs merge-base %s\n", len(a.changed), a.base[:12])

	g := &gate{}

	// Always: the whole module must still build; a diff-scoped build would
	// miss a caller two hops away, and the build is the cheapest leg here.
	g.leg("build", "always", command("go", "build", "./..."))

	// Always: gofmt on the changed files (CI fails on any drift under ./cmd
	// and ./pkg; the gate holds every changed file to it).
	g.gofmtLeg(p.goFiles)

	// Affected packages: vet and bounded -race tests. When go.mod moved,
	// everything is affected and the bounds widen to the full-suite ones
	// `make test` uses.
	if len(affected) == 0 {
		g.skip("vet", "no Go packages affected by this diff")
		g.skip("test", "no Go packages affected by this diff")
	} else if p.moduleWide {
		why := fmt.Sprintf("%s changed, every package is affected", p.reasons["module"])
		g.leg("vet", why, command("go", append([]string{"vet"}, "./...")...))
		g.leg("test", why,
			commandEnv([]string{"GOMEMLIMIT=2GiB"}, "go", "test", "-race", "-timeout", "900s", "./..."))
	} else {
		why := fmt.Sprintf("%d affected package(s)", len(affected))
		if len(exampleDataDeps) > 0 {
			why += fmt.Sprintf(" (%d via examples/ data dependency, not the import graph: %s)",
				len(exampleDataDeps), strings.Join(trimModulePrefix(exampleDataDeps), ", "))
		}
		g.leg("vet", why, command("go", append([]string{"vet"}, affected...)...))
		g.leg("test", why,
			commandEnv([]string{"GOMEMLIMIT=1GiB"}, "go", append([]string{"test", "-race", "-timeout", "300s"}, affected...)...))
	}

	// Conditional: the ordering leg, when the flowtest package (whose every
	// claim is an ordering claim; see CLAUDE.md on -cpu=1) is affected.
	if needsOrdering(affected) {
		g.leg("ordering", "flowtest package affected",
			commandEnv([]string{"GOMEMLIMIT=1GiB"}, "go", "test", "-race", "-cpu=1", "-count=20", "-timeout", "300s", "./pkg/flowstate/v1/flowtest/"))
	} else {
		g.skip("ordering", "flowtest package not affected")
	}

	// Conditional: the buf trio plus the descriptorset pin, when the schema
	// (or buf's own config) changed. The verification is scoped to the
	// generated artifacts so unrelated uncommitted work does not fail it,
	// and covers both drift in tracked artifacts and artifacts the
	// generator newly created (a .pb.go for a proto file this diff adds).
	if p.proto {
		g.leg("proto", p.reasons["proto"]+" changed",
			buf("lint"),
			buf("breaking", "--against", ".git#branch=origin/main"),
			buf("generate"),
			buf("build", "--exclude-imports", "-o", "pkg/flowstate/v1/protodoc/flowstate.descriptorset.binpb"),
			generatedClean("generated code disagrees with the schema; stage and commit the regenerated files",
				"*.pb.go", "pkg/flowstate/v1/protodoc/"),
		)
	} else {
		g.skip("proto", "no changes under proto/ or to buf config")
	}

	// Conditional: the derived-docs surfaces. Editing docs/DSL.md requires
	// `go generate ./cmd/flow/internal/reference` (the mirror test enforces
	// it), and the registry, cobra, MCP and schema surfaces feed
	// docs/reference/; the leg regenerates both and pins the result.
	//
	// Two triggers. buildPlan's path rules are the readable, unit-tested
	// approximation; needsDocs is the authoritative one, because `flow docs
	// generate` runs the cmd/flow binary and so its real source set is that
	// binary's dependency closure. Either firing runs the leg.
	docsWhy := ""
	switch {
	case p.docs:
		docsWhy = p.reasons["docs"] + " changed"
	case needsDocs(affected):
		docsWhy = "cmd/flow is affected, so the binary that generates docs/reference/ may emit different output"
	}
	if docsWhy != "" {
		g.leg("docs", docsWhy,
			command("go", "generate", "./cmd/flow/internal/reference"),
			command("go", "run", "./cmd/flow", "docs", "generate"),
			generatedClean("derived docs disagree with their sources; stage and commit the regenerated files",
				"docs/reference/", "cmd/flow/internal/reference/"),
		)
	} else {
		g.skip("docs", "no changes to docs/DSL.md, the schema, or the registry/cobra/MCP surfaces, and cmd/flow is unaffected")
	}

	// Conditional: the example corpus. Same three checks CI's test job runs
	// against examples/.
	if p.examples {
		g.leg("examples", p.reasons["examples"]+" changed",
			command("go", "run", "./cmd/flow", "fix", "--check", "examples/"),
			command("go", "run", "./cmd/flow", "test", "--coverage-required", "examples/"),
			command("go", "run", "./cmd/flow", "breaking", "--against", "origin/main", "examples/"),
		)
	} else {
		g.skip("examples", "no changes under examples/")
	}

	// Conditional: the recorded appearance goldens. This leg never fails
	// the gate and never claims a pass, because the test it would run
	// skips when vhs, ttyd or ffmpeg is absent, and a leg reporting green
	// by not running is worse than one that is honestly elsewhere. #483's
	// edition bump moved the sample Flowfile printed by `flow tasks http`,
	// the golden still said the old edition, and a silent local skip sent
	// that to CI to discover. So: run it where the tooling exists, and say
	// plainly that it is unverified where it does not.
	//
	// The trigger is needsAppearance rather than p.appearance alone, for
	// the reason needsDocs gives one level up: the goldens record what the
	// cmd/flow *binary* prints, so its dependency closure is the real
	// source set, and a path rule can only ever be the approximation.
	if needsAppearance(p, affected) {
		if missing := missingAppearanceTools(); len(missing) > 0 {
			fmt.Printf("gate: appearance: NOT VERIFIED locally (%s absent); CI's appearance job owns this. "+
				"If this change alters printed output, expect that job to fail and re-record with `make appearance-update`.\n",
				strings.Join(missing, ", "))
			g.unverified++
		} else {
			g.leg("appearance", appearanceWhy(p, affected),
				commandEnv([]string{"GOMEMLIMIT=2GiB"}, "go", "test", "-timeout", "900s", "-count=1", "-run", "TestAppearance", "./cmd/flow/internal/appearance/"))
		}
	} else {
		g.skip("appearance", "no changes to styled output, help text, or the current edition")
	}

	// Plugin modules are separate Go modules this gate does not walk. A
	// change under examples/plugins/<name>/ is the same #589 data
	// dependency one module further out: that plugin's own
	// reachable_test.go reads its shipped example with os.ReadFile, so
	// folded in here too when the name matches a real plugin module —
	// distinguished from an ordinary plugins/ file change in the message,
	// per the same "say why" rule the affected-set notice above follows.
	pluginNotices := pluginSkipNotices(p, func(mod string) bool {
		_, err := os.Stat(mod + "/go.mod")
		return err == nil
	})
	if len(pluginNotices) > 0 {
		g.skip("plugins", fmt.Sprintf("%s changed, but plugin modules are outside this gate; run `make test-plugins`", strings.Join(pluginNotices, ", ")))
	}

	return g.summary()
}

// changedFiles is the diff against the merge-base (committed, staged and
// unstaged tracked changes) plus untracked files, which a plain diff cannot
// see and which a new package arrives as.
func changedFiles(base string) ([]string, error) {
	diff, err := gitOutput("diff", "--name-only", base)
	if err != nil {
		return nil, err
	}
	untracked, err := gitOutput("ls-files", "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, f := range strings.Split(diff+"\n"+untracked, "\n") {
		if f = strings.TrimSpace(f); f != "" && !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	return out, nil
}

func gitOutput(args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// goList loads every package in the module in one process; see
// affectedPackages for why one walk is enough.
func goList() ([]pkgMeta, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("go", "list", "-e", "-json=ImportPath,Dir,Deps,TestImports,XTestImports", "./...")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("go list ./...: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	var pkgs []pkgMeta
	dec := json.NewDecoder(&stdout)
	for {
		var m pkgMeta
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("parsing go list output: %w", err)
		}
		pkgs = append(pkgs, m)
	}
	return pkgs, nil
}

// readTestSources concatenates every "*_test.go" file's contents per
// package directory, keyed by import path, for exampleDataDepPackages to
// scan. A package with no test files is simply absent from the result; a
// directory this run cannot read (deleted mid-diff) is skipped rather than
// failing the whole gate, the same tolerance gofmtLeg gives a deleted file.
func readTestSources(pkgs []pkgMeta) map[string][]byte {
	out := make(map[string][]byte, len(pkgs))
	for _, m := range pkgs {
		entries, err := os.ReadDir(m.Dir)
		if err != nil {
			continue
		}
		var buf bytes.Buffer
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(m.Dir, e.Name()))
			if err != nil {
				continue
			}
			buf.Write(data)
		}
		if buf.Len() > 0 {
			out[m.ImportPath] = buf.Bytes()
		}
	}
	return out
}

// trimModulePrefix shortens this module's import paths for a println,
// consistent with how CLAUDE.md and this repository's own docs refer to
// packages by their repo-relative path rather than their full import path.
func trimModulePrefix(paths []string) []string {
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = strings.TrimPrefix(p, modulePath+"/")
	}
	return out
}

// repoRelDir converts an absolute package directory to a repo-relative slash
// path ("." for the root), relative to the current directory, which run()
// pinned to the repository root.
func repoRelDir(dir string) (string, bool) {
	wd, err := os.Getwd()
	if err != nil {
		return "", false
	}
	if dir == wd {
		return ".", true
	}
	prefix := wd + string(os.PathSeparator)
	if !strings.HasPrefix(dir, prefix) {
		return "", false
	}
	return strings.ReplaceAll(strings.TrimPrefix(dir, prefix), string(os.PathSeparator), "/"), true
}

// cmdSpec is one step a leg runs: either an external command (argv, plus
// any environment additions) or an in-process check (verify), never both.
// failMsg carries an explanation so a failure says what it means rather
// than reporting a bare exit status.
type cmdSpec struct {
	argv    []string
	env     []string
	verify  func() error
	label   string
	failMsg string
}

func command(name string, args ...string) cmdSpec {
	return cmdSpec{argv: append([]string{name}, args...)}
}

func commandEnv(env []string, name string, args ...string) cmdSpec {
	return cmdSpec{argv: append([]string{name}, args...), env: env}
}

func buf(args ...string) cmdSpec {
	return command("go", append([]string{"run", "github.com/bufbuild/buf/cmd/buf@" + bufVersion}, args...)...)
}

// generatedClean is the pin that follows every generate step: after
// regenerating, the named pathspecs must hold nothing the commit does not
// already have.
//
// Two conditions, not one. `git diff --exit-code` sees a *tracked* artifact
// the generator rewrote, and is blind to one the generator newly *created*:
// a mirror for a newly added example, a `.pb.go` for a new proto file, a new
// page under docs/reference/ all arrive untracked, and a diff-only pin
// reports success while the artifact is missing from the commit. That is the
// worst failure a gate has — passing when it should fail — so the untracked
// half is checked explicitly with `git ls-files --others --exclude-standard`
// over the same pathspecs, and its failure names the files.
func generatedClean(failMsg string, pathspecs ...string) cmdSpec {
	return cmdSpec{
		label:   "verify " + strings.Join(pathspecs, " "),
		failMsg: failMsg,
		verify: func() error {
			if err := checkTrackedClean(pathspecs); err != nil {
				return err
			}
			return checkNoUntracked(pathspecs)
		},
	}
}

// Both halves take the *index* as their reference point, which is what
// makes them agree on one question: "is the regenerated output exactly what
// you have already recorded?" A tracked file that differs from the index
// answers no; a file the index has never heard of answers no as well. That
// is also the workflow-friendly reading — an author who edits the schema,
// runs the gate, and then stages and commits what it regenerated passes on
// the second run, where comparing against HEAD would fail them for the
// entirely normal state of having staged but not yet committed.

// checkTrackedClean fails when a tracked file under pathspecs differs from
// the index, which is `git diff --exit-code` with the names kept.
func checkTrackedClean(pathspecs []string) error {
	out, err := gitOutput(append([]string{"diff", "--name-only", "--"}, pathspecs...)...)
	if err != nil {
		return err
	}
	if files := strings.Fields(out); len(files) > 0 {
		return fmt.Errorf("regenerating rewrote files you have not staged: %s", strings.Join(files, " "))
	}
	return nil
}

// checkNoUntracked fails when regenerating produced a file the index has
// never seen. Ignored files are excluded (--exclude-standard), so build
// output a .gitignore already covers is not mistaken for a missing artifact.
func checkNoUntracked(pathspecs []string) error {
	out, err := gitOutput(append([]string{"ls-files", "--others", "--exclude-standard", "--"}, pathspecs...)...)
	if err != nil {
		return err
	}
	if files := strings.Fields(out); len(files) > 0 {
		return fmt.Errorf("regenerating created files that are not in the commit: %s (run `git add` on them)", strings.Join(files, " "))
	}
	return nil
}

// gate runs legs, records failures, and keeps going so one run reports
// everything it can rather than one failure per round trip.
type gate struct {
	ran        int
	skipped    int
	unverified int
	failures   []string
}

// appearanceTools are the three binaries charmbracelet/vhs needs to record
// a styled surface; without any one of them the appearance test skips.
var appearanceTools = []string{"vhs", "ttyd", "ffmpeg"}

func missingAppearanceTools() []string {
	var missing []string
	for _, tool := range appearanceTools {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, tool)
		}
	}
	return missing
}

func (g *gate) leg(name, why string, cmds ...cmdSpec) {
	fmt.Printf("gate: %s: running (%s)\n", name, why)
	g.ran++
	for _, c := range cmds {
		display := c.label
		if display == "" {
			display = strings.Join(c.argv, " ")
			if len(c.env) > 0 {
				display = strings.Join(c.env, " ") + " " + display
			}
		}
		fmt.Printf("gate: %s: $ %s\n", name, display)

		var err error
		if c.verify != nil {
			err = c.verify()
		} else {
			cmd := exec.Command(c.argv[0], c.argv[1:]...)
			cmd.Env = append(os.Environ(), c.env...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
		}
		if err != nil {
			msg := fmt.Sprintf("%s: `%s` failed: %v", name, display, err)
			if c.failMsg != "" {
				msg += " (" + c.failMsg + ")"
			}
			g.failures = append(g.failures, msg)
			// Later steps in the same leg depend on earlier ones
			// (generate before verify), so stop this leg here.
			return
		}
	}
}

func (g *gate) skip(name, why string) {
	fmt.Printf("gate: %s: skipped (%s)\n", name, why)
	g.skipped++
}

// gofmtLeg checks the changed .go files with go/format, the library face of
// gofmt: same printer, same toolchain as the build, and no dependency on a
// gofmt binary being on PATH. A file that no longer exists or does not parse
// is skipped; the build leg owns reporting those.
func (g *gate) gofmtLeg(files []string) {
	if len(files) == 0 {
		g.skip("gofmt", "no changed .go files")
		return
	}
	fmt.Printf("gate: gofmt: running (%d changed .go file(s))\n", len(files))
	g.ran++
	var bad []string
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			continue // deleted by this diff
		}
		formatted, err := format.Source(src)
		if err != nil {
			continue // does not parse; the build leg reports it
		}
		if !bytes.Equal(src, formatted) {
			bad = append(bad, f)
		}
	}
	if len(bad) > 0 {
		g.failures = append(g.failures,
			fmt.Sprintf("gofmt: not gofmt formatted (run `gofmt -w` on): %s", strings.Join(bad, " ")))
	}
}

func (g *gate) summary() error {
	if len(g.failures) > 0 {
		fmt.Println()
		for _, f := range g.failures {
			fmt.Printf("gate: FAIL %s\n", f)
		}
		return fmt.Errorf("%d leg(s) failed", len(g.failures))
	}
	// The unverified count rides on the pass line on purpose: a reader
	// should not have to scroll back to learn the gate left something to CI.
	unverified := ""
	if g.unverified > 0 {
		unverified = fmt.Sprintf(", %d NOT VERIFIED locally, see above", g.unverified)
	}
	fmt.Printf("gate: PASS (%d leg(s) run, %d skipped%s)\n", g.ran, g.skipped, unverified)
	return nil
}
