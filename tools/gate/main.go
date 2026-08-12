// Command gate is the diff-scoped local tier of flowstate's verification
// gate (#482). Run it from anywhere in the repository before pushing a PR
// branch:
//
//	go run ./tools/gate
//
// It computes the files changed against the merge-base with origin/main,
// maps them to Go packages, expands to every package whose build or tests
// can see a changed one, and runs the always tier: build, vet and bounded
// -race tests for the affected set, plus gofmt on the changed files. The
// conditional legs fire only when their inputs changed: the buf trio and the
// descriptorset pin on proto/, the docs mirror and reference drift checks on
// docs/DSL.md and the registry/cobra/MCP surfaces, example fix and coverage
// checks on examples/, and the -cpu=1 ordering line when the flowtest
// package is affected. Every leg prints one line saying it ran or why it was
// skipped. PR CI remains the full gate; `make check` remains the full local
// rehearsal. See CLAUDE.md's gate section.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/format"
	"io"
	"os"
	"os/exec"
	"strings"
)

// bufVersion pins buf to the same release the Makefile and CI run, so this
// tier cannot pass a schema the required jobs reject.
const bufVersion = "v1.72.0"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gate:", err)
		os.Exit(1)
	}
}

func run() error {
	// Root the whole run at the repository top level so the gate behaves
	// identically from any working directory.
	root, err := gitOutput("rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("not inside a git repository: %w", err)
	}
	if err := os.Chdir(strings.TrimSpace(root)); err != nil {
		return err
	}

	base, err := gitOutput("merge-base", "HEAD", "origin/main")
	if err != nil {
		return fmt.Errorf("cannot find the merge-base with origin/main (run `git fetch origin main` first): %w", err)
	}
	base = strings.TrimSpace(base)

	changed, err := changedFiles(base)
	if err != nil {
		return err
	}
	fmt.Printf("gate: %d changed file(s) vs merge-base %s\n", len(changed), base[:12])

	p := buildPlan(changed)

	pkgs, err := goList()
	if err != nil {
		return err
	}
	byDir := map[string]string{}
	for _, m := range pkgs {
		if d, ok := repoRelDir(m.Dir); ok {
			byDir[d] = m.ImportPath
		}
	}

	var affected []string
	if p.moduleWide {
		for _, m := range pkgs {
			affected = append(affected, m.ImportPath)
		}
	} else {
		changedSet := map[string]bool{}
		for _, ip := range resolveDirs(p.fileDirs, byDir) {
			changedSet[ip] = true
		}
		affected = affectedPackages(pkgs, changedSet)
	}

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
	// (or buf's own config) changed. The diff check is scoped to the
	// generated artifacts so unrelated uncommitted work does not fail it.
	if p.proto {
		g.leg("proto", p.reasons["proto"]+" changed",
			buf("lint"),
			buf("breaking", "--against", ".git#branch=origin/main"),
			buf("generate"),
			buf("build", "--exclude-imports", "-o", "pkg/flowstate/v1/protodoc/flowstate.descriptorset.binpb"),
			diffClean("generated code disagrees with the schema; stage and commit the regenerated files",
				"*.pb.go", "pkg/flowstate/v1/protodoc/"),
		)
	} else {
		g.skip("proto", "no changes under proto/ or to buf config")
	}

	// Conditional: the derived-docs surfaces. Editing docs/DSL.md requires
	// `go generate ./cmd/flow/internal/reference` (the mirror test enforces
	// it), and the registry/cobra/MCP surfaces feed docs/reference/; the leg
	// regenerates both and pins the result.
	if p.docs {
		g.leg("docs", p.reasons["docs"]+" changed",
			command("go", "generate", "./cmd/flow/internal/reference"),
			command("go", "run", "./cmd/flow", "docs", "generate"),
			diffClean("derived docs disagree with their sources; stage and commit the regenerated files",
				"docs/reference/", "cmd/flow/internal/reference/"),
		)
	} else {
		g.skip("docs", "no changes to docs/DSL.md or the registry/cobra/MCP surfaces")
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

	// Plugin modules are separate Go modules this gate does not walk.
	if len(p.plugins) > 0 {
		g.skip("plugins", fmt.Sprintf("%s changed, but plugin modules are outside this gate; run `make test-plugins`", strings.Join(p.plugins, ", ")))
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

// cmdSpec is one command a leg runs: argv plus any environment additions.
// checkDiff marks the git-diff-clean pin so its failure carries an
// explanation instead of a bare exit status.
type cmdSpec struct {
	argv    []string
	env     []string
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

func diffClean(failMsg string, pathspecs ...string) cmdSpec {
	c := command("git", append([]string{"diff", "--exit-code", "--"}, pathspecs...)...)
	c.failMsg = failMsg
	return c
}

// gate runs legs, records failures, and keeps going so one run reports
// everything it can rather than one failure per round trip.
type gate struct {
	ran      int
	skipped  int
	failures []string
}

func (g *gate) leg(name, why string, cmds ...cmdSpec) {
	fmt.Printf("gate: %s: running (%s)\n", name, why)
	g.ran++
	for _, c := range cmds {
		display := strings.Join(c.argv, " ")
		if len(c.env) > 0 {
			display = strings.Join(c.env, " ") + " " + display
		}
		fmt.Printf("gate: %s: $ %s\n", name, display)
		cmd := exec.Command(c.argv[0], c.argv[1:]...)
		cmd.Env = append(os.Environ(), c.env...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			msg := fmt.Sprintf("%s: `%s` failed: %v", name, display, err)
			if c.failMsg != "" {
				msg += " (" + c.failMsg + ")"
			}
			g.failures = append(g.failures, msg)
			// Later commands in the same leg depend on earlier ones
			// (generate before diff), so stop this leg here.
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
	fmt.Printf("gate: PASS (%d leg(s) run, %d skipped)\n", g.ran, g.skipped)
	return nil
}
