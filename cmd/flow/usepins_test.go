package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A module is a contract and a `use:` may pin it. These tests are the two commands
// that act on that: `flow fix --repin`, which adopts a changed module's digest on
// purpose, and `flow breaking`, which reports who a module's edit reaches.

const idsModuleSource = `edition: v2026.4
name: ids
functions:
  isUuid:
    params:
      s: string
    returns: bool
    body: ${s.matches("^[0-9a-f]{8}$")}
  domain:
    params:
      email: string
    returns: string
    body: ${email.lowerAscii()}
types:
  Uuid:
    type: string
    must: isUuid(this)
  Customer:
    fields:
      id:
        type: Uuid
        required: true
      email:
        type: string
errors:
  NotFound:
    description: No customer has this id.
`

// billSource is a workflow that uses ./lib/ids.yaml as ids, with the pin spelled as
// given (empty for none).
func billSource(pin string) string {
	use := "use:\n  ids:\n    # reviewed by the platform team\n    path: ./lib/ids.yaml\n"
	if pin != "" {
		use += "    digest: " + pin + " # 2026-10\n"
	}

	return "edition: v2026.4\nname: bill\n" + use + `inputs:
  customer:
    type: ids.Customer
    required: true
steps:
  - id: check
    if: ${!ids.isUuid(inputs.customer.id)}
    fail:
      error: ids.NotFound
`
}

func runFixRepin(t *testing.T, args ...string) (string, error) {
	t.Helper()

	stdout, stderr, err := runFixCommand(t, append([]string{"--repin"}, args...)...)

	return stdout + stderr, err
}

func TestFixRepinAdoptsTheModulesCurrentDigestInPlace(t *testing.T) {
	dir := t.TempDir()
	module := writeFixture(t, dir, "lib/ids.yaml", idsModuleSource)
	stale := v1.ContentDigest([]byte("the module before it changed\n"))
	bill := writeFixture(t, dir, "bill.yaml", billSource(stale))

	out, err := runFixRepin(t, bill)
	require.NoError(t, err, out)
	assert.Contains(t, out, "repinned module `ids`")
	assert.Contains(t, out, bill+":7:", "positioned at the digest it replaced")

	assert.Equal(t, strings.Replace(billSource(stale), stale, v1.ContentDigest([]byte(idsModuleSource)), 1), string(readFixture(t, bill)),
		"only the digest changed: the comments around it, the formatting, everything else")
	assert.Equal(t, idsModuleSource, string(readFixture(t, module)), "the module is read, never written")

	out, err = runFixRepin(t, bill)
	require.NoError(t, err, out)
	assert.Contains(t, out, "already current")
}

func TestFixRepinCheckReportsWithoutWriting(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "lib/ids.yaml", idsModuleSource)
	source := billSource(v1.ContentDigest([]byte("old\n")))
	bill := writeFixture(t, dir, "bill.yaml", source)

	out, err := runFixRepin(t, "--check", bill)
	require.Error(t, err, "a stale pin is pending work, so CI that runs --check fails")
	assert.Contains(t, out, "would repin module `ids`")
	assert.Equal(t, source, string(readFixture(t, bill)))
}

func TestFixRepinLeavesCurrentAndUnpinnedUsesAlone(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "lib/ids.yaml", idsModuleSource)
	current := writeFixture(t, dir, "current.yaml", billSource(v1.ContentDigest([]byte(idsModuleSource))))
	unpinned := writeFixture(t, dir, "unpinned.yaml", billSource(""))

	out, err := runFixRepin(t, current, unpinned)
	require.NoError(t, err, out)
	assert.Equal(t, billSource(v1.ContentDigest([]byte(idsModuleSource))), string(readFixture(t, current)))
	assert.Equal(t, billSource(""), string(readFixture(t, unpinned)), "an unpinned use is not made pinned by a repin")
}

func TestFixRepinRefusesWhatItCannotResolveAndWritesNothing(t *testing.T) {
	dir := t.TempDir()
	stale := v1.ContentDigest([]byte("old\n"))
	source := strings.Replace(billSource(stale), "./lib/ids.yaml", "./lib/gone.yaml", 1)
	bill := writeFixture(t, dir, "bill.yaml", source)

	out, err := runFixRepin(t, bill)
	require.Error(t, err)
	assert.Contains(t, out, "could not be read")
	assert.Equal(t, source, string(readFixture(t, bill)))
}

func TestFixRepinIsMachineReadable(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "lib/ids.yaml", idsModuleSource)
	bill := writeFixture(t, dir, "bill.yaml", billSource(v1.ContentDigest([]byte("old\n"))))

	out, err := runFixRepin(t, "--check", "-o", "json", bill)
	require.Error(t, err)
	assert.Regexp(t, `"changed":\s+true`, out)
	assert.Contains(t, out, "repinned module `ids`")
}

// TestFixRepinSettlesAModuleThatPinsAnotherInOneRun: repinning a module changes its
// own bytes, so a file that pins it needs repinning too, and the run orders modules
// before their importers so that both are current when it ends, whichever way the
// directory sorts them.
func TestFixRepinSettlesAModuleThatPinsAnotherInOneRun(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "lib/base.yaml", "edition: v2026.4\nname: base\nerrors:\n  Bad: {}\n")
	outer := func(pin string) string {
		return "edition: v2026.4\nname: outer\nuse:\n  base:\n    path: ./base.yaml\n    digest: " + pin + "\nerrors:\n  Worse: {}\n"
	}
	pinsOuter := func(pin string) string {
		return "edition: v2026.4\nname: top\nuse:\n  outer:\n    path: ./lib/outer.yaml\n    digest: " + pin + "\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	}
	stale := v1.ContentDigest([]byte("old base\n"))
	writeFixture(t, dir, "lib/outer.yaml", outer(stale))
	// Named so it sorts before lib/: the importer is visited first by name.
	top := writeFixture(t, dir, "a-top.yaml", pinsOuter(v1.ContentDigest(readFixture(t, filepath.Join(dir, "lib", "outer.yaml")))))

	out, err := runFixRepin(t, dir)
	require.NoError(t, err, out)
	assert.Contains(t, out, "repinned module `base`")
	assert.Contains(t, out, "repinned module `outer`")
	assert.Contains(t, string(readFixture(t, top)), v1.ContentDigest(readFixture(t, filepath.Join(dir, "lib", "outer.yaml"))))

	out, err = runFixRepin(t, "--check", dir)
	require.NoError(t, err, "the tree is current after one run: %s", out)
}

// TestFixReportsAUsePinItsMigrationInvalidated: the edition migration never
// re-stamps a use pin, but names it, the way it names a call's.
func TestFixReportsAUsePinItsMigrationInvalidated(t *testing.T) {
	dir := t.TempDir()
	// An older edition: `flow fix` brings it forward, which changes the module's bytes.
	old := strings.Replace(idsModuleSource, "edition: v2026.4\n", "edition: v2026.3\n", 1)
	module := writeFixture(t, dir, "lib/ids.yaml", old)
	source := billSource(v1.ContentDigest([]byte(old)))
	bill := writeFixture(t, dir, "bill.yaml", source)

	out, _, err := runFixCommand(t, dir)
	require.Error(t, err)
	assert.NotEqual(t, old, string(readFixture(t, module)), "premise: the module was rewritten")
	assert.Contains(t, out, "pin on module `ids`")
	assert.Contains(t, out, "flow fix --repin")
	assert.Equal(t, source, string(readFixture(t, bill)), "the migration reports a pin and does not re-stamp it")
}

// --- flow breaking over modules ---

func breakingTree(module string) map[string]string {
	return map[string]string{
		"lib/ids.yaml": module,
		"bill.yaml":    billSource(""),
		"refund.yaml":  strings.Replace(billSource(""), "name: bill", "name: refund", 1),
		"other.yaml":   "edition: v2026.4\nname: other\nsteps:\n  - id: a\n    log:\n      message: hi\n",
	}
}

func breakingAgainstEdit(t *testing.T, edit func(string) string) (string, error) {
	t.Helper()

	dir := gitInitRepoFiles(t, breakingTree(idsModuleSource))
	edited := edit(idsModuleSource)
	require.NotEqual(t, idsModuleSource, edited, "premise: the edit changed the module")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib", "ids.yaml"), []byte(edited), 0o644))

	return runBreakingCLI(t, dir, "--against", "HEAD", ".")
}

func TestBreakingReportsAModuleEditAndWhoItReaches(t *testing.T) {
	for name, tc := range map[string]struct {
		edit func(string) string
		want string
	}{
		"a type removed": {
			func(s string) string {
				return strings.Replace(s, "  Customer:\n    fields:\n      id:\n        type: Uuid\n        required: true\n      email:\n        type: string\n", "", 1)
			},
			`type "Customer" was removed`,
		},
		"a record field removed": {
			func(s string) string { return strings.Replace(s, "      email:\n        type: string\n", "", 1) },
			`field "email" was removed`,
		},
		"a record field added as required": {
			func(s string) string {
				return strings.Replace(s, "      email:\n        type: string\n", "      email:\n        type: string\n      phone:\n        type: string\n        required: true\n", 1)
			},
			`field "phone" is new and must be supplied`,
		},
		"a scalar's rule changed": {
			func(s string) string {
				return strings.Replace(s, "must: isUuid(this)", "must: isUuid(this) && size(this) == 8", 1)
			},
			`type "Uuid" changed incompatibly (its rule changed, which is read as tightened)`,
		},
		"a function removed": {
			func(s string) string {
				return strings.Replace(s, "  domain:\n    params:\n      email: string\n    returns: string\n    body: ${email.lowerAscii()}\n", "", 1)
			},
			`function "domain" was removed`,
		},
		"a parameter added": {
			func(s string) string {
				return strings.Replace(s, "      email: string\n", "      email: string\n      strict: bool\n", 1)
			},
			`function "domain" changed its signature (it took 1 parameter and now takes 2)`,
		},
		"a result weakened": {
			func(s string) string { return strings.Replace(s, "returns: string", "returns: dyn", 1) },
			`function "domain" changed its signature (it returned string and now returns dyn)`,
		},
		"an error removed": {
			func(s string) string {
				return strings.Replace(s, "errors:\n  NotFound:\n    description: No customer has this id.\n", "errors:\n  Gone: {}\n", 1)
			},
			`error "NotFound" was removed`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := breakingAgainstEdit(t, tc.edit)
			require.Error(t, err, out)
			assert.ErrorIs(t, err, errBreakingFound)
			assert.Contains(t, out, tc.want)
			assert.Contains(t, out, "lib/ids.yaml:", "positioned in the module")
			assert.Contains(t, out, "2 files use this module: bill.yaml (as ids), refund.yaml (as ids)", "the blast radius names the importers and not the bystander")
			assert.NotContains(t, out, "other.yaml")
		})
	}
}

func TestBreakingPassesACompatibleModuleEdit(t *testing.T) {
	for name, edit := range map[string]func(string) string{
		"a declaration added": func(s string) string { return s + "  Gone: {}\n" },
		"a description reworded": func(s string) string {
			return strings.Replace(s, "No customer has this id.", "No customer has that id.", 1)
		},
		"a parameter renamed": func(s string) string {
			return strings.Replace(strings.Replace(s, "      email: string", "      address: string", 1), "email.lower", "address.lower", 1)
		},
		"a body edited": func(s string) string { return strings.Replace(s, "lowerAscii", "upperAscii", 1) },
		"a field added as optional": func(s string) string {
			return strings.Replace(s, "      email:\n        type: string\n", "      email:\n        type: string\n      phone:\n        type: string\n", 1)
		},
		"reformatted": func(s string) string {
			return "# a comment\n\n" + strings.Replace(s, "description: No customer", "description:   No customer", 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := breakingAgainstEdit(t, edit)
			require.NoError(t, err, out)
			assert.Empty(t, strings.TrimSpace(out))
		})
	}
}

// A rule is compared as compiled, so editing a function a type's rule calls is the
// rule changing: fail closed, the way an input's changed `must:` is.
func TestBreakingReadsAnEditToAFunctionARuleCallsAsTheRuleChanging(t *testing.T) {
	out, err := breakingAgainstEdit(t, func(s string) string { return strings.Replace(s, "{8}$", "{8,9}$", 1) })
	require.Error(t, err, out)
	assert.Contains(t, out, `type "Uuid" changed incompatibly (its rule changed`)
}

func TestBreakingNamesImportersThatNoLongerCompile(t *testing.T) {
	dir := gitInitRepoFiles(t, breakingTree(idsModuleSource))
	removed := strings.Replace(idsModuleSource, "errors:\n  NotFound:\n    description: No customer has this id.\n", "", 1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib", "ids.yaml"), []byte(removed), 0o644))

	out, err := runBreakingCLI(t, dir, "--against", "HEAD", ".")
	require.Error(t, err)
	assert.Contains(t, out, "bill.yaml (as ids)", "bill.yaml raises ids.NotFound, so it no longer compiles; it is the importer most worth naming")
}

func TestBreakingRemovedModuleListsItsImporters(t *testing.T) {
	dir := gitInitRepoFiles(t, breakingTree(idsModuleSource))
	require.NoError(t, os.Remove(filepath.Join(dir, "lib", "ids.yaml")))

	out, err := runBreakingCLI(t, dir, "--against", "HEAD", ".")
	require.Error(t, err)
	assert.Contains(t, out, `module "ids" was removed`)
	assert.Contains(t, out, "bill.yaml (as ids)")

	out, err = runBreakingCLI(t, dir, "--against", "HEAD", "--removed", "lib/ids.yaml", ".")
	require.NoError(t, err, out)
}

func TestBreakingAModuleNoOneUsesSaysSo(t *testing.T) {
	tree := breakingTree(idsModuleSource)
	delete(tree, "bill.yaml")
	delete(tree, "refund.yaml")
	dir := gitInitRepoFiles(t, tree)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib", "ids.yaml"), []byte(strings.Replace(idsModuleSource, "      email:\n        type: string\n", "", 1)), 0o644))

	out, err := runBreakingCLI(t, dir, "--against", "HEAD", ".")
	require.Error(t, err)
	assert.Contains(t, out, "no file among the paths given uses this module")
}

func TestBreakingAModuleThatBecameAWorkflowBreaksItsImporters(t *testing.T) {
	dir := gitInitRepoFiles(t, breakingTree(idsModuleSource))
	workflow := idsModuleSource + "steps:\n  - id: a\n    log:\n      message: hi\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib", "ids.yaml"), []byte(workflow), 0o644))

	out, err := runBreakingCLI(t, dir, "--against", "HEAD", ".")
	require.Error(t, err)
	assert.Contains(t, out, "no longer a module")
}

// TestBreakingRemovingAScalarRuleIsNotBreaking: a rule that is gone loosens the type;
// a new or different one is read as tightened.
func TestBreakingRemovingAScalarRuleIsNotBreaking(t *testing.T) {
	base := "edition: v2026.4\nname: ids\ntypes:\n  Id:\n    type: string\n    must: this != \"\"\n"
	was, _, err := flowfile.Parse([]byte(base))
	require.NoError(t, err)
	wasType := was.GetDeclaredTypes()[0]

	loosened := proto.Clone(wasType).(*v1.TypeDeclaration)
	loosened.Must = nil
	assert.Empty(t, typeBreak(was, was, wasType, loosened))

	changed := proto.Clone(wasType).(*v1.TypeDeclaration)
	changed.Must = proto.String("this.size() > 3")
	assert.Contains(t, typeBreak(was, was, wasType, changed), "its rule changed")
}

func TestFixRepinRefusesCRLFFiles(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "lib/ids.yaml", idsModuleSource)
	source := strings.ReplaceAll(billSource(v1.ContentDigest([]byte("old\n"))), "\n", "\r\n")
	bill := writeFixture(t, dir, "bill.yaml", source)

	out, err := runFixRepin(t, bill)
	require.Error(t, err)
	assert.Contains(t, out, "CRLF")
	assert.Equal(t, source, string(readFixture(t, bill)))
}

// TestBreakingStillSeesAFileWhoseModulePinWasRepinned: the ref's version of a file is
// compiled against the working tree's modules, so a module edited and its importer
// repinned in one change leaves the ref's pin stale for the modules it now reads.
// That must not hide the importer's own incompatible change.
func TestBreakingStillSeesAFileWhoseModulePinWasRepinned(t *testing.T) {
	tree := breakingTree(idsModuleSource)
	tree["bill.yaml"] = billSource(v1.ContentDigest([]byte(idsModuleSource)))
	delete(tree, "refund.yaml")
	dir := gitInitRepoFiles(t, tree)

	edited := idsModuleSource + "  Extra: {}\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib", "ids.yaml"), []byte(edited), 0o644))
	repinned := strings.Replace(billSource(v1.ContentDigest([]byte(edited))),
		"inputs:\n", "inputs:\n  region:\n    type: string\n    required: true\n", 1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bill.yaml"), []byte(repinned), 0o644))

	out, err := runBreakingCLI(t, dir, "--against", "HEAD", ".")
	require.Error(t, err, out)
	assert.Contains(t, out, `input "region" now must be supplied`)
}

// TestFixRepinOrdersThroughAnInTreeSymlink: the dependency a file names is the module
// it resolves to, so a symlinked name does not hide the edge from the ordering.
func TestFixRepinOrdersThroughAnInTreeSymlink(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "lib/base.yaml", "edition: v2026.4\nname: base\nerrors:\n  Bad: {}\n")
	mid := "edition: v2026.4\nname: mid\nuse:\n  base:\n    path: ./base.yaml\n    digest: " + v1.ContentDigest([]byte("old base\n")) + "\nerrors:\n  Worse: {}\n"
	writeFixture(t, dir, "lib/mid.yaml", mid)
	if err := os.Symlink("mid.yaml", filepath.Join(dir, "lib", "z-link.yaml")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	user := func(name, path, pin string) string {
		return "edition: v2026.4\nname: " + name + "\nuse:\n  m:\n    path: " + path + "\n    digest: " + pin + "\nerrors:\n  E: {}\n"
	}
	writeFixture(t, dir, "lib/a-outer.yaml", user("outer", "./z-link.yaml", v1.ContentDigest([]byte(mid))))
	outerNow := v1.ContentDigest(readFixture(t, filepath.Join(dir, "lib", "a-outer.yaml")))
	writeFixture(t, dir, "a-top.yaml", "edition: v2026.4\nname: top\nuse:\n  o:\n    path: ./lib/a-outer.yaml\n    digest: "+outerNow+"\nsteps:\n  - id: a\n    log:\n      message: hi\n")

	out, err := runFixRepin(t, dir)
	require.NoError(t, err, out)
	out, err = runFixRepin(t, "--check", dir)
	require.NoError(t, err, "one run settles the chain through the symlink: %s", out)
}
