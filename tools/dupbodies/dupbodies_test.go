package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// duplicateBodies is the ratchet: every body the tree holds in more than one
// function, as the group's members joined by " = ", with why each is still
// written twice.
//
// Adding a copy fails the test below until the body is shared or the group is
// recorded here; removing one fails it until the entry goes, so the table is
// the tree's state rather than an allowlist. What the current entries are:
//
//   - plugins/git beside plugins/vcs — the vcs plugin is the git plugin's
//     successor and carries its code until git retires; a fix lands in both
//     until then.
//   - plugins/*/readme_test.go — each plugin module's README example walker
//     and repo-root finder, written per module because a plugin module
//     imports nothing of the root's test helpers.
//   - tokenFromValue, parseSince — the same across git, github and vcs, for
//     the same module reason.
//   - engine/*_test.go beside eval_test.go — the two drivers' conformance
//     entry points, identical by design (invariant 3).
//   - secrets.validScheme beside vault.validScheme, isLoopbackHost beside
//     vault.isLoopback — vault is a leaf that must not import the store.
//   - lsp/schema.go fieldNames beside flowfile/schema.go — the LSP's
//     schema walk, kept beside the compiler's until one owns it.
//   - github/validate.go — the issue and pull-request direction validators,
//     one per resource, sharing a body until the resource is a parameter.
//   - git/cursor.go and log_test.go ForEach — go-git's iterator contract,
//     written per iterator type.
//   - walkers_guard_test.go sameScopeContainers — the guard each walker
//     package keeps beside the walkers it guards.
//   - constraints tests nestedStruct — one fixture, two test files.
var duplicateBodies = map[string]bool{
	"pkg/flowstate/v1/constraints_task_output_test.go:taskOutputNestedStruct = pkg/flowstate/v1/constraints_test.go:nestedStruct": true,
	"pkg/flowstate/v1/engine/authority_test.go:TestAuthorityDenial = pkg/flowstate/v1/eval_test.go:TestAuthorityDenial": true,
	"pkg/flowstate/v1/engine/authority_test.go:TestCleartextCredential = pkg/flowstate/v1/eval_test.go:TestCleartextCredential": true,
	"pkg/flowstate/v1/engine/walkers_guard_test.go:sameScopeContainers = pkg/flowstate/v1/flowfile/walkers_guard_test.go:sameScopeContainers": true,
	"pkg/flowstate/v1/engine/workflow_test.go:TestRunWorkflow = pkg/flowstate/v1/eval_test.go:TestRunWorkflow": true,
	"pkg/flowstate/v1/eval_task_http_run.go:isLoopbackHost = pkg/flowstate/v1/secrets/vault/vault.go:isLoopback": true,
	"pkg/flowstate/v1/flowfile/lsp/schema.go:fieldNames = pkg/flowstate/v1/flowfile/schema.go:fieldNames = pkg/flowstate/v1/plugin/catalogtask_test.go:fieldNamesOf": true,
	"pkg/flowstate/v1/secrets/secrets.go:validScheme = pkg/flowstate/v1/secrets/vault/vault.go:validScheme": true,
	"plugins/codex/readme_test.go:extractExampleBlocks = plugins/github/readme_test.go:extractExampleBlocks = plugins/sql/readme_test.go:extractExampleBlocks": true,
	"plugins/codex/readme_test.go:repoRootFromCodexPlugin = plugins/github/readme_test.go:repoRootFromGithubPlugin = plugins/sql/readme_test.go:repoRootFromSQLPlugin": true,
	"plugins/git/clone.go:installEgressPolicy = plugins/vcs/clone.go:installEgressPolicy": true,
	"plugins/git/cursor.go:multiRootCommitIter.ForEach = plugins/git/cursor.go:pathFilteringCommitIter.ForEach = plugins/git/log_test.go:oneCommitIter.ForEach": true,
	"plugins/git/packbound_test.go:TestCloneBoundedDoesNotBoundASingleEnormousObject = plugins/vcs/packbound_test.go:TestCloneBoundedDoesNotBoundASingleEnormousObject": true,
	"plugins/git/packbound_test.go:TestPackBoundedStorerAllowsObjectsUnderTheBound = plugins/vcs/packbound_test.go:TestPackBoundedStorerAllowsObjectsUnderTheBound": true,
	"plugins/git/packbound_test.go:TestPackBoundedStorerBoundIsReached = plugins/vcs/packbound_test.go:TestPackBoundedStorerBoundIsReached": true,
	"plugins/git/secrets.go:envSegment = plugins/vcs/secrets.go:envSegment": true,
	"plugins/git/secrets.go:resolveSecret = plugins/vcs/secrets.go:resolveSecret": true,
	"plugins/git/secrets.go:tokenFromValue = plugins/github/client.go:tokenFromValue = plugins/vcs/secrets.go:tokenFromValue": true,
	"plugins/git/validate.go:clampMaxCommits = plugins/vcs/validate.go:clampMaxCommits": true,
	"plugins/git/validate.go:parseSince = plugins/github/validate.go:parseSince": true,
	"plugins/github/validate.go:validateIssueDirection = plugins/github/validate.go:validatePullRequestDirection": true,
}

// TestTheRepositoryDuplicateBodiesOnlyGoDown holds the table in both
// directions.
func TestTheRepositoryDuplicateBodiesOnlyGoDown(t *testing.T) {
	t.Parallel()

	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	groups, functions, err := Analyze(root)
	require.NoError(t, err)
	require.Greater(t, functions, 5000,
		"the walk read %d functions, which is too few to have reached the tree", functions)

	got := map[string]bool{}
	for _, group := range groups {
		got[group.Key()] = true
	}

	added, removed := ratchet(got, duplicateBodies)

	assert.Emptyf(t, added,
		"a function body now appears in more than one function. Share it, or record the "+
			"group in duplicateBodies with why it is written twice:\n\n%s", strings.Join(added, "\n"))
	assert.Emptyf(t, removed,
		"a copy is gone and duplicateBodies still records it; remove the entry so the table "+
			"keeps saying where the tree stands:\n\n%s", strings.Join(removed, "\n"))
}

// ratchet names the groups the tree has that the table does not, and the
// entries the table has that the tree no longer supports.
func ratchet(got, want map[string]bool) (added, removed []string) {
	for key := range got {
		if !want[key] {
			added = append(added, key)
		}
	}
	for key := range want {
		if !got[key] {
			removed = append(removed, key)
		}
	}
	slices.Sort(added)
	slices.Sort(removed)

	return added, removed
}

func TestTheRatchetHoldsInBothDirections(t *testing.T) {
	t.Parallel()

	want := map[string]bool{"a = b": true}

	added, removed := ratchet(map[string]bool{"a = b": true}, want)
	assert.Empty(t, added)
	assert.Empty(t, removed)

	added, removed = ratchet(map[string]bool{"a = b": true, "c = d": true}, want)
	assert.Equal(t, []string{"c = d"}, added)
	assert.Empty(t, removed)

	added, removed = ratchet(map[string]bool{}, want)
	assert.Empty(t, added)
	assert.Equal(t, []string{"a = b"}, removed)
}

// analyzeSource writes the sources into a fresh directory and analyzes it.
func analyzeSource(t *testing.T, sources map[string]string) []Group {
	t.Helper()

	dir := t.TempDir()
	for name, src := range sources {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(src), 0o600))
	}

	groups, _, err := Analyze(dir)
	require.NoError(t, err)

	return groups
}

// body is a function body over the bound: nine statements and three blocks.
const body = `{
	x := 1
	y := 2
	if x < y {
		x, y = y, x
	}
	for i := 0; i < 3; i++ {
		x += i
	}
	return x + y
}`

func TestTheSameBodyInTwoFunctionsIsAGroup(t *testing.T) {
	t.Parallel()

	groups := analyzeSource(t, map[string]string{
		"a/a.go": "package a\n\nfunc First() int " + body + "\n",
		"b/b.go": "package b\n\n// Documented, and laid out differently.\nfunc (s *Second) Same() int " + strings.ReplaceAll(body, "\n\t", "\n\t\t// noted\n\t") + "\n",
	})

	require.Len(t, groups, 1, "two identical bodies were not grouped, or a comment separated them")
	assert.Equal(t, "a/a.go:First = b/b.go:Second.Same", groups[0].Key())
	assert.GreaterOrEqual(t, groups[0].Statements, MinStatements)
}

func TestABodyUnderTheBoundIsNotReported(t *testing.T) {
	t.Parallel()

	groups := analyzeSource(t, map[string]string{
		"a.go": "package a\n\nfunc A() int { return 1 }\n\nfunc B() int { return 1 }\n",
	})

	assert.Empty(t, groups, "a one-statement accessor written twice is a coincidence, not a copy")
}

func TestARenamedCopyIsNotAMatch(t *testing.T) {
	t.Parallel()

	groups := analyzeSource(t, map[string]string{
		"a.go": "package a\n\nfunc A() int " + body + "\n\nfunc B() int " + strings.ReplaceAll(body, "x", "z") + "\n",
	})

	assert.Empty(t, groups, "a body with renamed identifiers matched; only the same code is a copy here")
}

func TestAGeneratedFileIsSkipped(t *testing.T) {
	t.Parallel()

	groups := analyzeSource(t, map[string]string{
		"a.go":    "package a\n\nfunc A() int " + body + "\n",
		"a.pb.go": "// Code generated by protoc-gen-go. DO NOT EDIT.\n\npackage a\n\nfunc B() int " + body + "\n",
	})

	assert.Empty(t, groups, "a generated file's body was compared; the copy there is the generator's")
}
