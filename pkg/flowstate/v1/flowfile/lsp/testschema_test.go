package lsp

import (
	"reflect"
	"slices"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/stretchr/testify/assert"
)

// TestTestDSLKeysMatchTheLoader is TestDSLKeysMatchTheDSL's counterpart for
// the test language: the guard on every level of testDocKeys that can be
// checked mechanically.
//
// Unlike the workflow table — whose document shape lives in unexported
// flowfile structs, so that test derives a key set by asking flowfile to
// render a fully-populated fixture — every flowtest type below is exported
// and decoded straight off its own `yaml:"..."` struct tags, with no custom
// UnmarshalYAML narrowing what a tag promises. [yamlKeys] reads those tags
// directly: the loader's own source of truth, with no fixture that must
// remember to populate every field standing between this test and it.
//
// Both directions are checked, for the reason TestDSLKeysMatchTheDSL checks
// both: a key testDocKeys names that the struct does not have would offer
// something `flow test` rejects as unknown, and a key the struct has that
// this table does not name is a key completion cannot yet offer — either is
// the drift CLAUDE.md's proto-first section describes, and it fails a build
// rather than waiting to be noticed in an editor.
//
// testLevelCheck is deliberately not included: [flowtest.CheckClaim] has no
// yaml struct tags at all — see checkClaimKeys' own doc for why, and for
// where its two names are cited instead.
func TestTestDSLKeysMatchTheLoader(t *testing.T) {
	t.Parallel()

	cases := []struct {
		level testDocLevel
		typ   reflect.Type
	}{
		{testLevelFile, reflect.TypeFor[flowtest.File]()},
		{testLevelCase, reflect.TypeFor[flowtest.Test]()},
		{testLevelDefaults, reflect.TypeFor[flowtest.Defaults]()},
		{testLevelCoverage, reflect.TypeFor[flowtest.CoverageStanza]()},
		{testLevelTrigger, reflect.TypeFor[flowtest.TriggerDelivery]()},
		{testLevelStub, reflect.TypeFor[flowtest.Stub]()},
		{testLevelFails, reflect.TypeFor[flowtest.StubFailure]()},
		{testLevelSignal, reflect.TypeFor[flowtest.SignalScript]()},
		{testLevelIdentity, reflect.TypeFor[flowtest.ScriptedIdentity]()},
		{testLevelExpect, reflect.TypeFor[flowtest.Expectation]()},
	}

	require := func(t *testing.T, level testDocLevel, typ reflect.Type) {
		real := yamlKeys(typ)
		assert.NotEmpty(t, real, "yamlKeys found nothing on %s — the reflection itself is broken, "+
			"not merely a table out of date", typ)

		var table []string
		for _, k := range testDocKeys[level] {
			table = append(table, k.name)
		}

		for _, name := range real {
			assert.Contains(t, table, name,
				"%s declares %q, which testDocKeys[%q] does not offer as a completion candidate", typ, name, level)
		}
		for _, name := range table {
			assert.True(t, slices.Contains(real, name),
				"testDocKeys[%q] offers %q, which %s does not accept — flow test would refuse it as unknown", level, name, typ)
		}
	}

	for _, c := range cases {
		t.Run(string(c.level), func(t *testing.T) {
			t.Parallel()
			require(t, c.level, c.typ)
		})
	}
}

// TestYAMLKeysSkipsUnexportedAndTaglessFields pins yamlKeys' two exclusion
// rules directly, rather than trusting them to fall out of the guard above:
// an unexported field ([flowtest.Stub.fromDefaults]) and a `yaml:"-"` field
// would each be silently absent from "real" in that test regardless of
// whether yamlKeys excludes them for the right reason or happens to skip
// them by accident, since an absent-either-way field cannot make that
// comparison fail.
func TestYAMLKeysSkipsUnexportedAndTaglessFields(t *testing.T) {
	t.Parallel()

	type withTagless struct {
		Kept       string `yaml:"kept"`
		Skipped    string `yaml:"-"`
		unexported string
	}
	v := withTagless{Kept: "a", Skipped: "b", unexported: "c"}

	got := yamlKeys(reflect.TypeFor[withTagless]())
	assert.Equal(t, []string{"kept"}, got)
	assert.Equal(t, "c", v.unexported, "the field the test is about must itself be read, or a linter would flag it unused")
}
