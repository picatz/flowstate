package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestEveryTaskDescribesItself is what `flow tasks`, the editor, and the
// validator all rest on.
//
// Each of those used to answer "what does this task take" separately — two
// implementations of required-ness, and a type namer that existed only in the
// language server, which is why `flow tasks` could say a task exists and not what
// it needs. One answer now, derived from the descriptor.
func TestEveryTaskDescribesItself(t *testing.T) {
	t.Parallel()

	for _, def := range v1.DefaultRegistry().All() {
		t.Run(def.Name, func(t *testing.T) {
			t.Parallel()

			inputs := v1.Inputs(def)
			require.NotEmpty(t, inputs, "task %q describes no inputs", def.Name)

			for _, field := range inputs {
				require.NotEmpty(t, field.Name)
				require.NotEmpty(t, field.Type, "input %q has no type an author could read", field.Name)
				require.NotContains(t, field.Type, "TYPE_",
					"input %q reports a Protobuf type name rather than the DSL's: %s", field.Name, field.Type)
			}

			// Required first, so a reader meets what they cannot leave out before
			// what merely tunes it.
			seenOptional := false
			for _, field := range inputs {
				if !field.Required {
					seenOptional = true
					continue
				}
				require.False(t, seenOptional,
					"required input %q sorts after an optional one", field.Name)
			}
		})
	}
}

// TestRequiredIsReadFromTheSchema checks the rule against a task whose answer is
// known, in both directions.
//
// The http task declares url required and method not, so a helper that reported
// everything required — or nothing — would pass a weaker test.
func TestRequiredIsReadFromTheSchema(t *testing.T) {
	t.Parallel()

	def, found := v1.LookupTask("http")
	require.True(t, found)

	required := map[string]bool{}
	for _, field := range v1.Inputs(def) {
		required[field.Name] = field.Required
	}

	require.True(t, required["url"], "url is required by the schema and is not reported as such")
	require.False(t, required["method"], "method is optional and is reported as required")
}

// TestADeferredInputIsMarked covers the distinction an author most needs and
// cannot infer.
//
// The engine resolves an expression before scheduling a step. A deferred input is
// evaluated by the task instead, against a scope the workflow does not have —
// which is why http's `outputs` may name `status_code` and an ordinary input may
// not. Nothing in the type says so.
func TestADeferredInputIsMarked(t *testing.T) {
	t.Parallel()

	def, found := v1.LookupTask("http")
	require.True(t, found)

	deferred := map[string]bool{}
	for _, field := range v1.Inputs(def) {
		deferred[field.Name] = field.Deferred
	}

	require.True(t, deferred["outputs"], "http's outputs is evaluated by the task and is not marked")
	require.False(t, deferred["url"], "url is resolved by the engine and is marked deferred")
}

// TestDescribeTaskCarriesTheClaimsWithSecurityWeight is #712: needs_scope,
// secret_inputs, shapes_outputs, deferred_inputs and expression_inputs were
// computable from a TaskDef and simply left off TaskDescription, so every reader
// of DescribeTask's result — the catalog, GetCatalog, `flow plugins`, and
// TaskSchemaDigest, which is computed over exactly this message — had no way to
// see a task's own claim to see the whole workflow's state or to receive a host
// secret.
//
// http exercises three of the five for a real, non-synthetic reason: it declares
// NeedsPrevOutputs, ShapesOutputs and ExpressionInputs today, so a regression that
// drops them from DescribeTask fails here rather than only in a test that builds
// its own TaskDef.
func TestDescribeTaskCarriesTheClaimsWithSecurityWeight(t *testing.T) {
	t.Parallel()

	def, found := v1.LookupTask("http")
	require.True(t, found)

	described := v1.DescribeTask(def)

	require.True(t, described.GetNeedsScope(),
		"http needs prior step outputs to evaluate its own outputs: input, and DescribeTask does not say so")
	require.True(t, described.GetShapesOutputs(),
		"http replaces its declared outputs with outputs:, and DescribeTask does not say so")
	require.Contains(t, described.GetExpressionInputs(), "expect",
		"http requires expect to be written as an expression, and DescribeTask does not say so")

	// SecretInputs and DeferredInputs are exercised against a task with real
	// values for them, since http's own DeferredInputs and SecretInputs are
	// empty (its secret-accepting inputs are NestedSecretInputs, a different
	// list — see TaskDef.SecretInputs' doc comment for why the two are not the
	// same question).
	synthetic := v1.TaskDef{
		Name:                 "synthetic",
		DeferredInputs:       []string{"outputs"},
		SecretInputs:         []string{"token"},
		RequiredSecretInputs: []string{"token"},
	}
	syntheticDescribed := v1.DescribeTask(synthetic)

	require.Equal(t, []string{"outputs"}, syntheticDescribed.GetDeferredInputs(),
		"DeferredInputs on the TaskDef is not reaching TaskDescription")
	require.Equal(t, []string{"token"}, syntheticDescribed.GetSecretInputs(),
		"SecretInputs on the TaskDef is not reaching TaskDescription")
	require.Equal(t, []string{"token"}, syntheticDescribed.GetRequiredSecretInputs(),
		"RequiredSecretInputs on the TaskDef is not reaching TaskDescription")
}

// digestOf reproduces the marshaling [pkg/flowstate/v1/plugin.Host.Catalog]
// does over a slice of TaskDescriptions — a deterministic marshal of a
// PluginDescription holding them.
func digestOf(t *testing.T, tasks ...*v1.TaskDescription) string {
	t.Helper()

	bytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&v1.PluginDescription{Tasks: tasks})
	require.NoError(t, err)

	return v1.ContentDigest(bytes)
}

// TestClaimsDigestChangesWithNeedsScope covers the claim-only digest —
// [v1.TaskDescriptionClaimsOnly] — flipping needs_scope changes it.
//
// Before #712 nothing did: the field existed on TaskManifest and on TaskDef
// and was enforced, but DescribeTask never wrote it into TaskDescription, so
// it was bytes no digest ever saw. A plugin update turning needs_scope from
// false to true — the largest quiet privilege escalation the protocol allows
// — pinned clean.
func TestClaimsDigestChangesWithNeedsScope(t *testing.T) {
	t.Parallel()

	before := v1.TaskDescriptionClaimsOnly(v1.DescribeTask(v1.TaskDef{Name: "commit_push"}))
	after := v1.TaskDescriptionClaimsOnly(v1.DescribeTask(v1.TaskDef{Name: "commit_push", NeedsPrevOutputs: true}))

	require.NotEqual(t, digestOf(t, before), digestOf(t, after),
		"needs_scope flipped and the claims-only digest did not change")
}

// TestClaimsDigestChangesWithSecretInputs is the same claim for the other
// field with security weight: which inputs the host will resolve a secret
// reference into before it crosses into the plugin.
func TestClaimsDigestChangesWithSecretInputs(t *testing.T) {
	t.Parallel()

	before := v1.TaskDescriptionClaimsOnly(v1.DescribeTask(v1.TaskDef{Name: "commit_push"}))
	after := v1.TaskDescriptionClaimsOnly(v1.DescribeTask(v1.TaskDef{Name: "commit_push", SecretInputs: []string{"token"}}))

	require.NotEqual(t, digestOf(t, before), digestOf(t, after),
		"secret_inputs gained an entry and the claims-only digest did not change")
}

func TestClaimsDigestChangesWithRequiredSecretInputs(t *testing.T) {
	t.Parallel()

	before := v1.TaskDescriptionClaimsOnly(v1.DescribeTask(v1.TaskDef{
		Name: "connect", SecretInputs: []string{"dsn"},
	}))
	after := v1.TaskDescriptionClaimsOnly(v1.DescribeTask(v1.TaskDef{
		Name: "connect", SecretInputs: []string{"dsn"}, RequiredSecretInputs: []string{"dsn"},
	}))

	require.NotEqual(t, digestOf(t, before), digestOf(t, after),
		"required_secret_inputs gained an entry and the claims-only digest did not change")
}

// TestTaskSchemaDigestIsStableAcrossAClaimsChange is the P1 a Codex review on
// #763 named: an in-flight durable run's ResolvedPlugin embeds
// TaskSchemaDigest at submission, and the worker admission check compares it
// exactly at every segment boundary, non-retryably. If TaskSchemaDigest moved
// whenever a claim field did, a routine worker upgrade to this commit would
// permanently fail every already-durable run touching a plugin with a
// non-default claim — including the shipped SQL and Codex plugins'
// secret_inputs — for a plugin whose behavior never changed. So
// TaskSchemaDigest ([v1.TaskDescriptionSansClaims]) must not move when only
// the claim fields do; [TestClaimsDigestChangesWithNeedsScope] above is where
// that change is supposed to be visible instead.
func TestTaskSchemaDigestIsStableAcrossAClaimsChange(t *testing.T) {
	t.Parallel()

	before := v1.TaskDescriptionSansClaims(v1.DescribeTask(v1.TaskDef{Name: "commit_push"}))
	after := v1.TaskDescriptionSansClaims(v1.DescribeTask(v1.TaskDef{
		Name:                 "commit_push",
		NeedsPrevOutputs:     true,
		SecretInputs:         []string{"token"},
		RequiredSecretInputs: []string{"token"},
		ShapesOutputs:        true,
		DeferredInputs:       []string{"outputs"},
		ExpressionInputs:     []string{"expect"},
	}))

	require.Equal(t, digestOf(t, before), digestOf(t, after),
		"every claim field changed and the task schema digest moved anyway; an in-flight run "+
			"pinned to the old digest would be permanently refused by a worker upgraded to this code, "+
			"for a plugin whose descriptors never changed")
}

// TestTaskSchemaDigestIsStableAcrossASecretInputsNoteChange is the same claim
// as [TestTaskSchemaDigestIsStableAcrossAClaimsChange], but for the one
// SecretInputs regression that test cannot see.
//
// That test's TaskDefs set SecretInputs to a name — "token" — that names no
// field on either TaskDef's Inputs (both leave Inputs nil), so
// taskInputNotes never has a real TaskField to attach the secret-reference
// note ("may hold a secret reference"; the constant naming it,
// secretReferenceNote, is unexported and this test lives in the external
// flowstatev1_test package) to, and the note this test exists to catch never
// appears in either digest. Only a TaskDef with a real Inputs descriptor,
// where SecretInputs names an
// actual field on it, exercises the path: before
// #763's fix, [v1.TaskDescriptionSansClaims] returned Inputs unchanged, so
// the note taskInputNotes writes for a plugin-declared SecretInputs field
// leaked into the digest that is supposed to be immune to a claims change —
// defeating the split TestTaskSchemaDigestIsStableAcrossAClaimsChange exists
// to protect. http is used as the real descriptor because it is a built-in
// task with real proto Inputs; "url" carries no note of its own today, so
// this test's before/after difference is exactly the SecretInputs note and
// nothing else.
func TestTaskSchemaDigestIsStableAcrossASecretInputsNoteChange(t *testing.T) {
	t.Parallel()

	def, found := v1.LookupTask("http")
	require.True(t, found)
	require.Empty(t, def.SecretInputs, "http must start with no SecretInputs for this test to isolate the change")

	withSecretInput := def
	withSecretInput.SecretInputs = []string{"url"}

	before := v1.TaskDescriptionSansClaims(v1.DescribeTask(def))
	after := v1.TaskDescriptionSansClaims(v1.DescribeTask(withSecretInput))

	require.Equal(t, digestOf(t, before), digestOf(t, after),
		"declaring SecretInputs for a real field moved the task schema digest; the "+
			"\"may hold a secret reference\" note taskInputNotes writes for it leaked into "+
			"TaskDescriptionSansClaims, undoing the claims/schema digest split")

	withSecretInput.RequiredSecretInputs = []string{"url"}
	afterRequired := v1.TaskDescriptionSansClaims(v1.DescribeTask(withSecretInput))
	require.Equal(t, digestOf(t, before), digestOf(t, afterRequired),
		"declaring RequiredSecretInputs for a real field moved the task schema digest; "+
			"its authoring note leaked into TaskDescriptionSansClaims")
}

// TestClaimsSchemaVersionDistinguishesUnknownFromFalse is the fail-closed
// direction a Codex review on #763 named: proto3 cannot mark a bool or a
// repeated string field `optional`, so NeedsScope=false and SecretInputs=nil
// decode identically whether a task genuinely claims nothing or a GetCatalog
// response came from a deployment still on a build that predates these
// fields entirely (a rolling upgrade's old-server, new-client case).
// ClaimsSchemaVersion is the presence signal that resolves it, and this is
// the test that a caller reading a catalog with no version set is told
// "unknown", not handed a false that reads as "safe".
func TestClaimsSchemaVersionDistinguishesUnknownFromFalse(t *testing.T) {
	t.Parallel()

	current := v1.Catalog()
	require.True(t, v1.TaskDescriptionClaimsKnown(current),
		"a catalog built by this binary reports its claim fields as unknown, "+
			"which would make GetNeedsScope() and GetSecretInputs() look like honest answers on every task")

	// The old-shaped response: every task's fields as DescribeTask would have
	// produced them before #712, and no version at all — exactly what an old
	// server's GetCatalog answers during a rolling upgrade.
	old := &v1.TaskCatalog{Tasks: current.Tasks}
	require.False(t, v1.TaskDescriptionClaimsKnown(old),
		"a catalog with no ClaimsSchemaVersion reads as known, so a remote GetCatalog "+
			"caller talking to an old deployment would trust its zero-valued needs_scope as an explicit no")
}

// TestClaimsSchemaVersionRequiresExactEquality is the review's second and
// fourth findings together: "known" bounded only by `> 0`, and later only by
// `<= CurrentClaimsSchemaVersion`, both fail open in one direction. A future
// version this build has never heard of might redefine an existing field, not
// only add one, so a v1-built client cannot trust a v2 catalog; and a v2
// client cannot trust a v1 catalog either, because a v1 response structurally
// cannot carry whatever field v2 introduced — its absence there is not the
// same fact as its absence on a genuine v2 "claims nothing" answer. Only
// exact equality is safe in both directions.
func TestClaimsSchemaVersionRequiresExactEquality(t *testing.T) {
	t.Parallel()

	tasks := v1.Catalog().Tasks

	future := &v1.TaskCatalog{Tasks: tasks, ClaimsSchemaVersion: v1.CurrentClaimsSchemaVersion + 1}
	require.False(t, v1.TaskDescriptionClaimsKnown(future),
		"a claims schema version newer than this build understands reads as known, "+
			"so an older client reading a newer deployment's catalog would trust claim fields it cannot interpret")

	if v1.CurrentClaimsSchemaVersion > 1 {
		older := &v1.TaskCatalog{Tasks: tasks, ClaimsSchemaVersion: v1.CurrentClaimsSchemaVersion - 1}
		require.False(t, v1.TaskDescriptionClaimsKnown(older),
			"a claims schema version older than this build's reads as known, so a newer client reading "+
				"an older deployment's catalog would trust the absence of a field that version cannot carry")
	}

	// The positive direction beside both, so neither above is simply
	// asserting the check always fails: the version this build actually
	// produces is accepted.
	require.True(t, v1.TaskDescriptionClaimsKnown(&v1.TaskCatalog{
		Tasks: tasks, ClaimsSchemaVersion: v1.CurrentClaimsSchemaVersion,
	}))
}

// TestClaimListsCanonicalizeRegardlessOfManifestOrder is the review's third
// finding: secret_inputs, deferred_inputs and expression_inputs are
// membership sets to every engine reader (MustBeExpression, IsDeferred,
// resolvePluginSecretInputs all ask "does this list contain X"), but the
// manifest schema bounds them only by size, so nothing stops two launches of
// one unchanged plugin binary from declaring the same set in a different
// order. TaskSchemaDigest hashes TaskDescription with deterministic
// marshaling, which fixes field order and nothing about a repeated string
// field's contents — so without this, reordering alone could change the
// digest and fail CheckPluginsAvailable's exact-match replay guard for a
// plugin that has not actually changed.
func TestClaimListsCanonicalizeRegardlessOfManifestOrder(t *testing.T) {
	t.Parallel()

	forward := v1.DescribeTask(v1.TaskDef{
		Name:             "commit_push",
		SecretInputs:     []string{"token", "webhook_secret"},
		DeferredInputs:   []string{"outputs", "body"},
		ExpressionInputs: []string{"expect", "auth"},
	})
	reversed := v1.DescribeTask(v1.TaskDef{
		Name:             "commit_push",
		SecretInputs:     []string{"webhook_secret", "token"},
		DeferredInputs:   []string{"body", "outputs"},
		ExpressionInputs: []string{"auth", "expect"},
	})

	digestOf := func(described *v1.TaskDescription) string {
		bytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(
			&v1.PluginDescription{Tasks: []*v1.TaskDescription{described}})
		require.NoError(t, err)
		return v1.ContentDigest(bytes)
	}

	require.Equal(t, digestOf(forward), digestOf(reversed),
		"the same plugin declaring the same set of claim inputs in a different order "+
			"produced a different task schema digest, which would fail an unchanged plugin's replay check")

	// The lists themselves are equal too, not merely their digests — a
	// consumer reading GetSecretInputs() directly should see one canonical
	// order rather than needing to know to sort before comparing.
	assert.Equal(t, forward.GetSecretInputs(), reversed.GetSecretInputs())
	assert.Equal(t, forward.GetDeferredInputs(), reversed.GetDeferredInputs())
	assert.Equal(t, forward.GetExpressionInputs(), reversed.GetExpressionInputs())
}
