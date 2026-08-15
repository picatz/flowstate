package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The miss case of the registry helpers (#656).
//
// Every assertion here is about a task name the registry does *not* have, which
// is the direction the original defect lived in and the direction no existing
// test looked. A test that registers a task first and then asserts the helper
// answers correctly is a functionality test wearing a security test's clothes:
// it cannot fail for the reason this bug happened, because the branch it never
// enters is the broken one.
//
// Each case is paired with its known-task counterpart, though, because "answer
// the conservative thing on a miss" and "answer the conservative thing always"
// are different functions and only one of them is wanted. Without the pair, a
// [v1.TaskNeedsAuthority] that returned `true` for anything unregistered would
// pass the reference cases below while granting the run's identity and credential
// runtime to every task nobody has — see
// [TestTaskNeedsAuthorityOnAMissWithNoVisibleReference] for why that is the
// permissive direction rather than the closed one.

// unregisteredTaskName is a name nothing in this build registers.
//
// Deliberately shaped like a plugin task's name, because that is what a real one
// is: plugin tasks are registered by a worker's plugin host into
// [v1.DefaultRegistry], from a different place than the built-ins, so a process
// that has not loaded that plugin asks about exactly this.
const unregisteredTaskName = "acme.provision"

func TestUnregisteredTaskNameIsActuallyUnregistered(t *testing.T) {
	_, found := v1.LookupTask(unregisteredTaskName)
	require.False(t, found,
		"the rest of this file asserts things about a registry miss, so a build that "+
			"registers %q would quietly turn every case below into a test of the hit path",
		unregisteredTaskName)
}

// TestTaskNeedsAuthorityOnAMissSeesAHeldSecretReference is the finding the
// staging pass on #656 sharpened, and the one case here that is a defect at the
// current package layout rather than a property of it.
//
// [v1.TaskNeedsAuthority] has two halves and only one of them consults the
// registry. The sweep for a held [v1.SecretRef] reads the invocation it was
// handed and nothing else — it is what lets a plugin task declare no authority
// inputs at all — and it sat *below* an early `return false` on a lookup miss.
// So an invocation visibly carrying a secret reference was routed away from the
// identity-aware activity because nobody had registered its name.
func TestTaskNeedsAuthorityOnAMissSeesAHeldSecretReference(t *testing.T) {
	t.Run("a whole input holding a reference", func(t *testing.T) {
		task := &v1.Task{
			Name: unregisteredTaskName,
			Inputs: map[string]*v1.Value{
				"url":    v1.NewLiteral("https://api.example.com/things"),
				"bearer": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}},
			},
		}

		assert.True(t, v1.TaskNeedsAuthority(task),
			"a task whose input holds a secret reference was sent to an activity with no "+
				"authority to resolve it, because a lookup of its name missed — the sweep "+
				"that finds the reference does not consult the registry and must not be "+
				"skipped by one")
	})

	t.Run("a reference nested inside a mapping", func(t *testing.T) {
		task := &v1.Task{
			Name: unregisteredTaskName,
			Inputs: map[string]*v1.Value{
				"headers": v1.NewStructureMap(map[string]*v1.Value{
					"Authorization": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}},
				}),
			},
		}

		require.True(t, v1.ValueHoldsSecretRef(task.GetInputs()["headers"]),
			"the fixture stopped holding a nested reference, so the case below proves nothing")
		assert.True(t, v1.TaskNeedsAuthority(task),
			"a reference nested in an unregistered task's input is still a reference the "+
				"worker has to resolve under the run's identity")
	})
}

// TestTaskNeedsAuthorityOnAMissWithNoVisibleReference pins the other half of
// #656's decision, which is the half that is easy to get wrong in the *other*
// direction.
//
// A blanket `true` on every miss looks like the fail-closed answer and is not,
// because this function does not gate a permission — both arms it selects run the
// same deployment task-shape policy check, and what the identity-aware arm adds is
// capability: the run's attested identity, a secret store and a credential broker.
// Handing those to a task nothing in the process can describe is granting, not
// withholding.
//
// It also has a demonstrated cost. `tests.ErrorKindCases` makes "unknown task is
// UnknownTask" a contract both drivers keep, on the stated grounds that it is
// permanent; a blanket `true` sends that step to an arm a worker may not have
// registered, where it comes back as a retryable ActivityNotRegisteredError
// instead. So the miss answers the registry-independent sweep and nothing more.
func TestTaskNeedsAuthorityOnAMissWithNoVisibleReference(t *testing.T) {
	task := &v1.Task{
		Name:   unregisteredTaskName,
		Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
	}

	assert.False(t, v1.TaskNeedsAuthority(task),
		"an unknown task carrying nothing to resolve was handed the run's identity and "+
			"credential runtime, and lost the permanent unknown-task classification on "+
			"the way")
}

// TestTaskNeedsAuthorityStillDiscriminatesForKnownTasks is the direction that
// keeps the fix above from being a different bug.
//
// A `true` on every miss is only correct while every *hit* still answers on the
// merits. If this file held only the negative cases, a one-line
// `return true` would pass all of them and route every `log` step in every
// workflow to the identity-aware activity — which is a live replay-compatibility
// change to specifications that run today, rather than to ones that fail.
func TestTaskNeedsAuthorityStillDiscriminatesForKnownTasks(t *testing.T) {
	_, found := v1.LookupTask("log")
	require.True(t, found, "the `log` built-in is the registered half of this comparison")

	t.Run("a registered task with no secret anywhere needs none", func(t *testing.T) {
		task := &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
		}

		assert.False(t, v1.TaskNeedsAuthority(task),
			"an ordinary registered task must stay on the activity name replay "+
				"compatibility depends on")
	})

	t.Run("a registered task's declared authority input still answers yes", func(t *testing.T) {
		task := &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url":    v1.NewLiteral("https://api.example.com/things"),
				"bearer": v1.NewLiteral("a-literal-token"),
			},
		}

		assert.True(t, v1.TaskNeedsAuthority(task),
			"`bearer:` is one of the http task's declared AuthorityInputs, and hoisting "+
				"the reference sweep above the lookup must not have dropped the declared half")
	})

	t.Run("a nil task needs nothing", func(t *testing.T) {
		assert.False(t, v1.TaskNeedsAuthority(nil),
			"there is no invocation to route, so there is nothing to fail closed about")
	})
}

// TestTheOtherRegistryHelpersDoNotCopyTheAuthorityLiteral is the rule #656
// settled on, asserted rather than only written down: the miss answer is chosen
// per question, so these six agree on a *rule* and deliberately not on a shape.
//
// It is here because the obvious tidy-up — "make the four consistent" — would
// flip [v1.AcceptsNestedSecret] from refusing a nested reference in an unknown
// task to permitting one, which is the exact opposite of what #656 is about.
func TestTheOtherRegistryHelpersDoNotCopyTheAuthorityLiteral(t *testing.T) {
	t.Run("an unknown task accepts a nested secret nowhere", func(t *testing.T) {
		assert.False(t, v1.AcceptsNestedSecret(unregisteredTaskName, "headers"),
			"a build that cannot describe a task cannot promise where that task resolves "+
				"a value; false is this question's closed direction")
		assert.Empty(t, v1.NestedSecretInputs(unregisteredTaskName),
			"and it has nowhere to suggest instead")
	})

	t.Run("a registered task's nested-secret inputs still answer on the merits", func(t *testing.T) {
		assert.True(t, v1.AcceptsNestedSecret("http", "headers"),
			"the negative direction above is only meaningful if the positive one still works")
	})

	t.Run("an unknown task gets no invented diagnostics", func(t *testing.T) {
		assert.NoError(t, v1.CheckLiteralInput(unregisteredTaskName, "url", v1.NewLiteral("ftp://example.com")),
			"the step's real problem is that nothing has this task; a second complaint "+
				"about one of its inputs would be a false diagnostic on top of a true one")
		assert.False(t, v1.MustBeExpression(unregisteredTaskName, "expect"),
			"whether an input of a task this build does not have must be an expression "+
				"is not a property of the file")
	})

	t.Run("an unknown task claims none of the run's earlier outputs", func(t *testing.T) {
		assert.False(t, v1.TaskNeedsPrevOutputs(unregisteredTaskName),
			"this decides how much of a run travels into an activity payload, so the "+
				"miss answer that sends the least is the conservative one")
	})
}
