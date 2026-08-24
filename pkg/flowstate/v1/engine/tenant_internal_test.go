package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/interceptor"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestTenantArgFindsScopedTaskIdentity(t *testing.T) {
	got, ok := tenantArg([]any{&v1.Task{}, &v1.Scope{Identity: &v1.WorkloadIdentity{Namespace: "team-a"}}})

	require.True(t, ok)
	require.Equal(t, "team-a", got)
}

// TestTenantArgReadsAScopeWithoutIdentityAsTheDefaultTenant is the direction a
// reading of "no identity, nothing to check" gets wrong, and it is the whole of
// the hole this closes.
//
// The default tenant's namespace is the empty string. So an activity carrying a
// scope with no identity is not declining to say whose work it is — it is
// saying the default tenant's, which is a tenant like any other and one a
// worker restricted to another must refuse.
func TestTenantArgReadsAScopeWithoutIdentityAsTheDefaultTenant(t *testing.T) {
	got, ok := tenantArg([]any{&v1.Task{}, &v1.Scope{}})

	require.True(t, ok, "a scope answers whose work this is whether or not it holds an identity")
	require.Equal(t, "", got)
}

// TestTenantArgFindsNothingWithoutAnIdentityOrScope is the arm that genuinely
// has nothing to check: TaskWithPrev predates scopes and carries neither shape.
// It matters that this stays unchecked rather than being read as the default
// tenant, because a refusal here would break replay of histories that name it.
func TestTenantArgFindsNothingWithoutAnIdentityOrScope(t *testing.T) {
	_, ok := tenantArg([]any{&v1.Task{}, &v1.Workflow_StepOutputs{}})

	require.False(t, ok)
}

// recordingActivityInbound stands in for the rest of the interceptor chain, so
// a test can tell "the guard refused" from "the guard let it through" — the
// distinction a test of tenantArg alone cannot make.
type recordingActivityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	reached bool
}

func (r *recordingActivityInbound) ExecuteActivity(
	context.Context, *interceptor.ExecuteActivityInput,
) (any, error) {
	r.reached = true

	return nil, nil
}

// TestTenantActivityGuardRefusesAnotherTenantsScopedTask walks the guard rather
// than the lookup it uses. tenantArg finding a scope's tenant is only half
// the claim; the half that matters is that a scoped activity belonging to
// another tenant is stopped before it runs.
//
// Written in the negative direction on purpose: a test that only asserts
// team-a's own scoped task is admitted passes just as happily against a guard
// that admits everything.
func TestTenantActivityGuardRefusesAnotherTenantsScopedTask(t *testing.T) {
	scopedTask := func(namespace string) []any {
		return []any{
			&v1.Task{Name: "http"},
			&v1.Scope{Identity: &v1.WorkloadIdentity{Namespace: namespace}},
		}
	}

	t.Run("another tenant's scoped task is refused", func(t *testing.T) {
		next := &recordingActivityInbound{}
		guard := &tenantActivityInbound{namespace: "team-a"}
		guard.Next = next

		_, err := guard.ExecuteActivity(t.Context(), &interceptor.ExecuteActivityInput{
			Args: scopedTask("team-b"),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "team-b")
		require.False(t, next.reached, "a refused activity must not reach the rest of the chain")
	})

	t.Run("this tenant's scoped task is admitted", func(t *testing.T) {
		next := &recordingActivityInbound{}
		guard := &tenantActivityInbound{namespace: "team-a"}
		guard.Next = next

		_, err := guard.ExecuteActivity(t.Context(), &interceptor.ExecuteActivityInput{
			Args: scopedTask("team-a"),
		})

		require.NoError(t, err)
		require.True(t, next.reached)
	})
}

// TestTenantGuardAdmitsAScopeBuiltAtItsCallSite covers the shapes that are not
// derived from a run's own scope but assembled where they are dispatched:
// WorkflowVars' scope of ambient vars and a profile, and runUndoTask's scope of
// a profile alone. Every field a run supplies is absent from both.
//
// Those are the scopes that make reading an identity-less one as the default
// tenant dangerous in the opposite direction from the hole it closes: each has
// to be given the run's identity at its call site, and if any is not, a
// `--tenant` worker refuses its own run's vars or its own run's compensation.
// This asserts the shapes are admitted once they carry it — the direction a
// test of the refusal alone cannot see.
func TestTenantGuardAdmitsAScopeBuiltAtItsCallSite(t *testing.T) {
	identity := &v1.WorkloadIdentity{Namespace: "team-a"}

	for name, args := range map[string][]any{
		"workflow vars": {&v1.Scope{
			AmbientVars: map[string]*v1.Value{"release": v1.NewLiteral("v1")},
			Profile:     "default",
			Identity:    identity,
		}},
		"compensation": {
			&v1.Task{Name: "http"},
			&v1.Scope{Profile: "default", Identity: identity},
		},
	} {
		t.Run(name, func(t *testing.T) {
			next := &recordingActivityInbound{}
			guard := &tenantActivityInbound{namespace: "team-a"}
			guard.Next = next

			_, err := guard.ExecuteActivity(t.Context(), &interceptor.ExecuteActivityInput{Args: args})

			require.NoError(t, err, "a worker restricted to team-a refused team-a's own %s activity", name)
			require.True(t, next.reached)

			// And the same shape without the identity its call site must set is
			// refused, which is what makes the assertion above load-bearing
			// rather than a restatement of "the guard admits things".
			stripped := make([]any, len(args))
			for i, arg := range args {
				if scope, ok := arg.(*v1.Scope); ok {
					clone := proto.Clone(scope).(*v1.Scope)
					clone.Identity = nil
					stripped[i] = clone
					continue
				}
				stripped[i] = arg
			}

			bare := &recordingActivityInbound{}
			guard = &tenantActivityInbound{namespace: "team-a"}
			guard.Next = bare

			_, err = guard.ExecuteActivity(t.Context(), &interceptor.ExecuteActivityInput{Args: stripped})

			require.Error(t, err, "a %s scope naming no tenant was admitted by a team-a worker", name)
			require.False(t, bare.reached)
		})
	}
}
