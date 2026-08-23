package authorization

import (
	"bytes"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func fixture() (*v1.AuthorizationRequest, *v1.AuthorizationPolicy) {
	q := &v1.AuthorizationRequest{Principal: &v1.Principal{Subject: "alice", Issuer: "issuer", Tenant: "tenant", Audience: "flow", ProofKey: "key", ActorChain: []string{"root"}}, Action: "read", Resource: "doc"}
	allow := &v1.AuthorizationRule{Effect: v1.AuthorizationEffect_AUTHORIZATION_EFFECT_ALLOW, Subjects: []string{"alice"}, Issuers: []string{"issuer"}, Tenants: []string{"tenant"}, Audiences: []string{"flow"}, Actions: []string{"read"}, Resources: []string{"doc"}, ProofKeys: []string{"key"}, ActorChains: []string{"root"}}
	return q, &v1.AuthorizationPolicy{Grants: []*v1.AuthorizationPolicyLayer{{Rules: []*v1.AuthorizationRule{allow}}}}
}
func decision(t *testing.T, q *v1.AuthorizationRequest, p *v1.AuthorizationPolicy) *v1.AuthorizationDecision {
	t.Helper()
	e, err := NewEvaluator(p)
	require.NoError(t, err)
	got, err := e.Evaluate(q)
	require.NoError(t, err)
	ref, err := ReferenceEvaluate(q, p)
	require.NoError(t, err)
	require.True(t, proto.Equal(ref, got))
	return got
}
func TestSecurityProperties(t *testing.T) {
	q, p := fixture()
	require.True(t, decision(t, q, p).GetAllowed())
	t.Run("deny wins", func(t *testing.T) {
		p.Grants[0].Rules = append(p.Grants[0].Rules, &v1.AuthorizationRule{Effect: v1.AuthorizationEffect_AUTHORIZATION_EFFECT_DENY, Subjects: []string{"alice"}})
		d := decision(t, q, p)
		require.False(t, d.GetAllowed())
		require.True(t, d.GetExplicitDeny())
	})
	q, p = fixture()
	t.Run("boundary cannot grant", func(t *testing.T) {
		p.Boundaries = []*v1.AuthorizationPolicyLayer{{Rules: []*v1.AuthorizationRule{{Effect: v1.AuthorizationEffect_AUTHORIZATION_EFFECT_ALLOW, Actions: []string{"write"}}}}}
		require.False(t, decision(t, q, p).GetAllowed())
	})
	q, p = fixture()
	t.Run("removing grant cannot allow", func(t *testing.T) { p.Grants[0].Rules = nil; require.False(t, decision(t, q, p).GetAllowed()) })
	q, p = fixture()
	t.Run("attestation fails closed", func(t *testing.T) {
		p.Grants[0].Rules[0].AttestedFacts = map[string]string{"mfa": "yes"}
		q.Facts = []*v1.AttestedFact{{Name: "mfa", Value: "yes", Attested: false}}
		require.False(t, decision(t, q, p).GetAllowed())
	})
	q, p = fixture()
	t.Run("unordered", func(t *testing.T) {
		p.Grants[0].Rules = append(p.Grants[0].Rules, &v1.AuthorizationRule{Effect: v1.AuthorizationEffect_AUTHORIZATION_EFFECT_ALLOW, Subjects: []string{"nobody"}})
		a := decision(t, q, p)
		p.Grants[0].Rules[0], p.Grants[0].Rules[1] = p.Grants[0].Rules[1], p.Grants[0].Rules[0]
		require.True(t, proto.Equal(a, decision(t, q, p)))
	})
	q, p = fixture()
	t.Run("identity fields bound", func(t *testing.T) {
		mutations := []func(*v1.AuthorizationRequest){func(x *v1.AuthorizationRequest) { x.Principal.Issuer = "other" }, func(x *v1.AuthorizationRequest) { x.Principal.Tenant = "other" }, func(x *v1.AuthorizationRequest) { x.Principal.Audience = "other" }, func(x *v1.AuthorizationRequest) { x.Resource = "other" }, func(x *v1.AuthorizationRequest) { x.Principal.ProofKey = "other" }, func(x *v1.AuthorizationRequest) { x.Principal.ActorChain = []string{"other"} }}
		for _, mutate := range mutations {
			x := proto.Clone(q).(*v1.AuthorizationRequest)
			mutate(x)
			require.False(t, decision(t, x, p).GetAllowed())
		}
	})
	q, p = fixture()
	t.Run("surfaces and concealment", func(t *testing.T) {
		e, err := NewEvaluator(p)
		require.NoError(t, err)
		var first *v1.AuthorizationDecision
		for _, s := range []Surface{Connect, MCP, API, Plugin, Internal} {
			d, err := e.EvaluateSurface(s, q)
			require.NoError(t, err)
			if first == nil {
				first = d
			} else {
				require.True(t, proto.Equal(first, d))
			}
		}
		denied := deny("no grant", false)
		require.EqualError(t, PublicError(denied, false), "not found")
		require.EqualError(t, PublicError(denied, true), "permission denied")
		require.False(t, denied.GetAllowed())
	})
}
func fuzzInput(b []byte) (*v1.AuthorizationRequest, *v1.AuthorizationPolicy) {
	if len(b) == 0 {
		b = []byte{0}
	}
	pick := func(i int, stringValue string) string {
		if int(b[i%len(b)])%2 == 0 {
			return stringValue
		}
		return "other"
	}
	q, p := fixture()
	q.Principal.Issuer = pick(0, "issuer")
	q.Principal.Tenant = pick(1, "tenant")
	q.Action = pick(2, "read")
	q.Resource = pick(3, "doc")
	if b[0]&4 != 0 {
		p.Grants[0].Rules = append(p.Grants[0].Rules, &v1.AuthorizationRule{Effect: v1.AuthorizationEffect_AUTHORIZATION_EFFECT_DENY, Subjects: []string{pick(4, "alice")}})
	}
	return q, p
}
func FuzzDecisionCombination(f *testing.F) {
	f.Add([]byte("regression-deny"))
	f.Fuzz(func(t *testing.T, b []byte) { q, p := fuzzInput(b); decision(t, q, p) })
}
func FuzzPolicyParsing(f *testing.F) {
	f.Add([]byte(`{"grants":[]}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		_, _ = ParsePolicy(b)
	})
}
func FuzzCELActivationConstruction(f *testing.F) {
	f.Add([]byte("context"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxFieldBytes {
			return
		}
		q, p := fixture()
		q.Context = map[string]string{"f": string(bytes.ToValidUTF8(b, nil))}
		decision(t, q, p)
	})
}
func FuzzRelationshipTraversal(f *testing.F) {
	f.Add([]byte("cycle"))
	f.Fuzz(func(t *testing.T, b []byte) {
		q, p := fixture()
		p.Grants[0].Rules[0].Relation = "owns"
		p.Relationships = []*v1.Relationship{{Subject: "alice", Relation: "owns", Resource: "group"}, {Subject: "group", Relation: "owns", Resource: map[bool]string{true: "doc", false: "alice"}[len(b)%2 == 0]}}
		decision(t, q, p)
	})
}
func FuzzIdentityChainParsing(f *testing.F) {
	f.Add([]byte("delegation"))
	f.Fuzz(func(t *testing.T, b []byte) {
		q, p := fixture()
		if len(b) > 0 && b[0]&1 != 0 {
			q.Principal.ActorChain = []string{"root"}
			q.Delegation = []*v1.Delegation{{Delegator: "root", Delegate: "alice", Actions: []string{"read"}, Resources: []string{"doc"}}}
		}
		decision(t, q, p)
	})
}
func FuzzAuditRendering(f *testing.F) {
	f.Add([]byte("audit"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxFieldBytes {
			return
		}
		q, _ := fixture()
		q.Context = map[string]string{string(bytes.ToValidUTF8(b, nil)): "redacted"}
		out, _ := RenderAudit(q, &v1.AuthorizationDecision{})
		if bytes.Contains(out, []byte("redacted")) {
			t.Fatal("audit rendered context value")
		}
	})
}
