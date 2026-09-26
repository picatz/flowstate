package celrule_test

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"

	"github.com/picatz/flowstate/pkg/flowstate/v1/celrule"
)

// ExampleSet_Decide builds a deny-first policy from an operator's CEL rules and
// decides three requests against it. The environment, which names what a rule
// may read, is the caller's: here a rule sees one string, host.
func ExampleSet_Decide() {
	env, err := cel.NewEnv(cel.Variable("host", cel.StringType))
	if err != nil {
		fmt.Println(err)
		return
	}
	const costLimit = 10_000
	wrap := func(kind celrule.Kind, err error) error { return fmt.Errorf("%s: %w", kind, err) }

	deny, err := celrule.CompileAll(env, celrule.Deny, []string{`host.endsWith(".internal")`}, costLimit, wrap)
	if err != nil {
		fmt.Println(err)
		return
	}
	allow, err := celrule.CompileAll(env, celrule.Allow, []string{`host.endsWith(".example.com")`}, costLimit, wrap)
	if err != nil {
		fmt.Println(err)
		return
	}
	policy := celrule.Set{Allow: allow, Deny: deny}

	for _, host := range []string{"api.example.com", "db.internal", "example.org"} {
		decision, err := policy.Decide(context.Background(), map[string]any{"host": host})
		if err != nil {
			// A rule that cannot be evaluated is a refusal, never a permit.
			fmt.Println(host, "refused:", err)
			continue
		}
		switch decision.Verdict {
		case celrule.Permitted:
			fmt.Printf("%s: permitted by %s\n", host, decision.Rule.Source())
		case celrule.DeniedByRule:
			fmt.Printf("%s: denied by %s\n", host, decision.Rule.Source())
		case celrule.NoAllowRuleMatched:
			fmt.Printf("%s: no allow rule matched\n", host)
		}
	}

	// Output:
	// api.example.com: permitted by host.endsWith(".example.com")
	// db.internal: denied by host.endsWith(".internal")
	// example.org: no allow rule matched
}

// ExampleCompile shows the check a rule gets at load: it must type-check against
// the environment and evaluate to a bool, so a mistake refuses start-up rather
// than a request.
func ExampleCompile() {
	env, err := cel.NewEnv(cel.Variable("host", cel.StringType))
	if err != nil {
		fmt.Println(err)
		return
	}

	rule, err := celrule.Compile(env, `host == "api.example.com"`, 10_000)
	fmt.Println(rule.Source(), err)

	_, err = celrule.Compile(env, `host + ".internal"`, 10_000)
	fmt.Println(err)

	// Output:
	// host == "api.example.com" <nil>
	// rule "host + \".internal\"" evaluates to string, want bool
}
