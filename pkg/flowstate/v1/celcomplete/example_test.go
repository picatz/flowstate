package celcomplete_test

import (
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
)

// ExampleComplete offers what may follow a partial reference. The scope here is
// what an editor would build from a Flowfile with two steps, and the answer
// changes with how much of `steps.<id>.<output>` has been typed.
func ExampleComplete() {
	scope := celcomplete.Scope{
		Roots: []celcomplete.Candidate{
			celcomplete.StepsRoot([]celcomplete.Candidate{
				{Name: "fetch", Members: []celcomplete.Candidate{
					{Name: "status", Kind: celcomplete.KindField},
					{Name: "body", Kind: celcomplete.KindField},
				}},
				{Name: "notify"},
			}),
		},
	}

	for _, text := range []string{"size(ste", "steps.f", "steps.fetch.st"} {
		result := celcomplete.Complete(text, scope)
		fmt.Printf("%-16q prefix %q:", text, result.Prefix)
		for _, c := range result.Candidates {
			fmt.Printf(" %s", c.Text())
		}
		fmt.Println()
	}

	// Output:
	// "size(ste"       prefix "ste": steps.
	// "steps.f"        prefix "f": fetch
	// "steps.fetch.st" prefix "st": status
}
