package nearest_test

import (
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// ExampleName turns an unknown name into a did-you-mean suggestion, or into no
// suggestion when nothing known is close enough to be the likely intent.
func ExampleName() {
	known := []string{"http", "log", "exec", "wait", "sleep"}

	for _, typed := range []string{"htpt", "slep", "deploy"} {
		if suggestion, ok := nearest.Name(typed, known); ok {
			fmt.Printf("%s: did you mean %s?\n", typed, suggestion)
		} else {
			fmt.Printf("%s: no suggestion\n", typed)
		}
	}

	// Output:
	// htpt: did you mean http?
	// slep: did you mean sleep?
	// deploy: no suggestion
}
