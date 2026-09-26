package payloadcodec_test

import (
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// ExampleValidateKeyID checks key ids against the grammar every codec's ids
// meet: one to MaxKeyIDBytes ASCII letters, digits, '.', '_' or '-'. A codec
// checks its own id at startup and every id it reads off a payload.
func ExampleValidateKeyID() {
	for _, id := range []string{"kms-key.v3", "3f9a_2026-01", "", "team/a key"} {
		if err := payloadcodec.ValidateKeyID(id); err != nil {
			fmt.Printf("%q: refused\n", id)
			continue
		}
		fmt.Printf("%q: ok\n", id)
	}

	// Output:
	// "kms-key.v3": ok
	// "3f9a_2026-01": ok
	// "": refused
	// "team/a key": refused
}

// ExampleConfig_DataConverter round-trips a value through the converter a
// client, worker, and local run are all built with. The zero Config is the null
// codec, so the payload is serialized but not encrypted.
func ExampleConfig_DataConverter() {
	var config payloadcodec.Config

	dc := config.DataConverter()
	payload, err := dc.ToPayload(map[string]string{"greeting": "hello"})
	if err != nil {
		fmt.Println(err)
		return
	}
	var back map[string]string
	if err := dc.FromPayload(payload, &back); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("round trip:", back["greeting"])

	// Output:
	// round trip: hello
}
