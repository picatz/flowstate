package envelope_test

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/hpke"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

// A namespace's codec: a local wrapping key, and an HPKE escrow recipient
// whose private key is kept elsewhere. Every data key is wrapped to both.
func Example() {
	primary, err := local.Parse(local.Generate())
	if err != nil {
		panic(err)
	}
	_, public, err := hpke.Generate(0)
	if err != nil {
		panic(err)
	}
	escrow, err := hpke.Parse(public, nil)
	if err != nil {
		panic(err)
	}

	codec, err := envelope.New(context.Background(), envelope.Options{
		Binding: "orders",
		Current: "orders-2026-09",
		Keys:    []envelope.Recipient{{ID: "orders-2026-09", Key: primary}},
		Escrow:  []envelope.Recipient{{ID: "break-glass", Key: escrow}},
	})
	if err != nil {
		panic(err)
	}

	sealed, err := codec.Encode([]*commonpb.Payload{{Data: []byte(`{"card":"4111..."}`)}})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(sealed[0].GetMetadata()["encoding"]))

	opened, err := codec.Decode(sealed)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(opened[0].GetData()))
	// Output:
	// binary/flowstate-envelope-v1
	// {"card":"4111..."}
}
