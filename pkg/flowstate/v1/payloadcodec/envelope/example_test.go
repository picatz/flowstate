package envelope_test

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
)

// A keyring configuration names each Temporal namespace's keys by location;
// Open reads them and returns a codec per namespace. Here the key comes from an
// environment variable a test supplies.
func Example() {
	env := map[string]string{"PAYLOAD_KEY": string(envelope.GenerateKey())}

	cfg, err := envelope.ParseConfig([]byte(`
namespaces:
  default:
    current: default-2026-09
    keys:
      - id: default-2026-09
        env: PAYLOAD_KEY
`))
	if err != nil {
		panic(err)
	}
	keyring, err := envelope.Open(cfg, envelope.OpenOptions{Getenv: func(name string) string { return env[name] }})
	if err != nil {
		panic(err)
	}

	codec, _ := keyring.Codec("default")
	sealed, err := codec.Encode([]*commonpb.Payload{{Data: []byte("customer record")}})
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
	// customer record
}
