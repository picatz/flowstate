module github.com/picatz/flowstate/plugins/github

go 1.25.4

toolchain go1.26.5

require (
	github.com/google/go-github/v75 v75.0.0
	github.com/picatz/flowstate v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.11
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.9-20250912141014-52f32327d4b0.1 // indirect
	buf.build/go/protovalidate v1.0.0 // indirect
	cel.dev/expr v0.25.1 // indirect
	connectrpc.com/authn v0.2.0 // indirect
	connectrpc.com/connect v1.20.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/cel-go v0.30.0 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/picatz/jose v0.0.0-20250624193854-494d48fb4d59 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20250911091902-df9299821621 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)

// This plugin is a separate module specifically so that go-github, a real
// third-party dependency, never enters the root module's dependency graph.
// The replace below is a local-development convenience only, so this module
// builds against the flowstate tree it ships beside rather than a tagged
// release; a plugin distributed on its own would instead require a published
// version of github.com/picatz/flowstate.
replace github.com/picatz/flowstate => ../..
