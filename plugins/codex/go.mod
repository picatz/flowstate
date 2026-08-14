module github.com/picatz/flowstate/plugins/codex

go 1.26.0

toolchain go1.26.6

require (
	github.com/picatz/flowstate v0.0.0-00010101000000-000000000000
	google.golang.org/genproto/googleapis/api v0.0.0-20260803160001-6ac0973c030d
	google.golang.org/protobuf v1.36.11
)

require github.com/BurntSushi/toml v1.6.0

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.45.0 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	go.opentelemetry.io/otel/trace v1.45.0 // indirect
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20260709200747-435963d16310.1 // indirect
	buf.build/go/protovalidate v1.2.0 // indirect
	cel.dev/expr v0.25.2 // indirect
	connectrpc.com/authn v0.2.0 // indirect
	connectrpc.com/connect v1.20.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/cel-go v0.30.0 // indirect
	github.com/picatz/jose v0.0.0-20250624193854-494d48fb4d59 // indirect
	github.com/picatz/openai v0.0.0-20251126013453-02ace0a229c7
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20250911091902-df9299821621 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d // indirect
)

// This plugin is a separate module specifically so that
// github.com/picatz/openai/codex, a real third-party dependency, never
// enters the root module's dependency graph - the same reasoning
// plugins/vcs's own go.mod gives for go-git. The replace below is a
// local-development convenience only, so this module builds against the
// flowstate tree it ships beside rather than a tagged release; a plugin
// distributed on its own would instead require a published version of
// github.com/picatz/flowstate.
replace github.com/picatz/flowstate => ../..
