module github.com/picatz/flowstate/plugins/sql

go 1.27.0

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/picatz/flowstate v0.0.0-00010101000000-000000000000
	google.golang.org/genproto/googleapis/api v0.0.0-20260803160001-6ac0973c030d
	google.golang.org/protobuf v1.36.12
	modernc.org/sqlite v1.57.0
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.1 // indirect
	buf.build/go/protovalidate v1.3.0 // indirect
	cel.dev/expr v0.25.2 // indirect
	connectrpc.com/authn v0.2.0 // indirect
	connectrpc.com/connect v1.20.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/cel-go v0.31.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/modelcontextprotocol/go-sdk v1.7.0 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/picatz/jose v0.0.0-20250624193854-494d48fb4d59 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	go.opentelemetry.io/otel/trace v1.46.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20250911091902-df9299821621 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d // indirect
	modernc.org/libc v1.74.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

// This plugin is a separate module specifically so that its SQL drivers
// (modernc.org/sqlite and github.com/jackc/pgx/v5 - see doc.go, "Drivers")
// never enter the root module's dependency graph. The replace below is a
// local-development convenience only; a plugin distributed on its own
// would instead require a published version of github.com/picatz/flowstate.
replace github.com/picatz/flowstate => ../..
