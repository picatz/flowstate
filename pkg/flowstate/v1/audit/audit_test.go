package audit_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
)

// TestTheRecordHasNoFieldAPayloadCouldGoIn is the structural half of the
// redaction decision, and the reason there is no scrubber in this package.
//
// The claim is not "nothing puts a payload in the record". It is that there is
// nowhere to put one: every field is either chosen by the server or is an
// identity this deployment attested, and none of them is a bytes field, a
// list, a map, or a message that can hold arbitrary values. A future
// `string reason` carrying err.Error() — the #993 defect exactly — fails here
// rather than at the sink.
func TestTheRecordHasNoFieldAPayloadCouldGoIn(t *testing.T) {
	t.Parallel()

	fields := (&v1.AuditRecord{}).ProtoReflect().Descriptor().Fields()

	// The pinned set. Adding a field to the schema fails this until somebody
	// has decided, in writing, that the new field is one of the two kinds
	// above.
	want := []string{
		"action", "decision", "rpc", "identity",
		"resource_kind", "resource_key", "decided_at", "deny_code",
		"mcp_tool", "issuer_name", "role",
	}

	got := make([]string, 0, fields.Len())
	for i := range fields.Len() {
		got = append(got, string(fields.Get(i).Name()))
	}
	slices.Sort(got)
	slices.Sort(want)
	require.Equal(t, want, got,
		"flowstate.v1.AuditRecord's fields changed; an audit record carries decisions, "+
			"never payloads — see the redaction note in proto/flowstate/v1/audit.proto")

	// Only two message types may appear, and both are bounded by their own
	// schemas. Anything else — Struct, Any, Value — is a field a payload fits
	// in.
	allowedMessages := map[protoreflect.FullName]bool{
		"flowstate.v1.WorkloadIdentity": true,
		"google.protobuf.Timestamp":     true,
	}

	for i := range fields.Len() {
		field := fields.Get(i)

		require.False(t, field.IsList(), "%s is repeated", field.Name())
		require.False(t, field.IsMap(), "%s is a map", field.Name())
		require.NotEqual(t, protoreflect.BytesKind, field.Kind(),
			"%s is bytes, which is a payload with another name", field.Name())

		if field.Kind() == protoreflect.MessageKind {
			require.True(t, allowedMessages[field.Message().FullName()],
				"%s carries %s, which is not one of the two bounded messages this record may hold",
				field.Name(), field.Message().FullName())
		}
	}
}

// TestADenialRecordsTheCodeAndNotTheRefusalsWords is the containment shape
// CLAUDE.md asks for, pointed at the value this record must never carry.
//
// A denial's own words are peer-influenced text: "no such run \"...\"" quotes
// what the caller sent, and an error further down may quote a great deal more.
// The record carries a code from a closed set instead, and this asserts that
// through every rendering — on the record, on a struct holding it, and on a
// slice of those — because a redacting method is not what protects this: there
// is no field for the text to be in.
func TestADenialRecordsTheCodeAndNotTheRefusalsWords(t *testing.T) {
	t.Parallel()

	const refusal = "no such run \"orders-42\": tenant acme-secret-project is not the caller's"

	var sink recordingEmitter
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(&sink))
	require.NoError(t, err)

	require.NoError(t, recorder.Deny(t.Context(), audit.Subject{
		RPC:          "Signal",
		Identity:     &v1.WorkloadIdentity{Subject: "deploy-bot", Namespace: "acme"},
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN,
		ResourceKey:  "orders-42",
	}, v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH))

	require.Len(t, sink.records, 1)
	record := sink.records[0]

	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH, record.GetDenyCode())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_SIGNAL, record.GetAction(),
		"the action is derived from the rpc, not passed in beside it")

	type holder struct{ Record *v1.AuditRecord }

	renderings := []string{
		fmt.Sprintf("%v", record),
		fmt.Sprintf("%+v", record),
		fmt.Sprintf("%#v", record),
		// The verb an operator's log line would use, spelled rather than
		// called: the point is what a %s of this value renders, so calling
		// String() here would test something else.
		//lint:ignore S1025 the containment shape under test is the verb, not the method
		fmt.Sprintf("%s", record),
		fmt.Sprintf("%v", holder{Record: record}),
		fmt.Sprintf("%+v", holder{Record: record}),
		fmt.Sprintf("%#v", holder{Record: record}),
		fmt.Sprintf("%v", []*v1.AuditRecord{record}),
		fmt.Sprintf("%+v", []*v1.AuditRecord{record}),
		fmt.Sprintf("%#v", []*v1.AuditRecord{record}),
	}

	for _, rendering := range renderings {
		require.NotContains(t, rendering, refusal,
			"a refusal's words reached a rendering of the record")
		require.NotContains(t, rendering, "acme-secret-project")
	}

	// And the same for what actually leaves the process, which is the sink's
	// own encoding rather than any of the above.
	var written bytes.Buffer
	writer, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithWriter(&written))
	require.NoError(t, err)
	require.NoError(t, writer.Deny(t.Context(), audit.Subject{
		RPC:          "Signal",
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN,
		ResourceKey:  "orders-42",
	}, v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH))

	require.NotContains(t, written.String(), refusal)
	require.Contains(t, written.String(), "AUDIT_DENY_CODE_TENANT_MISMATCH")
}

// TestTheAuditSchemaCanExpressEveryBoundAction pins the derived record
// vocabulary. Actual RPC and authenticated-MCP seam coverage is tested where
// those surfaces are registered.
func TestTheAuditSchemaCanExpressEveryBoundAction(t *testing.T) {
	t.Parallel()

	audited := audit.AuditedActions()
	require.NotEmpty(t, audited)

	inAudit := map[v1.AuthorizationAction]bool{}
	for _, action := range audited {
		require.False(t, inAudit[action], "%s appears twice in the audited surface", action)
		inAudit[action] = true
	}

	for _, binding := range v1.AuthorizationActionBindings() {
		require.True(t, inAudit[binding.GetAction()],
			"%s is bound to an operation and absent from the record vocabulary", binding.GetAction())
	}
	require.Len(t, inAudit, len(v1.AuthorizationActionBindings()),
		"the record vocabulary must contain every bound action exactly once")
}

// TestOldAndNewAuditDescriptorFixturesAreWireCompatible measures the additive
// choice directly. The old fixture has rpc at field 3; the new fixture adds
// mcp_tool at field 9 without changing it. An old record decodes under the new
// descriptor, and an old binary reader preserves the new field as unknown wire
// data so a relay does not erase it.
func TestOldAndNewAuditDescriptorFixturesAreWireCompatible(t *testing.T) {
	t.Parallel()

	oldRecord := auditRecordFixture(t, false)
	newRecord := auditRecordFixture(t, true)

	old := dynamicpb.NewMessage(oldRecord)
	old.Set(oldRecord.Fields().ByName("action"), protoreflect.ValueOfInt32(15))
	old.Set(oldRecord.Fields().ByName("rpc"), protoreflect.ValueOfString("Validate"))
	wire, err := proto.Marshal(old)
	require.NoError(t, err)

	decodedNew := dynamicpb.NewMessage(newRecord)
	require.NoError(t, proto.Unmarshal(wire, decodedNew))
	require.Equal(t, "Validate", decodedNew.Get(newRecord.Fields().ByName("rpc")).String())
	require.False(t, decodedNew.Has(newRecord.Fields().ByName("mcp_tool")))

	newer := dynamicpb.NewMessage(newRecord)
	newer.Set(newRecord.Fields().ByName("action"), protoreflect.ValueOfInt32(16))
	newer.Set(newRecord.Fields().ByName("mcp_tool"), protoreflect.ValueOfString("flowstate_test"))
	wire, err = proto.Marshal(newer)
	require.NoError(t, err)

	decodedOld := dynamicpb.NewMessage(oldRecord)
	require.NoError(t, proto.Unmarshal(wire, decodedOld))
	require.Equal(t, int32(16), int32(decodedOld.Get(oldRecord.Fields().ByName("action")).Int()))
	require.NotEmpty(t, decodedOld.GetUnknown(), "the old reader discarded additive field 9")

	relayed, err := proto.Marshal(decodedOld)
	require.NoError(t, err)
	recovered := dynamicpb.NewMessage(newRecord)
	require.NoError(t, proto.Unmarshal(relayed, recovered))
	require.Equal(t, "flowstate_test", recovered.Get(newRecord.Fields().ByName("mcp_tool")).String())

	actual := (&v1.AuditRecord{}).ProtoReflect().Descriptor().Fields()
	require.Equal(t, protoreflect.FieldNumber(3), actual.ByName("rpc").Number())
	require.Equal(t, protoreflect.FieldNumber(9), actual.ByName("mcp_tool").Number())
}

func auditRecordFixture(t *testing.T, withMCP bool) protoreflect.MessageDescriptor {
	t.Helper()

	fields := []*descriptorpb.FieldDescriptorProto{
		{
			Name: proto.String("action"), Number: proto.Int32(1),
			Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:  descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
		},
		{
			Name: proto.String("rpc"), Number: proto.Int32(3),
			Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		},
	}
	if withMCP {
		fields = append(fields, &descriptorpb.FieldDescriptorProto{
			Name: proto.String("mcp_tool"), Number: proto.Int32(9),
			Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		})
	}

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String(fmt.Sprintf("fixture/audit_%t.proto", withMCP)),
		Package: proto.String(fmt.Sprintf("fixture.audit%d", map[bool]int{false: 1, true: 2}[withMCP])),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String("AuditRecord"),
			Field: fields,
		}},
	}, nil)
	require.NoError(t, err)

	return file.Messages().ByName("AuditRecord")
}

// TestMCPRecordUsesItsOwnOperationFieldAndBoundedProvenance proves the new
// shape through the same recorder production uses. Unknown tools fail closed,
// and the schema refuses both ambiguous and absent operation identities.
func TestMCPRecordUsesItsOwnOperationFieldAndBoundedProvenance(t *testing.T) {
	t.Parallel()

	var sink recordingEmitter
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(&sink))
	require.NoError(t, err)

	long := strings.Repeat("é", audit.MaxProvenanceBytes)
	require.NoError(t, recorder.Deny(t.Context(), audit.Subject{
		MCPTool:    "flowstate_test",
		Identity:   &v1.WorkloadIdentity{Subject: "agent", Claims: map[string]string{"secret": "claim-value"}},
		IssuerName: long,
		Role:       long,
	}, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED))
	require.Len(t, sink.records, 1)
	record := sink.records[0]
	require.Equal(t, "flowstate_test", record.GetMcpTool())
	require.Empty(t, record.GetRpc())
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_MCP_TEST, record.GetAction())
	require.Empty(t, record.GetIdentity().GetClaims())
	require.LessOrEqual(t, len(record.GetIssuerName()), audit.MaxProvenanceBytes)
	require.LessOrEqual(t, len(record.GetRole()), audit.MaxProvenanceBytes)
	require.True(t, isValidUTF8(record.GetIssuerName()))
	require.True(t, isValidUTF8(record.GetRole()))
	require.NoError(t, v1.Validate(record))

	require.Error(t, recorder.Allow(t.Context(), audit.Subject{MCPTool: "flowstate_unknown"}))
	require.Len(t, sink.records, 1, "an unknown operation was emitted under a guessed action")

	for name, invalid := range map[string]*v1.AuditRecord{
		"no operation": {
			Action:    v1.AuthorizationAction_AUTHORIZATION_ACTION_MCP_TEST,
			Decision:  v1.AuditDecision_AUDIT_DECISION_ALLOW,
			DecidedAt: timestamppb.Now(),
		},
		"two operations": {
			Action:   v1.AuthorizationAction_AUTHORIZATION_ACTION_MCP_TEST,
			Decision: v1.AuditDecision_AUDIT_DECISION_ALLOW,
			Rpc:      "Validate", McpTool: "flowstate_test",
			DecidedAt: timestamppb.Now(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, v1.Validate(invalid))
		})
	}
}

// TestARequiredSinksFailureIsTheCallersFailure is the fail-closed claim: an
// action that cannot be recorded does not happen. Its other half — that a
// deployment which did not ask for that does not get an outage when its
// collector has one — is asserted beside it, because the two are one decision.
func TestARequiredSinksFailureIsTheCallersFailure(t *testing.T) {
	t.Parallel()

	broken := emitterFunc(func(context.Context, *v1.AuditRecord) error {
		return errors.New("the sink is down")
	})

	subject := audit.Subject{RPC: "Cancel", ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, ResourceKey: "orders-1"}

	required, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(broken), audit.Required())
	require.NoError(t, err)
	require.True(t, required.Required())
	require.Error(t, required.Allow(t.Context(), subject))
	require.Error(t, required.Deny(t.Context(), subject, v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH))

	advisory, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(broken))
	require.NoError(t, err)
	require.False(t, advisory.Required())
	require.NoError(t, advisory.Allow(t.Context(), subject),
		"a deployment that did not ask for a required sink does not get its collector's outage")
}

// TestAsyncWriterBoundsBackpressure is the availability half of best-effort
// auditing: a logging consumer that stops draining may occupy the one writer
// goroutine and fill a finite queue, but it cannot occupy an RPC handler.
func TestAsyncWriterBoundsBackpressure(t *testing.T) {
	t.Parallel()

	w := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	emitter, flush := audit.NewAsyncWriterEmitter(w, 1)
	record := &v1.AuditRecord{}

	require.NoError(t, emitter.Emit(t.Context(), record))
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("the writer goroutine did not receive the first record")
	}

	// One record waits in the bounded queue. The next is dropped immediately
	// rather than waiting behind the blocked writer.
	require.NoError(t, emitter.Emit(t.Context(), record))
	started := time.Now()
	require.Error(t, emitter.Emit(t.Context(), record))
	require.Less(t, time.Since(started), 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, flush(ctx), context.DeadlineExceeded)

	close(w.release)
	require.NoError(t, flush(t.Context()))
}

type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

// TestAsyncWriterReportsWhatTheFullQueueDropped is the visibility half of
// best-effort auditing: dropping under backpressure is the availability
// trade, but doc.go promises a trail that is complete rather than sampled,
// so the trail has to say when and how much it lost. The summary is one
// line naming the count — the stderrLimiter shape from #714 — placed before
// the next record written, and it must not itself parse as a record.
func TestAsyncWriterReportsWhatTheFullQueueDropped(t *testing.T) {
	t.Parallel()

	w := &capturingBlockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	emitter, flush := audit.NewAsyncWriterEmitter(w, 1)
	record := &v1.AuditRecord{Rpc: "Signal"}

	// The writer goroutine holds the first record, blocked mid-write.
	require.NoError(t, emitter.Emit(t.Context(), record))
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("the writer goroutine did not receive the first record")
	}

	// The second occupies the queue's one slot; the next three are dropped.
	require.NoError(t, emitter.Emit(t.Context(), record))
	for range 3 {
		require.Error(t, emitter.Emit(t.Context(), record))
	}

	close(w.release)
	require.NoError(t, flush(t.Context()))

	lines := strings.Split(strings.TrimSuffix(w.String(), "\n"), "\n")
	require.Len(t, lines, 3, "two records and one summary, not one line per drop")

	require.Equal(t, "audit: 3 records dropped: writer queue was full", lines[1],
		"the summary names the count, before the next record written")
	require.Error(t, protojson.Unmarshal([]byte(lines[1]), &v1.AuditRecord{}),
		"a consumer parsing records cannot mistake the summary for one")

	for _, line := range []string{lines[0], lines[2]} {
		require.NoError(t, protojson.Unmarshal([]byte(line), &v1.AuditRecord{}),
			"the records around the summary still parse")
	}
}

// capturingBlockedWriter blocks every Write until released, then records
// what was written.
type capturingBlockedWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *capturingBlockedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *capturingBlockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// TestARequiredRecorderMustHaveASink refuses the one combination that would be
// a lie: insisting every action be recorded, with nowhere to record it.
func TestARequiredRecorderMustHaveASink(t *testing.T) {
	t.Parallel()

	_, err := audit.NewRecorder(audit.WithoutStderr(), audit.Required())
	require.Error(t, err)

	// And the default really is a sink, so the ordinary required deployment
	// needs nothing else configured.
	recorder, err := audit.NewRecorder(audit.Required())
	require.NoError(t, err)
	require.NotNil(t, recorder)
}

// TestARecordForAnUnboundRPCIsRefused: the action is derived, so an RPC no
// binding names cannot be recorded under a guessed one. Reaching this is
// already a build-time failure (TestEveryRPCHasExactlyOneAuthorizationAction);
// this is what happens if somebody spells a name wrong at a call site.
func TestARecordForAnUnboundRPCIsRefused(t *testing.T) {
	t.Parallel()

	var sink recordingEmitter
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(&sink))
	require.NoError(t, err)

	require.Error(t, recorder.Allow(t.Context(), audit.Subject{RPC: "Singal"}),
		"an unbound rpc has no action, and a record under a guessed one is worse than none")
	require.Empty(t, sink.records)
}

// TestANilRecorderRecordsNothing: a component that was given no recorder does
// not panic, and does not fail requests either. Whether this deployment keeps
// an audit trail is the serving process's decision.
func TestANilRecorderRecordsNothing(t *testing.T) {
	t.Parallel()

	var recorder *audit.Recorder

	require.NoError(t, recorder.Allow(t.Context(), audit.Subject{RPC: "Get"}))
	require.NoError(t, recorder.Deny(t.Context(), audit.Subject{RPC: "Get"},
		v1.AuditDenyCode_AUDIT_DENY_CODE_RESOURCE_NOT_FOUND))
	require.False(t, recorder.Required())
}

// TestTheResourceKeyIsBoundedOnARuneBoundary: the one caller-influenced value
// in the record is bounded before a sink sees it, and truncating mid-sequence
// would produce a record that is bounded and unreadable.
func TestTheResourceKeyIsBoundedOnARuneBoundary(t *testing.T) {
	t.Parallel()

	var sink recordingEmitter
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(&sink))
	require.NoError(t, err)

	// Multi-byte runes arranged so the bound falls inside one.
	key := strings.Repeat("é", audit.MaxResourceKeyBytes)
	require.NoError(t, recorder.Allow(t.Context(), audit.Subject{
		RPC:          "Get",
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN,
		ResourceKey:  key,
	}))

	require.Len(t, sink.records, 1)
	got := sink.records[0].GetResourceKey()
	require.LessOrEqual(t, len(got), audit.MaxResourceKeyBytes)
	require.True(t, isValidUTF8(got), "the bound cut a rune in half")

	// Bounded to what the schema itself will accept, which is what makes the
	// two halves of this bound one number rather than two.
	require.NoError(t, v1.Validate(sink.records[0]))
}

// TestTheRecordCarriesTheServersClock: never the caller's, the rule
// SignalSender.accepted_at already states.
func TestTheRecordCarriesTheServersClock(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

	var sink recordingEmitter
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(&sink),
		audit.WithClock(func() time.Time { return at }))
	require.NoError(t, err)

	require.NoError(t, recorder.Allow(t.Context(), audit.Subject{RPC: "GetCatalog"}))
	require.Len(t, sink.records, 1)
	require.Equal(t, at, sink.records[0].GetDecidedAt().AsTime())
}

// TestTheWriterSinkWritesOneParsableRecordPerLine: the unconditional floor has
// to be readable by whatever an operator points at stderr.
func TestTheWriterSinkWritesOneParsableRecordPerLine(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithWriter(&out))
	require.NoError(t, err)

	require.NoError(t, recorder.Allow(t.Context(), audit.Subject{
		RPC:          "Get",
		Identity:     &v1.WorkloadIdentity{Subject: "alice", Namespace: "acme"},
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN,
		ResourceKey:  "orders-1",
	}))
	require.NoError(t, recorder.Deny(t.Context(), audit.Subject{
		RPC:          "Get",
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN,
		ResourceKey:  "orders-2",
	}, v1.AuditDenyCode_AUDIT_DENY_CODE_RESOURCE_NOT_FOUND))

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2)

	for _, line := range lines {
		var record v1.AuditRecord
		require.NoError(t, protojson.Unmarshal([]byte(line), &record))
		require.Equal(t, "Get", record.GetRpc())
		require.NoError(t, v1.Validate(&record))
	}
}

// TestTheSyncProcessorReportsAnExportFailureToTheEmitter is what makes a
// required OTLP sink possible at all: the SDK's Emit returns nothing, so
// without this an exporter's failure would reach the global error handler and
// the request would be answered as though it had been recorded.
func TestTheSyncProcessorReportsAnExportFailureToTheEmitter(t *testing.T) {
	t.Parallel()

	failing := &stubExporter{err: errors.New("collector refused the batch")}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(audit.NewSyncProcessor(failing)))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	recorder, err := audit.NewRecorder(audit.WithoutStderr(),
		audit.WithEmitter(audit.NewLogEmitter(provider)), audit.Required())
	require.NoError(t, err)

	err = recorder.Allow(t.Context(), audit.Subject{
		RPC:          "Signal",
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN,
		ResourceKey:  "orders-1",
	})
	require.Error(t, err, "a required sink whose exporter failed must fail the caller")
	require.Equal(t, 1, failing.calls, "the export happened before the caller was answered")

	// And the exported record is the decision, on the audit's own scope.
	working := &stubExporter{}
	ok := sdklog.NewLoggerProvider(sdklog.WithProcessor(audit.NewSyncProcessor(working)))
	t.Cleanup(func() { _ = ok.Shutdown(context.Background()) })

	recorder, err = audit.NewRecorder(audit.WithoutStderr(),
		audit.WithEmitter(audit.NewLogEmitter(ok)), audit.Required())
	require.NoError(t, err)
	require.NoError(t, recorder.Deny(t.Context(), audit.Subject{
		MCPTool:      "flowstate_test",
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN,
		ResourceKey:  "orders-9",
		IssuerName:   "agent-idp",
		Role:         "mcp-caller",
	}, v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH))

	require.Len(t, working.exported, 1)
	exported := working.exported[0]
	require.Equal(t, audit.ScopeName, exported.InstrumentationScope().Name)
	require.Equal(t, audit.EventName, exported.EventName())

	attributes := map[string]string{}
	exported.WalkAttributes(func(kv attribute.KeyValue) bool {
		attributes[string(kv.Key)] = kv.Value.AsString()

		return true
	})
	require.NotContains(t, attributes, "flowstate.audit.rpc")
	require.Equal(t, "flowstate_test", attributes["flowstate.audit.mcp.tool"])
	require.Equal(t, "agent-idp", attributes["flowstate.audit.identity.issuer_name"])
	require.Equal(t, "mcp-caller", attributes["flowstate.audit.identity.role"])
	require.Equal(t, "AUDIT_DECISION_DENY", attributes["flowstate.audit.decision"])
	require.Equal(t, "AUDIT_DENY_CODE_TENANT_MISMATCH", attributes["flowstate.audit.deny_code"])
	require.Equal(t, "AUTHORIZATION_ACTION_MCP_TEST", attributes["flowstate.audit.action"])
}

func isValidUTF8(s string) bool {
	for _, r := range s {
		if r == '�' {
			return false
		}
	}

	return true
}

type recordingEmitter struct {
	records []*v1.AuditRecord
}

func (e *recordingEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	e.records = append(e.records, record)

	return nil
}

type emitterFunc func(context.Context, *v1.AuditRecord) error

func (f emitterFunc) Emit(ctx context.Context, record *v1.AuditRecord) error {
	return f(ctx, record)
}

type stubExporter struct {
	err      error
	calls    int
	exported []sdklog.Record
}

func (e *stubExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.calls++
	e.exported = append(e.exported, records...)

	return e.err
}

func (e *stubExporter) Shutdown(context.Context) error { return nil }

func (e *stubExporter) ForceFlush(context.Context) error { return nil }
