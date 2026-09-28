package server_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
)

// Synthetic values, one per path a value takes into history, so a leak names
// the path it came through.
const (
	specMarker    = "synthetic-spec-4c1f"
	signalMarker  = "synthetic-signal-9a2e"
	failureMarker = "synthetic-failure-7d3b"
)

// TestEncryptedHistoryHoldsNoPlaintext is the claim payload encryption makes,
// checked against what Temporal actually stored rather than against what a
// converter says it would do.
//
// A run goes through every shape of payload a Flowstate run writes: its
// specification (the start input), a memo the server writes, a signal, a step
// that fails with an error quoting its input, and the result. Then the raw
// history is read with a client that has no codec, and searched for the
// plaintext. Every payload in it must be an envelope, every failure message
// must be encoded, and none of the synthetic values may appear anywhere in the
// serialized events. The same run, read through the server configured with
// the keyring, must still answer normally.
func TestEncryptedHistoryHoldsNoPlaintext(t *testing.T) {
	t.Parallel()

	plain, namespace := newTemporalNamespace(t)

	// The keyring an operator writes, and the configuration `flow server` and
	// `flow worker` resolve from it.
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "k.key"), local.Generate(), 0o600))
	cfg, err := envelope.ParseConfig([]byte("namespaces:\n  " + namespace + ":\n    current: e2e-1\n    keys:\n      - {id: e2e-1, file: k.key}\n"))
	require.NoError(t, err)
	keyring, err := envelope.Open(t.Context(), cfg, envelope.OpenOptions{BaseDir: dir})
	require.NoError(t, err)
	codecs := keyring.PayloadCodecConfig()
	require.NoError(t, codecs.Validate())

	encrypted := dialWithCodec(t, namespace, codecs)
	nsCodec, err := codecs.ForNamespace(namespace)
	require.NoError(t, err)

	w := worker.New(encrypted, engine.RunTaskQueueName, worker.Options{})
	engine.Register(w, engine.TaskRuntimeConfig{}.WithDataConverter(nsCodec.DataConverter()))
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	flowstate := mustNew(t, encrypted, server.WithDataConverter(codecs.DataConverter()))

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: encryptionWorkflow()}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()

	waitUntilParkedAtTheGate(t, encrypted, workflowID)

	_, err = flowstate.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
			"note":     v1.NewLiteral(signalMarker),
		}},
	}))
	require.NoError(t, err)

	// Read back through the configured server: the memos it wrote decode, so it
	// still recognizes the run as this tenant's, and the result decodes.
	var final *v1.GetResponse
	require.Eventually(t, func() bool {
		resp, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp.Msg
		return true
	}, 60*time.Second, 100*time.Millisecond, "the encrypted run did not complete")
	require.Contains(t, protoText(final), signalMarker, "the authorized reader could not read the signal back")

	// Now the storage-side view: raw history, no codec anywhere.
	events, err := historyOf(t.Context(), plain, workflowID)
	require.NoError(t, err)
	require.NotEmpty(t, events)

	sawFailure := false
	payloads := 0
	for _, event := range events {
		raw, err := proto.Marshal(event)
		require.NoError(t, err)
		for _, marker := range []string{specMarker, signalMarker, failureMarker} {
			require.NotContains(t, string(raw), marker,
				"event %d (%s) holds plaintext %q", event.GetEventId(), event.GetEventType(), marker)
		}

		walkHistoryEvent(event, func(path string, p *commonpb.Payload) {
			payloads++
			require.Equal(t, envelope.Encoding, string(p.GetMetadata()["encoding"]),
				"event %d (%s) %s is not an envelope", event.GetEventId(), event.GetEventType(), path)
		}, func(message string) {
			sawFailure = true
			require.Equal(t, "Encoded failure", message,
				"event %d (%s) carries a failure message in the clear", event.GetEventId(), event.GetEventType())
		})
	}
	require.Positive(t, payloads, "the walk found no payloads, so it proved nothing")
	require.True(t, sawFailure, "the run recorded no failure, so the failure path was not exercised")

	// And a client with the wrong namespace's view, or none, reads nothing: the
	// default converter cannot decode an envelope.
	var state v1.RunState
	start := events[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()[0]
	require.Error(t, payloadcodec.Serializer().FromPayload(start, &state))
}

// TestContinueAsNewCarriesOnlyCiphertext drives a run across Continue-As-New
// boundaries with a step budget of one, so the state carried from segment to
// segment, which holds every step output so far, is written to history once
// per step, and checks every segment's raw history.
func TestContinueAsNewCarriesOnlyCiphertext(t *testing.T) {
	t.Parallel()

	plain, namespace := newTemporalNamespace(t)

	env := map[string]string{"K": string(local.Generate())}
	cfg, err := envelope.ParseConfig([]byte("namespaces:\n  " + namespace + ":\n    current: can-1\n    keys:\n      - {id: can-1, env: K}\n"))
	require.NoError(t, err)
	keyring, err := envelope.Open(t.Context(), cfg, envelope.OpenOptions{Getenv: func(n string) string { return env[n] }})
	require.NoError(t, err)
	codecs := keyring.PayloadCodecConfig()
	encrypted := dialWithCodec(t, namespace, codecs)
	nsCodec, err := codecs.ForNamespace(namespace)
	require.NoError(t, err)

	w := worker.New(encrypted, engine.RunTaskQueueName, worker.Options{})
	engine.Register(w, engine.TaskRuntimeConfig{}.WithDataConverter(nsCodec.DataConverter()))
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	// A loop whose carried state holds the synthetic value: with a budget of
	// one, every iteration boundary suspends, so the state crosses a
	// Continue-As-New each time.
	steps := []*v1.Node{{
		Id: "grow",
		Kind: &v1.Node_Loop{Loop: &v1.Loop{
			State:         "acc",
			Initial:       v1.NewLiteral(specMarker),
			Update:        v1.NewExpr(`acc + "."`),
			Until:         v1.NewExpr(fmt.Sprintf("size(acc) >= %d", len(specMarker)+3)),
			MaxIterations: 10,
			Body: []*v1.Node{{
				Id: "tick",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("tick")},
				}},
			}},
		}},
	}}
	run, err := encrypted.ExecuteWorkflow(t.Context(),
		client.StartWorkflowOptions{TaskQueue: engine.RunTaskQueueName},
		engine.RunWorkflowType, &v1.RunState{Workflow: &v1.Workflow{Name: "segmented", Steps: steps}, StepsBudget: 1})
	require.NoError(t, err)
	// Taken before Get, which follows the chain and moves the handle's run id
	// to the last segment.
	firstRunID := run.GetRunID()

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, run.Get(t.Context(), &outputs))
	require.Contains(t, protoText(&outputs), specMarker, "the carried state did not survive the chain")

	// Every segment of the chain, from the first run id forward.
	segments := 0
	for runID := firstRunID; runID != ""; segments++ {
		iter := plain.GetWorkflowHistory(t.Context(), run.GetID(), runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		next := ""
		for iter.HasNext() {
			event, err := iter.Next()
			require.NoError(t, err)
			raw, err := proto.Marshal(event)
			require.NoError(t, err)
			require.NotContains(t, string(raw), specMarker, "segment %d event %d holds plaintext", segments, event.GetEventId())
			walkHistoryEvent(event, func(path string, p *commonpb.Payload) {
				require.Equal(t, envelope.Encoding, string(p.GetMetadata()["encoding"]), "segment %d %s", segments, path)
			}, func(string) {})
			if a := event.GetWorkflowExecutionContinuedAsNewEventAttributes(); a != nil {
				next = a.GetNewExecutionRunId()
			}
		}
		runID = next
	}
	require.Greater(t, segments, 1, "the run never continued as new, so the carried state was not exercised")
}

// encryptionWorkflow carries a synthetic value in its specification, fails a
// step with an error that quotes a second one, and waits for a signal that
// will carry a third.
func encryptionWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "encrypted",
		Steps: []*v1.Node{
			{
				Id: "request",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("requesting " + specMarker)},
				}},
			},
			{
				// Refused by the default egress posture, or unreachable: either
				// way the task fails, and its error names the URL it was given.
				Id: "boom",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "http",
					Inputs: map[string]*v1.Value{"url": v1.NewLiteral("http://127.0.0.1:9/" + failureMarker)},
				}},
				Policy: &v1.StepPolicy{ContinueOnError: true, Retry: &v1.RetryPolicy{MaxAttempts: 1}},
			},
			{
				Id: "approval",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}},
					Timeout: durationpb.New(2 * time.Minute),
				}},
			},
		},
	}
}

// dialWithCodec dials the test's namespace the way temporalclient.Config
// dials every deployment's: with that namespace's codec and the failure
// converter that must accompany it.
func dialWithCodec(t *testing.T, namespace string, codecs payloadcodec.Config) client.Client {
	t.Helper()

	opts := client.Options{
		HostPort:  devServer.FrontendHostPort(),
		Namespace: namespace,
		Logger:    newTestingLogger(t),
		ConnectionOptions: client.ConnectionOptions{
			DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(temporalclient.StartInterceptor())},
		},
	}
	one, err := codecs.ForNamespace(namespace)
	require.NoError(t, err)
	one.Apply(&opts)

	c, err := client.Dial(opts)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// walkHistoryEvent visits every payload in an event except the two kinds the
// SDK never passes through a codec, search attributes (the cluster indexes
// them) and headers (context propagators encode their own), and every failure
// message.
func walkHistoryEvent(event *historypb.HistoryEvent, payload func(string, *commonpb.Payload), failure func(string)) {
	var walk func(path string, m protoreflect.Message)
	walk = func(path string, m protoreflect.Message) {
		switch msg := m.Interface().(type) {
		case *commonpb.SearchAttributes, *commonpb.Header:
			return
		case *commonpb.Payload:
			payload(path, msg)
			return
		}
		if m.Descriptor().FullName() == "temporal.api.failure.v1.Failure" {
			failure(m.Get(m.Descriptor().Fields().ByName("message")).String())
		}
		m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			name := path + "." + string(fd.Name())
			switch {
			case fd.IsMap() && fd.MapValue().Message() != nil:
				v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
					walk(name+"["+k.String()+"]", mv.Message())
					return true
				})
			case fd.IsList() && fd.Message() != nil:
				for i := range v.List().Len() {
					walk(name, v.List().Get(i).Message())
				}
			case fd.Message() != nil && !fd.IsMap() && !fd.IsList():
				walk(name, v.Message())
			}
			return true
		})
	}
	walk(strings.ToLower(event.GetEventType().String()), event.ProtoReflect())
}

func protoText(m proto.Message) string {
	b, _ := proto.Marshal(m)
	return string(b)
}

// TestRotationKeepsAnInFlightRunReadable rotates keys underneath a run that is
// parked at a gate: the run starts under the first key, every worker and the
// server are replaced by ones whose current key is the second, and the run
// finishes. Its history then holds payloads under both ids, and the new fleet
// reads all of them because it kept the first key to decrypt with.
func TestRotationKeepsAnInFlightRunReadable(t *testing.T) {
	t.Parallel()

	plain, namespace := newTemporalNamespace(t)
	env := map[string]string{"OLD": string(local.Generate()), "NEW": string(local.Generate())}
	open := func(doc string) payloadcodec.Config {
		cfg, err := envelope.ParseConfig([]byte(doc))
		require.NoError(t, err)
		kr, err := envelope.Open(t.Context(), cfg, envelope.OpenOptions{Getenv: func(n string) string { return env[n] }})
		require.NoError(t, err)
		return kr.PayloadCodecConfig()
	}
	before := open("namespaces:\n  " + namespace + ":\n    current: old\n    keys: [{id: old, env: OLD}]\n")
	after := open("namespaces:\n  " + namespace + ":\n    current: new\n    keys: [{id: new, env: NEW}, {id: old, env: OLD}]\n")

	fleet := func(codecs payloadcodec.Config) (*server.FlowstateServer, func()) {
		c := dialWithCodec(t, namespace, codecs)
		one, err := codecs.ForNamespace(namespace)
		require.NoError(t, err)
		w := worker.New(c, engine.RunTaskQueueName, worker.Options{})
		engine.Register(w, engine.TaskRuntimeConfig{}.WithDataConverter(one.DataConverter()))
		require.NoError(t, w.Start())
		return mustNew(t, c, server.WithDataConverter(codecs.DataConverter())), w.Stop
	}

	first, stopFirst := fleet(before)
	started, err := first.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: gatedWorkflow()}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, plain, workflowID)
	stopFirst()

	second, stopSecond := fleet(after)
	t.Cleanup(stopSecond)

	_, err = second.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload:    &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
	}))
	require.NoError(t, err, "the rotated server could not read the memos the old key sealed")

	require.Eventually(t, func() bool {
		resp, err := second.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 90*time.Second, 200*time.Millisecond, "the run did not finish after rotation")

	events, err := historyOf(t.Context(), plain, workflowID)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, event := range events {
		walkHistoryEvent(event, func(_ string, p *commonpb.Payload) {
			ids[string(p.GetMetadata()[payloadcodec.KeyIDMetadataKey])] = true
		}, func(string) {})
	}
	require.True(t, ids["old"], "nothing in history was sealed under the key in use when the run started")
	require.True(t, ids["new"], "nothing in history was sealed under the rotated key")

	// And retiring the old key is what makes the history unreadable: the
	// start input, sealed under it, no longer decodes.
	retired := open("namespaces:\n  " + namespace + ":\n    current: new\n    keys: [{id: new, env: NEW}]\n")
	start := events[0].GetWorkflowExecutionStartedEventAttributes().GetInput()
	var state v1.RunState
	require.ErrorIs(t, retired.DataConverter().FromPayloads(start, &state), envelope.ErrUnknownKey)
	require.NoError(t, after.DataConverter().FromPayloads(start, &state))
}
