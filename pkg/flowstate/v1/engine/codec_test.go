package engine_test

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/toycodec"
)

// The durable half of the codec seam, exercised end to end through the
// substrate's own test environment: a run's input state, its signal payload, and
// its result all cross the data converter, so a codec configured there is on the
// path or it is not.
//
// These tests deliberately do not call t.Parallel. [engine.UseDataConverter]
// sets a process value — see engine/codec.go for why the SDK leaves no
// alternative — and Go runs serial tests alone, before any parallel test in the
// package resumes. A parallel test here would run beside every other test in the
// package with the codec still installed, and they would all lose their signals.

// countingCodec wraps a codec and counts what passed through it, which is how
// these tests tell "the value round-tripped" from "the value round-tripped
// *through the codec*". A round trip proves a converter is self-consistent; only
// the count proves the seam is on the path.
type countingCodec struct {
	inner   payloadcodec.Codec
	encoded atomic.Int64
	decoded atomic.Int64
}

func (c *countingCodec) Name() string { return c.inner.Name() }

func (c *countingCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.encoded.Add(int64(len(p)))
	return c.inner.Encode(p)
}

func (c *countingCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.decoded.Add(int64(len(p)))
	return c.inner.Decode(p)
}

// gatedWorkflow is a run that carries a payload in and a payload out and waits
// for a signal in between: input state, signal delivery, and result, which is
// every payload shape a run of this engine has.
func gatedWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "codec-gated",
		Steps: []*v1.Node{
			logStep("request", "requesting approval"),
			signalStep("approval", "deploy-approved", 2*time.Minute),
			logStep("deploy", "deploying"),
		},
	}
}

// TestCodecCoversInputsSignalsAndOutputs is the round trip the codec slot
// exists for.
func TestCodecCoversInputsSignalsAndOutputs(t *testing.T) {
	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)
	counting := &countingCodec{inner: toy}

	cfg := payloadcodec.Config{Codec: counting}

	// Both halves of a worker's construction, from one configuration: the
	// client's converter and the interpreter's.
	engine.UseCodec(cfg)
	t.Cleanup(func() { engine.UseDataConverter(nil) })

	env := newWaitEnv(t)
	env.SetDataConverter(cfg.DataConverter())

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", testSignalDelivery("approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
	}, time.Minute)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: gatedWorkflow()})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	// The signal arrived and released the gate, which is the assertion that
	// fails if the interpreter decodes with a converter the codec is not on.
	approval := outputs.GetStepValues()["approval"]
	require.NotNil(t, approval, "the wait produced no outputs at all")
	require.False(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue(),
		"the wait timed out: the signal never made it through the codec")
	require.True(t, payloadField(t, approval, "approved").GetBoolValue())
	require.NotNil(t, outputs.GetStepValues()["deploy"],
		"the gated step did not run")

	// And the payloads genuinely went through the codec rather than around it.
	require.Positive(t, counting.encoded.Load(), "nothing was ever encoded")
	require.Positive(t, counting.decoded.Load(), "nothing was ever decoded")
}

// TestSignalsAreLostWhenTheInterpreterBypassesTheCodec is the negative
// direction, and it is the reason the test above is worth trusting.
//
// The interpreter replaces the workflow context's data converter so a signal
// channel can decode either wire shape #194 straddles. Before engine/codec.go it
// replaced it with [converter.GetDefaultDataConverter] unconditionally, which is
// correct only while every deployment uses the default converter. This
// reproduces what that costs on a deployment with a codec: the payload is
// ciphertext, the default converter cannot read it, and channelImpl.Receive
// treats an undecodable signal as corrupted — logs it and keeps waiting. The run
// does not fail. The approval is simply gone.
func TestSignalsAreLostWhenTheInterpreterBypassesTheCodec(t *testing.T) {
	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)

	cfg := payloadcodec.Config{Codec: toy}

	// The client half configured and the interpreter half not: exactly the
	// bypass, expressed as configuration rather than by editing code.
	engine.UseDataConverter(nil)

	env := newWaitEnv(t)
	env.SetDataConverter(cfg.DataConverter())

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", testSignalDelivery("approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
	}, time.Minute)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: gatedWorkflow()})

	require.True(t, env.IsWorkflowCompleted())

	// The approval never reached the workflow. What that looks like from the
	// outside is the point: not a decode error surfaced to anyone, but a gate
	// that keeps waiting — the SDK logs "Corrupted signal received on channel
	// deploy-approved" and carries on. Here the run then runs out its clock. On
	// a real deployment it waits for as long as the wait allows, and an operator
	// sees a run that is simply stuck.
	//
	// Asserting the *absence* of the completion the positive test asserts is
	// what keeps these two honest as a pair: if this ever starts passing the
	// signal through, the seam has moved and both tests need re-reading.
	var outputs v1.Workflow_StepOutputs
	if err := env.GetWorkflowResult(&outputs); err == nil {
		approval := outputs.GetStepValues()["approval"]
		require.NotNil(t, approval)
		require.True(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue(),
			"the signal was delivered despite the interpreter having no codec, so this no longer reproduces the bypass")
		require.Nil(t, outputs.GetStepValues()["deploy"],
			"the gated step ran, so the signal was not lost after all")
		return
	}

	require.Error(t, env.GetWorkflowError(),
		"the run neither completed nor failed, so this says nothing about the lost signal")
}
