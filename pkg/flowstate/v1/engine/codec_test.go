package engine_test

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/toycodec"

	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The durable half of the codec seam, exercised end to end through the
// substrate's own test environment: a run's input state, its signal payload, and
// its result all cross the data converter, so a codec configured there is on the
// path or it is not.
//
// The interpreter's converter is bound to the worker registration that carries
// it ([engine.TaskRuntimeConfig.WithDataConverter]), not held by the process, so
// these tests run in parallel, including two workers with different codecs side
// by side.

// newCodecEnv is newWaitEnv with the interpreter registered the way a worker
// registers it: bound to the converter in runtime.
func newCodecEnv(t *testing.T, runtime engine.TaskRuntimeConfig) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	engine.RegisterWorkflows(env, runtime)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
	env.RegisterActivity(engine.WorkflowVars)

	return env
}

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

// Counting does not re-key anything either, so the id a payload carries is the
// wrapped codec's, for the same reason the size declaration is.
func (c *countingCodec) CurrentKeyID() string { return c.inner.CurrentKeyID() }

func (c *countingCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.encoded.Add(int64(len(p)))
	return c.inner.Encode(p)
}

func (c *countingCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.decoded.Add(int64(len(p)))
	return c.inner.Decode(p)
}

// Counting adds no bytes, so the declaration is the wrapped codec's. A wrapper
// that forwarded this without thinking would be declaring somebody else's
// overhead as its own, which is exactly the mistake the method exists to make
// visible; here it is true because this wrapper writes nothing.
func (c *countingCodec) MaxEncodedSize(plain int) int { return c.inner.MaxEncodedSize(plain) }

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
	t.Parallel()

	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)
	counting := &countingCodec{inner: toy}

	cfg := payloadcodec.Config{Codec: counting}
	counting.requireApprovedThroughCodec(t, cfg)
}

// TestTwoWorkersWithDifferentCodecsCoexist is the embedding case the process
// global could not serve: two workers in one process, each keyed differently,
// each decoding its own signals. With the converter held by the process, the
// second registration replaced the first's, and one of these two runs lost its
// approval to a decode under the wrong key.
func TestTwoWorkersWithDifferentCodecsCoexist(t *testing.T) {
	t.Parallel()

	for i, fill := range []byte{0x2a, 0x3b} {
		t.Run(fmt.Sprintf("worker-%d", i), func(t *testing.T) {
			t.Parallel()

			toy, err := toycodec.New(bytes.Repeat([]byte{fill}, 32))
			require.NoError(t, err)
			counting := &countingCodec{inner: toy}
			counting.requireApprovedThroughCodec(t, payloadcodec.Config{Codec: counting})
		})
	}
}

// requireApprovedThroughCodec runs the gated workflow on a worker whose client
// and interpreter both use cfg, and asserts the approval arrived and the
// payloads crossed c.
func (c *countingCodec) requireApprovedThroughCodec(t *testing.T, cfg payloadcodec.Config) {
	t.Helper()

	// Both halves of a worker's construction, from one configuration: the
	// client's converter and the interpreter's.
	env := newCodecEnv(t, engine.TaskRuntimeConfig{}.WithDataConverter(cfg.DataConverter()))
	env.SetDataConverter(cfg.DataConverter())

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", testSignalDelivery("approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
	}, time.Minute)

	env.ExecuteWorkflow(engine.RunWorkflowType, &v1.RunState{Workflow: gatedWorkflow()})

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
	require.True(t, conformance.PayloadField(t, approval, "approved").GetBoolValue())
	require.NotNil(t, outputs.GetStepValues()["deploy"],
		"the gated step did not run")

	// And the payloads genuinely went through the codec rather than around it.
	require.Positive(t, c.encoded.Load(), "nothing was ever encoded")
	require.Positive(t, c.decoded.Load(), "nothing was ever decoded")
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
// treats an undecodable signal as corrupted: it logs it and keeps waiting. The run
// does not fail. The approval is simply gone.
func TestSignalsAreLostWhenTheInterpreterBypassesTheCodec(t *testing.T) {
	t.Parallel()

	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)

	cfg := payloadcodec.Config{Codec: toy}

	// The client half configured and the interpreter half not: exactly the
	// bypass, expressed as configuration rather than by editing code.
	env := newCodecEnv(t, engine.TaskRuntimeConfig{})
	env.SetDataConverter(cfg.DataConverter())

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", testSignalDelivery("approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
	}, time.Minute)

	env.ExecuteWorkflow(engine.RunWorkflowType, &v1.RunState{Workflow: gatedWorkflow()})

	require.True(t, env.IsWorkflowCompleted())

	// The approval never reached the workflow. What that looks like from the
	// outside is the point: not a decode error surfaced to anyone, but a gate
	// that keeps waiting. The SDK logs "Corrupted signal received on channel
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

// unavailableCodec is a codec whose key provider stops answering once down is
// set: every decode after that fails with [payloadcodec.ErrUnavailable].
type unavailableCodec struct {
	payloadcodec.Codec
	down atomic.Bool
}

func (c *unavailableCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	if c.down.Load() {
		return nil, fmt.Errorf("toy key provider timed out: %w", payloadcodec.ErrUnavailable)
	}
	return c.Codec.Decode(p)
}

// TestASignalTheCodecCannotReachAKeyForFailsTheRun: a signal whose codec
// could not reach its key provider is not a corrupt signal. The SDK drops a
// signal whose decode returns an error, so returned, the approval would be
// lost and the gate would time out; the run fails instead, naming why, with
// the signal still in its history.
func TestASignalTheCodecCannotReachAKeyForFailsTheRun(t *testing.T) {
	t.Parallel()

	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)
	codec := &unavailableCodec{Codec: toy}
	cfg := payloadcodec.Config{Codec: codec}

	env := newCodecEnv(t, engine.TaskRuntimeConfig{}.WithDataConverter(cfg.DataConverter()))
	env.SetDataConverter(cfg.DataConverter())
	env.RegisterDelayedCallback(func() {
		codec.down.Store(true)
		env.SignalWorkflow("deploy-approved", testSignalDelivery("approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
	}, time.Minute)

	env.ExecuteWorkflow(engine.RunWorkflowType, &v1.RunState{Workflow: gatedWorkflow()})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "unavailable",
		"the signal was dropped as corrupt and the gate carried on without it")
}

// resultUnreadableCodec reads everything but an activity's result, the way a
// worker that did not seal it reads one while its key provider is down.
type resultUnreadableCodec struct{ payloadcodec.Codec }

func (c resultUnreadableCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out, err := c.Codec.Decode(p)
	if err != nil {
		return nil, err
	}
	for _, decoded := range out {
		if string(decoded.GetMetadata()["messageType"]) == "flowstate.v1.Node.Outputs" {
			return nil, fmt.Errorf("toy key provider timed out: %w", payloadcodec.ErrUnavailable)
		}
	}
	return out, nil
}

// TestAResultTheCodecCannotReadFailsTheRunNotTheStep: an activity's result
// that this worker could not decode is not a failed step. Returned as one, a
// step that succeeded would run its `continue_on_error:` or `undo:`, and a
// replay on a worker that can read it would take the other branch; the run
// fails instead, naming why.
func TestAResultTheCodecCannotReadFailsTheRunNotTheStep(t *testing.T) {
	t.Parallel()

	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)
	cfg := payloadcodec.Config{Codec: resultUnreadableCodec{Codec: toy}}

	step := logStep("tolerated", "a step whose failure would be tolerated")
	step.Policy = &v1.StepPolicy{ContinueOnError: true}

	env := newCodecEnv(t, engine.TaskRuntimeConfig{}.WithDataConverter(cfg.DataConverter()))
	env.SetDataConverter(cfg.DataConverter())
	env.ExecuteWorkflow(engine.RunWorkflowType, &v1.RunState{Workflow: &v1.Workflow{
		Name:  "codec-result",
		Steps: []*v1.Node{step},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "unavailable",
		"the result was taken for a failed step and the run went on without it")
}
