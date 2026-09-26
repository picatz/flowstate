// Package flowstatev1 holds the generated flowstate.v1 protobuf types and the
// hand-written engine that gives them behavior: the local interpreter, the task
// registry, and the CEL evaluator that both execution drivers share.
//
// "v1" names the schema edition, the wire contract a Flowfile compiles into,
// not a promise about this package's Go API. Types, function signatures, and
// even which symbols exist here change as the interpreter evolves; only the
// proto messages generated into it carry the compatibility the edition number
// implies. A Go program that wants a stable surface for compiling and running
// Flowfiles should import [github.com/picatz/flowstate/pkg/flowstate/embed]
// instead, which is curated and versioned for that use and reaches everything
// in this package an embedder needs.
//
// # The schema
//
// A compiled [Workflow] is a list of [Node] steps whose inputs are each a
// [Value]: a literal, a CEL expression, a secret reference, or a structure of
// them. [NewValue] and [NewExpr] build one, and [LiteralToGo] reads a literal
// back. A running step sees a [Scope] and produces [Node_Outputs]; a run's
// results are a [Workflow_StepOutputs]. The messages are generated from the
// .proto files under proto/flowstate/v1 and checked by [Validate].
//
// # Running and extending
//
//   - [RunWithInputs] runs a workflow in this process: the local driver.
//   - [Registry] holds the tasks a step can call, each described by a
//     [TaskDef]. [DefaultRegistry] is the build's own set, and
//     [NewContextWithRegistry] gives one run a registry of its own.
//   - [Evaluator] compiles and evaluates CEL under bounded [Limits];
//     [DefaultEvaluator] is the process-wide one both drivers use.
//
// # Related packages
//
//   - [github.com/picatz/flowstate/pkg/flowstate/v1/flowfile] parses and
//     validates a Flowfile into a [Workflow].
//   - [github.com/picatz/flowstate/pkg/flowstate/v1/engine] runs the same
//     workflow durably on Temporal.
//   - [github.com/picatz/flowstate/pkg/flowstate/v1/flowtest] tests a workflow
//     against stubbed tasks and a virtual clock.
//   - [github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk] builds an
//     out-of-process plugin that adds tasks or secret schemes.
package flowstatev1
