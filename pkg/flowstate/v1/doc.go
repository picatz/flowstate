// Package flowstatev1 holds the generated flowstate.v1 protobuf types and the
// hand-written engine that gives them behavior: the local interpreter
// ([RunWithInputs]), the task registry, the CEL evaluator, and the primitives
// [pkg/flowstate/v1/engine] wraps for durable execution against Temporal.
//
// "v1" names the *schema* edition — the wire contract [flowfile] compiles
// into and [pkg/flowstate/v1/engine] executes — not a promise about this
// package's Go API. Types, function signatures, and even which symbols exist
// here change as the interpreter evolves; only the proto messages generated
// into it carry the compatibility the edition number implies. A Go program
// that wants a stable surface for compiling and running Flowfiles as an
// embedded library should import [pkg/flowstate/embed] instead, which is
// curated and versioned for exactly that use, and reaches everything in this
// package it needs on an embedder's behalf.
package flowstatev1
