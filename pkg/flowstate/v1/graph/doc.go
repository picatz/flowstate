// Package graph builds and renders the graph of how Flowstate workflows connect.
//
// The graph itself is the schema message [v1.Graph]; this package is the
// behaviour around it. [Static] derives one from compiled workflows, which is
// what a directory of Flowfiles says before anything runs, and [Text] renders it
// for a terminal or an agent. Layout, overlays and live state are separate layers
// over the same node ids and are not here yet.
//
// Everything here is a pure function of its input: no clock, no filesystem, no
// network. The caller reads and compiles files, and hands in workflows.
package graph
