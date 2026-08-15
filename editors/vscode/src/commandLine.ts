// Pure command-line construction for the `flow` CLI. No `vscode` import here
// on purpose: this module composes an argv, nothing more, and it is what
// commandLine.test.ts exercises without a running editor.
//
// Per CLAUDE.md and #585 §3: the extension composes a command line and shows
// the CLI's own output. It never re-implements what a subcommand decides, so
// there is nothing here beyond argv construction.

export type FlowCommandKind = "validate" | "test" | "fix" | "runLocal";

const TITLES: Record<FlowCommandKind, string> = {
  validate: "Flowstate: Validate",
  test: "Flowstate: Test",
  fix: "Flowstate: Fix",
  runLocal: "Flowstate: Run Local",
};

export function commandTitle(kind: FlowCommandKind): string {
  return TITLES[kind];
}

// flowCommandArgs returns the argv (excluding the binary itself) that
// reproduces exactly what a person would type at a terminal for the given
// kind of check, applied to a single file. `run local` is two words in the
// CLI, which is why runLocal is not a 1:1 verb mapping.
export function flowCommandArgs(kind: FlowCommandKind, filePath: string): string[] {
  switch (kind) {
    case "validate":
      return ["validate", filePath];
    case "test":
      return ["test", filePath];
    case "fix":
      return ["fix", filePath];
    case "runLocal":
      return ["run", "local", filePath];
  }
}
