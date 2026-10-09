/**
 * What the mod keeps of one `flow validate` answer: the file and the positions
 * and prose it shows. The answer itself is the schema's
 * `flowstate.v1.DiagnosticReport`, declared in ./flowstate.d.ts by `buf
 * generate`; a plugin's state contract must be self-contained, so this is the
 * projection the mod stores, and hooks/flowfile.ts derives it from the
 * generated type so a renamed schema field fails type-checking there.
 */
export type FileReport = {
  file: string
  /** Empty when the file is clean. */
  diagnostics: { line: number; column: number; message: string }[]
  /** Set when `flow validate` itself could not run. */
  failure?: string
}

declare module 'claude-code' {
  interface PluginState {
    flowstate: { reports: FileReport[] }
  }
}
