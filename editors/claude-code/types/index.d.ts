export type Diagnostic = { line: number; column: number; message: string }

export type FileReport = {
  file: string
  /** Empty when the file is clean. */
  diagnostics: Diagnostic[]
  /** Set when `flow validate` itself could not run. */
  failure?: string
}

declare module 'claude-code' {
  interface PluginState {
    flowstate: { reports: FileReport[] }
  }
}
