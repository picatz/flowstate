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
    flowstate: {
      reports: FileReport[]
      /** The workflow id whose detail card the pane shows; empty for none. */
      selected: string
      /** The pressed run's name, status and times (cleaned, bounded), kept so its card outlives the listing's newest-runs window. */
      summary: { name: string; status: string; startTime: string; closeTime: string }
      /** The CEL text in the Runs filter box, passed to `flow list --filter` unchanged; empty for none. */
      filter: string
      /** A Send press awaiting its Confirm (hooks/signal.ts): the run, the signal and the server it was aimed at; empty id for none. Nothing is sent while this is set. */
      confirm: { id: string; signal: string; address: string }
      /** What the last Confirm did for the card: delivered, or the server's refusal (cleaned, bounded). */
      outcome: { id: string; signal: string; ok: boolean; text: string }
      /** This turn's Flowfile edits and the checks since (hooks/verify.ts); reset when a turn starts. */
      verify: { edited: string[]; validated: boolean; tested: boolean; nudged: boolean }
    }
  }
}
