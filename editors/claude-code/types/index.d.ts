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
      /** The Flowfile the pane's run form is for (hooks/form.ts); empty for none. */
      runFile: string
      /** What was typed into the run form's controls, by declared input name; an input not present reads as its declared default. */
      runValues: Record<string, string>
      /** A Run locally press awaiting its Confirm: the file and the exact inputs the question named. Nothing runs while this is set; empty file for none. */
      runConfirm: { file: string; inputs: { name: string; value: string }[] }
      /** What the last Confirm did: ok, failed (the engine's refusal or failure), unknown (a timeout or a thrown run), or notrun (a check before the run refused it); text and output lines are cleaned and bounded; cards are the declared outputs of a succeeded run, with sensitive values never held. */
      runResult: { file: string; kind: '' | 'ok' | 'failed' | 'unknown' | 'notrun'; text: string; lines: string[]; cards: { cards: { title: string; type: string; help: string; status: { kind: string; symbol: string; tone: string; word: string }; fact: string; raw: string; cut: boolean }[]; more: number } | null }
      /** The output cards show the raw values (sensitive ones still hidden) instead of the labelled cards (hooks/outputs.ts). */
      outputsRaw: boolean
      /** The `flow test` band above the prompt (hooks/testband.ts); null for none. Cleared when a Flowfile or test file is edited. */
      testBand: {
        outcome: 'passed' | 'failed' | 'unknown'
        detailed: boolean
        passed: number
        failed: number
        skipped: number
        uncovered: number
        failing: { name: string; file: string; line: number; reason: string }[]
        more: number
        cut: boolean
        note: string
      } | null
      /** This turn's Flowfile edits and the checks since (hooks/verify.ts); reset when a turn starts. */
      verify: { edited: string[]; validated: boolean; tested: boolean; nudged: boolean }
    }
  }
}
