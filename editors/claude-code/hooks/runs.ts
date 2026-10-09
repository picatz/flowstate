import type { RunSummary } from '../types/flowstate'

/** A pane shows the newest runs only; `flow list` is already newest first. */
export const MAX_RUNS = 8

/** What the pane knows about the server: its runs, or why it has none to show. */
export type Listing = { runs: RunSummary[] } | { offline: string }

/**
 * A name, an id and a server's error text come from other parties, and the pane
 * writes them to a terminal: drop C0/C1 controls (an ESC starts an escape
 * sequence) and bound the length.
 */
export const clean = (value: unknown, max = 80): string =>
  typeof value === 'string' ? value.replace(/[\u0000-\u001f\u007f-\u009f]/g, '').slice(0, max) : ''

/** The first line of what `flow list` said when it could not reach a server. */
const reason = (stderr: string): string => {
  const lines = stderr.split('\n').map(l => l.trim()).filter(l => l !== '' && l !== 'ERROR')
  return clean(lines[0], 100) || 'no server answered'
}

/**
 * Reads `flow list -o jsonl`: one protojson RunSummary per line. A line that is
 * not one, or names no run, is skipped, so a stray line cannot hide the rest.
 */
export const parseRuns = (stdout: string): RunSummary[] => {
  const runs: RunSummary[] = []
  for (const line of stdout.split('\n')) {
    if (!line.trim()) continue
    try {
      const run = JSON.parse(line) as Partial<RunSummary>
      if (typeof run.workflowId === 'string' && run.workflowId !== '') {
        runs.push(run as RunSummary)
      }
    } catch {
      // not a run
    }
  }
  return runs.slice(0, MAX_RUNS)
}

/** Local by default: a failed `flow list` means no server is configured or answering, not an error. */
export const toListing = (run: { exitCode: number; stdout: string; stderr: string }): Listing =>
  run.exitCode === 0 ? { runs: parseRuns(run.stdout) } : { offline: reason(run.stderr) }

/** `RUNNING`, `FAILED`, ...: the schema's name without its `STATUS_` prefix. */
export const statusLabel = (run: RunSummary): string =>
  clean(run.status ?? 'STATUS_UNSPECIFIED', 24).replace(/^STATUS_/, '').toLowerCase()

/** One row: the status, the declared name when there is one, and the id `flow get` takes. */
export const runLine = (run: RunSummary): string =>
  [statusLabel(run), run.name ? `${clean(run.name)} (${clean(run.workflowId)})` : clean(run.workflowId)].join(' ')
