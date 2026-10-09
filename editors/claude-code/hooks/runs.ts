import type { RunSummary } from '../types/flowstate'

/** A pane shows the newest runs only; `flow list` is already newest first. */
export const MAX_RUNS = 8

/** What the pane knows about the server: its runs, or why it has none to show. */
export type Listing = { runs: RunSummary[] } | { offline: string }

/** The first line of what `flow list` said when it could not reach a server. */
const reason = (stderr: string): string => {
  const lines = stderr.split('\n').map(l => l.trim()).filter(l => l !== '' && l !== 'ERROR')
  return lines[0] ?? 'no server answered'
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
  (run.status ?? 'STATUS_UNSPECIFIED').replace(/^STATUS_/, '').toLowerCase()

/** One row: the status, the declared name when there is one, and the id `flow get` takes. */
export const runLine = (run: RunSummary): string =>
  [statusLabel(run), run.name ? `${run.name} (${run.workflowId})` : run.workflowId].join(' ')
