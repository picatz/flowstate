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

/** A page-walk stops after this many calls: a bounded scan can return short pages, but the pane never walks a whole history. */
export const MAX_PAGES = 4

/** One page of `flow list -o json`: its runs, and the token that continues it. */
export interface Page {
  runs: RunSummary[]
  next: string
}

/**
 * Reads one `flow list -o json` document (`{runs, nextPageToken}`). A run that
 * names no workflow is skipped, so a stray entry cannot hide the rest, and a
 * document that is not JSON is an empty page.
 */
export const parsePage = (stdout: string): Page => {
  try {
    const doc = JSON.parse(stdout) as { runs?: unknown; nextPageToken?: unknown }
    const runs = Array.isArray(doc.runs) ? doc.runs : []
    return {
      runs: runs.filter(
        (r): r is RunSummary => typeof r?.workflowId === 'string' && r.workflowId !== '',
      ),
      next: typeof doc.nextPageToken === 'string' ? doc.nextPageToken : '',
    }
  } catch {
    return { runs: [], next: '' }
  }
}

/**
 * Local by default, but not every failure is "no server": `flow list` also
 * exits non-zero for a refused credential or a bad flag. The pane says the runs
 * are unavailable and shows what `flow` said.
 */
export const toListing = (run: { exitCode: number; stdout: string; stderr: string }): Listing =>
  run.exitCode === 0 ? { runs: parsePage(run.stdout).runs.slice(0, MAX_RUNS) } : { offline: reason(run.stderr) }

/** `RUNNING`, `FAILED`, ...: the schema's name without its `STATUS_` prefix. */
export const statusLabel = (run: RunSummary): string =>
  clean(run.status ?? 'STATUS_UNSPECIFIED', 24).replace(/^STATUS_/, '').toLowerCase()

/** One row: the status, the declared name when there is one, and the id `flow get` takes. */
export const runLine = (run: RunSummary): string =>
  [statusLabel(run), run.name ? `${clean(run.name)} (${clean(run.workflowId)})` : clean(run.workflowId)].join(' ')
