import type { RunSummary } from '../types/flowstate'

/** A pane shows the newest runs only; `flow list` is already newest first. */
export const MAX_RUNS = 8

/** What the pane knows about the server: its runs, or why it has none to show. */
export type Listing = { runs: RunSummary[] } | { offline: string }

/**
 * A name, an id and a server's error text come from other parties, and the pane
 * writes them to a terminal: drop C0/C1 controls (an ESC starts an escape
 * sequence) and the invisible format characters that reorder or hide text (zero-width and bidi marks, BOM) and bound the length.
 */
export const clean = (value: unknown, max = 80): string =>
  typeof value === 'string' ? value.replace(/[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]/g, '').slice(0, max) : ''

/** The first line of what `flow list` said when it could not reach a server. */
export const reason = (stderr: string): string => {
  const lines = stderr.split('\n').map(l => l.trim()).filter(l => l !== '' && l !== 'ERROR')
  return clean(lines[0], 100) || 'no server answered'
}

/**
 * The CLI's whole message for a rejected filter: its error wraps over several
 * lines (the CEL position and a caret) before the blank line that precedes any
 * `NEXT` hint, and the first line alone would cut it off mid-sentence.
 */
export const rejection = (stderr: string): string => {
  const body: string[] = []
  for (const l of stderr.split('\n').map(x => x.trim())) {
    if (l === 'ERROR' && body.length === 0) continue
    if (l === '' || l === 'NEXT') break
    body.push(l)
  }
  return clean(body.join(' '), 240) || 'no server answered'
}

/**
 * What a successful `flow timeline` said on stderr (it explains a gap in the
 * account, such as a step waiting out a retry backoff), cleaned and bounded;
 * empty when it said nothing.
 */
export const stderrNote = (stderr: string): string =>
  clean(stderr.split('\n').map(l => l.trim()).filter(l => l !== '' && l !== 'ERROR').join(' '), 240)

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
export const toListing = (run: { exitCode: number; stdout: string; stderr: string }, filtered = false): Listing =>
  run.exitCode === 0
    ? { runs: parsePage(run.stdout).runs.slice(0, MAX_RUNS) }
    : { offline: filtered ? rejection(run.stderr) : reason(run.stderr) }
