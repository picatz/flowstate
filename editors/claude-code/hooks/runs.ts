import type { RunSummary } from '../types/flowstate'

/** A pane shows the newest runs only; `flow list` is already newest first. */
export const MAX_RUNS = 8

/** What the pane knows about the server: its runs, or why it has none to show. */
export type Listing = { runs: RunSummary[] } | { offline: string }

/**
 * A name, an id and a server's error text come from other parties, and the pane
 * writes them to a terminal: drop C0/C1 controls (an ESC starts an escape
 * sequence) and the invisible format characters that reorder or hide text (zero-width, bidi, word-joiner and tag characters, soft hyphen, line and paragraph separators, BOM, and lone surrogates) and bound the length.
 */
export const clean = (value: unknown, max = 80): string =>
  typeof value === 'string'
    ? value.replace(/[\u0000-\u001f\u007f-\u009f\u00ad\u034f\u061c\u180e\u200b-\u200f\u2028\u2029\u202a-\u202e\u2060-\u2064\u2066-\u2069\ufeff\u{e0000}-\u{e007f}\ud800-\udfff]/gu, '').slice(0, max)
    : ''

/** The first line of what `flow list` said when it could not reach a server. */
export const reason = (stderr: string): string => {
  const lines = stderr.split('\n').map(l => l.trim()).filter(l => l !== '' && l !== 'ERROR')
  // A runaway line is cut to its start and its end before anything walks it: the cause comes last, and the work stays bounded.
  const first = lines[0] ?? ''
  const line = clean(first.length > MAX_LINE ? `${first.slice(0, MAX_LINE / 2)} ${first.slice(first.length - MAX_LINE / 2)}` : first, Number.MAX_SAFE_INTEGER)
  return line === '' ? 'no server answered' : shortCause(line)
}

/** The longest a one-line reason gets before its middle is dropped. */
const MAX_REASON = 100
/** How much of the start survives; the rest of the budget is the tail. */
const HEAD = 35
/** The most of one stderr line that is read; anything longer keeps its start and its end. */
const MAX_LINE = 4000

/**
 * A transport error names its cause last (`rpc error: code = Unavailable desc = ... dial tcp ...: connection
 * refused`), so a long line keeps its head and its tail rather than losing the cause to a cut at the end.
 */
export const shortCause = (line: string): string => {
  const cells = Array.from(line)
  if (cells.length <= MAX_REASON) return line
  return `${cells.slice(0, HEAD).join('').trimEnd()} … ${cells.slice(cells.length - (MAX_REASON - HEAD - 3)).join('').trimStart()}`
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

/**
 * Whether what `flow timeline` said on stderr is the CLI's refusal for a run the server has no record of: `no run "<id>" is
 * addressable: ...` (`refusedRun`, cmd/flow/client.go). The id is `%q`-quoted, so it is matched as a quoted string and nothing
 * after it is read: an id that merely contains "not found", or a transport error that quotes one, is not it. Tested on the raw
 * line, because the one-line reason cuts the middle of a long id.
 */
export const isGone = (stderr: string): boolean => {
  const first = stderr.split('\n').map(l => l.trim()).find(l => l !== '' && l !== 'ERROR') ?? ''
  return /^no run "(?:[^"\\]|\\.)*" is addressable:/.test(first)
}
