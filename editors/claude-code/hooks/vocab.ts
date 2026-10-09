import type { RunSummary } from '../types/flowstate'
import { clean } from './runs'

/**
 * The one visual vocabulary (docs: the plugin roadmap, "Visual vocabulary").
 * Every view composes these; none invents a symbol, colour or word. Pure, so the
 * terminal and desktop forms and the plain-text form agree by construction.
 */

/** Semantic colour tokens, never raw colours; `muted` is the dim one. */
export type Tone = 'ok' | 'fail' | 'active' | 'wait' | 'undone' | 'muted'

/** The ANSI colour each token draws as, so the user's terminal theme wins. */
export const COLOR: Record<Tone, string> = {
  ok: 'green',
  fail: 'red',
  active: 'blue',
  wait: 'yellow',
  undone: 'magenta',
  muted: 'gray',
}

export type StatusKind =
  | 'succeeded'
  | 'failed'
  | 'running'
  | 'waiting'
  | 'cancelled'
  | 'skipped'
  | 'compensated'
  | 'unknown'

/** A status as shown: the symbol and the word each carry it alone; the colour only repeats it. */
export interface Status {
  kind: StatusKind
  symbol: string
  tone: Tone
  /** The schema's own distinction where the kind folds two (`timed out`, `terminated`). */
  word: string
}

const BASE: Record<StatusKind, { symbol: string; tone: Tone; word: string }> = {
  succeeded: { symbol: '✓', tone: 'ok', word: 'succeeded' },
  failed: { symbol: '✗', tone: 'fail', word: 'failed' },
  running: { symbol: '●', tone: 'active', word: 'running' },
  waiting: { symbol: '◔', tone: 'wait', word: 'waiting' },
  cancelled: { symbol: '⊘', tone: 'muted', word: 'cancelled' },
  skipped: { symbol: '–', tone: 'muted', word: 'skipped' },
  compensated: { symbol: '↺', tone: 'undone', word: 'compensated' },
  unknown: { symbol: '?', tone: 'muted', word: 'unknown' },
}

/** The braille frames of the running spinner, for a caller that animates. */
export const SPINNER = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'] as const

/**
 * `frame` is set only by a caller that animates, and only when the output is
 * interactive and motion is allowed; without it a running status is the static `●`.
 */
export const statusFor = (kind: StatusKind, word?: string, frame?: number): Status => {
  const base = BASE[kind]
  const symbol = kind === 'running' && frame !== undefined ? SPINNER[Math.abs(Math.trunc(frame)) % SPINNER.length] : base.symbol
  return { kind, symbol, tone: base.tone, word: word ?? base.word }
}

/**
 * Words from `STATUS_*` (a run, `flow get`/`flow list`), `KIND_*` (a timeline
 * row), a bare `RUNNING`/`FAILED` as a CEL filter spells them, or a plain word.
 * Anything else is `unknown`, shown as such rather than guessed at.
 */
const BY_NAME: Record<string, [StatusKind, string?]> = {
  running: ['running'],
  completed: ['succeeded'],
  succeeded: ['succeeded'],
  failed: ['failed'],
  canceled: ['cancelled'],
  cancelled: ['cancelled'],
  terminated: ['cancelled', 'terminated'],
  timed_out: ['failed', 'timed out'],
  skipped: ['skipped'],
  compensated: ['compensated'],
  waiting: ['waiting'],
  // Timeline rows: what the last row for a step says about it.
  step_scheduled: ['running'],
  step_completed: ['succeeded'],
  step_failed: ['failed'],
  step_timed_out: ['failed', 'timed out'],
  step_canceled: ['cancelled'],
  timer_started: ['waiting'],
  timer_fired: ['succeeded'],
  signal_received: ['succeeded'],
}

export const statusOf = (raw: unknown, frame?: number): Status => {
  const name = clean(raw, 40).toLowerCase().replace(/^(status|kind)_/, '')
  const found = Object.hasOwn(BY_NAME, name) ? BY_NAME[name] : undefined
  return found ? statusFor(found[0], found[1], frame) : statusFor('unknown')
}

/** `✓ succeeded`: the StatusChip's plain text. */
export const chip = (s: Status): string => `${s.symbol} ${s.word}`

/**
 * `███░░░`: filled in proportion to done/total. The numbers travel beside it
 * (`2/3`), so the bar is never the only signal. A bar always has `width` cells,
 * and a total of nothing is an empty one.
 */
export const progressBar = (done: number, total: number, width = 12): string => {
  const cells = Number.isFinite(width) ? Math.max(1, Math.min(80, Math.trunc(width))) : 12
  const ratio = total > 0 && Number.isFinite(done) && Number.isFinite(total) ? Math.min(1, Math.max(0, done / total)) : 0
  const filled = Math.round(ratio * cells)
  return '█'.repeat(filled) + '░'.repeat(cells - filled)
}

/** `450ms`, `1.5s`, `1m 5s`, `3h 4m`, `2d 3h`; nothing for a duration that is not one. */
export const duration = (ms: number | undefined): string => {
  if (ms === undefined || !Number.isFinite(ms) || ms < 0) return ''
  if (ms < 1000) return `${Math.round(ms)}ms`
  const s = Math.floor(ms / 1000)
  if (s < 10) return `${Math.floor(ms / 100) / 10}s`
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ${s % 60}s`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ${m % 60}m`
  return `${Math.floor(h / 24)}d ${h % 24}h`
}

/** `wf-2f…9c1`: an id keeps its head and its tail, the parts people recognise. */
export const middleTruncate = (id: unknown, max = 24): string => {
  const text = clean(id, 512)
  const limit = Math.max(3, Math.trunc(max) || 24)
  if (text.length <= limit) return text
  const head = Math.ceil((limit - 1) / 2)
  const tail = limit - 1 - head
  return `${text.slice(0, head)}…${tail > 0 ? text.slice(-tail) : ''}`
}

/** What `story` needs; the pane derives it from `flow list` and `flow timeline`. */
export interface RunFacts {
  name?: string
  workflowId: string
  /** The run's raw status, as the schema spells it. */
  status: unknown
  /** Steps finished, and steps the run has reached (all it knows of the total). */
  done: number
  total: number
  /** What it waits on (a timer or signal label), when it does. */
  waitingOn?: string
  /** The first step that failed, and the sentence it failed with. */
  failedStep?: string
  failure?: string
  elapsedMs?: number
}

const plural = (n: number) => `${n} step${n === 1 ? '' : 's'}`

/**
 * The run as one sentence: `Deploy: 2 of 3 steps done, waiting for approval`.
 * A failure leads with the reason, not a stack. Every string from the run is
 * cleaned here, so a caller cannot forget.
 */
export const story = (run: RunFacts): string => {
  const who = clean(run.name, 60) || middleTruncate(run.workflowId)
  const s = statusOf(run.status)
  const done = Math.max(0, Math.trunc(run.done) || 0)
  const total = Math.max(done, Math.trunc(run.total) || 0)
  const progress = total > 0 && done < total ? `${done} of ${plural(total)} done` : total > 0 ? `${plural(total)} done` : 'no steps yet'
  const took = duration(run.elapsedMs)
  const tail = took ? ` (${took})` : ''
  const step = clean(run.failedStep, 60)
  const why = clean(run.failure, 160)

  switch (s.kind) {
    case 'succeeded':
      return `${who}: succeeded, ${progress}${tail}`
    case 'failed':
      return `${who}: ${s.word}${step ? ` in ${step}` : ''}${why ? `, ${why}` : ''} (${progress})${tail}`
    case 'cancelled':
      return `${who}: ${s.word} after ${progress}${tail}`
    case 'running': {
      const waits = clean(run.waitingOn, 60)
      return `${who}: ${progress}, ${waits ? `waiting for ${waits}` : 'running'}${tail}`
    }
    default:
      return `${who}: status unknown, ${progress}${tail}`
  }
}

/** One row of the Runs list: its status, then the declared name and the id `flow get` takes. */
export const runRow = (run: RunSummary): { status: Status; text: string } => {
  const status = statusOf(run.status)
  const id = middleTruncate(run.workflowId, 28)
  return { status, text: `${status.word} ${run.name ? `${clean(run.name)} (${id})` : id}` }
}
