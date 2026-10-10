import { clean } from './runs'
import { statusFor, statusOf } from './vocab'
import type { RunFacts, Status } from './vocab'

/** A card lists this many steps; the rest is "and N more", one `flow timeline` away. */
export const MAX_STEPS = 30
/** Rows read from one timeline answer; the CLI is also asked for no more than this. */
export const MAX_ENTRIES = 500
/** The longest failure sentence a step row shows. */
const MAX_REASON = 160

/** One step as the card draws it: the latest thing its rows say. */
export interface Step {
  name: string
  status: Status
  /** Attempts seen; more than one means it retried. */
  attempts: number
  durationMs?: number
  /** The failure sentence of the latest failed attempt, cleaned. */
  reason: string
  /** When the step's first row happened: a waiting step's elapsed time is counted from here, locally, between reads. */
  startedMs?: number
}

/**
 * One execution of a step as the timeline shows it, unfolded: a retry attempt
 * or a repeated run of the same label is its own entry. The label is the
 * engine's full one (bounded, cleaned), where a `Step`'s name is cut for the card.
 */
export interface Execution {
  label: string
  status: Status
  attempt: number
  /** The label reached the bound, so it may be a cut one and is never matched to a step. */
  cut: boolean
}

/** The engine's suffix on the timer a `wait_for_signal` opens beside its gate. */
const WAIT_TIMER = / · wait timeout$/

/** Longest full label kept: a 128-byte id, backticks and the engine's ` · wait timeout` suffix fit. */
const MAX_LABEL = 160

export interface Detail {
  /** Every execution in the order they began (for the graph overlay; the card reads `steps`). */
  executions: Execution[]
  /** The steps in the order they began. */
  steps: Step[]
  /** A run-level failure (a row with no step), cleaned. */
  runFailure: string
  /** The server clipped the account: there are more rows than were read. */
  truncated: boolean
}

/** `note` is what a successful `flow timeline` said on stderr, cleaned. */
export type Parsed = { detail: Detail; note?: string } | { error: string }

const toMs = (t: unknown): number | undefined => {
  const ms = typeof t === 'string' ? Date.parse(t) : NaN
  return Number.isFinite(ms) ? ms : undefined
}

interface Raw {
  kind: string
  step: string
  /** The label before the card's cut to 60. */
  full: string
  cut: boolean
  at?: number
  attempt: number
  failure: string
}

/**
 * Reads `flow timeline -o json` (`{entries: [{eventId, time, kind, step,
 * attempt, failure}], truncated}`). A document that is not one is an error the
 * card says plainly; a row that is not an object, or names a kind this build does
 * not know, is skipped so one stray row cannot hide the rest. Work is bounded:
 * only the first MAX_ENTRIES rows are read, whatever the server sent.
 */
export const parseTimeline = (stdout: string): Parsed => {
  let doc: { entries?: unknown; truncated?: unknown }
  try {
    doc = JSON.parse(stdout)
  } catch {
    return { error: 'flow printed something that is not a timeline' }
  }
  if (typeof doc !== 'object' || doc === null || !Array.isArray(doc.entries)) {
    // protojson leaves `entries` out of an empty account.
    if (typeof doc === 'object' && doc !== null && !Array.isArray(doc)) return { detail: { executions: [], steps: [], runFailure: '', truncated: doc.truncated === true } }
    return { error: 'flow printed something that is not a timeline' }
  }

  const rows: Raw[] = []
  for (const e of doc.entries.slice(0, MAX_ENTRIES)) {
    if (typeof e !== 'object' || e === null || typeof e.kind !== 'string') continue
    rows.push({
      kind: e.kind,
      step: clean(e.step, 60),
      full: clean(e.step, MAX_LABEL),
      cut: typeof e.step === 'string' && e.step.length >= MAX_LABEL,
      at: toMs(e.time),
      attempt: Number.isFinite(e.attempt) ? e.attempt : 0,
      failure: clean(e.failure, MAX_REASON),
    })
  }

  const byName = new Map<string, Step & { began?: number }>()
  const executions: Execution[] = []
  const lastOf = new Map<string, Execution>()
  let runFailure = ''
  for (const r of rows) {
    if (r.kind === 'KIND_RUN_ENDED' || r.kind === 'KIND_RUN_CONTINUED' || r.step === '') {
      if (r.failure !== '' && r.step === '') runFailure = r.failure
      continue
    }
    const known = statusOf(r.kind)
    if (known.kind === 'unknown') continue
    // Unfolded: a row opens a new execution when it starts one and the label's last is over or on another attempt.
    const attempt = Math.max(r.attempt, 1)
    const last = lastOf.get(r.full)
    const starts = r.kind === 'KIND_STEP_SCHEDULED' || r.kind === 'KIND_TIMER_STARTED'
    if (last === undefined || (starts && (last.attempt !== attempt || (last.status.kind !== 'running' && last.status.kind !== 'waiting')))) {
      const one = { label: r.full, status: known, attempt, cut: r.cut }
      executions.push(one)
      lastOf.set(r.full, one)
    } else {
      last.status = known
      last.attempt = Math.max(last.attempt, attempt)
    }
    let step = byName.get(r.step)
    if (!step) {
      step = { name: r.step, status: known, attempts: 0, reason: '', began: r.at, startedMs: r.at }
      byName.set(r.step, step)
    }
    step.status = known
    step.attempts = Math.max(step.attempts, r.attempt, 1)
    if (r.failure !== '') step.reason = r.failure
    else if (known.kind === 'succeeded') step.reason = ''
    if (known.kind !== 'running' && known.kind !== 'waiting' && step.began !== undefined && r.at !== undefined) {
      step.durationMs = r.at - step.began
    }
  }

  const steps = [...byName.values()].map(({ began: _began, ...step }) => step)
  return { detail: { executions, steps, runFailure, truncated: doc.truncated === true || doc.entries.length > MAX_ENTRIES } }
}

/**
 * The steps a card shows: at most `max`, and when there are more, the ones that
 * need a reader (failed, waiting, running) before the ones that went well, kept
 * in the order they began. `more` is what was left out.
 */
export const visibleSteps = (steps: readonly Step[], max = MAX_STEPS): { shown: Step[]; more: number } => {
  if (steps.length <= max) return { shown: [...steps], more: 0 }
  const needs = (s: Step) => s.status.kind !== 'succeeded' && s.status.kind !== 'skipped'
  const chosen = new Set<Step>()
  for (const s of steps) if (chosen.size < max && needs(s)) chosen.add(s)
  for (const s of steps) if (chosen.size < max) chosen.add(s)
  return { shown: steps.filter(s => chosen.has(s)), more: steps.length - max }
}

/** The facts `story` and the progress bar read, from the run's row and its steps. */
export const factsFor = (
  run: { workflowId: string; name?: string; status?: unknown; startTime?: string | null; closeTime?: string | null },
  detail: Detail | undefined,
  now = 0,
): RunFacts => {
  const steps = detail?.steps ?? []
  const waiting = steps.find(s => s.status.kind === 'waiting')
  const failed = steps.find(s => s.status.kind === 'failed')
  const start = toMs(run.startTime)
  const end = toMs(run.closeTime) ?? (now > 0 ? now : undefined)
  return {
    name: run.name,
    workflowId: run.workflowId,
    status: run.status,
    done: steps.filter(s => s.status.kind === 'succeeded').length,
    total: steps.length,
    waitingOn: waiting?.name,
    failedStep: failed?.name,
    failure: failed?.reason || detail?.runFailure,
    elapsedMs: start !== undefined && end !== undefined ? end - start : undefined,
  }
}

/**
 * A run that COMPLETED cannot still be waiting at a gate, yet the engine leaves the
 * `wait_for_signal` timer row open when the gate is released (no KIND_TIMER_FIRED).
 * So on a completed run only, each open `· wait timeout` timer is shown as
 * succeeded/`released`, on the executions (graph overlay) and the steps (card)
 * alike. `released` claims neither the signal nor the timeout; no signal row is
 * read, because it carries only a name and cannot say which gate it answered.
 * A failed or cancelled run (the live refresh is what reads one that has just ended) is
 * never shown as released: its open waiting or running rows read `closed`, as cancelled,
 * since the run ended with them unanswered. A run still running, or of unknown status,
 * leaves the detail exactly as the timeline said.
 *
 * This is the fallback for histories already written; closing the timer row at
 * the source is an engine-side fix and a separate change.
 */
export const settleWaits = (detail: Detail | undefined, runStatus: unknown): Detail | undefined => {
  if (detail === undefined) return detail
  const kind = statusOf(runStatus).kind
  if (kind === 'failed' || kind === 'cancelled') {
    const closed = statusFor('cancelled', 'closed')
    const ended = (status: Status) => status.kind === 'waiting' || status.kind === 'running'
    return {
      ...detail,
      executions: detail.executions.map(e => (!e.cut && ended(e.status) ? { ...e, status: closed } : e)),
      steps: detail.steps.map(s => (ended(s.status) ? { ...s, status: closed } : s)),
    }
  }
  if (kind !== 'succeeded') return detail
  const open = (label: string, status: Status) => status.kind === 'waiting' && WAIT_TIMER.test(label)
  const released = statusFor('succeeded', 'released')
  return {
    ...detail,
    executions: detail.executions.map(e => (!e.cut && open(e.label, e.status) ? { ...e, status: released } : e)),
    steps: detail.steps.map(s => (open(s.name, s.status) ? { ...s, status: released } : s)),
  }
}
/** A step's time as the row shows it: its recorded duration, else for a running or waiting step the time since it began, counted against `now` so it moves between reads. */
export const stepElapsed = (s: Step, now: number): number | undefined => {
  if (s.durationMs !== undefined) return s.durationMs
  if ((s.status.kind === 'waiting' || s.status.kind === 'running') && s.startedMs !== undefined && now >= s.startedMs) return now - s.startedMs
  return undefined
}

/** What changed in a read, for the poller's backoff: status, and each step's name, status and attempts. Times are left out, they always move. */
export const fingerprint = (status: unknown, detail: Detail | undefined): string =>
  `${clean(status, 40)}|${(detail?.steps ?? []).map(s => `${s.name}:${s.status.kind}:${s.attempts}`).join(',')}|${detail?.truncated === true}`
