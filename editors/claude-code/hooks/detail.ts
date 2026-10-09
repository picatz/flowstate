import { clean } from './runs'
import { statusOf } from './vocab'
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
}

export interface Detail {
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
    if (typeof doc === 'object' && doc !== null && !Array.isArray(doc)) return { detail: { steps: [], runFailure: '', truncated: doc.truncated === true } }
    return { error: 'flow printed something that is not a timeline' }
  }

  const rows: Raw[] = []
  for (const e of doc.entries.slice(0, MAX_ENTRIES)) {
    if (typeof e !== 'object' || e === null || typeof e.kind !== 'string') continue
    rows.push({
      kind: e.kind,
      step: clean(e.step, 60),
      at: toMs(e.time),
      attempt: Number.isFinite(e.attempt) ? e.attempt : 0,
      failure: clean(e.failure, MAX_REASON),
    })
  }

  const byName = new Map<string, Step & { began?: number }>()
  let runFailure = ''
  for (const r of rows) {
    if (r.kind === 'KIND_RUN_ENDED' || r.kind === 'KIND_RUN_CONTINUED' || r.step === '') {
      if (r.failure !== '' && r.step === '') runFailure = r.failure
      continue
    }
    const known = statusOf(r.kind)
    if (known.kind === 'unknown') continue
    let step = byName.get(r.step)
    if (!step) {
      step = { name: r.step, status: known, attempts: 0, reason: '', began: r.at }
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
  return { detail: { steps, runFailure, truncated: doc.truncated === true || doc.entries.length > MAX_ENTRIES } }
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
