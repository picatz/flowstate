import type { FileReport } from '../types'
import type { RunSummary } from '../types/flowstate'
import { DEFAULT_ADDRESS } from './guard'
import { clean } from './runs'
import { baseName, middleTruncate, statusOf } from './vocab'

/** What the Runs pane last learned from a server: when, which address, and the runs that need a person. */
export interface Seen {
  at: number
  address: string
  /** Runs the listing reports as failed, timed out or terminated; a run waiting on a gate is `running` there, so none are counted as waiting. */
  failed: number
}

export const NO_SEEN: Seen = { at: 0, address: '', failed: 0 }

/** A server's answer older than this is not shown: the line is only redrawn on events, so it also says when it knew. */
export const FRESH_MS = 120_000
/** A count is shown up to this and then as `99+`. */
const MAX_COUNT = 99

/** Counts the failed, timed-out and terminated runs of an unfiltered listing (a filtered one counts something else). */
export const seenFrom = (runs: readonly RunSummary[], address: string, at: number): Seen => {
  let failed = 0
  for (const r of runs) {
    const s = statusOf(r.status)
    if (s.kind === 'failed' || s.word === 'terminated') failed++
  }
  return { at, address, failed }
}

export const count = (n: number): string => (n > MAX_COUNT ? `${MAX_COUNT}+` : String(Math.max(0, Math.trunc(n) || 0)))
const file = (name: unknown): string => baseName(name, 28)
const clock = (at: number): string => {
  const d = new Date(at)
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
}

export interface Inputs {
  /** The newest `flow validate` result. */
  report?: Pick<FileReport, 'file' | 'diagnostics' | 'failure'>
  /** The last local run from the run form. */
  run?: { file: string; kind: '' | 'ok' | 'failed' | 'unknown' | 'notrun' }
  /** The leg verify-before-done still owes (hooks/verify.ts `missingLeg`). */
  owes?: string
  seen?: Seen
  now: number
}

/**
 * The status line, plain text: empty unless something needs a person. Only
 * real attention shows, behind one leading `⚠`: a validate with errors, a
 * failed local run, a verification still owed after an edit, and runs the
 * server reports as failed. A passing check, a validate that could not run, and
 * nothing known at all print nothing, and the line never suggests a command
 * (it cannot tell whether the pane is open). Every fact comes from state
 * already held, so drawing it runs nothing; a server's count shows only while
 * fresh and carries the time it was read, since the line is redrawn on events,
 * not by a clock. The host already prefixes the line with the mod's name.
 */
export const statusText = ({ report, run, owes, seen, now }: Inputs): string => {
  const parts: string[] = []
  const n = report?.failure === undefined ? (report?.diagnostics.length ?? 0) : 0
  if (report !== undefined && n > 0) parts.push(`validate ${count(n)} error${n === 1 ? '' : 's'} ${file(report.file)}`)
  if (run?.kind === 'failed') parts.push(`run failed ${file(run.file)}`)
  if (owes !== undefined) parts.push(`owes ${clean(owes, 20)}`)
  if (seen !== undefined && seen.at > 0 && now >= seen.at && now - seen.at < FRESH_MS && seen.failed > 0) {
    parts.push(`server ${middleTruncate(seen.address || DEFAULT_ADDRESS, 30)} ${count(seen.failed)} need attention at ${clock(seen.at)}`)
  }
  return parts.length === 0 ? '' : `⚠ ${parts.join(' · ')}`
}
