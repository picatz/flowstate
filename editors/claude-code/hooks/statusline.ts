import type { FileReport } from '../types'
import type { RunSummary } from '../types/flowstate'
import { DEFAULT_ADDRESS } from './guard'
import { clean } from './runs'
import { chip, middleTruncate, statusFor, statusOf } from './vocab'

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
const file = (name: unknown): string => middleTruncate(name, 28).replaceAll('`', "'")
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
 * The status line, plain text: every status is a symbol and a word, and every
 * fact comes from state already held, so drawing it runs nothing. With nothing
 * known it names the one command to start. A server's counts show only while
 * fresh and only when someone needs attending to; they carry the time they
 * were read, since the line is redrawn on events, not by a clock.
 */
export const statusText = ({ report, run, owes, seen, now }: Inputs): string => {
  const parts: string[] = []
  if (report !== undefined) {
    const n = report.diagnostics.length
    const s =
      report.failure !== undefined
        ? statusFor('unknown', 'did not run')
        : n > 0
          ? statusFor('failed', `${count(n)} error${n === 1 ? '' : 's'}`)
          : statusFor('succeeded', 'ok')
    parts.push(`validate ${chip(s)} ${file(report.file)}`)
  }
  if (run !== undefined && run.kind !== '') {
    const s =
      run.kind === 'ok'
        ? statusFor('succeeded')
        : run.kind === 'failed'
          ? statusFor('failed')
          : run.kind === 'notrun'
            ? statusFor('skipped', 'not run')
            : statusFor('unknown')
    parts.push(`run ${chip(s)} ${file(run.file)}`)
  }
  if (owes !== undefined) parts.push(chip(statusFor('waiting', `owes ${clean(owes, 20)}`)))
  if (seen !== undefined && seen.at > 0 && now >= seen.at && now - seen.at < FRESH_MS && seen.failed > 0) {
    parts.push(`server ${middleTruncate(seen.address || DEFAULT_ADDRESS, 30)} ${chip(statusFor('failed', `${count(seen.failed)} need attention`))} at ${clock(seen.at)}`)
  }
  return parts.length === 0 ? 'flowstate: nothing checked yet, run /flowstate' : `flowstate: ${parts.join(' · ')}`
}
