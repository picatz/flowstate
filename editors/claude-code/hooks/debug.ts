import { clean } from './runs'
import { middleTruncate } from './vocab'
import { WORKFLOW_ID } from './signal'

/**
 * The debug story of a run, from the one surface `flow` has for it: `flow debug get
 * <id> -o json`, the schema's `flowstate.v1.DebugSnapshot` (its state, why it is held,
 * where, and the most recent observations). It is a snapshot of now: the timeline
 * names no breakpoint hit and no resume, and the snapshot keeps no history of past
 * pauses, so a pause that was resumed is not recoverable here and is not invented.
 * Read only, server only, and bounded like every CLI-derived text.
 */

/** A snapshot over this is not parsed; the document is bounded by the schema but not by the server's goodwill. */
export const MAX_SNAPSHOT = 256 * 1024
/** Observation lines drawn, newest last; and the longest of each. */
export const MAX_NOTES = 3
const MAX_NOTE = 120
const MAX_ADDRESS = 80
export const DEBUG_TIMEOUT_MS = 5000

/** The argv of `flow debug get -o json`, or undefined when the id is not a plain one. Operands follow `--`. */
export const debugArgv = (flow: string, address: string, id: string): string[] | undefined =>
  WORKFLOW_ID.test(id) ? [flow, 'debug', 'get', '-o', 'json', ...(address !== '' ? [`--address=${address}`] : []), '--', id] : undefined

const word = (v: unknown, prefix: string): string => {
  const raw = clean(v, 80).toLowerCase()
  const s = raw.startsWith(prefix.toLowerCase()) ? raw.slice(prefix.length) : raw
  return s.replaceAll('_', ' ').trim()
}

/** One line of text from the server: whitespace (newlines included) folded to single spaces first, then cleaned and cut. */
const flat = (v: unknown): string => (typeof v === 'string' ? clean(v.replace(/\s+/g, ' ').trim(), MAX_NOTE) : '')

/** Observation kinds worth a line: what the author's run said and what the debugger noticed; step bookkeeping is the timeline's. */
const NOTE_KINDS = new Set(['notice', 'log', 'failed', 'tolerated', 'waiting'])

export interface Story {
  lines: string[]
}

/**
 * The story as plain lines, or undefined when the document is not a snapshot (a run with
 * no debug session answers an error, which never reaches here). Unknown states are shown
 * as what they say, not guessed at.
 */
export const storyOf = (stdout: string, truncated = false): Story | undefined => {
  if (truncated || stdout.length > MAX_SNAPSHOT) return undefined
  let doc: Record<string, unknown>
  try {
    const parsed: unknown = JSON.parse(stdout)
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return undefined
    doc = parsed as Record<string, unknown>
  } catch {
    return undefined
  }
  if (typeof doc.state !== 'string' && typeof doc.session !== 'object') return undefined
  const state = word(doc.state, 'DEBUG_RUN_STATE_')
  const reason = word(doc.reason, 'DEBUG_STOP_REASON_')
  const occ = typeof doc.occurrence === 'object' && doc.occurrence !== null ? (doc.occurrence as Record<string, unknown>) : {}
  const at = middleTruncate(clean(occ.address, 512), MAX_ADDRESS)
  const where = at === '' ? '' : ` ${at}`
  const lines: string[] = []
  switch (state) {
    case 'held':
      lines.push(`◉ paused${where}${reason && reason !== 'unspecified' ? ` · ${reason}` : ''}`)
      break
    case 'running':
      lines.push(`◉ debug session running${where}`)
      break
    case 'pause requested':
      lines.push(`◉ debug session pause requested${where}`)
      break
    case '':
    case 'unspecified':
      return undefined
    default:
      lines.push(`◉ debug session ${state}${at === '' ? '' : ` · last at ${at}`}`)
  }
  const ids = Array.isArray(doc.breakpointIds) ? doc.breakpointIds.length : 0
  if (ids > 0 && state === 'held') lines[0] += ` · ${ids} breakpoint${ids === 1 ? '' : 's'} hit`
  const failure = flat(doc.failure)
  if (failure !== '') lines.push(`  ✗ ${failure}`)
  const seen = Array.isArray(doc.observations) ? doc.observations.slice(-50) : []
  const notes = seen
    .filter((o): o is Record<string, unknown> => typeof o === 'object' && o !== null && NOTE_KINDS.has(word((o as Record<string, unknown>).kind, 'DEBUG_OBSERVATION_KIND_')))
    .map(o => flat(o.text))
    .filter(t => t !== '')
    .slice(-MAX_NOTES)
  for (const n of notes) lines.push(`  · ${n}`)
  return { lines }
}
