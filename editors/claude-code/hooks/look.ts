import { clean } from './runs'
import type { Tone } from './vocab'

/**
 * Text-level helpers for the pane's look: section headers, short paths and the
 * empty-state copy. Pure and colour-free (a caller draws `tone` with
 * `COLOR[tone]`), so the terminal and desktop forms and the tests agree.
 */

/** A section header as parts the pane draws: the title and chip on one line, the rule under it. */
export interface Section {
  /** `▍ Runs`: bold, drawn in `tone`. */
  title: string
  tone: Tone
  /** A dim count or short fact beside the title (`3`); empty for none. */
  chip: string
  /** A thin dim line under the header: modest, bounded. */
  rule: string
}

const MAX_TITLE = 40
const MAX_CHIP = 16
const MAX_RULE = 28

/** `▍ Runs` with an optional dim chip; the name and detail are cleaned and bounded. */
export const sectionTitle = (name: string, detail?: string | number): Section => {
  const word = clean(name, MAX_TITLE).trim()
  const text = typeof detail === 'number' ? (Number.isFinite(detail) ? String(Math.trunc(detail)) : '') : detail
  const chip = clean(text, MAX_CHIP).trim()
  const title = `▍ ${word}`
  const width = Math.min(MAX_RULE, Math.max(12, title.length + (chip === '' ? 0 : chip.length + 1)))
  return { title, tone: 'active', chip, rule: '─'.repeat(width) }
}

const MAX_PATH = 48

const slashes = (p: string): string => p.replace(/\\/g, '/').replace(/\/+/g, '/')
const isAbsolute = (p: string): boolean => p.startsWith('/') || /^[A-Za-z]:\//.test(p)

/**
 * A Flowfile as the pane names it, never an absolute path. A path under `cwd`
 * shows relative to it; a relative one stays (without `./`); anything else
 * (absolute elsewhere, a `..` escape, a Windows drive) shrinks to its base name.
 * Cleaned, and an over-long result is cut so the base name survives.
 */
export const relPath = (file: unknown, cwd?: string): string => {
  // The tail is kept: the base name is the end of a path.
  const raw = slashes(clean(typeof file === 'string' ? file.slice(-1024) : file, 1024).trim())
  if (raw === '') return ''
  const base = raw.split('/').filter(s => s !== '' && s !== '.' && s !== '..').pop() ?? ''
  let p = raw
  if (isAbsolute(p)) {
    const root = cwd === undefined ? '' : slashes(clean(cwd, 1024).trim()).replace(/\/$/, '')
    // A Windows path compares without case; a POSIX one does not.
    const windows = /^[A-Za-z]:/.test(root)
    const head = windows ? p.slice(0, root.length + 1).toLowerCase() : p.slice(0, root.length + 1)
    const want = windows ? `${root}/`.toLowerCase() : `${root}/`
    p = root !== '' && p.length > root.length + 1 && head === want ? p.slice(root.length + 1) : base
  }
  const parts = p.split('/').filter(s => s !== '' && s !== '.')
  p = parts.includes('..') ? base : parts.join('/')
  return fit(p, base)
}

/** Cut an over-long path so its base name survives. */
const fit = (p: string, base: string): string => {
  if (p.length <= MAX_PATH) return p
  if (base.length >= MAX_PATH - 2) return `…${base.slice(-(MAX_PATH - 1))}`
  return `${p.slice(0, Math.max(1, MAX_PATH - base.length - 2))}…/${base}`
}

const MAX_SEGMENTS = 3

/**
 * One label per file, in order: the base name when it is unique, else the
 * shortest path suffix (up to the last three segments) that tells it from the
 * others. Never absolute, cleaned and bounded like `relPath`; identical paths
 * share a label, so the result is stable.
 */
export const labelsFor = (files: readonly unknown[]): string[] => {
  const segs = files.map(f =>
    slashes(clean(typeof f === 'string' ? f.slice(-1024) : f, 1024).trim())
      .split('/')
      .filter(s => s !== '' && s !== '.' && s !== '..')
      .filter((s, n) => !(n === 0 && /^[A-Za-z]:$/.test(s))),
  )
  const tail = (i: number, k: number) => segs[i].slice(-k).join('/')
  return segs.map((own, i) => {
    let k = 1
    while (k < MAX_SEGMENTS && k < own.length && segs.some((other, j) => other.join('/') !== own.join('/') && tail(j, k) === tail(i, k))) k++
    return fit(tail(i, k), own[own.length - 1] ?? '')
  })
}

/** One short line per empty state: what is missing, then the one next action. */
export const COPY = {
  runs: 'No runs yet.',
  runsFiltered: 'No runs match the filter.',
  noFlowfile: 'No Flowfile here.',
  graph: 'Pick a Flowfile to see its steps.',
  flowfiles: 'Edit a Flowfile and it shows up here.',
} as const

/** The one dim hint under "Run a Flowfile": the safety property, not the mechanism. */
export const RUN_HINT = 'Runs locally and asks before it does.'
