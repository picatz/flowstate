import { count } from './statusline'
import { clean } from './runs'
import { chip, middleTruncate, statusFor } from './vocab'
import type { Status } from './vocab'

/**
 * The band above the prompt after a `flow test` the model ran. It reads the
 * schema's `flowstate.v1.TestReports` (`flow test -o json` or `-o jsonl`: per
 * file `cases[]` with `name`, `passed`, `failures[]`, `error`; `refused`;
 * `skipped[]`; `coverage[].unreached`) and nothing else. The text report is not
 * a documented format, so a run without JSON earns the exit status alone. All of
 * it is another party's text: cleaned, and bounded where it is spent.
 */

/** Output larger than this is not parsed: the band is a convenience, not a reason to hold a megabyte. */
export const MAX_STDOUT = 1 << 20
const MAX_FILES = 200
/** Cases read before the scan stops; a suite past it is reported as cut, never as passed. */
export const MAX_CASES = 5000
/** Failing cases named on the band. */
export const MAX_FAILING = 3
const MAX_NAME = 60
const MAX_REASON = 100

export interface Failing {
  name: string
  /** The test file, and the line of the first unmet expectation (0 when the CLI gave none). */
  file: string
  line: number
  reason: string
}

export interface Band {
  outcome: 'passed' | 'failed' | 'unknown'
  /** Counts come from the CLI's JSON; false means the exit status alone. */
  detailed: boolean
  passed: number
  failed: number
  skipped: number
  /** Workflow steps no case reached. */
  uncovered: number
  failing: Failing[]
  /** Failing cases beyond the ones named. */
  more: number
  /** The scan stopped at a bound, so the counts are lower bounds. */
  cut: boolean
  /** Why the outcome is what it is, when the counts do not say. */
  note: string
}

const blank = (outcome: Band['outcome'], note: string, detailed = false): Band => ({
  outcome, detailed, passed: 0, failed: 0, skipped: 0, uncovered: 0, failing: [], more: 0, cut: false, note,
})

type Obj = Record<string, unknown>
const obj = (v: unknown): Obj | undefined => (typeof v === 'object' && v !== null && !Array.isArray(v) ? (v as Obj) : undefined)
const list = (v: unknown): unknown[] => (Array.isArray(v) ? v : [])

/** The per-file reports in `-o json` (`{files:[...]}`) or `-o jsonl` (one file per line); undefined for anything else. */
export const filesOf = (stdout: string): unknown[] | undefined => {
  const text = stdout.trim()
  if (text === '' || text.length > MAX_STDOUT || text[0] !== '{') return undefined
  try {
    const doc = obj(JSON.parse(text))
    if (doc !== undefined && Array.isArray(doc.files)) return doc.files
    if (doc !== undefined && Array.isArray(doc.cases)) return [doc]
    return undefined
  } catch {
    // Fall through to one document per line.
  }
  const files: unknown[] = []
  for (const line of text.split('\n')) {
    if (line.trim() === '') continue
    try {
      const doc = obj(JSON.parse(line))
      if (doc === undefined || !Array.isArray(doc.cases)) return undefined
      files.push(doc)
    } catch {
      return undefined
    }
    if (files.length > MAX_FILES) break
  }
  return files.length > 0 ? files : undefined
}

/** What a finished (or not) Bash `flow test` gave: its stdout and whether the tool called it a success. */
export interface TestRun {
  stdout: string
  /** Exit status 0, not interrupted, not backgrounded, not timed out. */
  ok: boolean
  /** The run was interrupted, backgrounded or timed out: there is no verdict. */
  unfinished?: boolean
  /** The tool kept only part of the output. */
  partial?: boolean
}

/**
 * The band for one run. Passed only when the exit status was 0, every case in
 * the JSON passed, at least one ran, and nothing was cut. Output that claims to
 * be JSON and is not, or was cut, is unknown; output that never tried (the text
 * report) is the exit status alone.
 */
export const bandFor = ({ stdout, ok, unfinished, partial }: TestRun): Band => {
  if (unfinished) return blank('unknown', 'the run did not finish')
  const files = partial ? undefined : filesOf(stdout)
  if (files === undefined) {
    if (partial || /^[{[]/.test(stdout.trim())) return blank('unknown', 'the result could not be read')
    return blank(ok ? 'passed' : 'failed', ok ? 'exit 0, no case detail; add -o json' : 'exit status not 0, no case detail; add -o json')
  }
  const band = blank('unknown', '', true)
  let seen = 0
  let refused = 0
  let unreadable = 0
  band.cut = files.length > MAX_FILES
  for (const raw of files.slice(0, MAX_FILES)) {
    const file = obj(raw)
    if (file === undefined) {
      unreadable++
      continue
    }
    const name = clean(file.file, 80)
    const why = clean(file.refused, MAX_REASON)
    if (typeof file.refused === 'string' && file.refused !== '') {
      refused++
      if (band.failing.length < MAX_FAILING) band.failing.push({ name: 'file refused', file: name, line: 0, reason: why || 'refused' })
      else band.more++
    }
    for (const rawCase of list(file.cases)) {
      if (seen++ >= MAX_CASES) {
        band.cut = true
        break
      }
      const c = obj(rawCase)
      if (c?.passed === true) band.passed++
      else if (c?.passed === false) {
        band.failed++
        if (band.failing.length >= MAX_FAILING) {
          band.more++
          continue
        }
        const first = obj(list(c.failures)[0])
        const line = typeof first?.line === 'number' && Number.isFinite(first.line) ? Math.max(0, Math.trunc(first.line)) : 0
        band.failing.push({
          name: clean(c.name, MAX_NAME) || 'unnamed case',
          file: name,
          line,
          reason: clean(first?.message, MAX_REASON) || clean(c.error, MAX_REASON) || 'no reason given',
        })
      } else unreadable++
    }
    band.skipped += list(file.skipped).length
    for (const cov of list(file.coverage)) band.uncovered += list(obj(cov)?.unreached).length
  }
  const failed = band.failed + refused > 0
  if (failed || !ok) {
    band.outcome = 'failed'
    if (!failed) band.note = 'exit status not 0 though no case failed'
  } else if (band.cut || unreadable > 0) {
    band.note = band.cut ? 'the report was cut at a bound' : 'part of the report could not be read'
  } else if (band.passed === 0) {
    band.note = 'no case ran'
  } else band.outcome = 'passed'
  return band
}

/** The band's headline chip: a symbol and a word, never a colour alone. */
export const headOf = (b: Band): Status =>
  b.outcome === 'passed' ? statusFor('succeeded', 'passed') : b.outcome === 'failed' ? statusFor('failed') : statusFor('unknown')

/** The counts and the note after the headline chip, as one line. */
export const summaryOf = (b: Band): string => {
  const n = (v: number): string => `${count(v)}${b.cut && v < 99 ? '+' : ''}`
  const parts = b.detailed
    ? [
        chip(statusFor('failed', `${n(b.failed)} failed`)),
        chip(statusFor('succeeded', `${n(b.passed)} passed`)),
        ...(b.skipped > 0 ? [chip(statusFor('skipped', `${count(b.skipped)} skipped`))] : []),
        ...(b.uncovered > 0 ? [chip(statusFor('skipped', `${count(b.uncovered)} uncovered`))] : []),
      ]
    : []
  return [...parts, ...(b.note === '' ? [] : [b.note])].join(' · ')
}

/** One failing case: its name, where, and why. */
export const failingLine = (f: Failing): string =>
  `${chip(statusFor('failed'))} ${f.name} (${middleTruncate(f.file, 40)}${f.line > 0 ? `:${f.line}` : ''}): ${f.reason}`

/** The band as plain text, the same facts as the drawn form. */
export const bandText = (b: Band): string[] => [
  `test ${chip(headOf(b))}${summaryOf(b) === '' ? '' : ` · ${summaryOf(b)}`}`,
  ...b.failing.map(f => `  ${failingLine(f)}`),
  ...(b.more > 0 ? [`  and ${count(b.more)} more`] : []),
]
