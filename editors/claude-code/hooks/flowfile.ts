import type { FileReport } from '../types'
import { clean } from './runs'
import type { Diagnostic, DiagnosticReport } from '../types/flowstate'

/** `flow test`'s suites and fixtures are named for a loader of their own, never validated as workflows. */
const TEST_FILE = /(\.test\.ya?ml|(^|\/)testdefaults\.ya?ml)$/

/**
 * The names docs/EDITORS.md ("Which files are Flowfiles") gives: `Flowfile`,
 * `Flowfile.yaml`, `workflow.yaml`, `*.flow.yaml`, and anything under a
 * `workflows/` directory.
 */
const FLOWFILE = /(^|\/)(Flowfile(\.ya?ml)?|workflow\.ya?ml|[^/]*\.flow\.ya?ml)$|(^|\/)workflows\/.*\.ya?ml$/

/** A `flow test` suite or its shared defaults: an edit to one makes an earlier test result stale. */
export const isTestFile = (path: string): boolean => TEST_FILE.test(path.replaceAll('\\', '/'))

export const isFlowfile = (path: string): boolean => {
  const unix = path.replaceAll('\\', '/')
  return FLOWFILE.test(unix) && !TEST_FILE.test(unix)
}

/** What a field the CLI omitted reads as: the schema's zero values, except `code`, which reads as the "general" class. */
const EMPTY_DIAGNOSTIC: Diagnostic = {
  line: 0,
  column: 0,
  message: '',
  step: '',
  field: '',
  kind: '',
  value: '',
  code: 'general',
  edits: [],
  path: [],
  related: [],
}

/**
 * Reads `flow validate -o jsonl` output: one JSON object per file. A line that
 * is not that object is skipped, since the command prints its summary after.
 */
export const parseReports = (stdout: string): DiagnosticReport[] => {
  const reports: DiagnosticReport[] = []
  for (const line of stdout.split('\n')) {
    if (!line.startsWith('{')) continue
    try {
      const one = JSON.parse(line)
      if (typeof one.file !== 'string' || !Array.isArray(one.diagnostics)) continue
      reports.push({
        file: one.file,
        diagnostics: one.diagnostics.map((d: Partial<Diagnostic>) => ({ ...EMPTY_DIAGNOSTIC, ...d })),
      })
    } catch {
      continue
    }
  }
  return reports
}

/**
 * The problems `flow validate` found in files the report for `file` called,
 * which it prints as their own reports after the caller's: a callee's defect is
 * reported under the callee's file, not at the caller's `call:`.
 */
export const calledProblems = (reports: readonly DiagnosticReport[], file: string): { file: string; problems: number }[] =>
  reports.flatMap(r => (r.file !== file && r.diagnostics.length > 0 ? [{ file: r.file, problems: r.diagnostics.length }] : []))

/** The part of a report the mod stores and shows; `called` is every other report in the same output. */
export const toFileReport = (r: DiagnosticReport, called: readonly DiagnosticReport[] = []): FileReport => {
  const others = calledProblems(called, r.file)
  return {
    file: r.file,
    diagnostics: r.diagnostics.map(({ line, column, message }) => ({ line, column, message })),
    ...(others.length > 0 ? { called: others } : {}),
  }
}

/**
 * One line naming the called files that have problems of their own, or none.
 * A path comes from the workspace, so it is cleaned and the list is capped.
 */
export const calledLine = (called: readonly { file: string; problems: number }[] | undefined): string[] => {
  if (!called || called.length === 0) return []
  const shown = called.slice(0, 10).map(c => `${clean(c.file, 200)} (${c.problems})`)
  const more = called.length - shown.length
  return [`  called files with problems of their own: ${shown.join(', ')}${more > 0 ? `, and ${more} more` : ''}; run flow validate on them`]
}

/** What the model is told after an edit leaves a Flowfile with problems. */
export const summarize = (r: FileReport): string => {
  if (r.failure) return `flow validate could not run on ${r.file}: ${r.failure}`
  if (r.diagnostics.length === 0 && !r.called?.length) return `${r.file}: valid`
  const lines = r.diagnostics
    .slice(0, 10)
    .map(d => `  ${d.line > 0 ? `line ${d.line}: ` : ''}${d.message}`)
  const more = r.diagnostics.length - lines.length
  return [
    `flow validate found ${r.diagnostics.length} problem(s) in ${r.file}:`,
    ...lines,
    ...(more > 0 ? [`  and ${more} more`] : []),
    ...calledLine(r.called),
  ].join('\n')
}
