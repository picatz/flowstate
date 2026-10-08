import type { FileReport } from '../types'

/** `flow test`'s suites and fixtures are named for a loader of their own, never validated as workflows. */
const TEST_FILE = /(\.test\.ya?ml|(^|\/)testdefaults\.ya?ml)$/

/**
 * The names docs/EDITORS.md ("Which files are Flowfiles") gives: `Flowfile`,
 * `Flowfile.yaml`, `workflow.yaml`, `*.flow.yaml`, and anything under a
 * `workflows/` directory.
 */
const FLOWFILE = /(^|\/)(Flowfile(\.ya?ml)?|workflow\.ya?ml|[^/]*\.flow\.ya?ml)$|(^|\/)workflows\/.*\.ya?ml$/

export const isFlowfile = (path: string): boolean => {
  const unix = path.replaceAll('\\', '/')
  return FLOWFILE.test(unix) && !TEST_FILE.test(unix)
}

/**
 * Reads `flow validate -o jsonl` output: one JSON object per file. A line that
 * is not that object is skipped, since the command prints its summary after.
 */
export const parseReports = (stdout: string): FileReport[] => {
  const reports: FileReport[] = []
  for (const line of stdout.split('\n')) {
    if (!line.startsWith('{')) continue
    try {
      const one = JSON.parse(line)
      if (typeof one.file !== 'string' || !Array.isArray(one.diagnostics)) continue
      reports.push({
        file: one.file,
        diagnostics: one.diagnostics.map((d: any) => ({
          line: Number(d.line) || 0,
          column: Number(d.column) || 0,
          message: String(d.message),
        })),
      })
    } catch {
      continue
    }
  }
  return reports
}

/** What the model is told after an edit leaves a Flowfile with problems. */
export const summarize = (r: FileReport): string => {
  if (r.failure) return `flow validate could not run on ${r.file}: ${r.failure}`
  if (r.diagnostics.length === 0) return `${r.file}: valid`
  const lines = r.diagnostics
    .slice(0, 10)
    .map(d => `  ${d.line > 0 ? `line ${d.line}: ` : ''}${d.message}`)
  const more = r.diagnostics.length - lines.length
  return [
    `flow validate found ${r.diagnostics.length} problem(s) in ${r.file}:`,
    ...lines,
    ...(more > 0 ? [`  and ${more} more`] : []),
  ].join('\n')
}
