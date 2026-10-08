import type { FileReport } from '../types'

/** Flowfiles are `workflow.yaml`, `*.workflow.yaml` and `*.flow.yaml`; test files are not. */
const FLOWFILE = /(^|\/)([^/]*\.)?(workflow|flow)\.ya?ml$/

export const isFlowfile = (path: string): boolean => FLOWFILE.test(path)

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
