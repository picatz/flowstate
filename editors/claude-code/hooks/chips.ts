import type { RunSummary } from './runs'

/** A quick filter on the Runs list: the label, and the CEL that `flow list --filter` takes. */
export interface Chip {
  label: string
  expr: string
}

/** `status` matches in any case and by alias on the server, so these read as a person would say them. */
export const CHIPS: readonly Chip[] = [
  { label: 'All', expr: '' },
  { label: 'Running', expr: 'status == "RUNNING"' },
  { label: 'Failed', expr: 'status == "FAILED"' },
  { label: 'Completed', expr: 'status == "COMPLETED"' },
]

/** The chip the current filter text is, if it is exactly one; a hand-typed filter lights none. */
export const chipFor = (expr: string): Chip | undefined => CHIPS.find(c => c.expr === expr.trim())

/** One chip's text: the active one is bracketed, so it reads without colour. */
export const chipText = (c: Chip, active: boolean): string => (active ? `[${c.label}]` : c.label)

/**
 * The last good listing for a filter, kept so one failed read does not blank the list the user is looking at.
 * A listing is kept per filter text: another filter's runs are never shown under this one.
 */
export class LastGood {
  private expr = ''
  private runs: RunSummary[] | undefined

  remember(expr: string, runs: RunSummary[]): void {
    this.expr = expr
    this.runs = runs
  }

  /** The kept runs for this filter, or undefined when none were read under it. */
  recall(expr: string): RunSummary[] | undefined {
    return this.runs !== undefined && this.expr === expr ? this.runs : undefined
  }
}
