import { parseTimeline } from '../hooks/detail'
import type { Execution } from '../hooks/detail'
import { MAX_NAME, NOT_RUN, parseGraph } from '../hooks/graph'
import type { Graph } from '../hooks/graph'
import { aggregate, liveLines, overlayNotes, overlayStatus, sameWorkflow } from '../hooks/graphstatus'
import { statusFor } from '../hooks/vocab'
import { expect, test } from 'claude-code/testing'

const node = (id: string, kind: string, label: string, address: string, detail = '') => ({ id, kind: `GRAPH_NODE_KIND_${kind}`, label, address, detail })
const edge = (from: string, to: string) => ({ from, to, kind: 'GRAPH_EDGE_KIND_CONTAINS', count: 1 })
const doc = (nodes: unknown[], edges: unknown[], extra: Record<string, unknown> = {}) => JSON.stringify({ nodes, edges, partial: false, notes: [], overlays: [], ...extra })
const graphOf = (stdout: string): Graph => {
  const p = parseGraph(stdout)
  if (!('graph' in p)) throw new Error(p.unreadable)
  return p.graph
}

/** examples/fan-out-and-parallel as `flow graph --workflow fan-out-and-parallel -o json` printed it, recorded against a dev server. */
const FAN = graphOf(
  doc(
    [
      node('workflow:fan', 'WORKFLOW', 'fan-out-and-parallel', ''),
      node('step:fan/process', 'STEP', 'process', 'process'),
      node('step:fan/process[0]/label', 'STEP', 'label', 'process[0]/label'),
      node('step:fan/checks', 'STEP', 'checks', 'checks'),
      node('step:fan/checks#0/check_config', 'STEP', 'check_config', 'checks#0/check_config'),
      node('step:fan/checks#1/check_quota', 'STEP', 'check_quota', 'checks#1/check_quota'),
      node('step:fan/summary', 'STEP', 'summary', 'summary'),
    ],
    [
      edge('workflow:fan', 'step:fan/process'),
      edge('step:fan/process', 'step:fan/process[0]/label'),
      edge('workflow:fan', 'step:fan/checks'),
      edge('step:fan/checks', 'step:fan/checks#0/check_config'),
      edge('step:fan/checks', 'step:fan/checks#1/check_quota'),
      edge('workflow:fan', 'step:fan/summary'),
    ],
  ),
)

/** The timeline of that graph's run, as `flow timeline -o json` printed it (kinds and steps as recorded). */
const entry = (kind: string, step: string, attempt = 1, failure = '') => ({ eventId: '1', time: '2026-10-10T03:34:30Z', kind: `KIND_${kind}`, step, attempt, failure })
const stepsOf = (...entries: ReturnType<typeof entry>[]): Execution[] => {
  const p = parseTimeline(JSON.stringify({ entries }))
  if (!('detail' in p)) throw new Error(p.error)
  return p.detail.executions
}
const FAN_RUN = stepsOf(
  entry('STEP_SCHEDULED', 'task capability admission'),
  entry('STEP_COMPLETED', 'task capability admission'),
  entry('STEP_SCHEDULED', 'run vars'),
  entry('STEP_COMPLETED', 'run vars'),
  entry('STEP_SCHEDULED', '`process` > `label`'),
  entry('STEP_COMPLETED', '`process` > `label`'),
  entry('STEP_SCHEDULED', '`checks` > `check_config`'),
  entry('STEP_COMPLETED', '`checks` > `check_config`'),
  entry('STEP_SCHEDULED', '`summary`'),
  entry('STEP_COMPLETED', '`summary`'),
)

test('a top-level step takes its status from the timeline step named by exactly its id; every nested step keeps the not-run mark', () => {
  const o = overlayStatus(FAN, FAN_RUN)
  expect(liveLines(FAN, o, 'fan.yaml', 'wf-1', false)).toEqual([
    'fan.yaml: 6 steps as declared; status from run wf-1: 1 of 6 have one; ○ means no status from that run, not that the step did not run; the file may have changed since that run',
    'workflow fan-out-and-parallel',
    `  ${NOT_RUN} process`,
    `    ${NOT_RUN} label`,
    `  ${NOT_RUN} checks`,
    `    ${NOT_RUN} check_config`,
    `    ${NOT_RUN} check_quota`,
    '  ✓ summary · succeeded',
    "note: 4 timeline steps not in this graph's top-level steps (nested steps, timers, compensations and engine steps are not mapped)",
    `note: 3 nested steps keep ${NOT_RUN}: the timeline does not say which iteration, branch or arm a row belongs to`,
  ])
  expect(o).toMatchObject({ unmatched: 4, nested: 3, ambiguous: 0, silent: 2 })
})

test('a nested timeline label is never matched to a graph address, even when the ids agree', () => {
  // "`process` > `label`" would fit process[0]/label; the label has no iteration, so the pairing is a guess and is refused.
  const o = overlayStatus(FAN, stepsOf(entry('STEP_COMPLETED', '`process` > `label`')))
  expect(o.marks.size).toBe(0)
  expect(o.unmatched).toBe(1)
})

test('a graph step with no timeline step keeps the mark and is not called failed or skipped', () => {
  const o = overlayStatus(FAN, stepsOf(entry('STEP_COMPLETED', '`summary`')))
  const lines = liveLines(FAN, o, 'f', 'r', false)
  expect(lines.some(l => l.includes(`${NOT_RUN} process`))).toBe(true)
  expect(lines.join('\n')).not.toMatch(/skipped|failed/)
})

test('matching is exact: a prefix, a suffix, another case and an unquoted id match nothing', () => {
  const o = overlayStatus(
    FAN,
    stepsOf(entry('STEP_COMPLETED', '`summary` · sleep'), entry('STEP_COMPLETED', '`Summary`'), entry('STEP_COMPLETED', 'summary'), entry('STEP_COMPLETED', '`summar`'), entry('STEP_COMPLETED', '… > `summary`')),
  )
  expect(o.marks.size).toBe(0)
  expect(o.unmatched).toBe(5)
})

test('a failed step is marked with the symbol and the word, with the attempt count', () => {
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('s', 'STEP', 'build', 'build')], [edge('w', 's')]))
  const o = overlayStatus(g, stepsOf(entry('STEP_SCHEDULED', '`build`'), entry('STEP_FAILED', '`build`', 3, 'tests failed')))
  expect(liveLines(g, o, 'f', 'r', false)[2]).toBe('  ✗ build · failed, attempt 3')
})

test('a step still running or waiting is shown as such', () => {
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('a', 'STEP', 'a', 'a'), node('b', 'STEP', 'b', 'b')], [edge('w', 'a'), edge('w', 'b')]))
  const o = overlayStatus(g, stepsOf(entry('STEP_SCHEDULED', '`a`'), entry('TIMER_STARTED', '`b`')))
  expect(liveLines(g, o, 'f', 'r', false).slice(2, 4)).toEqual(['  ● a · running', '  ◔ b · waiting'])
})

test('several timeline steps for one address aggregate conservatively and the count is said', () => {
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('s', 'STEP', 's', 's')], [edge('w', 's')]))
  const mk = (...kinds: Parameters<typeof statusFor>[0][]): Execution[] => kinds.map(k => ({ label: '`s`', status: statusFor(k), attempt: 1 }))
  const line = (steps: Step[]) => liveLines(g, overlayStatus(g, steps), 'f', 'r', false)[2]
  expect(line(mk('succeeded', 'failed', 'succeeded'))).toBe('  ✗ s · failed, 3 executions')
  expect(line(mk('succeeded', 'running'))).toBe('  ● s · running, 2 executions')
  expect(line(mk('succeeded', 'waiting'))).toBe('  ◔ s · waiting, 2 executions')
  expect(line(mk('succeeded', 'succeeded'))).toBe('  ✓ s · succeeded, 2 executions')
  // A mix that is neither failed, live nor all done is not picked.
  expect(line(mk('succeeded', 'cancelled'))).toBe('  ? s · mixed, 2 executions')
  expect(aggregate([statusFor('failed', 'timed out'), statusFor('failed')]).word).toBe('failed')
  expect(aggregate([statusFor('failed', 'timed out'), statusFor('failed', 'timed out')]).word).toBe('timed out')
})

test('two graph rows with one address get no status, and the timeline step is reported as unplaced', () => {
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('a', 'STEP', 'x', 'x'), node('b', 'STEP', 'x', 'x')], [edge('w', 'a'), edge('w', 'b')]))
  const o = overlayStatus(g, stepsOf(entry('STEP_COMPLETED', '`x`')))
  expect(o.marks.size).toBe(0)
  expect(o).toMatchObject({ ambiguous: 2, unmatched: 1 })
  expect(overlayNotes(o, false)).toContain('2 graph steps share an address and get no status')
})

test('hostile strings: a timeline label or address with escapes, bidi or a prototype key marks nothing and prints clean', () => {
  const g = graphOf(
    doc(
      [node('w', 'WORKFLOW', 'w', ''), node('a', 'STEP', 'a\u001b[31m', 'a\u001b[31m'), node('b', 'STEP', '__proto__', '__proto__'), node('c', 'STEP', 'ok‮', 'ok‮')],
      [edge('w', 'a'), edge('w', 'b'), edge('w', 'c')],
    ),
  )
  const o = overlayStatus(g, stepsOf(entry('STEP_COMPLETED', '`a\u001b[31m`'), entry('STEP_COMPLETED', '`constructor`'), entry('STEP_COMPLETED', '`ok`')))
  const text = liveLines(g, o, 'f\u001b', 'r', false).join('\n')
  expect(text).not.toMatch(/[\u001b‮]/)
  // Cleaning is applied to both sides before they meet, so "ok" matches "ok"; an address that is not a plain id ("a[31m") is never a top-level key, and "__proto__" finds no step.
  expect(o.marks.size).toBe(1)
  expect(text).toMatch(/✓ ok · succeeded/)
})

test('a run of a different workflow is not overlaid: the names must be equal', () => {
  expect(sameWorkflow(FAN, 'fan-out-and-parallel')).toBe(true)
  expect(sameWorkflow(FAN, 'other')).toBe(false)
  expect(sameWorkflow(FAN, '')).toBe(false)
  expect(sameWorkflow(FAN, undefined)).toBe(false)
  expect(sameWorkflow({ ...FAN, workflow: '' }, '')).toBe(false)
})

test('a partial graph is still labelled partial in the headline, and rows it lacks are simply absent', () => {
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('s', 'STEP', 's', 's')], [edge('w', 's')], { partial: true, notes: ['a module did not compile'] }))
  const lines = liveLines(g, overlayStatus(g, stepsOf(entry('STEP_COMPLETED', '`s`'), entry('STEP_COMPLETED', '`gone`'))), 'f', 'r', false)
  expect(lines[0]).toMatch(/; partial$/)
  expect(lines).toContain('note: a module did not compile')
  expect(lines).toContain("note: 1 timeline step not in this graph's top-level steps (nested steps, timers, compensations and engine steps are not mapped)")
})

test('a clipped timeline says that an unmarked step may not have been read', () => {
  const o = overlayStatus(FAN, FAN_RUN)
  expect(overlayNotes(o, true).at(-1)).toBe('the server clipped the timeline, so a step without a status may not have been read')
  expect(overlayNotes(o, false).join('\n')).not.toMatch(/clipped/)
})

test('no timeline steps: every row keeps the mark and the headline counts none', () => {
  const o = overlayStatus(FAN, [])
  expect(o.marks.size).toBe(0)
  expect(liveLines(FAN, o, 'f', 'r', false)[0]).toMatch(/0 of 6 have one/)
})

test('a retried step reaches the overlay as separate executions: failed then succeeded is failed, counted, with the attempt', () => {
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('s', 'STEP', 'build', 'build')], [edge('w', 's')]))
  const runs = stepsOf(entry('STEP_SCHEDULED', '`build`', 1), entry('STEP_FAILED', '`build`', 1, 'boom'), entry('STEP_SCHEDULED', '`build`', 2), entry('STEP_COMPLETED', '`build`', 2))
  expect(runs).toHaveLength(2)
  expect(liveLines(g, overlayStatus(g, runs), 'f', 'r', false)[2]).toBe('  ✗ build · failed, attempt 2, 2 executions')
  // Still retrying: the running attempt does not hide the earlier failure.
  const live = stepsOf(entry('STEP_SCHEDULED', '`build`', 1), entry('STEP_FAILED', '`build`', 1, 'boom'), entry('STEP_SCHEDULED', '`build`', 2))
  expect(liveLines(g, overlayStatus(g, live), 'f', 'r', false)[2]).toBe('  ✗ build · failed, attempt 2, 2 executions')
  // A card is unchanged: one folded step, latest status.
  const card = parseTimeline(JSON.stringify({ entries: [entry('STEP_SCHEDULED', '`build`', 1), entry('STEP_FAILED', '`build`', 1), entry('STEP_COMPLETED', '`build`', 2)] }))
  expect('detail' in card && card.detail.steps.length).toBe(1)
})

test('a top-level id of 70 characters matches through parseTimeline, while the card still cuts its name to 60', () => {
  const id = 'a'.repeat(70)
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('s', 'STEP', 'long', id)], [edge('w', 's')]))
  const raw = JSON.stringify({ entries: [entry('STEP_SCHEDULED', `\`${id}\``), entry('STEP_COMPLETED', `\`${id}\``)] })
  const p = parseTimeline(raw)
  if (!('detail' in p)) throw new Error(p.error)
  expect(p.detail.steps[0].name).toHaveLength(60)
  expect(liveLines(g, overlayStatus(g, p.detail.executions), 'f', 'r', false)[2]).toBe('  ✓ long · succeeded')
  // One character off matches nothing.
  const other = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('s', 'STEP', 'long', `${id}b`)], [edge('w', 's')]))
  expect(overlayStatus(other, p.detail.executions).marks.size).toBe(0)
})

test('two 128-character names sharing a 60-character prefix are different workflows', () => {
  const a = `${'x'.repeat(60)}${'a'.repeat(68)}`
  const b = `${'x'.repeat(60)}${'b'.repeat(68)}`
  expect(a).toHaveLength(MAX_NAME)
  const g = graphOf(doc([node('w', 'WORKFLOW', a, '')], []))
  expect(g.workflow).toHaveLength(60)
  expect(sameWorkflow(g, a)).toBe(true)
  expect(sameWorkflow(g, b)).toBe(false)
  expect(sameWorkflow(g, 'x'.repeat(60))).toBe(false)
})

test('a name cut on either side matches nothing: past 128 characters fails closed', () => {
  const long = 'n'.repeat(129)
  expect(sameWorkflow(graphOf(doc([node('w', 'WORKFLOW', long, '')], [])), long)).toBe(false)
  expect(sameWorkflow(graphOf(doc([node('w', 'WORKFLOW', 'n'.repeat(128), '')], [])), long)).toBe(false)
})

for (const n of [58, 59, 60, 61, 128]) test(`a container with a ${n}-character id never takes its child's status; only its own bare label does`, () => {
  const id = 'c'.repeat(n)
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('s', 'STEP', 'box', id)], [edge('w', 's')]))
  const child = stepsOf(entry('STEP_SCHEDULED', `\`${id}\` > \`work\``), entry('STEP_COMPLETED', `\`${id}\` > \`work\``))
  expect(overlayStatus(g, child).marks.size).toBe(0)
  const own = stepsOf(entry('STEP_SCHEDULED', `\`${id}\``), entry('STEP_FAILED', `\`${id}\``))
  expect(liveLines(g, overlayStatus(g, own), 'f', 'r', false)[2]).toBe('  ✗ box · failed')
})

test('a label at the read bound is never matched and is said', () => {
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('s', 'STEP', 's', 's')], [edge('w', 's')]))
  const o = overlayStatus(g, stepsOf(entry('STEP_COMPLETED', `\`s\` > ${'x'.repeat(200)}`)))
  expect(o.marks.size).toBe(0)
  expect(o.cut).toBe(1)
  expect(overlayNotes(o, false).join('\n')).toMatch(/1 timeline step had a label too long to read whole and is not matched/)
})

test('a saga: a step that succeeded and was then undone reads compensated, not a plain success', () => {
  const g = graphOf(doc([node('w', 'WORKFLOW', 'w', ''), node('a', 'STEP', 'build', 'build'), node('b', 'STEP', 'ship', 'ship')], [edge('w', 'a'), edge('w', 'b')]))
  const run = stepsOf(
    entry('STEP_SCHEDULED', '`build`'),
    entry('STEP_COMPLETED', '`build`'),
    entry('STEP_SCHEDULED', '`ship`'),
    entry('STEP_FAILED', '`ship`', 1, 'boom'),
    entry('STEP_SCHEDULED', '`build` · undo'),
    entry('STEP_COMPLETED', '`build` · undo'),
  )
  const o = overlayStatus(g, run)
  expect(liveLines(g, o, 'f', 'r', false).slice(2, 4)).toEqual(['  ↺ build · compensated', '  ✗ ship · failed'])
  expect(o.unmatched).toBe(0)
  // An undo still running is appended, and the step is not called compensated.
  const part = overlayStatus(g, stepsOf(entry('STEP_COMPLETED', '`build`'), entry('STEP_SCHEDULED', '`build` · undo')))
  expect(liveLines(g, part, 'f', 'r', false)[2]).toBe('  ✓ build · succeeded, undo running')
})
