import { GRAPH_TIMEOUT_MS, MAX_DEPTH, MAX_EDGES, MAX_NODES, MAX_STDOUT, NOT_RUN, graphArgv, graphLines, graphOf, parseGraph, rowText, workflowName } from '../hooks/graph'
import type { Graph } from '../hooks/graph'
import { expect, test } from 'claude-code/testing'

const node = (id: string, kind: string, label: string, detail = '') => ({ id, kind: `GRAPH_NODE_KIND_${kind}`, label, address: label, detail })
const edge = (from: string, to: string, kind = 'CONTAINS') => ({ from, to, kind: `GRAPH_EDGE_KIND_${kind}`, count: 1 })
const doc = (nodes: unknown[], edges: unknown[], extra: Record<string, unknown> = {}) => JSON.stringify({ nodes, edges, partial: false, notes: [], overlays: [], ...extra })

/** examples/approval-gate as `flow graph --workflow approval-gate -o json` prints it (trimmed). */
const APPROVAL = doc(
  [
    node('workflow:approval-gate', 'WORKFLOW', 'approval-gate'),
    node('step:approval-gate/request', 'STEP', 'request', 'task "log"'),
    node('step:approval-gate/approval', 'STEP', 'approval', 'wait_for_signal "deploy-approved"'),
    node('step:approval-gate/decision', 'STEP', 'decision', 'switch'),
    node('step:approval-gate/decision?0/deploy', 'STEP', 'deploy', 'task "log"'),
    node('step:approval-gate/decision?1/rejected', 'STEP', 'rejected', 'task "log"'),
  ],
  [
    edge('workflow:approval-gate', 'step:approval-gate/request'),
    edge('workflow:approval-gate', 'step:approval-gate/approval'),
    edge('workflow:approval-gate', 'step:approval-gate/decision'),
    edge('step:approval-gate/decision', 'step:approval-gate/decision?0/deploy'),
    edge('step:approval-gate/decision', 'step:approval-gate/decision?1/rejected'),
  ],
)

const graphOrFail = (stdout: unknown, cut = false): Graph => {
  const p = parseGraph(stdout, cut)
  if (!('graph' in p)) throw new Error(p.unreadable)
  return p.graph
}
const unreadable = (stdout: unknown, cut = false): string => {
  const p = parseGraph(stdout, cut)
  if (!('unreadable' in p)) throw new Error('expected unreadable')
  return p.unreadable
}

test('a switch shows its arms under it, one row per step in document order, each with the not-run mark', () => {
  expect(graphLines(graphOrFail(APPROVAL), 'workflow.yaml')).toEqual([
    `workflow.yaml: 5 steps as declared; ${NOT_RUN} means not run (no run is shown)`,
    'workflow approval-gate',
    `  ${NOT_RUN} request — task "log"`,
    `  ${NOT_RUN} approval — wait_for_signal "deploy-approved"`,
    `  ${NOT_RUN} decision — switch`,
    `    ${NOT_RUN} deploy — task "log"`,
    `    ${NOT_RUN} rejected — task "log"`,
  ])
})

test('parallel branches and a loop body nest under their parent, and the next sibling returns to the parent level', () => {
  const g = graphOrFail(
    doc(
      [
        node('workflow:w', 'WORKFLOW', 'w'),
        node('step:w/orders', 'STEP', 'orders', 'for_each'),
        node('step:w/orders[0]/charge', 'STEP', 'charge', 'task "log"'),
        node('step:w/checks', 'STEP', 'checks', 'parallel'),
        node('step:w/checks#0/stock', 'STEP', 'stock', 'task "log"'),
        node('step:w/checks#1/fraud', 'STEP', 'fraud', 'task "log"'),
        node('step:w/done', 'STEP', 'done', 'task "log"'),
      ],
      [
        edge('workflow:w', 'step:w/orders'),
        edge('step:w/orders', 'step:w/orders[0]/charge'),
        edge('workflow:w', 'step:w/checks'),
        edge('step:w/checks', 'step:w/checks#0/stock'),
        edge('step:w/checks', 'step:w/checks#1/fraud'),
        edge('workflow:w', 'step:w/done'),
      ],
    ),
  )
  expect(g.rows.map(r => [r.depth, r.label])).toEqual([[0, 'w'], [1, 'orders'], [2, 'charge'], [1, 'checks'], [2, 'stock'], [2, 'fraud'], [1, 'done']])
  expect(g.nested).toBe(true)
})

test('edges that are not containment order nothing: a call, task or wait edge never reorders or nests rows', () => {
  const g = graphOrFail(
    doc(
      [node('workflow:w', 'WORKFLOW', 'w'), node('step:w/b', 'STEP', 'b'), node('step:w/a', 'STEP', 'a'), node('task:log', 'TASK', 'log')],
      [edge('workflow:w', 'step:w/b'), edge('workflow:w', 'step:w/a'), edge('step:w/a', 'step:w/b', 'USES'), edge('step:w/b', 'task:log', 'WAITS')],
    ),
  )
  expect(g.rows.map(r => [r.depth, r.label])).toEqual([[0, 'w'], [1, 'b'], [1, 'a'], [1, 'log']])
  expect(rowText(g.rows[3])).toBe(`  ${NOT_RUN} log — task`)
})

test('no containment edges: the node order is kept and the graph says the nesting is not stated', () => {
  const g = graphOrFail(doc([node('workflow:w', 'WORKFLOW', 'w'), node('step:w/x', 'STEP', 'x'), node('step:w/y', 'STEP', 'y')], []))
  expect(g.rows.map(r => r.label)).toEqual(['w', 'x', 'y'])
  expect(g.nested).toBe(false)
  expect(g.notes.join(' ')).toMatch(/states no nesting/)
})

test('a containment cycle is listed unnested, once, and marks the graph partial; it cannot hang the walk', () => {
  const g = graphOrFail(
    doc(
      [node('workflow:w', 'WORKFLOW', 'w'), node('step:w/a', 'STEP', 'a'), node('step:w/b', 'STEP', 'b'), node('step:w/c', 'STEP', 'c')],
      [edge('workflow:w', 'step:w/c'), edge('step:w/a', 'step:w/b'), edge('step:w/b', 'step:w/a'), edge('step:w/b', 'step:w/b')],
    ),
  )
  expect(g.rows.map(r => r.label).toSorted()).toEqual(['a', 'b', 'c', 'w'])
  expect(g.rows.filter(r => r.label === 'a')).toHaveLength(1)
  expect(g.partial).toBe(true)
  expect(g.notes.join(' ')).toMatch(/cycle/)
})

test('a node with two parents is shown once, under the first; an edge to a missing node is ignored', () => {
  const g = graphOrFail(
    doc(
      [node('workflow:w', 'WORKFLOW', 'w'), node('step:w/a', 'STEP', 'a'), node('step:w/b', 'STEP', 'b')],
      [edge('workflow:w', 'step:w/a'), edge('workflow:w', 'step:w/b'), edge('step:w/a', 'step:w/b'), edge('step:w/a', 'step:w/ghost'), edge('ghost', 'step:w/a')],
    ),
  )
  expect(g.rows.map(r => [r.depth, r.label])).toEqual([[0, 'w'], [1, 'a'], [1, 'b']])
})

test('depth is capped so a deeply nested document stays bounded', () => {
  const n = 30
  const nodes = Array.from({ length: n }, (_, i) => node(`step:w/s${i}`, 'STEP', `s${i}`))
  const edges = Array.from({ length: n - 1 }, (_, i) => edge(`step:w/s${i}`, `step:w/s${i + 1}`))
  const g = graphOrFail(doc(nodes, edges))
  expect(g.rows).toHaveLength(n)
  expect(Math.max(...g.rows.map(r => r.depth))).toBe(MAX_DEPTH)
})

test('hostile labels and details are cleaned and cut: no escape, bidi, control or zero-width character survives', () => {
  const hostile = '\u001b[31mred\u001b]0;pwn\u0007' + String.fromCharCode(0x202e) + 'evil' + String.fromCharCode(0x200b, 0x85, 0x2028) + 'x' + 'A'.repeat(500)
  const g = graphOrFail(doc([node('workflow:w', 'WORKFLOW', hostile, hostile), node('step:w/x', 'STEP', hostile, hostile)], [edge('workflow:w', 'step:w/x')], { notes: [hostile], partial: true }))
  const lines = graphLines(g, `\u001bfile${hostile}`)
  const bad = [...lines.join('\n')].filter(c => {
    const n = c.codePointAt(0)!
    return (n < 32 && n !== 10) || (n >= 0x7f && n <= 0x9f) || (n >= 0x202a && n <= 0x202e) || n === 0x200b || n === 0x2028
  })
  expect(bad).toEqual([])
  expect(g.rows[1].label.length).toBeLessThanOrEqual(60)
  expect(g.rows[1].detail.length).toBeLessThanOrEqual(80)
  expect(g.notes[0].length).toBeLessThanOrEqual(160)
  for (const l of lines) expect(l.length).toBeLessThan(300)
})

test('a node with no label falls back to its id, and an unknown kind is a plain node', () => {
  const g = graphOrFail(doc([{ id: 'step:w/x', kind: 'GRAPH_NODE_KIND_NEW', label: '' }, { id: 'step:w/y', kind: 7, label: 'y' }], []))
  expect(g.rows.map(r => [r.kind, r.label])).toEqual([['node', 'step:w/x'], ['node', 'y']])
})

test('a huge graph is cut at the node and edge bounds, says so, and never shows more than the caps', () => {
  const nodes = [node('workflow:w', 'WORKFLOW', 'w'), ...Array.from({ length: 1000 }, (_, i) => node(`step:w/s${i}`, 'STEP', `s${i}`))]
  const edges = nodes.slice(1).map(n => edge('workflow:w', n.id))
  const g = graphOrFail(doc(nodes, edges))
  expect(g.rows).toHaveLength(MAX_NODES)
  expect(g.partial).toBe(true)
  expect(g.notes.join(' ')).toContain(`first ${MAX_NODES} of 1001 nodes`)
  expect(g.notes.join(' ')).toContain(`first ${MAX_EDGES} of 1000 edges`)
  expect(g.rows.slice(1, 4).map(r => r.label)).toEqual(['s0', 's1', 's2'])
})

test('output over the stdout cap, or cut by the tool, is unreadable and is not parsed', () => {
  expect(unreadable('{"nodes":[],"x":"' + 'a'.repeat(MAX_STDOUT) + '"}')).toMatch(/larger than the view reads/)
  expect(unreadable(APPROVAL, true)).toMatch(/larger than the view reads/)
})

test('malformed or foreign output is unreadable with a reason, and nothing throws', () => {
  for (const bad of ['', '   ', 'not json', '{', '[]', 'null', '42', '"nodes"', '{}', '{"nodes":"x"}', '{"nodes":[],"edges":{}}', undefined, null, 5, {}]) {
    expect(() => parseGraph(bad)).not.toThrow()
    expect('unreadable' in parseGraph(bad)).toBe(true)
  }
  // Bad entries among good ones are skipped and counted, not fatal.
  const g = graphOrFail(JSON.stringify({ nodes: [null, 3, { id: 4 }, { id: '' }, { id: 'step:w/a', label: 'a' }, { id: 'step:w/a', label: 'dup' }], edges: [null, 'x', { from: 1 }] }))
  expect(g.rows.map(r => r.label)).toEqual(['a'])
  expect(g.partial).toBe(true)
  expect(g.notes.join(' ')).toContain('5 nodes could not be read')
})

test('a partial graph is labelled partial in the headline and keeps the CLI notes', () => {
  const g = graphOrFail(doc([node('workflow:w', 'WORKFLOW', 'w')], [], { partial: true, notes: ['other.yaml does not compile and was left out'] }))
  const lines = graphLines(g, 'w.yaml')
  expect(lines[0]).toMatch(/; partial$/)
  expect(lines).toContain('note: other.yaml does not compile and was left out')
  const noReason = graphOrFail(doc([node('workflow:w', 'WORKFLOW', 'w')], [], { partial: true }))
  expect(noReason.notes).toEqual(['the graph says it is partial and gives no reason'])
})

test('an empty graph is shown as no nodes, not as a failure', () => {
  expect(graphLines(graphOrFail(doc([], [])), 'f.yaml')).toEqual([`f.yaml: 0 steps as declared; ${NOT_RUN} means not run (no run is shown)`, '(no nodes)'])
})

test('no row claims a status: only the not-run mark and the workflow word lead a row', () => {
  const lines = graphLines(graphOrFail(APPROVAL), 'f').slice(1)
  for (const l of lines) expect(l.trimStart().startsWith(`${NOT_RUN} `) || l.startsWith('workflow ')).toBe(true)
  expect(lines.join('')).not.toMatch(/[✓✗●◔⊘↺]/)
})

test('workflowName reads the top-level name only, plain or quoted, and refuses anything --workflow could mistake', () => {
  expect(workflowName('name: approval-gate\nsteps: []\n')).toBe('approval-gate')
  expect(workflowName('# c\nname: "a_b-1"  # note\n')).toBe('a_b-1')
  expect(workflowName("name: 'x'\n")).toBe('x')
  expect(workflowName('steps:\n  - name: nested\n')).toBeUndefined()
  expect(workflowName('name: -rf\n')).toBeUndefined()
  expect(workflowName('name: --live\n')).toBeUndefined()
  expect(workflowName('name: has space\n')).toBeUndefined()
  expect(workflowName('name: a;b\n')).toBeUndefined()
  expect(workflowName('name: ' + 'a'.repeat(129) + '\n')).toBeUndefined()
  expect(workflowName('')).toBeUndefined()
})

test('graphArgv is one argv with the name checked and the file after --, never --live', () => {
  expect(graphArgv('flow', 'workflow.yaml', 'approval-gate')).toEqual(['flow', 'graph', '-o', 'json', '--workflow', 'approval-gate', '--', 'workflow.yaml'])
  expect(graphArgv('flow', 'workflows/deep.yaml', 'x')).toEqual(['flow', 'graph', '-o', 'json', '--workflow', 'x', '--', 'workflows/deep.yaml'])
  // Flag-like or off-list files and names are refused, not rewritten.
  for (const file of ['-o', '--live', '-x.flow.yaml', '.hidden.flow.yaml', '../a.flow.yaml', '/etc/passwd', 'a b.flow.yaml', 'notes.md', 'x.test.yaml', '']) expect(graphArgv('flow', file, 'x')).toBeUndefined()
  for (const name of ['-x', '--live', '--address=evil:1', 'a b', 'a;b', '', '$(x)']) expect(graphArgv('flow', 'workflow.yaml', name)).toBeUndefined()
  expect(graphArgv('flow', 'workflow.yaml', 'x')).not.toContain('--live')
})

test('graphOf: only exit 0 with a whole document is a graph; refusals give one line; a throw or timeout is unreadable', () => {
  const ran = (o: Partial<{ exitCode: number; stdout: string; stderr: string; isStdoutTruncated: boolean }>) => ({ exitCode: 0, stdout: APPROVAL, stderr: '', ...o })
  expect('graph' in graphOf(ran({}))).toBe(true)
  expect(graphOf(ran({ exitCode: 1, stdout: '', stderr: 'ERROR\nno workflow named "nope"; the files declare: debugging\n' }))).toEqual({ unreadable: 'no workflow named "nope"; the files declare: debugging' })
  expect(graphOf(ran({ exitCode: 2, stderr: '' }))).toEqual({ unreadable: 'flow graph exited 2' })
  expect(graphOf(ran({ exitCode: 1, stderr: '\u001b[31mbad\u0007\n' }))).toEqual({ unreadable: '[31mbad' })
  expect(graphOf(ran({ isStdoutTruncated: true }))).toEqual({ unreadable: 'the graph is larger than the view reads' })
  expect(graphOf(undefined, new Error('timed out after 10000ms'))).toEqual({ unreadable: 'Error: timed out after 10000ms' })
  expect(GRAPH_TIMEOUT_MS).toBe(10000)
})
