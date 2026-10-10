import { FRESH_MS, NO_SEEN, seenFrom, statusText } from '../hooks/statusline'
import type { Seen } from '../hooks/statusline'
import type { RunSummary } from '../types/flowstate'
import { expect, test } from 'claude-code/testing'

const NOW = Date.UTC(2026, 9, 9, 12, 0, 0)
const seen = (over: Partial<Seen> = {}): Seen => ({ at: NOW - 1000, address: 'flow.example:9233', failed: 2, ...over })
const clean = { file: 'a.flow.yaml', diagnostics: [] }

test('idle prints nothing: no warning, no command, no product name', () => {
  expect(statusText({ now: NOW })).toBe('')
  expect(statusText({ now: NOW, run: { file: 'a', kind: '' }, seen: NO_SEEN })).toBe('')
  expect(statusText({ now: NOW, report: clean, run: { file: 'a', kind: 'ok' }, seen: seen({ failed: 0 }) })).toBe('')
  expect(statusText({ now: NOW, run: { file: 'a', kind: 'notrun' } })).toBe('')
  expect(statusText({ now: NOW, run: { file: 'a', kind: 'unknown' } })).toBe('')
})

test('a validate with errors shows the warning and the file; a passing or unrun one is quiet', () => {
  const bad = { file: 'a.flow.yaml', diagnostics: [{ line: 1, column: 1, message: 'x' }] }
  expect(statusText({ now: NOW, report: bad })).toBe('⚠ validate 1 error a.flow.yaml')
  expect(statusText({ now: NOW, report: { ...bad, diagnostics: [...bad.diagnostics, ...bad.diagnostics] } })).toBe('⚠ validate 2 errors a.flow.yaml')
  expect(statusText({ now: NOW, report: { file: 'a', diagnostics: [], failure: 'no flow' } })).toBe('')
})

test('file names are cleaned and bounded, never trusted', () => {
  const bad = [{ line: 1, column: 1, message: 'x' }]
  const text = statusText({ now: NOW, report: { file: `x\u001b[2J\u202e\u200bev\`il${'n'.repeat(300)}.flow.yaml`, diagnostics: bad } })
  expect(text).toContain('⚠ validate 1 error')
  expect(text).not.toMatch(/[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e`]/)
  expect(text.length).toBeLessThan(80)
})

test('the line carries no name of its own, suggests no command, and shows base names only', () => {
  const bad = [{ line: 1, column: 1, message: 'x' }]
  const text = statusText({ now: NOW, report: { file: '/private/tmp/claude-1/deep/workflow.yaml', diagnostics: bad }, run: { file: 'C:\\w\\d.flow.yaml', kind: 'failed' }, owes: 'flow test', seen: seen() })
  expect(text).toMatch(/^⚠ validate 1 error workflow\.yaml · run failed d\.flow\.yaml · owes flow test · server /)
  expect(text.match(/⚠/g)).toHaveLength(1)
  expect(text).not.toMatch(/flowstate|\/flowstate|nothing checked/)
  expect(statusText({ now: NOW, report: { file: `/x/${'n'.repeat(80)}.flow.yaml`, diagnostics: bad } })).toMatch(/^⚠ validate 1 error …n+\.flow\.yaml$/)
})

test('only a failed local run is attention', () => {
  const line = (kind: 'ok' | 'failed' | 'unknown' | 'notrun') => statusText({ now: NOW, run: { file: 'a.flow.yaml', kind } })
  expect(line('failed')).toBe('⚠ run failed a.flow.yaml')
  expect(line('ok')).toBe('')
  expect(line('unknown')).toBe('')
  expect(line('notrun')).toBe('')
})

test('an owed verification leg is shown with its command', () => {
  expect(statusText({ now: NOW, owes: 'flow test' })).toBe('⚠ owes flow test')
})

test('a server answer shows only while fresh and only when someone needs attending to', () => {
  expect(statusText({ now: NOW, seen: seen() })).toMatch(/^⚠ server flow\.example:9233 2 need attention at \d\d:\d\d$/)
  expect(statusText({ now: NOW, seen: seen({ at: NOW - FRESH_MS }) })).toBe('')
  expect(statusText({ now: NOW, seen: seen({ at: NOW + 5000 }) })).toBe('')
  expect(statusText({ now: NOW, seen: seen({ failed: 0 }) })).toBe('')
  expect(statusText({ now: NOW, seen: seen({ address: '' }) })).toContain('server localhost:9233')
})

test('counts are capped and an address is cleaned and bounded', () => {
  const text = statusText({ now: NOW, seen: seen({ failed: 1e9, address: `h\u202e${'x'.repeat(200)}\u001b:9233` }) })
  expect(text).toContain('99+ need attention')
  expect(text).not.toMatch(/[\u0000-\u001f\u007f-\u009f\u202a-\u202e]/)
  expect(text.length).toBeLessThan(120)
})

test('seenFrom counts failed, timed-out and terminated runs; running and unknown ones are never attention', () => {
  const statuses = ['STATUS_FAILED', 'STATUS_TIMED_OUT', 'STATUS_TERMINATED', 'STATUS_RUNNING', 'STATUS_COMPLETED', 'STATUS_CANCELED', 'STATUS_UNSPECIFIED']
  const runs = statuses.map(status => ({ workflowId: 'w', status }) as RunSummary)
  expect(seenFrom(runs, 'h:1', 7)).toEqual({ at: 7, address: 'h:1', failed: 3 })
})

// The hooks, with the engine stubbed: every process the line could have started is recorded.
const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const
const mount = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })
const ok = (stdout: string) => ({ value: { exitCode: 0, stdout, stderr: '', isStdoutTruncated: false, isStderrTruncated: false } })
const down = { value: { exitCode: 1, stdout: '', stderr: 'connection refused', isStdoutTruncated: false, isStderrTruncated: false } }
const stub = (on: any, o: { validate?: string; list?: string; address?: string; files?: string[] } = {}) => {
  const argvs: string[][] = []
  const lines: string[] = []
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    argvs.push(e.argv)
    if (e.argv[1] === 'list') return o.list === undefined ? down : ok(o.list)
    return ok(o.validate ?? '{"file":"a.flow.yaml","diagnostics":[]}\n')
  })
  on('tool.call', () => ({ result: 'ok' }))
  on('turn.start', () => ({ turnId: 't' }))
  on('fs.list', () => ({ value: (o.files ?? []).map(name => ({ name, kind: 'file', size: 1, mtimeMs: 0, isLink: false })) }))
  on('env.get', () => ({ value: o.address }))
  on('ui.status', (_$: unknown, e: unknown) => {
    lines.push(typeof e === 'string' ? e : String((e as { text?: string }).text))
    return { value: undefined }
  })
  return { argvs, lines }
}
const edit = ($: any, path = 'a.flow.yaml') => $.tool.call({ tool: 'Edit', file_path: path, old_string: 'x', new_string: 'y' })
const failedRun = JSON.stringify({ runs: [{ workflowId: 'w1', status: 'STATUS_FAILED' }, { workflowId: 'w2', status: 'STATUS_COMPLETED' }] })

test('an edit shows the validate result and the owed leg, and starts nothing for the line', async ($, on) => {
  const { argvs, lines } = stub(on, { validate: '{"file":"a.flow.yaml","diagnostics":[{"line":2,"column":1,"message":"bad"}]}\n' })
  await $.turn.start({ text: 'go', turnId: 't' })
  expect(lines.at(-1)).toBe('')
  await edit($)
  expect(lines.at(-1)).toBe('⚠ validate 1 error a.flow.yaml · owes flow validate')
  // The only process in the whole sequence is the validation of the edit itself.
  expect(argvs).toEqual([['flow', 'validate', '-o', 'jsonl', '--', 'a.flow.yaml']])
})

test('a passing check clears the owed leg, and a test suite changes which leg is owed', async ($, on) => {
  const { lines } = stub(on, { files: ['a.test.yaml'] })
  await $.turn.start({ text: 'go', turnId: 't' })
  await edit($)
  expect(lines.at(-1)).toBe('⚠ owes flow test')
  await $.tool.call({ tool: 'Bash', command: 'flow test' })
  expect(lines.at(-1)).toBe('')
})

test('the Runs pane puts the attention count and address on the line; a filtered listing does not', async ($, on) => {
  const { argvs, lines } = stub(on, { list: failedRun, address: 'flow.example:9233' })
  const ui = await mount($)
  expect(lines.at(-1)).toMatch(/^⚠ server flow\.example:9233 1 need attention at \d\d:\d\d$/)
  // The line does not make the pane redraw in a loop.
  expect(argvs.filter(a => a[1] === 'list').length).toBeLessThan(4)
  await ui.input({ key: 'filter', text: 'status == "FAILED"' })
  expect(lines.at(-1)).toBe('')
  await ui.unmount()
})

test('a server that did not answer is not shown', async ($, on) => {
  const { lines } = stub(on, { address: 'flow.example:9233' })
  const ui = await mount($)
  expect(lines.filter(l => l.includes('server'))).toEqual([])
  await ui.unmount()
})

test('a local run from the form puts its result on the line, success or failure, and starts nothing extra', async ($, on) => {
  const argvs: string[][] = []
  const lines: string[] = []
  let exitCode = 0
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    argvs.push(e.argv)
    const compile = e.argv[1] === 'compile'
    return { value: { exitCode: e.argv[1] === 'run' ? exitCode : compile ? 0 : 1, stdout: compile ? '{"type":"object","properties":{}}' : 'COMPLETED\n', stderr: exitCode ? 'boom' : '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('fs.list', () => ({ value: [{ name: 'deploy.flow.yaml', kind: 'file', size: 1, mtimeMs: 1, isLink: false }] }))
  on('env.get', () => ({ value: undefined }))
  on('ui.status', (_$: unknown, e: unknown) => {
    lines.push(typeof e === 'string' ? e : String((e as { text?: string }).text))
    return { value: undefined }
  })
  const ui = await mount($)
  await ui.select({ plugin: 'flowstate', key: 'run-file', value: 'deploy.flow.yaml' })
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(lines.at(-1)).toBe('')
  exitCode = 1
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(lines.at(-1)).toBe('⚠ run failed deploy.flow.yaml')
  expect(argvs.filter(a => a[1] === 'run').length).toBe(2)
  await ui.unmount()
})

test('a refusal before the run replaces the previous result on the line, and spawns nothing', async ($, on) => {
  const argvs: string[][] = []
  const lines: string[] = []
  let listed = true
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    argvs.push(e.argv)
    const compile = e.argv[1] === 'compile'
    return { value: { exitCode: compile || e.argv[1] === 'run' ? 0 : 1, stdout: compile ? '{"type":"object","properties":{}}' : 'COMPLETED\n', stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('fs.list', () => ({ value: listed ? [{ name: 'deploy.flow.yaml', kind: 'file', size: 1, mtimeMs: 1, isLink: false }] : [] }))
  on('env.get', () => ({ value: undefined }))
  on('ui.status', (_$: unknown, e: unknown) => {
    lines.push(typeof e === 'string' ? e : String((e as { text?: string }).text))
    return { value: undefined }
  })
  const ui = await mount($)
  await ui.select({ plugin: 'flowstate', key: 'run-file', value: 'deploy.flow.yaml' })
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(lines.at(-1)).toBe('')
  await ui.press({ key: 'run-local' })
  listed = false
  await ui.press({ key: 'confirm-run' })
  expect(lines.at(-1)).toBe('')
  expect(argvs.filter(a => a[1] === 'run').length).toBe(1)
  await ui.unmount()
})

test('a server whose address is unreadable is unknown, so nothing is claimed about it', async ($, on) => {
  const { lines } = stub(on, { list: failedRun, address: 'bad address;rm' })
  const ui = await mount($)
  expect(lines.filter(l => l.includes('server'))).toEqual([])
  await ui.unmount()
})
