import { RERUN_TIMEOUT_MS, bandFor, quoteMeta, rerunArgv, rerunBand } from '../hooks/testband'
import type { Failing } from '../hooks/testband'
import { expect, test } from 'claude-code/testing'

const kase = (name: string, passed: boolean) => ({ name, passed, failures: passed ? [] : [{ line: 18, message: 'expected false, got true' }] })
const doc = (cases: unknown[], file = 'w.test.yaml') => JSON.stringify({ files: [{ file, cases, refused: '', coverage: [], skipped: [] }] })
const failing = (name: string, file = 'w.test.yaml'): Failing => bandFor({ stdout: doc([kase(name, false)], file), ok: false }).failing[0]

test('the argv is one --run=^quoted$ element, then -- and the file, with no shell', () => {
  expect(rerunArgv('flow', failing('rolls back'))).toEqual(['flow', 'test', '-o', 'json', '--run=^rolls back$', '--', 'w.test.yaml'])
})

test('a hostile name stays one literal element: regex metacharacters are quoted, shell text is inert', () => {
  const name = 'a.b (c|d)* $(rm -rf ~); `x` && y [z]'
  const argv = rerunArgv('flow', failing(name))!
  expect(argv).toHaveLength(7)
  expect(argv[4]).toBe(`--run=^${quoteMeta(name)}$`)
  expect(argv[4]).toBe('--run=^a\\.b \\(c\\|d\\)\\* \\$\\(rm -rf ~\\); `x` && y \\[z\\]$')
  // The quoted pattern matches the name and only the name.
  expect(new RegExp(`^${quoteMeta(name)}$`).test(name)).toBe(true)
  expect(new RegExp(`^${quoteMeta('a.b')}$`).test('aXb')).toBe(false)
})

test('a name starting with - cannot become a flag', () => {
  const argv = rerunArgv('flow', failing('--fail-fast'))!
  expect(argv[4]).toBe('--run=^--fail-fast$')
  expect(argv.indexOf('--')).toBe(5)
  expect(argv.slice(0, 5).filter(a => a === '--fail-fast')).toEqual([])
})

test('a name or file that clean() altered or cut is not rerun', () => {
  expect(rerunArgv('flow', failing('x'.repeat(61)))).toBeUndefined()
  expect(rerunArgv('flow', failing('bad\u001b[31mname'))).toBeUndefined()
  expect(rerunArgv('flow', failing('zero​width'))).toBeUndefined()
  expect(rerunArgv('flow', failing('ok', `${'d/'.repeat(40)}w.test.yaml`))).toBeUndefined()
  expect(rerunArgv('flow', failing(''))).toBeUndefined()
})

test('a missing, flag-like, parent or non-test file is not rerun', () => {
  expect(rerunArgv('flow', { ...failing('a'), file: '' })).toBeUndefined()
  expect(rerunArgv('flow', failing('a', '-o'))).toBeUndefined()
  expect(rerunArgv('flow', failing('a', '--x.test.yaml'))).toBeUndefined()
  expect(rerunArgv('flow', failing('a', '../w.test.yaml'))).toBeUndefined()
  expect(rerunArgv('flow', failing('a', 'a.flow.yaml'))).toBeUndefined()
  expect(rerunArgv('flow', failing('a', 'a b.test.yaml'))).toBeUndefined()
  expect(rerunArgv('flow', { ...failing('a'), exact: undefined })).toBeUndefined()
})

test('a refused file has no case to rerun', () => {
  const b = bandFor({ stdout: JSON.stringify({ files: [{ file: 'w.test.yaml', cases: [], refused: 'bad', coverage: [], skipped: [] }] }), ok: false })
  expect(b.failing[0].name).toBe('file refused')
  expect(rerunArgv('flow', b.failing[0])).toBeUndefined()
})

test('the rerun band is the lone-test result, unknown when it did not finish or cannot be read', () => {
  expect(rerunBand({ exitCode: 0, stdout: doc([kase('a', true)]) })).toMatchObject({ outcome: 'passed', passed: 1, note: 'rerun of one case' })
  expect(rerunBand({ exitCode: 1, stdout: doc([kase('a', false)]) }).outcome).toBe('failed')
  expect(rerunBand(undefined, new Error('timed out')).outcome).toBe('unknown')
  expect(rerunBand({ exitCode: 0, stdout: '{"files": [' }).outcome).toBe('unknown')
  expect(rerunBand({ exitCode: 0, stdout: doc([kase('a', true)]), isStdoutTruncated: true }).outcome).toBe('unknown')
  // A name matching nothing is no pass.
  expect(rerunBand({ exitCode: 0, stdout: doc([]) }).outcome).toBe('unknown')
  expect(RERUN_TIMEOUT_MS).toBe(60000)
})

// The drawn band and its buttons.
const FAILING = doc([kase('wrong output', false), kase('-dash', false)])
type Reply = { exitCode: number; stdout: string; stderr?: string } | 'deny'
const stub = (on: any, rerun: Reply, bash = FAILING) => {
  const seen: string[][] = []
  const limits: unknown[] = []
  on('process.run', (_$: unknown, e: { argv: string[]; init?: { timeoutMs?: number }; timeoutMs?: number }) => {
    seen.push(e.argv)
    limits.push(e.init?.timeoutMs ?? e.timeoutMs)
    if (rerun === 'deny') return { deny: 'timed out' }
    return { value: { stderr: '', isStdoutTruncated: false, isStderrTruncated: false, ...rerun } }
  })
  on('tool.call', () => ({ result: { stdout: bash, stderr: '', interrupted: false }, isError: true }))
  on('turn.start', () => ({ turnId: 't' }))
  on('fs.list', () => ({ value: [] }))
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
  on('ui.render', () => ({ type: 'Box', props: {}, children: [] }))
  return { seen, limits }
}
const band = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', component: 'AbovePrompt', props: { hasSurvey: false }, requestId: 'band', viewport: { columns: 100, rows: 20 } })
const start = async ($: any) => {
  await $.tool.call({ tool: 'Bash', command: 'flow test -o json w.test.yaml' })
  return band($)
}
const argvs = (seen: string[][]) => seen.filter(a => a[1] === 'test')

test('the first press asks and runs nothing; Cancel runs nothing', async ($, on) => {
  const { seen } = stub(on, { exitCode: 0, stdout: doc([kase('wrong output', true)]) })
  const ui = await start($)
  await ui.press({ key: 'rerun:0' })
  expect(await ui.find({ type: 'Text', text: /Rerun the case "wrong output" of w\.test\.yaml locally\? Nothing runs until you confirm\./ })).toBeDefined()
  expect(argvs(seen)).toEqual([])
  await ui.press({ key: 'cancel-rerun:0' })
  expect(argvs(seen)).toEqual([])
  expect(await ui.find({ type: 'Text', text: /Nothing runs until you confirm/ })).toBeUndefined()
  await ui.unmount()
})

test('Confirm runs once and rebuilds the band; a second press runs nothing more', async ($, on) => {
  const { seen, limits } = stub(on, { exitCode: 0, stdout: doc([kase('wrong output', true)]) })
  const ui = await start($)
  await ui.press({ key: 'rerun:0' })
  await ui.press({ key: 'confirm-rerun:0' })
  expect(argvs(seen)).toEqual([['flow', 'test', '-o', 'json', '--run=^wrong output$', '--', 'w.test.yaml']])
  expect(limits).toEqual([RERUN_TIMEOUT_MS])
  expect(await ui.find({ type: 'Text', text: /✓ passed.*✓ 1 passed.*rerun of one case/ })).toBeDefined()
  // The old failing list is gone, and the buttons with it: nothing is left to press twice.
  await expect(ui.press({ key: 'confirm-rerun:0' })).rejects.toThrow()
  expect(argvs(seen)).toHaveLength(1)
  await ui.unmount()
})

test('a name starting with - is passed as a value after --run=, never as a flag', async ($, on) => {
  const { seen } = stub(on, { exitCode: 1, stdout: doc([kase('-dash', false)]) })
  const ui = await start($)
  await ui.press({ key: 'rerun:1' })
  await ui.press({ key: 'confirm-rerun:1' })
  expect(argvs(seen)[0]).toEqual(['flow', 'test', '-o', 'json', '--run=^-dash$', '--', 'w.test.yaml'])
  expect(await ui.find({ type: 'Text', text: /✗ failed.*✗ 1 failed/ })).toBeDefined()
  await ui.unmount()
})

test('a rerun that times out or cannot start reads unknown, not passed or failed', async ($, on) => {
  const { seen } = stub(on, 'deny')
  const ui = await start($)
  await ui.press({ key: 'rerun:0' })
  await ui.press({ key: 'confirm-rerun:0' })
  expect(argvs(seen)).toHaveLength(1)
  expect(await ui.find({ type: 'Text', text: /\? unknown · rerun outcome unknown/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /✓ passed|✗ failed/ })).toBeUndefined()
  await ui.unmount()
})

test('unreadable rerun output fails closed to unknown', async ($, on) => {
  stub(on, { exitCode: 0, stdout: '{"files": [{' })
  const ui = await start($)
  await ui.press({ key: 'rerun:0' })
  await ui.press({ key: 'confirm-rerun:0' })
  expect(await ui.find({ type: 'Text', text: /\? unknown · the result could not be read/ })).toBeDefined()
  await ui.unmount()
})

test('a case with a name too long to quote exactly gets no Rerun button', async ($, on) => {
  const { seen } = stub(on, { exitCode: 0, stdout: '' }, doc([kase('x'.repeat(80), false)]))
  const ui = await start($)
  expect(await ui.find({ type: 'Button', key: 'rerun:0' })).toBeUndefined()
  expect(argvs(seen)).toEqual([])
  await ui.unmount()
})

test('a text-only run has no case to rerun', async ($, on) => {
  stub(on, { exitCode: 0, stdout: '' }, 'FAIL w.test.yaml: a\n')
  const ui = await start($)
  expect(await ui.find({ type: 'Button', key: 'rerun:0' })).toBeUndefined()
  await ui.unmount()
})
