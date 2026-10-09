import { MAX_CASES, RERUN_TIMEOUT_MS, bandFor, quoteMeta, rerunArgv, applyRerun, bandText } from '../hooks/testband'
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

test('a case whose name another case of its file repeats gets no argv: --run would select both', () => {
  const dup = bandFor({ stdout: doc([kase('same', false), kase('same', true), kase('solo', false)]), ok: false })
  expect(dup.failing.map(f => [f.name, rerunArgv('flow', f) !== undefined])).toEqual([['same', false], ['solo', true]])
  // The repeat may come later and may be a failing case too.
  const both = bandFor({ stdout: doc([kase('same', false), kase('same', false)]), ok: false })
  expect(both.failing.map(f => rerunArgv('flow', f))).toEqual([undefined, undefined])
  // The same name in another file does not matter: the argv names one file.
  const two = JSON.stringify({ files: [{ file: 'a.test.yaml', cases: [kase('same', false)] }, { file: 'b.test.yaml', cases: [kase('same', true)] }] })
  expect(rerunArgv('flow', bandFor({ stdout: two, ok: false }).failing[0])).toBeDefined()
})

test('a file the scan did not finish cannot vouch that a name is unique', () => {
  const many = Array.from({ length: MAX_CASES + 5 }, (_, i) => kase(`p${i}`, true))
  const cut = bandFor({ stdout: doc([kase('early', false), ...many]), ok: false })
  expect(cut.cut).toBe(true)
  expect(rerunArgv('flow', cut.failing[0])).toBeUndefined()
})

test('only a path ending .test.yaml or .test.yml is rerun: no backup suffix, no testdefaults', () => {
  for (const file of ['suite.test.yaml.bak', 'testdefaults.yaml', 'dir/testdefaults.yml', 'suite.test.yamlx']) expect(rerunArgv('flow', failing('a', file))).toBeUndefined()
  for (const file of ['suite.test.yaml', 'd/suite.test.yml']) expect(rerunArgv('flow', failing('a', file))).toBeDefined()
})

const SUITE =bandFor({ stdout: JSON.stringify({ files: [{ file: 'w.test.yaml', cases: [kase('a', false), kase('b', false), kase('c', true)], refused: '', coverage: [{ unreached: ['s1'] }], skipped: [{ name: 's', reason: 'x' }] }] }), ok: false })

test('a passing rerun leaves the suite verdict, the other failures, and the counts as they were', () => {
  const b = applyRerun(SUITE, SUITE.failing[0], { exitCode: 0, stdout: doc([kase('a', true)]) })
  expect([b.outcome, b.failed, b.passed, b.skipped, b.uncovered, b.failing.map(f => f.name)]).toEqual(['failed', 2, 1, 1, 1, ['a', 'b']])
  expect(b.rerun).toMatchObject({ name: 'a', outcome: 'passed' })
  const lines = bandText(b)
  expect(lines[0]).toMatch(/^test ✗ failed · ✗ 2 failed · ✓ 1 passed/)
  expect(lines.at(-1)).toBe("  rerun of a with default flags (the run's own flags are not carried): ✓ passed")
})

test('a failing rerun refreshes that case only', () => {
  const stdout = JSON.stringify({ files: [{ file: 'w.test.yaml', cases: [{ name: 'a', passed: false, failures: [{ line: 99, message: 'new reason' }] }] }] })
  const b = applyRerun(SUITE, SUITE.failing[0], { exitCode: 1, stdout })
  expect(b.rerun?.outcome).toBe('failed')
  expect([b.failing[0].line, b.failing[0].reason, b.failing[1]]).toEqual([99, 'new reason', SUITE.failing[1]])
})

test('a rerun is passed or failed only when its JSON was read; everything else is unknown', () => {
  const cases: [string, Parameters<typeof applyRerun>[2]][] = [
    ['exit 0, empty stdout', { exitCode: 0, stdout: '' }],
    ['exit 1, empty stdout', { exitCode: 1, stdout: '' }],
    ['exit 0, text report', { exitCode: 0, stdout: 'PASS w.test.yaml: a\n' }],
    ['unparsable JSON', { exitCode: 0, stdout: '{"files": [' }],
    ['cut output', { exitCode: 0, stdout: doc([kase('a', true)]), isStdoutTruncated: true }],
    ['no case matched', { exitCode: 0, stdout: doc([]) }],
    ['threw or timed out', undefined],
  ]
  for (const [label, ran] of cases) {
    const b = applyRerun(SUITE, SUITE.failing[0], ran, new Error('timed out'))
    expect([label, b.rerun?.outcome]).toEqual([label, 'unknown'])
    expect([b.outcome, b.failing.length]).toEqual(['failed', 2])
  }
  expect(bandText(applyRerun(SUITE, SUITE.failing[0], undefined, new Error('timed out'))).at(-1)).toMatch(/\? unknown \(Error: timed out\)$/)
})

// The drawn band and its buttons.
const FAILING = doc([kase('wrong output', false), kase('-dash', false)])
type Reply = { exitCode: number; stdout: string; stderr?: string } | 'deny'
const stub = (on: any, rerun: Reply, bash = FAILING, gate?: Promise<void>) => {
  const seen: string[][] = []
  const limits: unknown[] = []
  on('process.run', async (_$: unknown, e: { argv: string[]; init?: { timeoutMs?: number }; timeoutMs?: number }) => {
    seen.push(e.argv)
    limits.push(e.init?.timeoutMs ?? e.timeoutMs)
    await gate
    if (rerun === 'deny') return { deny: 'timed out' }
    return { value: { stderr: '', isStdoutTruncated: false, isStderrTruncated: false, ...rerun } }
  })
  on('tool.call', (_$: unknown, e: { tool: string }) =>
    e.tool === 'Bash' ? { result: { stdout: bash, stderr: '', interrupted: false }, isError: true } : { result: 'ok' },
  )
  on('turn.start', () => ({ turnId: 't' }))
  on('fs.list', () => ({ value: [] }))
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
  on('ui.render', () => ({ type: 'Box', props: {}, children: [] }))
  return { seen, limits }
}
const band = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', component: 'AbovePrompt', props: { hasSurvey: false }, requestId: 'band', viewport: { columns: 100, rows: 20 } })
const flowTest = ($: any) => $.tool.call({ tool: 'Bash', command: 'flow test -o json w.test.yaml' })
const start = async ($: any) => {
  await flowTest($)
  return band($)
}
const argvs = (seen: string[][]) => seen.filter(a => a[1] === 'test')
const gated = () => {
  let open!: () => void
  const gate = new Promise<void>(r => (open = r))
  return { gate, open }
}
const settle = () => new Promise(r => setTimeout(r, 20))

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

test('a passing rerun runs once and leaves the headline failed, with the other failure and a labelled line', async ($, on) => {
  const { seen, limits } = stub(on, { exitCode: 0, stdout: doc([kase('wrong output', true)]) })
  const ui = await start($)
  await ui.press({ key: 'rerun:0' })
  await ui.press({ key: 'confirm-rerun:0' })
  expect(argvs(seen)).toEqual([['flow', 'test', '-o', 'json', '--run=^wrong output$', '--', 'w.test.yaml']])
  expect(limits).toEqual([RERUN_TIMEOUT_MS])
  expect(await ui.find({ type: 'Text', text: /test ✗ failed · ✗ 2 failed/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /test ✓ passed/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /✗ failed -dash \(w\.test\.yaml:18\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /rerun of wrong output with default flags \(the run's own flags are not carried\): ✓ passed/ })).toBeDefined()
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
  expect(await ui.find({ type: 'Text', text: /rerun of -dash .*✗ failed/ })).toBeDefined()
  await ui.unmount()
})

test('a rerun that times out or cannot start reads unknown on its line, and the verdict stays', async ($, on) => {
  stub(on, 'deny')
  const ui = await start($)
  await ui.press({ key: 'rerun:0' })
  await ui.press({ key: 'confirm-rerun:0' })
  expect(await ui.find({ type: 'Text', text: /rerun of wrong output .*\? unknown/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /test ✗ failed/ })).toBeDefined()
  await ui.unmount()
})

test('exit 0 with empty output is unknown, not passed', async ($, on) => {
  stub(on, { exitCode: 0, stdout: '' })
  const ui = await start($)
  await ui.press({ key: 'rerun:0' })
  await ui.press({ key: 'confirm-rerun:0' })
  expect(await ui.find({ type: 'Text', text: /rerun of wrong output .*\? unknown/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /rerun of .*✓ passed/ })).toBeUndefined()
  await ui.unmount()
})

for (const change of ['edit', 'replace', 'hide'] as const) {
  test(`a band changed by ${change} during the run is not written over`, async ($, on) => {
    const { gate, open } = gated()
    const { seen } = stub(on, { exitCode: 0, stdout: doc([kase('wrong output', true)]) }, FAILING, gate)
    const ui = await start($)
    await ui.press({ key: 'rerun:0' })
    const pressed = ui.press({ key: 'confirm-rerun:0' })
    await settle()
    expect(argvs(seen)).toHaveLength(1)
    if (change === 'edit') await $.tool.call({ tool: 'Edit', file_path: 'w.test.yaml', old_string: 'x', new_string: 'y' })
    else if (change === 'replace') await flowTest($)
    else await ui.press({ key: 'hide' })
    open()
    await pressed.catch(() => undefined)
    await settle()
    // Nothing of the old rerun appears: cleared stays cleared, and the new band carries no rerun line.
    expect(await ui.find({ type: 'Text', text: /rerun of/ })).toBeUndefined()
    expect((await ui.find({ type: 'Text', text: /^test/ })) === undefined).toBe(change !== 'replace')
    await ui.unmount()
  })
}

test('a concurrent second Confirm press runs nothing more', async ($, on) => {
  const { gate, open } = gated()
  const { seen } = stub(on, { exitCode: 0, stdout: doc([kase('wrong output', true)]) }, FAILING, gate)
  const ui = await start($)
  await ui.press({ key: 'rerun:0' })
  const first = ui.press({ key: 'confirm-rerun:0' })
  await settle()
  await ui.press({ key: 'confirm-rerun:0' }).catch(() => undefined)
  open()
  await first.catch(() => undefined)
  await settle()
  expect(argvs(seen)).toHaveLength(1)
  await ui.unmount()
})

for (const change of ['replace', 'hide'] as const) {
  test(`a band ${change}d between the question and Confirm runs nothing, and the question does not come back`, async ($, on) => {
    const { seen } = stub(on, { exitCode: 0, stdout: doc([kase('wrong output', true)]) })
    const ui = await start($)
    await ui.press({ key: 'rerun:0' })
    if (change === 'replace') await flowTest($)
    else await ui.press({ key: 'hide' })
    // Even a later band holding the same file and name shows no open question.
    await flowTest($)
    expect(await ui.find({ type: 'Text', text: /Nothing runs until you confirm/ })).toBeUndefined()
    await expect(ui.press({ key: 'confirm-rerun:0' })).rejects.toThrow()
    expect(argvs(seen)).toEqual([])
    await ui.unmount()
  })
}

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
