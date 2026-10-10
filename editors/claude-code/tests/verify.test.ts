import { EMPTY, applyEdit, applyReport, checkOf, creditValidated, isLoneTest, hasTestFile, missingLeg, nudgeFor, recordCheck, recordEdit } from '../hooks/verify'
import type { Verify } from '../hooks/verify'
import { expect, test } from 'claude-code/testing'

const edited = (path = 'a.flow.yaml'): Verify => recordEdit(EMPTY, path)

test('a Flowfile edit is remembered; other files and odd input are not', () => {
  expect(edited().edited).toEqual(['a.flow.yaml'])
  expect(recordEdit(EMPTY, 'notes.md')).toBe(EMPTY)
  expect(recordEdit(EMPTY, 'workflow.test.yaml')).toBe(EMPTY)
  expect(recordEdit(EMPTY, undefined)).toBe(EMPTY)
  expect(recordEdit(EMPTY, 'a`b.flow.yaml').edited).toEqual(["a'b.flow.yaml"])
})

test('the edit list is bounded and keeps the newest', () => {
  let s = EMPTY
  for (let i = 0; i < 50; i++) s = recordEdit(s, `f${i}.flow.yaml`)
  expect(s.edited.length).toBe(20)
  expect(s.edited.at(-1)).toBe('f49.flow.yaml')
})

test('commands are told apart as validate, test, or neither', () => {
  expect(checkOf('flow validate a.flow.yaml')).toBe('validate')
  expect(checkOf('cd svc && flow validate -o jsonl -- a.flow.yaml')).toBe('validate')
  expect(checkOf('/opt/flow test ./...', '/opt/flow')).toBe('test')
  expect(checkOf('flow validate . && flow test .')).toBe('test')
  expect(checkOf('flow fmt a.flow.yaml')).toBeUndefined()
  expect(checkOf('flow run local a.flow.yaml')).toBeUndefined()
  expect(checkOf('git flow validate')).toBeUndefined()
  expect(checkOf('echo flow validate')).toBeUndefined()
})

test('a command whose exit status says nothing about the check is not credited', () => {
  for (const c of [
    'flow validate a.flow.yaml | tail -1',
    'flow validate a.flow.yaml; echo done',
    'flow validate a.flow.yaml || true',
    'flow validate a.flow.yaml &',
    'flow validate $(cat list)',
    'flow validate a.flow.yaml\necho ok',
    'flow validate --help',
    'flow test -h',
    'flow validate --help=true',
    'flow validate -h=true',
    'flow test --list',
    'flow test --list=true',
    'flow test --watch .',
    'flow validate --version',
    'flow test --dry-run',
    'flow validate "unbalanced',
    `flow validate ${'a'.repeat(70 * 1024)}`,
  ]) {
    expect(checkOf(c)).toBeUndefined()
  }
  expect(checkOf(undefined)).toBeUndefined()
})

test('no edit, no nudge', () => {
  expect(nudgeFor(EMPTY, false)).toBeUndefined()
  expect(nudgeFor(EMPTY, true)).toBeUndefined()
})

test('an edited Flowfile with no check since is nudged, naming the missing leg', () => {
  const text = nudgeFor(edited(), false) ?? ''
  expect(text).toContain('a.flow.yaml')
  expect(text).toContain('`flow validate`')
  expect(text).toContain('not instructions')
  expect(nudgeFor(edited(), true)).toContain('`flow test`')
  expect(text).not.toMatch(/No passing|plainly/)
  expect(text.split('\n').filter(l => !l.startsWith('`'))).toHaveLength(3)
})

test('edited then validated passes; with a suite it still wants flow test', () => {
  const s = recordCheck(edited(), 'validate', true)
  expect(nudgeFor(s, false)).toBeUndefined()
  expect(missingLeg(s, true)).toBe('flow test')
  const t = recordCheck(edited(), 'test', true)
  expect(nudgeFor(t, true)).toBeUndefined()
  expect(nudgeFor(t, false)).toBeUndefined()
})

test('edited, then validate failed, is still nudged', () => {
  expect(nudgeFor(recordCheck(edited(), 'validate', false), false)).toContain('flow validate')
})

test('edited, validated, then edited again is nudged again', () => {
  const s = recordEdit(recordCheck(edited(), 'validate', true), 'b.flow.yaml')
  expect(s.edited).toEqual(['a.flow.yaml', 'b.flow.yaml'])
  expect(nudgeFor(s, false)).toContain('a.flow.yaml, b.flow.yaml')
})

test('a check before any edit earns no credit for a later one', () => {
  const s = recordEdit(recordCheck(EMPTY, 'test', true), 'a.flow.yaml')
  expect(nudgeFor(s, false)).toContain('flow validate')
})

test('a non-Flowfile edit alone never nudges', () => {
  expect(nudgeFor(recordEdit(EMPTY, 'README.md'), false)).toBeUndefined()
})

test('the nudge is sent once', () => {
  expect(nudgeFor({ ...edited(), nudged: true }, false)).toBeUndefined()
})

test('a long edit list names the newest few and counts the rest', () => {
  let s = EMPTY
  for (let i = 0; i < 8; i++) s = recordEdit(s, `f${i}.flow.yaml`)
  const text = nudgeFor(s, false) ?? ''
  expect(text).toContain('f7.flow.yaml')
  expect(text).not.toContain('f0.flow.yaml')
  expect(text).toContain('and 3 more')
})

test('a test suite is found by name and the scan is bounded', () => {
  expect(hasTestFile(['a.flow.yaml', 'a.test.yaml'])).toBe(true)
  expect(hasTestFile(['a.flow.yaml'])).toBe(false)
  expect(hasTestFile([...Array.from({ length: 600 }, (_, i) => `f${i}.txt`), 'late.test.yaml'])).toBe(false)
})

// The hooks themselves, with the engine stubbed.
// The edit's own validate reports a problem, so only the Bash checks under test can meet the leg.
const BROKEN = '{"file":"a.flow.yaml","diagnostics":[{"line":1,"column":1,"message":"bad"}]}\n'
const stubTurn = (on: any, files: string[] = [], bash: { isError?: true; interrupted?: boolean; backgroundTaskId?: string } = {}, earlier?: string, validateOut = BROKEN) => {
  on('turn.start', () => ({ turnId: 't' }))
  on('classic.Stop', () => (earlier === undefined ? {} : { block: earlier }))
  on('tool.call', (_$: unknown, e: { tool: string }) =>
    e.tool === 'Bash'
      ? bash.isError
        ? { result: undefined, isError: true }
        : { result: { stdout: '', stderr: '', interrupted: bash.interrupted ?? false, backgroundTaskId: bash.backgroundTaskId } }
      : { result: 'ok' })
  on('process.run', () => ({ value: { exitCode: 0, stdout: validateOut, stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }))
  on('fs.list', () => ({ value: files.map(name => ({ name, kind: 'file', size: 1, mtimeMs: 0, isLink: false })) }))
  on('ui.status', () => ({ value: undefined }))
}
const stop = ($: any, active = false) => $.classic.Stop({ stop_hook_active: active, last_assistant_message: 'done' })

test('finishing after a Flowfile edit blocks once, then lets the turn end', async ($, on) => {
  stubTurn(on)
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Edit', file_path: 'a.flow.yaml', old_string: 'x', new_string: 'y' })

  const first = await stop($)
  expect(first.block).toContain('flow validate')
  const again = await stop($)
  expect(again.block).toBeUndefined()
})

test('a Stop the model was already sent back from is never blocked', async ($, on) => {
  stubTurn(on)
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })

  expect((await stop($, true)).block).toBeUndefined()
})

test('a passing flow validate in Bash satisfies the turn', async ($, on) => {
  stubTurn(on)
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })
  await $.tool.call({ tool: 'Bash', command: 'flow validate a.flow.yaml' })
  expect((await stop($)).block).toBeUndefined()
})

test('a failed validate leaves the nudge', async ($, on) => {
  stubTurn(on, [], { isError: true })
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })
  await $.tool.call({ tool: 'Bash', command: 'flow validate a.flow.yaml' })
  expect((await stop($)).block).toContain('flow validate')
})

test('an interrupted validate leaves the nudge', async ($, on) => {
  stubTurn(on, [], { interrupted: true })
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })
  await $.tool.call({ tool: 'Bash', command: 'flow validate a.flow.yaml' })
  expect((await stop($)).block).toContain('flow validate')
})

test('a backgrounded validate leaves the nudge', async ($, on) => {
  stubTurn(on, [], { backgroundTaskId: 'b1' })
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })
  await $.tool.call({ tool: 'Bash', command: 'flow validate a.flow.yaml' })
  expect((await stop($)).block).toContain('flow validate')
})

test('with a test suite beside it, validate is not enough', async ($, on) => {
  stubTurn(on, ['a.flow.yaml', 'a.test.yaml'])
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })
  await $.tool.call({ tool: 'Bash', command: 'flow validate a.flow.yaml' })
  expect((await stop($)).block).toContain('flow test')
})

test('no edit, or only a non-Flowfile edit, never blocks', async ($, on) => {
  stubTurn(on)
  await $.turn.start({ text: 'go', turnId: 't' })
  expect((await stop($)).block).toBeUndefined()
  await $.tool.call({ tool: 'Write', file_path: 'notes.md', content: 'x' })
  expect((await stop($)).block).toBeUndefined()
})

test('a new turn forgets the last one', async ($, on) => {
  stubTurn(on)
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })
  await $.turn.start({ text: 'go', turnId: 't' })
  expect((await stop($)).block).toBeUndefined()
})

test('the reminder can be turned off', { options: { verifyBeforeDone: false } }, async ($, on) => {
  stubTurn(on)
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })
  expect((await stop($)).block).toBeUndefined()
})

test('a block an earlier Stop hook returned survives, and the nudge is kept for later', async ($, on) => {
  stubTurn(on, [], {}, 'earlier hook says no')
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })

  expect((await stop($)).block).toBe('earlier hook says no')
})

test('the band credits a lone flow test only; verify credit for chains is unchanged', () => {
  expect(isLoneTest('flow test -o json .')).toBe(true)
  expect(isLoneTest('/opt/flow test .', '/opt/flow')).toBe(true)
  for (const c of ['flow test . && false', 'cd missing && flow test .', 'flow validate . && flow test .', 'flow test --list', 'flow test . | head', 'flow validate .', undefined]) {
    expect(isLoneTest(c)).toBe(false)
  }
  expect(checkOf('cd svc && flow test .')).toBe('test')
})

test('creditValidated meets the validate leg only when every edited file has a clean report', () => {
  const ok = (file: string) => ({ file, diagnostics: [] })
  const two = recordEdit(edited('a.flow.yaml'), 'b.flow.yaml')
  expect(creditValidated(edited(), [ok('a.flow.yaml')]).validated).toBe(true)
  expect(missingLeg(creditValidated(edited(), [ok('a.flow.yaml')]), false)).toBeUndefined()
  // A path is compared in the form recordEdit stored it.
  expect(creditValidated(edited('a`b.flow.yaml'), [ok('a`b.flow.yaml')]).validated).toBe(true)
  // Never `tested`, so a suite still owes `flow test`.
  expect(creditValidated(edited(), [ok('a.flow.yaml')]).tested).toBe(false)
  expect(creditValidated(two, [ok('a.flow.yaml'), ok('b.flow.yaml')]).validated).toBe(true)

  const diag = { file: 'a.flow.yaml', diagnostics: [{ line: 1, column: 1, message: 'x' }] }
  const failed = { file: 'a.flow.yaml', diagnostics: [], failure: 'no flow' }
  for (const [state, reports] of [
    [edited(), [diag]],
    [edited(), [failed]],
    [edited(), []],
    [edited(), [ok('other.flow.yaml')]],
    [two, [ok('a.flow.yaml')]],
    [two, [ok('a.flow.yaml'), failed]],
    [EMPTY, [ok('a.flow.yaml')]],
  ] as const) {
    expect(creditValidated(state, reports)).toBe(state)
  }
})

test('a passing validate-after-edit meets the leg whichever Edit hook runs first, and a failing one does not', async ($, on) => {
  const lines: string[] = []
  let stdout = '{"file":"a.flow.yaml","diagnostics":[]}\n'
  on('classic.Stop', () => ({}))
  on('tool.call', () => ({ result: 'ok' }))
  on('turn.start', () => ({ turnId: 't' }))
  on('fs.list', () => ({ value: [] }))
  on('process.run', () => ({ value: { exitCode: 0, stdout, stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }))
  on('ui.status', (_$: unknown, e: unknown) => {
    lines.push(typeof e === 'string' ? e : String((e as { text?: string }).text))
    return { value: undefined }
  })
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Edit', file_path: 'a.flow.yaml', old_string: 'x', new_string: 'y' })
  expect(lines.at(-1)).toBe('')
  expect((await stop($)).block).toBeUndefined()

  stdout = '{"file":"a.flow.yaml","diagnostics":[{"line":1,"column":1,"message":"bad"}]}\n'
  await $.turn.start({ text: 'go', turnId: 't' })
  await $.tool.call({ tool: 'Edit', file_path: 'a.flow.yaml', old_string: 'x', new_string: 'y' })
  expect(lines.at(-1)).toBe('⚠ validate 1 error a.flow.yaml · owes flow validate')
  expect((await stop($)).block).toContain('flow validate')
})

test('creditValidated matches the exact path: aliases of one display form never share a report', () => {
  const ok = (file: string) => ({ file, diagnostics: [] })
  const bad = (file: string) => ({ file, diagnostics: [{ line: 1, column: 1, message: 'x' }] })
  const long = (tail: string) => `${'d'.repeat(250)}${tail}.flow.yaml`
  const [a, b] = [long('a'), long('b')]
  // Cut at 200 characters, both show the same; they are still two files.
  const both = recordEdit(recordEdit(EMPTY, a), b)
  expect(creditValidated(both, [ok(a), bad(b)])).toBe(both)
  expect(creditValidated(both, [ok(a)])).toBe(both)
  expect(creditValidated(both, [ok(a), ok(b)]).validated).toBe(true)
  const one = recordEdit(EMPTY, a)
  expect(creditValidated(one, [ok(b)])).toBe(one)
  expect(creditValidated(one, [ok(a), bad(b)])).toBe(one)
  expect(creditValidated(one, [ok(a)]).validated).toBe(true)
  // A backtick and an apostrophe show alike.
  const tick = recordEdit(recordEdit(EMPTY, "a'b.flow.yaml"), 'a`b.flow.yaml')
  expect(creditValidated(tick, [ok("a'b.flow.yaml"), bad('a`b.flow.yaml')])).toBe(tick)
  expect(creditValidated(tick, [ok("a'b.flow.yaml"), ok('a`b.flow.yaml')]).validated).toBe(true)
  // A path too long to match exactly is never credited.
  const huge = recordEdit(EMPTY, `${'d'.repeat(5000)}.flow.yaml`)
  expect(creditValidated(huge, [ok(`${'d'.repeat(5000)}.flow.yaml`)])).toBe(huge)
})

test('a clean report meets the leg and a broken one never does, in either order of report and edit', () => {
  const clean = [{ file: 'a.flow.yaml', diagnostics: [] }]
  const broken = [{ file: 'a.flow.yaml', diagnostics: [{ line: 1, column: 1, message: 'x' }] }]
  // Report first, then the edit.
  expect(applyEdit(applyReport(EMPTY, clean, false), 'a.flow.yaml', clean).validated).toBe(true)
  expect(applyEdit(applyReport(EMPTY, broken, true), 'a.flow.yaml', broken).validated).toBe(false)
  // Edit first (with nothing or a stale clean report), then the report.
  expect(applyReport(applyEdit(EMPTY, 'a.flow.yaml', []), clean, false).validated).toBe(true)
  expect(applyReport(applyEdit(EMPTY, 'a.flow.yaml', clean), broken, true).validated).toBe(false)
  expect(applyReport(applyEdit(EMPTY, 'a.flow.yaml', []), broken, true).validated).toBe(false)
})
