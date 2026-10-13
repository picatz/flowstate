import { clean, isGone, needsSignIn, reason } from '../hooks/runs'
import { expect, test } from 'claude-code/testing'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const

const run = (id: string, status: string, name = '') =>
  JSON.stringify({ workflowId: id, runId: 'r', status, ...(name ? { name } : {}) })
const page = (runs: string[], next = '') =>
  `{"runs":[${runs.join(',')}]${next ? `,"nextPageToken":"${next}"` : ''}}\n`

type Reply = { exitCode: number; stdout: string; stderr: string }

// Answer `flow list` from a queue of replies, one per call; the last repeats.
const stub = (on: any, replies: Reply[], seen: string[][] = []) => {
  let i = 0
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    const reply = replies[Math.min(i++, replies.length - 1)]
    return { value: { ...reply, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
}
const ok = (stdout: string): Reply => ({ exitCode: 0, stdout, stderr: '' })

test('the pane lists the runs a server reports', async ($, on) => {
  const seen: string[][] = []
  stub(on, [ok(page([run('nightly-1', 'STATUS_RUNNING', 'nightly-etl'), run('w-2', 'STATUS_FAILED')]))], seen)
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(seen[0]).toEqual(['flow', 'list', '-o', 'json'])
  expect(await ui.find({ type: 'Button', text: /running nightly-etl \(nightly-1\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /failed w-2/ })).toBeDefined()
  await ui.unmount()
})

test('a short page with a token is followed until the pane has enough', async ($, on) => {
  const seen: string[][] = []
  // The first page is empty because the bounded scan met other tenants' runs first.
  stub(on, [ok(page([], 't1')), ok(page([run('mine', 'STATUS_RUNNING')]))], seen)
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(seen[1]).toEqual(['flow', 'list', '-o', 'json', '--page-token', 't1'])
  expect(await ui.find({ type: 'Button', text: /running mine/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /No runs yet/ })).toBeUndefined()
  await ui.unmount()
})

test('the page walk is bounded even when the server never stops continuing', async ($, on) => {
  const seen: string[][] = []
  stub(on, [ok(page([], 'again'))], seen)
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(seen.filter(a => a[1] === 'list').length).toBe(4)
  expect(await ui.find({ type: 'Text', text: /No runs yet/ })).toBeDefined()
  await ui.unmount()
})

test('a failing list says the runs are unavailable and shows why, and local use is untouched', async ($, on) => {
  stub(on, [{ exitCode: 1, stdout: '', stderr: 'ERROR\nno Flowstate server answered at 127.0.0.1:7233\n' }])
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(await ui.find({ type: 'Text', text: /Runs unavailable \(no Flowstate server answered/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /Edit a Flowfile and it shows up here/ })).toBeDefined()
  await ui.unmount()
})

test('an entry that is not a run does not hide the others and the list stays bounded', async ($, on) => {
  const many = Array.from({ length: 20 }, (_, i) => run(`w${i}`, 'STATUS_COMPLETED'))
  stub(on, [ok(page(['"junk"', '{}', ...many]))])
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(await ui.find({ type: 'Button', text: /succeeded w0$/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /succeeded w7$/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /succeeded w8$/ })).toBeUndefined()
  await ui.unmount()
})

test('terminal control characters in a name never reach the pane', async ($, on) => {
  stub(on, [ok(page([run('w1', 'STATUS_RUNNING', 'a\u001b[31mred\u009b')]))])
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(await ui.find({ type: 'Button', text: /running a\[31mred \(w1\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /[\u001b\u009b]/ })).toBeUndefined()
  await ui.unmount()
})

// The pane cleans a rejected `flow list` with this same helper (the engine skips a
// stub that throws, so the rejection path cannot be driven through a mount).
test('clean strips controls and bounds server text', () => {
  const hostile = 'spawn failed \u001b]0;x\u0007\u009b31m' + 'y'.repeat(10_000)
  const out = clean(`Error: ${hostile}`, 100)

  expect(out).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/)
  expect(out.length).toBe(100)
  expect(clean(42)).toBe('')
})

test('a long transport error keeps its cause, which comes last', () => {
  const why = reason('rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:9233: connect: connection refused"')
  expect(why.length).toBeLessThanOrEqual(100)
  expect(why).toMatch(/^rpc error: code = Unavailable/)
  expect(why).toMatch(/connection refused"?$/)
  expect(reason('short one')).toBe('short one')
  expect(reason('')).toBe('no server answered')
})

test('a very long error still ends in its cause, and a cut never splits a character', () => {
  const long = `rpc error: ${'x'.repeat(900)} connection refused`
  expect(reason(long)).toMatch(/connection refused$/)
  const emoji = `${'😀'.repeat(120)} end`
  const cut = reason(emoji)
  expect(Array.from(cut).every(c => c.codePointAt(0)! < 0xd800 || c.codePointAt(0)! > 0xdfff)).toBe(true)
  expect(cut).toMatch(/ end$/)
  expect(cut).toContain(' … ')
  expect(Array.from(cut).length).toBeLessThanOrEqual(100)
  expect(reason('a'.repeat(100))).toBe('a'.repeat(100))
  expect(reason('a'.repeat(101))).toContain(' … ')
  const huge = reason(`start ${'y'.repeat(50_000)} the real cause`)
  expect(huge).toMatch(/the real cause$/)
})

test('only the CLI refusal for an unaddressable run reads as gone', () => {
  const real = 'ERROR\nno run "flowstate-workflow-3f2b9c1e-8a64-4d2e-9b7a-0c5d1e2f3a4b" is addressable: check the id, or it belongs to a tenant your credentials do not establish, or it has aged out of Temporal\'s retention\n'
  expect(isGone(real)).toBe(true)
  expect(isGone('no run "weird\\"id" is addressable: check the id')).toBe(true)
  // not on a terminal the CLI wraps at 80 columns, which leaves a long id's `no run` alone on a line
  const id = `flowstate-request-${'0123456789abcdef'.repeat(4)}`
  const wrapped = `ERROR\nno run\n"${id}"\nis addressable: check the id, or it belongs to a tenant your credentials do not\nestablish, or it has aged out of Temporal's retention\n`
  expect(isGone(wrapped)).toBe(true)
  expect(isGone('ERROR\nno run\nsomething else entirely\n')).toBe(false)
  // a long id is cut in the one-line reason, and the answer must not depend on that
  expect(reason(real)).not.toContain('addressable')
  for (const other of [
    'fork/exec flow: executable file not found in $PATH',
    'dial tcp: lookup notfound-host: no such host',
    'reading the timeline of "deploy-not-found": connection refused',
    'refused while reading "no run \"x\" is addressable:": permission denied',
    'workflow execution not found',
    '',
  ])
    expect(isGone(other)).toBe(false)
})

// Real stderr from `flow list` and `flow timeline` against `flow server dev --auth` with no token (cmd/flow/client.go, list.go).
test('a refusal for a missing or bad credential is a sign-in, however the CLI wrapped it', () => {
  expect(needsSignIn('ERROR\nrefused while listing runs: unauthenticated: missing bearer token\n')).toBe(true)
  expect(needsSignIn('ERROR\nrefused while reading the timeline of "some-run": unauthenticated: missing\nbearer token\n')).toBe(true)
  expect(needsSignIn('ERROR\nrefused while reading the timeline of\n"flowstate-request-0123456789abcdef":\nunauthenticated: token expired\n')).toBe(true)
  expect(needsSignIn('ERROR\nrefused while reading the timeline of "wf: \\"x\\"": unauthenticated: token expired\n')).toBe(true)
})

test('only the unauthenticated code reads as a sign-in: a signed-in caller who may not act cannot fix it by signing in', () => {
  expect(needsSignIn('ERROR\nrefused while listing runs: permission_denied: workload.read\n')).toBe(false)
  expect(needsSignIn('ERROR\nno run "wf-1" is addressable: check the id\n')).toBe(false)
  expect(needsSignIn('ERROR\nlisting runs: unavailable: connection refused\n')).toBe(false)
  expect(needsSignIn('')).toBe(false)
})

test('an id that spells the code does not pass for the refusal', () => {
  expect(needsSignIn('ERROR\nrefused while reading the timeline of "x: unauthenticated: y": permission_denied: no\n')).toBe(false)
  expect(needsSignIn('ERROR\nreading the timeline of "x": connection reset (refused while a: unauthenticated: b)\n')).toBe(false)
})

test('a credential that expires after a good list keeps the list and still offers the sign-in', async ($, on) => {
  const auth = 'ERROR\nrefused while listing runs: unauthenticated: token expired\n'
  let expired = false
  on('process.run', () => ({
    value: expired
      ? { exitCode: 1, stdout: '', stderr: auth, isStdoutTruncated: false, isStderrTruncated: false }
      : { exitCode: 0, stdout: page([run('w-1', 'STATUS_RUNNING', 'Deploy')]), stderr: '', isStdoutTruncated: false, isStderrTruncated: false },
  }))
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: 'dev.example.com:9233' }))
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })
  expect(await ui.find({ type: 'Button', text: /running Deploy/ })).toBeDefined()

  expired = true
  await ui.input({ key: 'filter', text: 'status == "FAILED"' })
  expect(await ui.find({ type: 'Button', text: /Copy sign-in command/ })).toBeDefined()
  await ui.input({ key: 'filter', text: '' })

  expect(await ui.find({ type: 'Button', text: /running Deploy/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /server no longer accepts your credentials/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /Copy sign-in command/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /the server did not answer/ })).toBeUndefined()
  await ui.unmount()
})
