import { clean } from '../hooks/runs'
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
  expect(await ui.find({ type: 'Text', text: /No Flowfile edited yet/ })).toBeDefined()
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
