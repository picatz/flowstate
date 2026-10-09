import { cardLines, cardsOf, parseOutputs, MAX_CARDS, MAX_FACT, MAX_RAW, MAX_TOTAL_RAW } from '../hooks/outputs'
import { expect, test } from 'claude-code/testing'

/** What `flow compile -o json --schema=outputs` prints for examples/computed-outputs (trimmed). */
const SCHEMA = JSON.stringify({
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  additionalProperties: false,
  properties: {
    complete: { description: 'whether every requested host was reached', type: 'boolean' },
    coverage: { type: 'number' },
    detail: { additionalProperties: {}, description: 'the release and the host count', type: 'object' },
    hosts_placed: { description: 'how many hosts the run reached', type: 'integer' },
    scope: { enum: ['canary', 'fleet'], type: 'string' },
    summary: { type: 'string' },
    targets: { items: {}, type: 'array' },
    token: { type: 'string', 'x-flowstate-sensitive': true },
  },
  required: ['complete', 'coverage', 'detail', 'hosts_placed', 'scope', 'summary', 'targets', 'token'],
  title: 'computed-outputs outputs',
  type: 'object',
})
/** The document `flow run local` prints to stdout: the transcript and `runOutputs`. */
const RUN = (out: unknown) => `{"steps":{"a":{"value":3}},"runOutputs":${typeof out === 'string' ? out : JSON.stringify(out)}}`
const GOOD = RUN('{"complete":true,"coverage":1,"detail":{"hosts_placed":3},"hosts_placed":3,"scope":"fleet","summary":"placed 3","targets":["alpha","beta"],"token":"s3cret"}')

const cards = (schema: string, stdout: string) => {
  const c = cardsOf(parseOutputs(schema), stdout)
  if ('error' in c) throw new Error(c.error)
  return c
}

test('one card per declared output, in declared order, with the schema type and the value', () => {
  const c = cards(SCHEMA, GOOD)
  expect(c.cards.map(k => [k.title, k.type, k.fact])).toEqual([
    ['complete', 'bool', 'true'],
    ['coverage', 'number', '1'],
    ['detail', 'map', '{"hosts_placed":3}'],
    ['hosts_placed', 'int', '3'],
    ['scope', 'enum', 'fleet'],
    ['summary', 'string', 'placed 3'],
    ['targets', 'list', '["alpha","beta"]'],
    ['token', 'string', 'hidden (sensitive)'],
  ])
  expect(c.cards[0].status).toMatchObject({ symbol: '✓', tone: 'ok' })
  expect(c.cards[0].help).toBe('whether every requested host was reached')
})

test('the plain-text form carries the same facts, and the raw form the whole values', () => {
  const c = cards(SCHEMA, GOOD)
  expect(cardLines(c)).toContain('✓ hosts_placed (int): 3')
  expect(cardLines(c)).toContain('– token (string): hidden (sensitive)')
  expect(cardLines(c, true)).toContain('targets = ["alpha","beta"]')
  expect(cardLines(c, true)).toContain('token = hidden (sensitive)')
})

test('a sensitive output is never rendered, in any form, whatever the run printed', () => {
  const c = cards(SCHEMA, GOOD)
  expect(JSON.stringify(c)).not.toContain('s3cret')
  expect([...cardLines(c), ...cardLines(c, true)].join('\n')).not.toContain('s3cret')
})

test('a declared output the run did not report says so; an undeclared one is ignored', () => {
  const c = cards(SCHEMA, RUN({ complete: false, extra: 'x' }))
  expect(c.cards[1]).toMatchObject({ fact: 'not reported by this run', status: { word: 'not reported', symbol: '?' } })
  expect(JSON.stringify(c)).not.toContain('extra')
})

test('numbers keep their original text: a huge integer or long float is never rounded', () => {
  const c = cards(SCHEMA, RUN('{"hosts_placed":9007199254740993,"coverage":0.10000000000000000001,"targets":[18446744073709551616,1.50,-0,1e400]}'))
  const by = Object.fromEntries(c.cards.map(k => [k.title, k]))
  expect(by.hosts_placed.fact).toBe('9007199254740993')
  expect(by.coverage.raw).toBe('0.10000000000000000001')
  expect(by.targets.raw).toBe('[18446744073709551616,1.50,-0,1e400]')
})

test('a string that looks like a number stays a string, and a NUL cannot forge a number', () => {
  const c = cards(SCHEMA, RUN({ summary: '12345678901234567890', scope: '9' }))
  expect(c.cards[5].raw).toBe('"12345678901234567890"')
  for (const forged of [RUN('{"summary":"\\u0000n:7"}'), RUN({ summary: '\u0000n:7' })]) {
    expect(cardsOf(parseOutputs(SCHEMA), forged)).toMatchObject({ error: expect.stringMatching(/cannot read/) })
  }
})

test('__proto__ keys are data: no prototype is read or polluted, and an own one is shown', () => {
  const schema = JSON.stringify({ type: 'object', properties: JSON.parse('{"__proto__":{"type":"string"},"constructor":{"type":"string"}}') })
  const c = cards(schema, RUN('{"__proto__":"own","extra":{"__proto__":{"polluted":true}}}'))
  expect(c.cards.map(k => [k.title, k.fact])).toEqual([['__proto__', 'own'], ['constructor', 'not reported by this run']])
  expect(({} as Record<string, unknown>).polluted).toBeUndefined()
  // A nested own __proto__ key is shown as data too.
  expect(cards(SCHEMA, RUN('{"detail":{"__proto__":{"a":1}}}')).cards[2].raw).toBe('{"__proto__":{"a":1}}')
})

test('hidden and control characters are dropped from what is drawn and the card says it was cleaned', () => {
  const c = cards(SCHEMA, RUN({ summary: 'ok\u001b[2J‮​go', detail: { 'k\u001b': 'v\u0007' } }))
  expect(c.cards[5]).toMatchObject({ fact: 'ok[2Jgo', cut: true })
  expect(c.cards[2]).toMatchObject({ raw: '{"k":"v"}', cut: true })
  expect(cardLines(c).join('\n')).toMatch(/summary \(string\): ok\[2Jgo \(cut or cleaned\)/)
  expect(JSON.stringify(cardLines(c, true))).not.toMatch(/[\u0000-\u0008\u001b‮​]/)
})

test('values are bounded in length, depth, width and total, and every cut is marked', () => {
  const long = cards(SCHEMA, RUN({ summary: 'x'.repeat(5000) })).cards[5]
  expect(long.fact).toHaveLength(MAX_FACT)
  expect(long.raw.length).toBeLessThanOrEqual(MAX_RAW)
  expect(long.cut).toBe(true)
  const deep = cards(SCHEMA, RUN('{"detail":' + '{"a":'.repeat(30) + '1' + '}'.repeat(30) + '}')).cards[2]
  expect(deep.raw).toContain('…')
  expect(deep.cut).toBe(true)
  const wide = cards(SCHEMA, RUN({ targets: Array.from({ length: 500 }, (_, i) => i) })).cards[6]
  expect(wide.raw.split(',').length).toBeLessThanOrEqual(22)
  expect(wide.cut).toBe(true)
  const props = Object.fromEntries(Array.from({ length: 40 }, (_, i) => [`o${i}`, { type: 'string' }]))
  const many = cards(JSON.stringify({ type: 'object', properties: props }), RUN(Object.fromEntries(Object.keys(props).map(k => [k, 'y'.repeat(900)]))))
  expect(many.cards).toHaveLength(MAX_CARDS)
  expect(many.more).toBe(40 - MAX_CARDS)
  expect(many.cards.reduce((n, k) => n + k.raw.length, 0)).toBeLessThanOrEqual(MAX_TOTAL_RAW + MAX_CARDS)
  expect(cardLines(many).at(-1)).toBe('and 16 more declared outputs not shown')
})

for (const [label, schema] of [
  ['not JSON', 'nope'],
  ['an array', '[1]'],
  ['a non-object schema', '{"type":"string"}'],
  ['properties that are not a map', '{"type":"object","properties":[1]}'],
  ['an oversize schema', `{"type":"object","title":"${'x'.repeat(300000)}"}`],
] as const) {
  test(`a malformed schema is an error, never guessed cards: ${label}`, () => {
    expect(cardsOf(parseOutputs(schema), GOOD)).toHaveProperty('error')
  })
}

for (const [label, run, truncated] of [
  ['not JSON', 'COMPLETED workflow x', false],
  ['an array', '[1]', false],
  ['a runOutputs that is not a map', '{"runOutputs":[1]}', false],
  ['truncated by the host', GOOD, true],
  ['oversize', RUN({ summary: 'x'.repeat(70000) }), false],
] as const) {
  test(`a run document the cards cannot read is an error: ${label}`, () => {
    expect(cardsOf(parseOutputs(SCHEMA), run, truncated)).toHaveProperty('error')
  })
}

test('no declared outputs is no cards, whatever the run printed', () => {
  expect(cardsOf(parseOutputs('{"type":"object","properties":{}}'), 'COMPLETED workflow x')).toEqual({ cards: [], more: 0 })
})

// The pane: the cards appear after a confirmed run, from one cached schema read.

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 80 } } as const
const entry = (name: string) => ({ name, kind: 'file', size: 1, mtimeMs: 1, isLink: false })
const texts = async (ui: any) => (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')
const INPUTS = JSON.stringify({ type: 'object', properties: {} })

const session = async ($: any, on: any, outputs: { exitCode: number; stdout: string; stderr: string }, run = GOOD) => {
  const seen: string[][] = []
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    const reply = e.argv[1] === 'run' ? { exitCode: 0, stdout: run, stderr: '' } : e.argv.includes('--schema=outputs') ? outputs : e.argv[1] === 'compile' ? { exitCode: 0, stdout: INPUTS, stderr: '' } : { exitCode: 1, stdout: '', stderr: 'none' }
    return { value: { ...reply, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('fs.list', () => ({ value: [entry('a.flow.yaml')] }))
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })
  await ui.select({ plugin: 'flowstate', key: 'run-file', value: 'a.flow.yaml' })
  return { ui, seen, run: async () => { await ui.press({ key: 'run-local' }); await ui.press({ key: 'confirm-run' }) } }
}
const ok = (stdout: string) => ({ exitCode: 0, stdout, stderr: '' })
const outputCompiles = (seen: string[][]) => seen.filter(a => a.includes('--schema=outputs'))

test('a succeeded run draws the cards, hides the sensitive one, drops the raw document, and toggles raw JSON', async ($, on) => {
  const { ui, seen, run } = await session($, on, ok(SCHEMA))
  expect(outputCompiles(seen)).toEqual([]) // nothing is read before a run
  await run()
  expect(outputCompiles(seen)).toEqual([['flow', 'compile', '-o', 'json', '--schema=outputs', '--', 'a.flow.yaml']])
  let all = await texts(ui)
  expect(all).toMatch(/✓ reported\s+hosts_placed\s+\(int\)/)
  expect(all).toMatch(/hidden \(sensitive\)/)
  expect(all).not.toMatch(/s3cret|"steps"/)
  await ui.press({ key: 'outputs-raw' })
  all = await texts(ui)
  expect(all).toMatch(/targets = \["alpha","beta"\]/)
  expect(all).not.toMatch(/s3cret/)
  await ui.press({ key: 'outputs-raw' })
  expect(await texts(ui)).toMatch(/placed 3/)
  await ui.unmount()
})

test('the outputs schema is read once per file and modification time, failures included', async ($, on) => {
  const { ui, seen, run } = await session($, on, { exitCode: 1, stdout: '', stderr: 'a.flow.yaml:1:1: bad\n' })
  await run()
  await run()
  expect(outputCompiles(seen)).toHaveLength(1)
  expect(await texts(ui)).toMatch(/Outputs not shown \(a\.flow\.yaml:1:1: bad\)/)
  await ui.unmount()
})

test('when cards cannot be made nothing from the run document is shown, so a sensitive value cannot leak', async ($, on) => {
  const { ui, run } = await session($, on, ok(SCHEMA), 'not the run document s3cret')
  await run()
  const all = await texts(ui)
  expect(all).toMatch(/Outputs not shown \(the run printed a document the cards cannot read\)/)
  expect(all).not.toMatch(/s3cret/)
  await ui.unmount()
})
