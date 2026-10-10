import { expect, test } from 'claude-code/testing'

import { COPY, RUN_HINT, labelsFor, relPath, sectionTitle } from '../hooks/look'
import { COLOR } from '../hooks/vocab'

test('a section title is a bar, the name, an optional dim chip and a modest rule', () => {
  const s = sectionTitle('Runs', 3)
  expect(s).toMatchObject({ title: '▍ Runs', tone: 'active', chip: '3' })
  expect(COLOR[s.tone]).toBe('blue')
  expect(s.rule).toMatch(/^─+$/)
  expect(s.rule.length).toBeGreaterThanOrEqual(12)
  expect(s.rule.length).toBeLessThanOrEqual(28)
  expect(sectionTitle('Graph').chip).toBe('')
  expect(sectionTitle('Flowfiles', '2 invalid').chip).toBe('2 invalid')
})

test('a title and chip are cleaned and bounded', () => {
  const s = sectionTitle('Ru\u001b[31mns\n`x`'.repeat(30), 'a\u0007b'.repeat(40))
  expect(s.title.length).toBeLessThanOrEqual(42)
  expect(s.chip.length).toBeLessThanOrEqual(16)
  expect(`${s.title}${s.chip}`).not.toMatch(/[\u0000-\u001f]/)
  expect(s.rule.length).toBeLessThanOrEqual(28)
  expect(sectionTitle('Runs', NaN).chip).toBe('')
  expect(sectionTitle('Runs', 2.9).chip).toBe('2')
})

test('relPath never shows an absolute path', () => {
  expect(relPath('/private/tmp/flow-demo/hello.flow.yaml')).toBe('hello.flow.yaml')
  expect(relPath('/home/u/work/a.flow.yaml', '/home/u/work')).toBe('a.flow.yaml')
  expect(relPath('/home/u/work/workflows/a.flow.yaml', '/home/u/work/')).toBe('workflows/a.flow.yaml')
  expect(relPath('/home/u/workshop/a.flow.yaml', '/home/u/work')).toBe('a.flow.yaml')
  expect(relPath('/home/u/work', '/home/u/work')).toBe('work')
  expect(relPath('C:\\Users\\me\\proj\\a.flow.yaml')).toBe('a.flow.yaml')
  expect(relPath('C:\\Users\\me\\proj\\a.flow.yaml', 'C:\\Users\\me\\proj')).toBe('a.flow.yaml')
  expect(relPath('c:\\users\\me\\proj\\sub\\a.flow.yaml', 'C:\\Users\\me\\proj')).toBe('sub/a.flow.yaml')
  for (const p of ['/private/tmp/x/a.flow.yaml', 'C:\\x\\a.flow.yaml', '/a/b/c/d/e/f.flow.yaml', '//srv//share//a.flow.yaml']) {
    expect(relPath(p)).not.toMatch(/^(\/|[A-Za-z]:)/)
  }
})

test('relative paths stay, minus ./, and escapes shrink to the base name', () => {
  expect(relPath('a.flow.yaml')).toBe('a.flow.yaml')
  expect(relPath('./workflows/a.flow.yaml')).toBe('workflows/a.flow.yaml')
  expect(relPath('workflows//a.flow.yaml')).toBe('workflows/a.flow.yaml')
  expect(relPath('../secret/a.flow.yaml')).toBe('a.flow.yaml')
  expect(relPath('a/../../b.flow.yaml')).toBe('b.flow.yaml')
})

test('empty and non-string input is nothing, not a crash', () => {
  expect(relPath('')).toBe('')
  expect(relPath('   ')).toBe('')
  expect(relPath(undefined)).toBe('')
  expect(relPath(42)).toBe('')
  expect(relPath('/')).toBe('')
})

test('control characters and backticks are cleaned or inert; huge paths are bounded and keep the base name', () => {
  expect(relPath('/tmp/a\u001b[2Jb\n.flow.yaml')).not.toMatch(/[\u0000-\u001f]/)
  expect(relPath('/tmp/\u202eevil.flow.yaml')).toBe('evil.flow.yaml')
  expect(relPath('/tmp/`rm -rf`.flow.yaml')).toBe('`rm -rf`.flow.yaml')
  const out = relPath(`${'d/'.repeat(5000)}end.flow.yaml`)
  expect(out.length).toBeLessThanOrEqual(48)
  expect(out.endsWith('end.flow.yaml')).toBe(true)
  const long = relPath(`/tmp/${'n'.repeat(5000)}.flow.yaml`)
  expect(long.length).toBeLessThanOrEqual(48)
  expect(long.endsWith('.flow.yaml')).toBe(true)
  expect(relPath(`${'a/'.repeat(100)}b.flow.yaml`).length).toBeLessThanOrEqual(48)
})

test('empty-state copy is short, names one next action, and has no mechanism talk', () => {
  for (const line of [...Object.values(COPY), RUN_HINT]) {
    expect(line.length).toBeLessThanOrEqual(60)
    expect(line).not.toMatch(/flow run local|flow graph|no server/)
  }
  expect(COPY.runs).toBe('No runs yet. Run a Flowfile below.')
  expect(COPY.noFlowfile).toBe('No Flowfile here. Try /flowstate:new <what you want>.')
  expect(COPY.graph).toBe('Pick a Flowfile to see its steps.')
  expect(COPY.flowfiles).toBe('Edit a Flowfile and it shows up here.')
})

test('labelsFor keeps the base name when unique and adds parents only where names collide', () => {
  expect(labelsFor(['/p/a.flow.yaml', '/p/b.flow.yaml'])).toEqual(['a.flow.yaml', 'b.flow.yaml'])
  expect(labelsFor(['/project/a.flow.yaml', '/project/workflows/a.flow.yaml'])).toEqual(['project/a.flow.yaml', 'workflows/a.flow.yaml'])
  expect(labelsFor(['/x/one/a.flow.yaml', '/x/two/a.flow.yaml', '/x/two/b.flow.yaml'])).toEqual(['one/a.flow.yaml', 'two/a.flow.yaml', 'b.flow.yaml'])
  // Three-way collision, two of which share a parent name: more segments are added.
  expect(labelsFor(['/r/a/x/f.yaml', '/r/b/x/f.yaml', '/r/c/f.yaml'])).toEqual(['a/x/f.yaml', 'b/x/f.yaml', 'c/f.yaml'])
})

test('labelsFor is stable for identical paths, handles Windows separators and empty input', () => {
  expect(labelsFor(['/p/a.yaml', '/p/a.yaml'])).toEqual(['a.yaml', 'a.yaml'])
  expect(labelsFor(['C:\\p\\a.yaml', 'C:\\p\\w\\a.yaml'])).toEqual(['p/a.yaml', 'w/a.yaml'])
  expect(labelsFor([])).toEqual([])
  expect(labelsFor(['', 42])).toEqual(['', ''])
})

test('labelsFor never yields an absolute path and is bounded, whatever the input', () => {
  const long = 'n'.repeat(500)
  const out = labelsFor([`/a/${long}/f.yaml`, `/b/${long}/f.yaml`, '/c/\u001b[2Jx\n/f.yaml', `${'d/'.repeat(3000)}z.yaml`, '//srv//a.yaml', 'C:\\a.yaml'])
  for (const l of out) {
    expect(l.length).toBeLessThanOrEqual(48)
    expect(l).not.toMatch(/^(\/|[A-Za-z]:)|[\u0000-\u001f]/)
  }
  expect(out[3].endsWith('z.yaml')).toBe(true)
})
