import { expect, test } from 'claude-code/testing'
import { CHIPS, LastGood, chipFor, chipText } from '../hooks/chips'

const run = (id: string) => ({ workflowId: id, name: id, status: 'STATUS_RUNNING', startTime: null, closeTime: null })

test('the active chip is the one whose filter is exactly the text, and a typed filter lights none', () => {
  expect(chipFor('')?.label).toBe('All')
  expect(chipFor(' status == "FAILED" ')?.label).toBe('Failed')
  expect(chipFor('status == "FAILED" && name == "x"')).toBeUndefined()
})

test('the active chip reads without colour', () => {
  expect(chipText(CHIPS[1], true)).toBe('[Running]')
  expect(chipText(CHIPS[1], false)).toBe('Running')
})

test('a kept listing is recalled only under the filter it was read with', () => {
  const k = new LastGood()
  expect(k.recall('')).toBeUndefined()
  k.remember('', [run('a')])
  expect(k.recall('')?.[0].workflowId).toBe('a')
  expect(k.recall('status == "FAILED"')).toBeUndefined()
})
