import { atom, read, update } from 'claude-code'
import type { Engine, Register } from 'claude-code'

import type { FileReport } from '../types'
import { isFlowfile, parseReports, summarize } from './flowfile'

const PANE = 'flowstate'
const reports = atom({ plugin: 'flowstate', key: 'reports' } as const, [])

const validate = async (
  $: Engine,
  path: string,
): Promise<FileReport> => {
  try {
    const ran = await $.process.run(['flow', 'validate', '-o', 'jsonl', path], {
      timeoutMs: 20000,
    })
    const found = parseReports(ran.stdout).find(r => r.file === path)
    if (found) return found
    return { file: path, diagnostics: [], failure: ran.stderr.trim().split('\n')[0] || 'no report' }
  } catch (err) {
    return { file: path, diagnostics: [], failure: String(err) }
  }
}

export const register: Register = on => {
  on('session.start', async ($, e, next) => {
    await $.command.register({
      name: 'flowstate',
      description: 'Show the validation state of Flowfiles touched this session',
    })
    return next(e)
  })

  on('command.run', { command: 'flowstate' }, async $ => {
    await $.ui.open({ id: PANE, title: 'Flowstate' })
    return { text: 'Flowstate pane opened.' }
  })

  for (const tool of ['Edit', 'Write', 'MultiEdit'] as const) {
    on('tool.call', { tool }, async ($, e, next) => {
      const ran = await next(e)
      if (ran.deny !== undefined || ran.isError === true || !isFlowfile(e.file_path)) {
        return ran
      }

      const report = await validate($, e.file_path)
      await update($, reports, list => [
        ...list.filter(r => r.file !== report.file),
        report,
      ])
      const broken = report.diagnostics.length > 0 || report.failure !== undefined
      $.ui.status(broken ? `flowstate: ${report.diagnostics.length || '!'} problem(s)` : undefined)

      return broken ? { ...ran, context: [...(ran.context ?? []), summarize(report)] } : ran
    })
  }

  on('ui.render', { component: 'Pane', requestId: PANE }, async ($, e) => {
    const { Box, Text } = $.ui.resolve(e)
    const list = await read($, reports)

    return (
      <Box flexDirection="column">
        {list.length === 0 && <Text dimColor>No Flowfile edited yet this session.</Text>}
        {list.map(r => (
          <Box flexDirection="column">
            <Text bold>
              {r.failure ? 'could not check' : r.diagnostics.length === 0 ? 'valid' : 'invalid'}{' '}
              {r.file}
            </Text>
            {r.failure && <Text dimColor>  {r.failure}</Text>}
            {r.diagnostics.slice(0, 8).map(d => (
              <Text dimColor>
                {'  '}
                {d.line > 0 ? `${d.line}:${d.column} ` : ''}
                {d.message.slice(0, 120)}
              </Text>
            ))}
          </Box>
        ))}
      </Box>
    )
  })
}
