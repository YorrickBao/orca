import { afterEach, describe, expect, it, vi } from 'vitest'
import type * as nodeFs from 'node:fs'
import { appendFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const syncCalls = vi.hoisted(() => ({ counting: false, names: [] as string[] }))

vi.mock('node:fs', async (importOriginal) => {
  const actual = await importOriginal<typeof nodeFs>()
  const counted = Object.fromEntries(
    Object.entries(actual).map(([name, value]) =>
      name.endsWith('Sync') && typeof value === 'function'
        ? [
            name,
            (...args: unknown[]) => {
              if (syncCalls.counting) {
                syncCalls.names.push(name)
              }
              return (value as (...inner: unknown[]) => unknown)(...args)
            }
          ]
        : [name, value]
    )
  )
  return { ...counted, default: counted }
})

import { createHookListenerState } from '../listener-state'
import { normalizeHookPayload } from '../normalization/normalize-hook-payload'
import { seedLegacyAgentStatusForTests } from '../legacy-agent-status-test-seed'
import { pollCodexTranscriptStatus } from '../providers/codex/codex-transcript-poll'
import { makePaneKey } from '../../workspace-layout/stable-pane-id'
import { enterHookTranscriptTurn, enterTranscriptPollTurn } from './hook-transcript-read-ahead'

const PANE_KEY = makePaneKey('tab-1', '11111111-1111-4111-8111-111111111111')

function line(value: unknown): string {
  return `${JSON.stringify(value)}\n`
}

describe('Codex transcript reads', () => {
  const dirs: string[] = []

  afterEach(() => {
    syncCalls.counting = false
    syncCalls.names.length = 0
    for (const dir of dirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  // Why: one synchronous stat of a rollout on a slow disk froze every keystroke for 417 ms.
  it.each([1, 8])(
    'make no synchronous filesystem call from a hook or a poll with %i children',
    async (childCount) => {
      const root = mkdtempSync(join(tmpdir(), 'codex-read-ahead-'))
      dirs.push(root)
      const now = new Date()
      const day = join(
        root,
        String(now.getFullYear()),
        String(now.getMonth() + 1).padStart(2, '0'),
        String(now.getDate()).padStart(2, '0')
      )
      mkdirSync(day, { recursive: true })
      const parentPath = join(day, 'rollout-parent.jsonl')
      let parent = line({ type: 'turn_context', payload: { approvals_reviewer: 'auto_review' } })
      for (let index = 0; index < childCount; index++) {
        const id = `019fa65f-3144-7151-9c02-${String(index).padStart(12, '0')}`
        parent += line({
          type: 'event_msg',
          payload: {
            type: 'sub_agent_activity',
            agent_thread_id: id,
            agent_path: `/root/task_${index}`,
            kind: 'started'
          }
        })
        writeFileSync(
          join(day, `rollout-child-${id}.jsonl`),
          line({ type: 'session_meta', payload: { id } }) +
            line({ type: 'event_msg', payload: { type: 'task_started' } })
        )
      }
      writeFileSync(parentPath, parent)
      const state = createHookListenerState()
      const body = {
        paneKey: PANE_KEY,
        payload: {
          hook_event_name: 'PermissionRequest',
          session_id: 'root-session',
          tool_name: 'Bash',
          transcript_path: parentPath
        }
      }

      syncCalls.counting = true
      const hookTurn = await enterHookTranscriptTurn(state, 'codex', body)
      const event = hookTurn.apply(() => normalizeHookPayload(state, 'codex', body, 'production'))
      hookTurn.release()
      expect(event?.payload.subagents).toHaveLength(childCount)
      // The rollout says Codex's own reviewer answers this approval.
      expect(event?.payload.state).toBe('working')
      seedLegacyAgentStatusForTests(state, event!)
      const pollTurn = await enterTranscriptPollTurn(state, 'codex', PANE_KEY, body)
      pollTurn.apply(() => pollCodexTranscriptStatus(state, event!, body, 'production'))
      pollTurn.release()
      syncCalls.counting = false

      expect(syncCalls.names).toEqual([])
    }
  )

  it('applies a pending hook, its poll and the next hook in pane order', async () => {
    const root = mkdtempSync(join(tmpdir(), 'codex-transcript-order-'))
    dirs.push(root)
    const filePath = join(root, 'parent.jsonl')
    writeFileSync(filePath, '')
    const state = createHookListenerState()
    const body = (turnId: string) => ({
      paneKey: PANE_KEY,
      payload: {
        hook_event_name: 'UserPromptSubmit',
        session_id: 'root-session',
        turn_id: turnId,
        prompt: turnId,
        transcript_path: filePath
      }
    })
    const first = await enterHookTranscriptTurn(state, 'codex', body('first'))
    const states: string[] = []
    let original: NonNullable<ReturnType<typeof normalizeHookPayload>>
    const polled = enterTranscriptPollTurn(state, 'codex', PANE_KEY, body('first')).then((turn) => {
      try {
        const event = turn.apply(() => pollCodexTranscriptStatus(state, original, body('first'), 'production'))
        states.push(event!.payload.state)
        seedLegacyAgentStatusForTests(state, event!)
      } finally {
        turn.release()
      }
    })
    const next = enterHookTranscriptTurn(state, 'codex', body('next')).then((turn) => {
      try {
        const event = turn.apply(() => normalizeHookPayload(state, 'codex', body('next'), 'production'))
        states.push(event!.payload.state)
      } finally {
        turn.release()
      }
    })
    try {
      original = first.apply(() => normalizeHookPayload(state, 'codex', body('first'), 'production'))!
      states.push(original.payload.state)
      seedLegacyAgentStatusForTests(state, original)
      appendFileSync(filePath, line({ type: 'event_msg', payload: { type: 'task_complete', turn_id: 'first' } }))
    } finally {
      first.release()
    }
    await Promise.all([polled, next])
    expect(states).toEqual(['working', 'done', 'working'])
  })

})
