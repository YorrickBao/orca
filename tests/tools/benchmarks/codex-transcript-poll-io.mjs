// Run with node --import tsx; pass the pre-change cursor module as the first argument.
import assert from 'node:assert/strict'
import fs from 'node:fs'
import fsPromises from 'node:fs/promises'
import { syncBuiltinESMExports } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { performance } from 'node:perf_hooks'
import { setTimeout as delay } from 'node:timers/promises'

const baseline = await import(pathToFileURL(process.argv[2]).href)
const { createHookListenerState } = await import('../../../src/shared/agent-hook-listener/listener-state.ts')
const { normalizeHookPayload } = await import('../../../src/shared/agent-hook-listener/normalization/normalize-hook-payload.ts')
const { seedLegacyAgentStatusForTests } = await import('../../../src/shared/agent-hook-listener/legacy-agent-status-test-seed.ts')
const { pollCodexTranscriptStatus } = await import('../../../src/shared/agent-hook-listener/providers/codex/codex-transcript-poll.ts')
const { enterHookTranscriptTurn, enterTranscriptPollTurn } = await import('../../../src/shared/agent-hook-listener/transcripts/hook-transcript-read-ahead.ts')

const root = fs.mkdtempSync(join(tmpdir(), 'codex-poll-io-bench-'))
const line = (payload) => JSON.stringify(payload) + '\n'
const started = line({ type: 'event_msg', payload: { type: 'task_started' } })
const filler = line({ type: 'response_item', payload: { text: 'x'.repeat(16 * 1024) } })
const panes = []
const state = createHookListenerState()
for (let pane = 0; pane < 50; pane++) {
  const directory = join(root, String(pane))
  fs.mkdirSync(directory)
  const paths = [join(directory, 'parent.jsonl')]
  let parent = line({ type: 'turn_context', payload: { approvals_reviewer: 'auto_review' } })
  for (let child = 0; child < 8; child++) {
    const id = `child-${child}`
    parent += line({ type: 'event_msg', payload: { type: 'sub_agent_activity', agent_thread_id: id, kind: 'started', agent_path: `/root/task_${child}` } })
    const childPath = join(directory, `rollout-${id}.jsonl`)
    fs.writeFileSync(childPath, filler + started)
    paths.push(childPath)
  }
  fs.writeFileSync(paths[0], filler.repeat(16) + parent)
  const paneKey = `tab-${pane}:11111111-1111-4111-8111-111111111111`
  const body = { paneKey, payload: { hook_event_name: 'PermissionRequest', session_id: `session-${pane}`, tool_name: 'Bash', transcript_path: paths[0] } }
  const turn = await enterHookTranscriptTurn(state, 'codex', body)
  let original
  try {
    original = turn.apply(() => normalizeHookPayload(state, 'codex', body, 'production'))
    assert.equal(original?.payload.state, 'working')
    assert.equal(original?.payload.subagents?.length, 8)
    seedLegacyAgentStatusForTests(state, original)
  } finally {
    turn.release()
  }
  panes.push({ paneKey, body, original, paths })
}

const sleepWord = new Int32Array(new SharedArrayBuffer(4))
let latency = 0
let syncCalls = 0
const restore = []
for (const name of ['statSync', 'openSync', 'readSync', 'closeSync']) {
  const original = fs[name]
  fs[name] = (...args) => {
    syncCalls++
    if (name === 'statSync' && latency) Atomics.wait(sleepWord, 0, 0, latency)
    return original(...args)
  }
  restore.push(() => { fs[name] = original })
}
syncBuiltinESMExports()
const open = fsPromises.open
fsPromises.open = async (...args) => {
  const handle = await open(...args)
  const stat = handle.stat.bind(handle)
  handle.stat = async (...statArgs) => {
    if (latency) await delay(latency)
    return stat(...statArgs)
  }
  return handle
}
restore.push(() => { fsPromises.open = open })

async function measure(mode, count, slowStatMs) {
  latency = slowStatMs
  const selected = panes.slice(0, count)
  // Model one append arriving between polls; setup is excluded from the measurement.
  for (const pane of selected) {
    for (const path of pane.paths) fs.appendFileSync(path, started)
  }
  const oldCursors = selected.flatMap((pane) => pane.paths.map((path) => ({ filePath: path, offset: fs.statSync(path).size - Buffer.byteLength(started), carry: '' })))
  let previous = performance.now()
  let maxLag = 0
  const heartbeat = setInterval(() => {
    const now = performance.now()
    maxLag = Math.max(maxLag, now - previous - 1)
    previous = now
  }, 1)
  await delay(10)
  maxLag = 0
  previous = performance.now()
  syncCalls = 0
  const start = performance.now()
  if (mode === 'old') {
    // The old poll reads the root and every tracked child synchronously.
    for (const cursor of oldCursors) baseline.readJsonlCursor(cursor)
  } else {
    await Promise.all(selected.map(async (pane) => {
      const turn = await enterTranscriptPollTurn(state, 'codex', pane.paneKey, pane.body)
      try {
        const event = turn.apply(() => pollCodexTranscriptStatus(state, pane.original, pane.body, 'production'))
        assert.equal(event?.payload.state, 'working')
        assert.equal(event?.payload.subagents?.length, 8)
      } finally {
        turn.release()
      }
    }))
  }
  const wallMs = performance.now() - start
  await delay(5)
  clearInterval(heartbeat)
  const row = { mode, panes: count, childrenPerPane: 8, slowStatMs, wallMs: +wallMs.toFixed(2), maxEventLoopBlockMs: +Math.max(0, maxLag).toFixed(2), syncCalls }
  console.log(JSON.stringify(row))
  if (mode === 'new') assert.equal(syncCalls, 0)
}

try {
  for (const slowStatMs of [0, 20]) {
    for (const count of [5, 50]) {
      for (const mode of ['old', 'new']) await measure(mode, count, slowStatMs)
    }
  }
} finally {
  for (const undo of restore) undo()
  syncBuiltinESMExports()
  fs.rmSync(root, { recursive: true, force: true })
}
