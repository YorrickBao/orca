import {
  prefetchCodexTranscripts,
  type CodexTranscriptReadHint
} from '../providers/codex/codex-transcript-prefetch'
import { prefetchMuseSessionLog } from '../providers/muse/muse-session-log'
import {
  admitTranscriptRound,
  applyTranscriptReads,
  TranscriptReads,
  type TranscriptReadPriority
} from './transcript-read-ahead'
import { readFirstString } from '../interactive-tool'
import type { AgentHookEventPayload } from '../listener-event'
import type { HookListenerState } from '../listener-state'
import { parseAgentHookJson } from '../request-body'
import { readString } from '../tool-input-preview'
import type { AgentHookSource } from '../transport/agent-hook-relay'

/** A pane's place in its hook and poll order, holding the rollout bytes it read until it has applied them. */
export type TranscriptTurn = {
  /** Runs the synchronous status step with this turn's reads; nothing else may consume them. */
  apply: <T>(step: () => T) => T
  release: () => void
}

const turns = new WeakMap<HookListenerState, Map<string, Promise<void>>>()

const noTurn: TranscriptTurn = {
  apply: (step) => applyTranscriptReads(undefined, step),
  release: () => {}
}

/** Waits until this pane's earlier hooks and polls applied, then reads; the caller applies and releases. */
function enterPaneTranscriptTurn(
  state: HookListenerState,
  paneKey: string,
  read: ((reads: TranscriptReads) => Promise<void>) | undefined,
  priority: TranscriptReadPriority
): Promise<TranscriptTurn> | undefined {
  let panes = turns.get(state)
  const previous = panes?.get(paneKey)
  if (!read && !previous) {
    return undefined
  }
  if (!panes) {
    panes = new Map()
    turns.set(state, panes)
  }
  let release = (): void => {}
  const released = new Promise<void>((resolve) => {
    release = resolve
  })
  const owner = panes
  owner.set(paneKey, released)
  void released.then(() => {
    if (owner.get(paneKey) === released) {
      owner.delete(paneKey)
    }
  })
  const reads = new TranscriptReads(priority)
  const turn: TranscriptTurn = {
    apply: (step) => applyTranscriptReads(reads, step),
    release
  }
  // A read failure must not drop the hook; normalization keeps its existing unreadable-file behavior.
  return (previous ?? Promise.resolve())
    .then(() => (read ? admitTranscriptRound(reads, () => read(reads)) : undefined))
    .catch(() => undefined)
    .then(() => turn)
}

function prefetchPaneTranscripts(
  reads: TranscriptReads,
  state: HookListenerState,
  source: AgentHookSource,
  paneKey: string,
  hook: Record<string, unknown> | undefined,
  statusOf: (paneKey: string) => AgentHookEventPayload | undefined
): Promise<void> {
  if (source === 'muse') {
    const log = state.musePaneStateByPaneKey.get(paneKey)?.log
    const sessionId = (hook && readString(hook, 'session_id')) ?? log?.sessionId
    return sessionId ? prefetchMuseSessionLog(reads, sessionId, log) : Promise.resolve()
  }
  return prefetchCodexTranscripts(
    reads,
    state.codexSubagentTranscriptByPaneKey.get(paneKey),
    state.codexSubagentRosterByPaneKey.get(paneKey),
    hook ? codexHookReadHint(state, paneKey, hook, statusOf) : {}
  )
}

/** Mirrors which rollouts `normalizeCodexEvent` reads for this hook. */
function codexHookReadHint(
  state: HookListenerState,
  paneKey: string,
  hook: Record<string, unknown>,
  statusOf: (paneKey: string) => AgentHookEventPayload | undefined
): CodexTranscriptReadHint {
  const eventName = hook.hook_event_name ?? hook.hookEventName
  const agentId = readString(hook, 'agent_id')?.trim()
  const transcriptPath = readFirstString(hook, ['transcript_path', 'transcriptPath'])
  if (eventName === 'SubagentStart') {
    return {
      child: agentId ? { agentId, path: transcriptPath, starting: true } : undefined,
      parentPath:
        state.codexSubagentTranscriptByPaneKey.get(paneKey)?.parent.filePath ??
        statusOf(paneKey)?.providerSession?.transcriptPath
    }
  }
  if (!agentId) {
    return { parentPath: transcriptPath, restart: eventName === 'SessionStart' }
  }
  return {
    reviewerPath: transcriptPath,
    child: { agentId, path: transcriptPath, starting: false }
  }
}

function hookPayloadOf(
  body: unknown
): { paneKey: string; payload: Record<string, unknown> } | undefined {
  if (typeof body !== 'object' || body === null) {
    return undefined
  }
  const envelope = body as Record<string, unknown>
  const paneKey = typeof envelope.paneKey === 'string' ? envelope.paneKey.trim() : ''
  let payload = envelope.payload
  if (typeof payload === 'string') {
    try {
      payload = parseAgentHookJson(payload)
    } catch {
      return undefined
    }
  }
  return paneKey && typeof payload === 'object' && payload !== null
    ? {
        paneKey,
        payload: {
          ...(payload as Record<string, unknown>),
          hook_event_name:
            readFirstString(envelope, ['hook_event_name', 'hookEventName', 'hook_type', 'hookType']) ??
            (payload as Record<string, unknown>).hook_event_name ??
            (payload as Record<string, unknown>).hookEventName
        }
      }
    : undefined
}

/** Await before a hook's owner check, normalize inside `apply`, and release once applied, so each pane's hooks and polls apply in arrival order with rollout bytes read off the event loop. `statusOf` is the status map the hook normalizes against. */
export async function enterHookTranscriptTurn(
  state: HookListenerState,
  source: AgentHookSource,
  body: unknown,
  statusOf: (paneKey: string) => AgentHookEventPayload | undefined = (paneKey) =>
    state.lastStatusByPaneKey.get(paneKey)
): Promise<TranscriptTurn> {
  const hook = hookPayloadOf(body)
  if (!hook) {
    return noTurn
  }
  const reads = source === 'codex' || source === 'muse'
  const turn = enterPaneTranscriptTurn(
    state,
    hook.paneKey,
    reads
      ? (into) => prefetchPaneTranscripts(into, state, source, hook.paneKey, hook.payload, statusOf)
      : undefined,
    'hook'
  )
  return turn ? await turn : noTurn
}

/** Await before a transcript poll's synchronous step, run it inside `apply`, then release; Codex polls read every tracked rollout, Muse the hook's session log. */
export function enterTranscriptPollTurn(
  state: HookListenerState,
  source: 'codex' | 'muse',
  paneKey: string,
  body: unknown
): Promise<TranscriptTurn> {
  const hook = source === 'muse' ? hookPayloadOf(body)?.payload : undefined
  return (
    enterPaneTranscriptTurn(
      state,
      paneKey,
      (reads) =>
        prefetchPaneTranscripts(reads, state, source, paneKey, hook, (key) =>
          state.lastStatusByPaneKey.get(key)
        ),
      'poll'
    ) ?? Promise.resolve(noTurn)
  )
}
