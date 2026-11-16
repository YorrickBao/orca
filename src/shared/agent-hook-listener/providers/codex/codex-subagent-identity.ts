import { extname, isAbsolute } from 'node:path'
import { readJsonlFirstRecord, record } from './codex-rollout-jsonl-cursor'
import { codexSubagentPathLabel } from './codex-agent-path'

function text(value: unknown): string | undefined {
  return typeof value === 'string' ? value.trim() || undefined : undefined
}

/** Immutable provider metadata names the child even when its parent emits no activity items. `size` (null while the file is absent) keys a retry of a pending first line. */
export function readCodexSubagentIdentity(
  transcriptPath: string,
  agentId: string
):
  | { size: number | null; identity: { description?: string } | 'pending' | undefined }
  | 'unreadable'
  | 'unfetched' {
  if (!isAbsolute(transcriptPath) || extname(transcriptPath) !== '.jsonl') {
    return { size: null, identity: undefined }
  }
  const head = readJsonlFirstRecord(transcriptPath)
  if (head === 'unreadable' || head === 'unfetched') {
    return head
  }
  if (head === 'pending') {
    return { size: null, identity: 'pending' }
  }
  if (head.record === 'pending') {
    return { size: head.size, identity: 'pending' }
  }
  const payload = record(head.record?.payload)
  if (head.record?.type !== 'session_meta' || payload?.id !== agentId) {
    return { size: head.size, identity: undefined }
  }
  const spawn = record(record(record(payload.source)?.subagent)?.thread_spawn)
  return {
    size: head.size,
    identity: {
      description:
        codexSubagentPathLabel(text(spawn?.agent_path) ?? text(payload.agent_path) ?? null) ??
        text(spawn?.agent_nickname) ??
        text(payload.agent_nickname)
    }
  }
}
