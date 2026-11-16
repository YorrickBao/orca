import { dirname } from 'node:path'

import { decodeJsonlTail, normalizedTranscriptPath } from './codex-rollout-jsonl-cursor'
import { isCodexStatusTranscriptLine } from './codex-status-transcript-line'
import {
  childDayDirectory,
  readSubagentActivity,
  resolveChildTranscript,
  type CodexSubagentTranscriptState
} from './codex-subagent-transcript'
import {
  pendingCodexSubagentIdentityPaths,
  type CodexSubagentRoster
} from './codex-subagent-roster'
import {
  fetchTranscriptDirectory,
  fetchTranscriptHead,
  fetchTranscriptTail,
  peekTranscriptTail,
  TRANSCRIPT_LINE_MAX_BYTES,
  type TranscriptReads
} from '../../transcripts/transcript-read-ahead'

export type CodexTranscriptReadHint = {
  /** The rollout a root hook names, which may replace the tracked parent. */
  parentPath?: string
  /** A child rollout whose approval reviewer a child PermissionRequest reads. */
  reviewerPath?: string
  /** The hook's child: its own rollout names it, and a SubagentStart adds it to the roster first. */
  child?: { agentId: string; path?: string; starting: boolean }
  /** SessionStart replaces the pane's transcript state, so the parent restarts at offset 0. */
  restart?: boolean
  /** An owner challenge may retire provider cursors before normalization. */
  mayReset?: boolean
}

/** Fetches into `reads`, off the event loop, every rollout read the synchronous reconcile, reviewer and identity code make next. */
export async function prefetchCodexTranscripts(
  reads: TranscriptReads,
  transcript: CodexSubagentTranscriptState | undefined,
  roster: CodexSubagentRoster | undefined,
  hint: CodexTranscriptReadHint
): Promise<void> {
  const jobs: Promise<void>[] = []
  const tracked = transcript?.parent.filePath
  const parentPath = normalizedTranscriptPath(hint.parentPath) ?? tracked
  if (parentPath) {
    const continuing = Boolean(transcript && parentPath === tracked && !hint.restart)
    jobs.push(fetchParentAndChildren(reads, transcript, roster, parentPath, continuing, hint))
  }
  const reviewerPath = normalizedTranscriptPath(hint.reviewerPath)
  if (reviewerPath && reviewerPath !== parentPath) {
    const offset = transcript?.reviewerCursorsByPath.get(reviewerPath)?.offset ?? 0
    jobs.push(fetchTranscriptTail(reads, reviewerPath, offset))
    if (hint.mayReset && offset > 0) {
      jobs.push(fetchTranscriptTail(reads, reviewerPath, 0))
    }
  }
  const heads = new Set(roster ? pendingCodexSubagentIdentityPaths(roster) : [])
  const childPath = normalizedTranscriptPath(hint.child?.path)
  const known = hint.child && roster?.get(hint.child.agentId.trim())
  if (
    childPath &&
    known?.identityTranscriptPath !== childPath &&
    known?.identityMissKey !== childPath
  ) {
    heads.add(childPath)
  }
  for (const path of heads) {
    jobs.push(fetchTranscriptHead(reads, path))
  }
  await Promise.all(jobs)
}

async function fetchParentAndChildren(
  reads: TranscriptReads,
  transcript: CodexSubagentTranscriptState | undefined,
  roster: CodexSubagentRoster | undefined,
  parentPath: string,
  continuing: boolean,
  hint: CodexTranscriptReadHint
): Promise<void> {
  const parent =
    continuing && transcript
      ? { ...transcript.parent }
      : { filePath: parentPath, offset: 0, carry: '' }
  const jobs: Promise<void>[] = [fetchTranscriptTail(reads, parentPath, parent.offset)]
  if (hint.mayReset && parent.offset > 0) {
    jobs.push(fetchTranscriptTail(reads, parentPath, 0))
  }
  const unresolved = new Map<string, number>()
  for (const [id, child] of continuing ? (transcript?.subagents ?? []) : []) {
    if (child.filePath) {
      jobs.push(fetchTranscriptTail(reads, child.filePath, child.offset))
    } else {
      unresolved.set(id, child.startedAt)
    }
  }
  // Hook-only and restored roster children join the reconcile, after a new parent finishes the tracked ones.
  for (const [id, child] of roster ?? []) {
    if (!transcript?.subagents.has(id)) {
      unresolved.set(id, child.startedAt)
    }
  }
  if (hint.child?.starting && !transcript?.subagents.get(hint.child.agentId)?.filePath) {
    unresolved.set(hint.child.agentId, Date.now())
  }
  await jobs[0]
  // Why: a child the parent announces in these bytes is reconciled in the same pass, as before.
  const tail = peekTranscriptTail(reads, parentPath, parent.offset)
  for (const recordValue of (tail && tail !== 'refused'
    ? decodeJsonlTail(parent, tail, isCodexStatusTranscriptLine)
    : undefined) ?? []) {
    const activity = readSubagentActivity(recordValue)
    if (activity && activity.kind !== 'interrupted' && !unresolved.has(activity.id)) {
      const child = continuing ? transcript?.subagents.get(activity.id) : undefined
      if (!child?.filePath) {
        unresolved.set(activity.id, child?.startedAt ?? activity.startedAt)
      }
    }
  }
  if (unresolved.size > 0) {
    jobs.push(fetchUnresolvedChildren(reads, parentPath, unresolved))
  }
  await Promise.all(jobs)
}

async function fetchUnresolvedChildren(
  reads: TranscriptReads,
  parentPath: string,
  children: Map<string, number>
): Promise<void> {
  const directories = new Set([dirname(parentPath)])
  for (const startedAt of children.values()) {
    const childDir = childDayDirectory(parentPath, startedAt)
    if (childDir) {
      directories.add(childDir)
    }
  }
  await Promise.all([...directories].map((directory) => fetchTranscriptDirectory(reads, directory)))
  const entriesByDirectory = new Map(reads.directories)
  await Promise.all(
    [...children].flatMap(([id, startedAt]) => {
      const filePath = resolveChildTranscript(parentPath, id, startedAt, entriesByDirectory)
      return filePath && filePath !== 'unfetched' ? [fetchChildTranscript(reads, filePath)] : []
    })
  )
}

async function fetchChildTranscript(reads: TranscriptReads, filePath: string): Promise<void> {
  await fetchTranscriptTail(reads, filePath, 0)
  const tail = peekTranscriptTail(reads, filePath, 0)
  if (tail && typeof tail === 'object') {
    // Newly discovered children can owe an identity retry in this same reconcile.
    if (tail.start === 0) {
      reads.heads.set(filePath, {
        size: tail.size,
        bytes: tail.bytes.subarray(0, TRANSCRIPT_LINE_MAX_BYTES)
      })
    } else {
      await fetchTranscriptHead(reads, filePath)
    }
  }
}
