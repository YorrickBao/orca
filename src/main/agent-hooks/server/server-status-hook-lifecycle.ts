import {
  discardHookTranscriptTurns,
  enterHookTranscriptTurn
} from '../../../shared/agent-hook-listener/transcripts/hook-transcript-read-ahead'
import { parsePaneKey } from '../../../shared/workspace-layout/stable-pane-id'
import type { EnrichedAgentHookEventPayload } from './server-types'
import {
  drainAgentHookSpoolAsync,
  type SpoolRecord
} from '../../../shared/agent-hook-listener/transport/agent-hook-spool'
import { hookRootAnswerDeadline } from '../../../shared/agent-hook-listener/presence/hook-root-attempts'
import { AgentHookServerRuntimeEnv } from './server-runtime-env'
import { ClaudeAgentViewBridge } from '../../claude/agent-view/agent-view-bridge'
import type { IncomingMessage, ServerResponse } from 'node:http'
import type { AgentHookSource } from '../../../shared/agent-hook-listener/transport/agent-hook-relay'
import type { AgentHookEventPayload } from '../../../shared/agent-hook-listener/listener-event'
import { respondAfterAgentToolPluginHooks } from '../tool-events/agent-tool-plugin-hooks'
import { ClaudePermissionHookHolds } from './claude-permission-hook-hold'

export abstract class AgentHookServerStatusHookLifecycle extends AgentHookServerRuntimeEnv {
  readonly claudeAgentViews = new ClaudeAgentViewBridge()
  protected readonly claudePermissionHooks = new ClaudePermissionHookHolds()
  private statusHookInitialization: Promise<void> | null = null

  /** Answers a hook post: a worker's PermissionRequest waits for its coordinator's or person's answer;
   *  plugins see only calls status accepted, so a fenced or reused pane's old agent is skipped. */
  protected async respondToHook(
    req: IncomingMessage,
    res: ServerResponse,
    source: AgentHookSource,
    body: unknown,
    accepted: AgentHookEventPayload | null
  ): Promise<void> {
    if (accepted && source === 'claude' && this.claudePermissionHooks.hold(req, res, accepted)) {
      return
    }
    await respondAfterAgentToolPluginHooks(res, source, body, req.headers, accepted)
  }

  setStatusHooksEnabled(enabled: boolean): void {
    if (enabled === this.statusHooksEnabled) {
      return
    }
    this.statusHookGeneration += 1
    discardHookTranscriptTurns(this.state)
    if (!enabled) {
      this.claudeAgentViews.clear()
      this.flushStatusPersistSync()
      this.stopOpenCodeBinderLoop()
      for (const timer of this.assistantMessageRetryTimers.values()) {
        clearTimeout(timer)
      }
      this.assistantMessageRetryTimers.clear()
      this.clearAllTranscriptPolls()
    }
    this.statusHooksEnabled = enabled
    if (enabled && this.server) {
      void this.initializeStatusHookOwner().catch((error: unknown) => {
        console.error('[agent-hooks] status initialization failed', error)
      })
      this.startOpenCodeBinderLoop()
    }
  }

  protected initializeStatusHookOwner(): Promise<void> {
    this.statusHookInitialization ??= this.loadStatusHookOwner().finally(() => {
      this.statusHookInitialization = null
    })
    return this.statusHookInitialization
  }

  private async loadStatusHookOwner(): Promise<void> {
    const token = this.token
    const statusHookGeneration = this.statusHookGeneration
    if (!this.ownerStateInitialized) {
      // Hydrate before binding so the first live hook sees restored ownership.
      if (this.lastStatusFilePath) {
        this.hydrateLastStatusFromDisk()
      }
      this.captureHydratedAuthorityCommitments()
      // Drain before binding so replay cannot race live hooks.
      if (this.endpointDir) {
        const replayedPaneKeys = new Set<string>()
        // Why: records replay in order, so a per-record allowance would add up across panes.
        const answerBy = hookRootAnswerDeadline()
        await drainAgentHookSpoolAsync({
          endpointDir: this.endpointDir,
          getPersistedLaunchTokenHash: (paneKey) =>
            this.hydratedLaunchTokenHashByPaneKey.get(this.resolvePaneKeyAlias(paneKey)),
          ingest: async (record: SpoolRecord) => {
            if (this.token !== token || this.statusHookGeneration !== statusHookGeneration) {
              throw new Error('Status hook spool replay interrupted')
            }
            await this.ingestSpoolRecord(record, answerBy)
            replayedPaneKeys.add(this.resolvePaneKeyAlias(record.paneKey))
          }
        })
        if (this.token !== token || this.statusHookGeneration !== statusHookGeneration) {
          return
        }
        // Replayed owners may have died while Orca was down; probe each pane once.
        for (const paneKey of replayedPaneKeys) {
          void this.checkAgentPresence(paneKey)
        }
      }
      this.ownerStateInitialized = true
      this.activateClaudeHelperObservations()
    }
  }

  /** Reads the pane's rollouts off the event loop, then admits, normalizes and applies the hook in that pane's arrival order; undefined once status hooks are no longer current. */
  protected async ingestLocalHookInTurn(
    source: AgentHookSource,
    body: unknown,
    isCurrent: () => boolean
  ): Promise<{ toolCallOwner: EnrichedAgentHookEventPayload | null } | undefined> {
    const turn = await enterHookTranscriptTurn(this.state, source, body)
    try {
      if (!isCurrent()) {
        return undefined
      }
      await this.checkHookOwner(source, body)
      if (!isCurrent()) {
        return undefined
      }
      const normalized = turn.apply(() => this.normalizeLocalHookPayload(source, body))
      if (source === 'muse' && !normalized.event) {
        this.rearmIgnoredMuseHook(body)
      }
      const statusDisposition = normalized.event
        ? this.getAgentStatusDisposition(normalized.event.paneKey, {
            source,
            hookEventName: normalized.event.hookEventName,
            isReplay: normalized.event.isReplay,
            hasExplicitPrompt: normalized.event.hasExplicitPrompt,
            launchToken: normalized.event.launchToken
          })
        : 'suppress'
      let toolCallOwner: typeof normalized.event = null
      if (normalized.event && statusDisposition !== 'suppress') {
        const restartedAuthority =
          statusDisposition === 'restart' && source === 'omp'
            ? this.restoreRetiredStatusRestart(normalized.event.paneKey)
            : undefined
        const event =
          statusDisposition === 'restart'
            ? {
                ...normalized.event,
                launchToken: undefined,
                ...(restartedAuthority
                  ? {
                      ...restartedAuthority,
                      tabId: parsePaneKey(restartedAuthority.paneKey)?.tabId
                    }
                  : {})
              }
            : normalized.event
        if (statusDisposition === 'restart') {
          // A new session must not inherit the retired one's observation order.
          this.observations.rebind(event.paneKey)
        }
        this.recordCurrentAuthorityObservation(event)
        toolCallOwner = event
        const enriched = this.applyObservedLocalHook(event, normalized.onAccepted)
        if (enriched) {
          this.checkAgentPresenceAfterHook(event, enriched)
          this.scheduleAssistantMessageRetry(source, body, enriched)
          this.scheduleTranscriptPoll(source, body, enriched)
        }
      }
      return { toolCallOwner }
    } finally {
      turn.release()
    }
  }
}
