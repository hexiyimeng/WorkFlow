import { useState, useEffect, useRef, useCallback, useReducer } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type {
  NodeData,
  WSMessage,
  NodeSpec,
  LogEntry,
  ExecutionRuntimeState,
  ExecutionPhase,
  WebSocketStatus,
  RunState,
  PluginDiagnostics,
  ReloadNodesResponse,
  ExecutionConfig,
  ExecutionPreflightResponse,
  WindowExecutionProgress,
  RecoveredGraphView,
  RecoveryDeleteResponse,
  RecoveryOpenResponse,
  RecoverySummary,
  ResumeAction,
  ServerDirectoryListing,
} from '../types';
import {
  hydrateFlowWithLatestSpecs,
  hydrateNodesWithLatestSpecs,
  serializeFlowForStorage,
  serializeNodesForStorage,
  type SerializedFlow,
} from '../utils/workflowPersistence';
import {
  isValidMaxInFlightWindows,
  isValidOutputShape,
  isValidWindowShape,
  preflightResourcesAllowExecution,
  preflightOutputShape,
  sameServerPath,
} from '../utils/executionConfig';
import {
  createWindowProgressProtocolState,
  resolveWindowProgressProtocolEvent,
} from '../utils/windowProgress';
import {
  buildRecoveryExecutionRequest,
  executionGraphToSerializedFlow,
  normalizeDirectoryListing,
  normalizeRecoveryOpenResponse,
  normalizeRecoverySummary,
} from '../utils/recovery';
import {
  blocksExecutionChanges,
  markExecutionConnectionLost,
  markExecutionInterrupted,
} from '../utils/executionRuntime';
import { workerResourcePayload } from '../utils/workerResources';

// === Reset helper ===
const resetRuntimeNodeState = (
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
) => {
  setNodes(nds => nds.map(n => ({
    ...n,
    data: { ...n.data, message: '', runState: undefined, waitingFor: undefined, device: undefined, executionId: undefined },
  })));
};

// === Connection constants ===
const isDev = import.meta.env.DEV;
const API_BASE = import.meta.env.VITE_API_BASE_URL || (isDev ? 'http://localhost:8000' : window.location.origin);
const WS_BASE = import.meta.env.VITE_WS_URL || (isDev
  ? 'ws://localhost:8000'
  : `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}`);
const PREFLIGHT_TIMEOUT_MS = 60_000;

const normalizeDashboardUrl = (url?: string | null): string => {
  if (!url) return '';
  return url.endsWith('/status') ? url : `${url}/status`;
};

const fetchJson = async <T,>(path: string, init?: RequestInit): Promise<T> => {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, init);
  const text = await res.text();
  let data: T;
  try {
    data = JSON.parse(text) as T;
  } catch {
    if (text.trim().startsWith('<') || text.trim().startsWith('<!DOCTYPE')) {
      throw new Error(
        `Received HTML instead of JSON from ${url}. ` +
        `In dev mode, ensure the backend is running on http://localhost:8000 ` +
        `or set VITE_API_BASE_URL to the correct backend address.`
      );
    }
    throw new Error(`Invalid JSON from ${url}`);
  }
  if (!res.ok) {
    const payload = data as Record<string, unknown>;
    const detail = typeof payload.detail === 'string' ? payload.detail : null;
    throw new Error(String(
      payload.message
      || payload.error_message
      || detail
      || `HTTP ${res.status} from ${url}`,
    ));
  }
  return data;
};

type ExecutionGraph = Record<string, {
  type: string;
  inputs: Record<string, unknown>;
}>;

const buildExecutionGraph = (
  nodes: Node<NodeData>[],
  edges: Edge[],
): ExecutionGraph => {
  const graph: ExecutionGraph = {};
  nodes.forEach(node => {
    const inputs = { ...node.data.values };
    edges.forEach(edge => {
      if (edge.target === node.id && edge.targetHandle) {
        inputs[edge.targetHandle] = [edge.source, parseInt(edge.sourceHandle || '0')];
      }
    });
    graph[node.id] = { type: node.data.opType, inputs };
  });
  return graph;
};

// === Execution state reducer ===
type ExecutionAction =
  | { type: 'START'; executionId: string }
  | { type: 'SET_PHASE'; phase: ExecutionPhase }
  | { type: 'SET_WINDOW_PROGRESS'; progress: WindowExecutionProgress }
  | { type: 'SET_SNAPSHOT'; snapshot: Partial<ExecutionRuntimeState> }
  | { type: 'CONNECTION_LOST'; executionId?: string | null }
  | { type: 'EXECUTION_NOT_FOUND'; executionId?: string | null }
  | { type: 'FINISH'; status: 'succeeded' | 'failed' | 'cancelled' | 'interrupted'; message?: string }
  | { type: 'RESET' };

type TerminalStatus = 'succeeded' | 'failed' | 'cancelled' | 'interrupted';

const isTerminalStatus = (status: unknown): status is TerminalStatus =>
  status === 'succeeded' || status === 'failed' || status === 'cancelled' || status === 'interrupted';

const initialExecutionState: ExecutionRuntimeState = {
  phase: 'idle',
  executionId: null,
  startedAt: null,
  finishedAt: null,
  totalNodes: 0,
  lastError: null,
  windowProgress: null,
};

function executionReducer(state: ExecutionRuntimeState, action: ExecutionAction): ExecutionRuntimeState {
  switch (action.type) {
    case 'START':
      return {
        ...state,
        phase: 'graph_building',
        executionId: action.executionId,
        startedAt: Date.now(),
        finishedAt: null,
        lastError: null,
        windowProgress: null,
      };
    case 'SET_PHASE':
      if (isTerminalStatus(state.phase)) return state;
      return { ...state, phase: action.phase };
    case 'SET_WINDOW_PROGRESS':
      if (isTerminalStatus(state.phase)) return state;
      return {
        ...state,
        phase: state.phase === 'cancelling' ? 'cancelling' : 'running',
        windowProgress: action.progress,
      };
    case 'SET_SNAPSHOT':
      return { ...state, ...action.snapshot };
    case 'CONNECTION_LOST':
      return markExecutionConnectionLost(state, action.executionId);
    case 'EXECUTION_NOT_FOUND':
      return markExecutionInterrupted(state, action.executionId);
    case 'FINISH':
      return {
        ...state,
        phase: action.status,
        finishedAt: Date.now(),
        lastError: action.status === 'succeeded' ? null : action.message ?? null,
      };
    case 'RESET':
      return { ...initialExecutionState };
    default:
      return state;
  }
}

  // ============================================================
  // useFlowEngine - single WebSocket connection model
  // WebSocket lifecycle is controlled by one mount-only effect.
  // Message handlers read current state through refs rather than stale closures.
  // ============================================================
export const useFlowEngine = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  addLog: (msg: string, type?: LogEntry['type']) => void
) => {
  // --- Connection state ---
  const [websocketStatus, setWebsocketStatus] = useState<WebSocketStatus>('disconnected');
  const [nodeDefs, setNodeDefs] = useState<Record<string, NodeSpec>>({});
  const [pluginDiagnostics, setPluginDiagnostics] = useState<PluginDiagnostics | null>(null);
  const [pluginStatusError, setPluginStatusError] = useState<string | null>(null);
  const [dashboardUrl, setDashboardUrl] = useState<string>('');
  const [isReloadingNodes, setIsReloadingNodes] = useState(false);
  const [isPreflighting, setIsPreflighting] = useState(false);
  const [executionPreflight, setExecutionPreflight] = useState<ExecutionPreflightResponse | null>(null);
  const [lastSubmittedExecutionConfig, setLastSubmittedExecutionConfig] = useState<ExecutionConfig | null>(null);
  const [isRecoveryBrowserOpen, setIsRecoveryBrowserOpen] = useState(false);
  const [recoveredGraphView, setRecoveredGraphView] = useState<RecoveredGraphView | null>(null);

  // --- Execution state (reducer) ---
  const [executionState, dispatchExecution] = useReducer(executionReducer, initialExecutionState);
  const executionStateRef = useRef<ExecutionRuntimeState>(initialExecutionState);

  // --- Refs ---
  const wsRef = useRef<WebSocket | null>(null);
  const mountedRef = useRef(true);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const finishedRef = useRef(false);
  const restoredExecutionRef = useRef(false); // tracks if current session was restored via subscribe
  const reconciliationSnapshotReceivedRef = useRef(false);
  const isSubmittingRunRef = useRef(false); // prevents double-submit
  const recoveryRequestInFlightRef = useRef(false);
  const pendingGraphRef = useRef<ExecutionGraph | null>(null);
  const executionPreflightRef = useRef<ExecutionPreflightResponse | null>(null);
  const preflightInFlightRef = useRef(false);
  const preflightRequestIdRef = useRef(0);
  const preflightAbortRef = useRef<AbortController | null>(null);
  const editableFlowRef = useRef<SerializedFlow | null>(null);
  const windowProgressProtocolRef = useRef(
    createWindowProgressProtocolState(),
  );

  useEffect(() => {
    executionStateRef.current = executionState;
  }, [executionState]);

  // ============================================================
  // 1. Fetch node definitions once on mount
  // ============================================================
  useEffect(() => {
    const loadStartupData = async () => {
      try {
        const data = await fetchJson<Record<string, NodeSpec>>('/object_info');
        setNodeDefs(data);
        if (Object.keys(data).length === 0) {
          addLog('Node definitions loaded but empty. Ensure the backend is running.', 'warning');
        }
      } catch (err) {
        addLog(`Failed to load node definitions: ${(err as Error).message}`, 'error');
      }

      try {
        const status = await fetchJson<PluginDiagnostics>('/plugin_status');
        setPluginDiagnostics(status);
        setPluginStatusError(null);
        const nodeInfoErrors = status.node_info_errors?.length ?? status.node_info_error_count ?? 0;
        if (!status.ok) {
          addLog(
            `Some nodes failed to load: ${status.failed_count} import failure(s), ${nodeInfoErrors} object_info error(s).`,
            'warning',
          );
        }
      } catch (err) {
        setPluginStatusError((err as Error).message);
        addLog('Could not fetch plugin status from backend.', 'warning');
      }

      try {
        const dashboard = await fetchJson<{ dashboard_url?: string | null }>('/dashboard_url');
        setDashboardUrl(normalizeDashboardUrl(dashboard.dashboard_url));
      } catch {
        setDashboardUrl('');
      }
    };

    loadStartupData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // intentionally empty — startup fetch runs once

  const reloadNodes = useCallback(async () => {
    if (isReloadingNodes) return;
    if (blocksExecutionChanges(executionStateRef.current.phase)) {
      addLog('Cannot reload nodes while execution is running.', 'warning');
      return;
    }

    setIsReloadingNodes(true);
    try {
      const res = await fetch(`${API_BASE}/reload_nodes`, { method: 'POST' });
      const text = await res.text();
      let payload: ReloadNodesResponse;
      try {
        payload = JSON.parse(text) as ReloadNodesResponse;
      } catch {
        throw new Error('Invalid JSON from /reload_nodes');
      }

      if (payload.plugin_status) {
        setPluginDiagnostics(payload.plugin_status);
        setPluginStatusError(null);
      }
      if (payload.dashboard_url !== undefined) {
        setDashboardUrl(normalizeDashboardUrl(payload.dashboard_url));
      }

      const freshNodeDefs = payload.object_info
        ?? (res.ok ? await fetchJson<Record<string, NodeSpec>>('/object_info') : null);
      if (freshNodeDefs) {
        setNodeDefs(freshNodeDefs);
        setNodes(hydrateNodesWithLatestSpecs(serializeNodesForStorage(nodes), freshNodeDefs));
      }

      if (!res.ok || !payload.ok) {
        addLog(payload.message || payload.error_message || 'Node reload failed.', 'error');
        return;
      }

      const invalidCount = freshNodeDefs
        ? hydrateNodesWithLatestSpecs(serializeNodesForStorage(nodes), freshNodeDefs)
            .filter(node => node.data._invalid).length
        : 0;
      const loadedCount = payload.loaded?.length ?? 0;
      addLog(
        invalidCount > 0
          ? `Reloaded ${loadedCount} plugin module(s); ${invalidCount} existing node(s) are unavailable.`
          : `Reloaded ${loadedCount} plugin module(s).`,
        invalidCount > 0 ? 'warning' : 'success',
      );
    } catch (err) {
      addLog(`Node reload failed: ${(err as Error).message}`, 'error');
    } finally {
      setIsReloadingNodes(false);
    }
  }, [addLog, isReloadingNodes, nodes, setNodes]);

  // ============================================================
  // 2. Stop edge animation when disconnected
  useEffect(() => {
    if (websocketStatus === 'disconnected') {
      setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
    }
  }, [websocketStatus, setEdges]);

  // ============================================================
  // 3. WebSocket single-connection lifecycle (deps=[])
  // Message handling is inline and uses refs for latest mutable state.
  // ============================================================
  useEffect(() => {
    console.log('[useFlowEngine] connect effect run');
    mountedRef.current = true;

    const clearReconnectTimer = () => {
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const connectWs = () => {
      if (!mountedRef.current) {
        console.log('[useFlowEngine] connectWs: mountedRef=false, skip');
        return;
      }

      // Prevent duplicate open/connecting sockets.
      const existing = wsRef.current;
      if (existing && existing.readyState === WebSocket.OPEN || existing?.readyState === WebSocket.CONNECTING) {
        console.log('[useFlowEngine] connectWs: already connecting/open, skip');
        return;
      }

      clearReconnectTimer();
      setWebsocketStatus('reconnecting');

      const wsUrl = `${WS_BASE}/ws/run`;
      console.info('[useFlowEngine] connecting to', wsUrl);
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      const handleExecutionNotFound = (executionId?: string | null, message?: string) => {
        const retainedExecutionId = executionId
          || sessionStorage.getItem('WorkFlow_execution_id')
          || executionStateRef.current.executionId;
        sessionStorage.removeItem('WorkFlow_execution_id');
        finishedRef.current = true;
        isSubmittingRunRef.current = false;
        restoredExecutionRef.current = false;
        reconciliationSnapshotReceivedRef.current = false;
        windowProgressProtocolRef.current = createWindowProgressProtocolState();
        setWebsocketStatus('connected');
        dispatchExecution({
          type: 'EXECUTION_NOT_FOUND',
          executionId: retainedExecutionId,
        });
        setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
        resetRuntimeNodeState(setNodes);
        addLog(
          message || 'The previous backend execution was interrupted. If it was a Window run, open its recovery directory to resume.',
          'warning',
        );
      };

      ws.onopen = () => {
        if (!mountedRef.current) { ws.close(); return; }
        console.log('[useFlowEngine] ws open');

        const storedExecutionId = sessionStorage.getItem('WorkFlow_execution_id');
        if (storedExecutionId) {
          restoredExecutionRef.current = true;
          reconciliationSnapshotReceivedRef.current = false;
          dispatchExecution({
            type: 'CONNECTION_LOST',
            executionId: storedExecutionId,
          });
          ws.send(JSON.stringify({ command: 'subscribe', executionId: storedExecutionId }));
          addLog(`Connection restored. Checking execution ${storedExecutionId}...`, 'info');
        } else {
          setWebsocketStatus('connected');
          addLog('System Connected', 'success');
        }
      };

      ws.onclose = (event) => {
        console.log('[useFlowEngine] ws close', event.code, event.reason);
        if (!mountedRef.current) return;
        if (wsRef.current !== ws) return;
        setWebsocketStatus('disconnected');
        wsRef.current = null;
        reconciliationSnapshotReceivedRef.current = false;
        preflightRequestIdRef.current += 1;
        preflightAbortRef.current?.abort();
        preflightAbortRef.current = null;
        preflightInFlightRef.current = false;
        pendingGraphRef.current = null;
        setIsPreflighting(false);
        executionPreflightRef.current = null;
        setExecutionPreflight(null);
        setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
        const storedExecutionId = sessionStorage.getItem('WorkFlow_execution_id');
        if (!finishedRef.current && storedExecutionId) {
          dispatchExecution({
            type: 'CONNECTION_LOST',
            executionId: storedExecutionId,
          });
          resetRuntimeNodeState(setNodes);
        }

        // Always reconnect unless component is unmounting
        addLog(
          storedExecutionId && !finishedRef.current
            ? 'Backend connection lost during execution. Status is unknown; reconnecting...'
            : 'Connection lost. Reconnecting...',
          'warning',
        );
        clearReconnectTimer();
        reconnectTimerRef.current = setTimeout(connectWs, 1000);
      };

      ws.onerror = (event) => {
        console.log('[useFlowEngine] ws error', event);
        if (
          wsRef.current === ws
          && ws.readyState !== WebSocket.CLOSING
          && ws.readyState !== WebSocket.CLOSED
        ) {
          ws.close();
        }
      };

      // ========================================================
      // Message handling uses refs for latest state.
      // ========================================================
      ws.onmessage = (e) => {
        if (wsRef.current !== ws) return;
        let msg: WSMessage;
        try {
          msg = JSON.parse(e.data);
        } catch {
          console.error('[useFlowEngine] parse error', e.data);
          return;
        }

        const msgType = msg.type;

        if (msgType === 'slurm_job_submitted') {
          dispatchExecution({ type: 'SET_PHASE', phase: 'submitted' });
          addLog(
            msg.message || `Slurm job ${msg.jobId ?? ''} submitted.`,
            'info',
          );
          return;
        }

        if (msgType === 'slurm_job_state') {
          const state = (msg.state || 'UNKNOWN').toUpperCase();
          if (state === 'PENDING' || state === 'CONFIGURING') {
            dispatchExecution({ type: 'SET_PHASE', phase: 'submitted' });
          }
          addLog(
            msg.message || `Slurm job ${msg.jobId ?? ''}: ${state}.`,
            'info',
          );
          return;
        }

        if (msgType === 'cluster_ready') {
          setDashboardUrl(normalizeDashboardUrl(msg.dashboardUrl));
          const cpuWorkers = Number.isSafeInteger(msg.cpuWorkers)
            ? Number(msg.cpuWorkers)
            : 0;
          const gpuWorkers = Number.isSafeInteger(msg.gpuWorkers)
            ? Number(msg.gpuWorkers)
            : 0;
          addLog(
            `Dask cluster ready: ${cpuWorkers} CPU / ${gpuWorkers} GPU Worker(s).`,
            'success',
          );
          return;
        }

        if (msgType === 'execution_not_found') {
          handleExecutionNotFound(msg.executionId, msg.message);
          return;
        }

        // Log messages
        if (msgType === 'log') { addLog(msg.message || '', 'info'); return; }
        if (msgType === 'success') { addLog(msg.message || '', 'success'); return; }
        if (msgType === 'warning') { addLog(msg.message || '', 'warning'); return; }

        // execution_finished: authoritative terminal event
        if (msgType === 'error') {
          const msgText = msg.message || '';
          const hasStatus = isTerminalStatus(msg.status);
          if (
            restoredExecutionRef.current
            && msgText.startsWith('Execution ')
            && msgText.endsWith(' not found')
          ) {
            handleExecutionNotFound(msg.executionId, msgText);
            return;
          }
          if (hasStatus && !finishedRef.current) {
            const status = msg.status;
            if (isTerminalStatus(status)) {
              finishedRef.current = true;
              windowProgressProtocolRef.current = createWindowProgressProtocolState();
              sessionStorage.removeItem('WorkFlow_execution_id');
              setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
              dispatchExecution({ type: 'FINISH', status, message: msg.message });
              addLog(
                msgText || `Execution ${status}`,
                status === 'interrupted' || status === 'cancelled' ? 'warning' : 'error',
              );
            }
            return;
          }
          // Generic error: reset execution state and node runtime state so user can retry.
          finishedRef.current = true;
          isSubmittingRunRef.current = false;
          restoredExecutionRef.current = false;
          reconciliationSnapshotReceivedRef.current = false;
          windowProgressProtocolRef.current = createWindowProgressProtocolState();
          setWebsocketStatus('connected');
          sessionStorage.removeItem('WorkFlow_execution_id');
          dispatchExecution({ type: 'RESET' });
          addLog(msgText || 'Unknown Error', 'error');
          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          resetRuntimeNodeState(setNodes);
          return;
        }

        // execution_started
        if (msgType === 'execution_started') {
          if (!msg.executionId) return;
          finishedRef.current = false;
          windowProgressProtocolRef.current = createWindowProgressProtocolState(
            msg.executionId,
          );
          sessionStorage.setItem('WorkFlow_execution_id', msg.executionId);
          executionStateRef.current = {
            ...executionStateRef.current,
            phase: 'submitted',
            executionId: msg.executionId,
          };
          dispatchExecution({ type: 'START', executionId: msg.executionId });
          dispatchExecution({ type: 'SET_PHASE', phase: 'submitted' });
          isSubmittingRunRef.current = false;
          return;
        }

        // execution_rejected: another execution already running
        if (msgType === 'execution_rejected') {
          isSubmittingRunRef.current = false;
          finishedRef.current = true;
          executionStateRef.current = { ...initialExecutionState };
          windowProgressProtocolRef.current = createWindowProgressProtocolState();
          sessionStorage.removeItem('WorkFlow_execution_id');
          dispatchExecution({ type: 'RESET' });
          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          resetRuntimeNodeState(setNodes);
          addLog(msg.message || 'Execution rejected: another workflow is already running.', 'warning');
          return;
        }

        // execution_snapshot: restore execution state on reconnect
        if (msgType === 'execution_snapshot') {
          isSubmittingRunRef.current = false;
          if (restoredExecutionRef.current) {
            reconciliationSnapshotReceivedRef.current = true;
          }
          const snapshotPhase =
            msg.status === 'running' ? 'running'
            : msg.status === 'cancelling' ? 'cancelling'
            : msg.status === 'succeeded' ? 'succeeded'
            : msg.status === 'failed' ? 'failed'
            : msg.status === 'cancelled' ? 'cancelled'
            : msg.status === 'interrupted' ? 'interrupted'
            : msg.status === 'submitted' ? 'submitted'
            : msg.status === 'graph_building' ? 'graph_building'
            : 'idle';

          const snapshotWindowProgress = resolveWindowProgressProtocolEvent(
            windowProgressProtocolRef.current,
            {
              source: 'structured',
              executionId: msg.executionId,
              value: msg.windowProgress,
            },
            executionStateRef.current.executionId,
          );
          windowProgressProtocolRef.current = snapshotWindowProgress.state;

          dispatchExecution({
            type: 'SET_SNAPSHOT',
            snapshot: {
              phase: snapshotPhase,
              executionId: msg.executionId ?? null,
              startedAt: msg.createdAt ?? null,
              finishedAt: msg.finishedAt ?? null,
              totalNodes: msg.nodeCount ?? 0,
              windowProgress: snapshotWindowProgress.progress,
              lastError: null,
            }
          });

          const terminal = isTerminalStatus(msg.status);
          if (terminal) {
            // Terminal state: clean up all execution state
            finishedRef.current = true;
            isSubmittingRunRef.current = false;
            windowProgressProtocolRef.current = createWindowProgressProtocolState();
            sessionStorage.removeItem('WorkFlow_execution_id');
            setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          } else if (['running', 'submitted', 'graph_building', 'cancelling'].includes(msg.status ?? '')) {
            // Active state: restore edge animation
            setEdges(eds => eds.map(e => ({ ...e, animated: true })));
          }
          return;
        }

        if (msgType === 'window_progress') {
          const result = resolveWindowProgressProtocolEvent(
            windowProgressProtocolRef.current,
            {
              source: 'structured',
              executionId: msg.executionId,
              value: msg,
            },
            executionStateRef.current.executionId,
          );
          windowProgressProtocolRef.current = result.state;
          if (result.progress) {
            dispatchExecution({ type: 'SET_WINDOW_PROGRESS', progress: result.progress });
          }
          return;
        }

        // node_status messages carry simple node status updates.
        // Legacy progress remains a node status and also restores Window progress
        // for executions started by a backend that predates window_progress.
        if (msgType === 'node_status' || msgType === 'progress') {
          if (msgType === 'progress') {
            const result = resolveWindowProgressProtocolEvent(
              windowProgressProtocolRef.current,
              {
                source: 'legacy',
                executionId: msg.executionId,
                value: msg.message,
              },
              executionStateRef.current.executionId,
            );
            windowProgressProtocolRef.current = result.state;
            if (result.progress) {
              dispatchExecution({
                type: 'SET_WINDOW_PROGRESS',
                progress: result.progress,
              });
            }
          }
          if (!msg.taskId) return;
          setNodes(nds => nds.map(n => {
            if (n.id !== msg.taskId) return n;
            return {
              ...n,
              className: '',
              data: {
                ...n.data,
                message: msg.message ?? '',
                runState: msg.runState,
                waitingFor: msg.waitingFor,
                device: msg.device,
              },
            };
          }));

          if (msg.runState === 'submitted') {
            dispatchExecution({ type: 'SET_PHASE', phase: 'submitted' });
          } else if (msg.runState === 'running') {
            dispatchExecution({ type: 'SET_PHASE', phase: 'running' });
          }

          if (msg.message?.toLowerCase().includes('error')) {
            addLog(`[${msg.taskId}] ${msg.message}`, 'error');
          }
          return;
        }

        // execution_finished: authoritative terminal event
        if (msgType === 'execution_finished') {
          if (finishedRef.current) return;

          const status = msg.status;
          // cancelling is not terminal — handled separately
          if (status === 'cancelling') {
            dispatchExecution({ type: 'SET_PHASE', phase: 'cancelling' });
            return;
          }

          if (!isTerminalStatus(status)) {
            if (status) {
              dispatchExecution({ type: 'SET_PHASE', phase: status });
            }
            return;
          }
          finishedRef.current = true;
          isSubmittingRunRef.current = false;
          windowProgressProtocolRef.current = createWindowProgressProtocolState();
          sessionStorage.removeItem('WorkFlow_execution_id');
          dispatchExecution({ type: 'FINISH', status, message: msg.message });

          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          resetRuntimeNodeState(setNodes);

          const logType: LogEntry['type'] = status === 'succeeded'
            ? 'success'
            : status === 'interrupted'
              ? 'warning'
              : 'error';
          addLog(msg.message || `Execution ${status}`, logType);
          return;
        }

        // LEGACY: done means succeeded
        if (msgType === 'done') {
          if (finishedRef.current) return;
          finishedRef.current = true;
          windowProgressProtocolRef.current = createWindowProgressProtocolState();
          sessionStorage.removeItem('WorkFlow_execution_id');
          dispatchExecution({ type: 'FINISH', status: 'succeeded', message: msg.message });
          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          resetRuntimeNodeState(setNodes);
          addLog(msg.message || 'Execution succeeded', 'success');
          return;
        }

        // LEGACY: non-final error compatibility path
        // @ts-expect-error - legacy compatibility path
        if (msgType === 'error' && !finishedRef.current) {
          // Legacy error path — reset node state so user can retry
          const status = msg.status === 'cancelled' ? 'cancelled' : 'failed';
          finishedRef.current = true;
          isSubmittingRunRef.current = false;
          windowProgressProtocolRef.current = createWindowProgressProtocolState();
          sessionStorage.removeItem('WorkFlow_execution_id');
          dispatchExecution({ type: 'SET_PHASE', phase: 'idle' });
          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          resetRuntimeNodeState(setNodes);
          addLog(msg.message || `Execution ${status}`, 'error');
          return;
        }

        // subscribed: restore execution on reconnect
        if (msgType === 'subscribed') {
          if (msg.executionId) {
            if (
              restoredExecutionRef.current
              && !reconciliationSnapshotReceivedRef.current
            ) {
              addLog(
                `Execution ${msg.executionId} could not be reconciled; retrying connection.`,
                'warning',
              );
              ws.close(1012, 'Execution snapshot missing');
              return;
            }
            restoredExecutionRef.current = false;
            reconciliationSnapshotReceivedRef.current = false;
            setWebsocketStatus('connected');
            addLog(`Restored execution ${msg.executionId}`, 'success');
          }
          return;
        }

        // execution_control_ack stop response
        if (msgType === 'execution_control_ack') {
          if (msg.message) {
            addLog(msg.message, 'warning');
          }
          return;
        }

        // ping / pong
        if (msgType === 'ping') {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ command: 'pong' }));
          }
          return;
        }
        if (msgType === 'pong') return;
      };
    };

    connectWs();

    return () => {
      console.log('[useFlowEngine] connect effect cleanup (unmount)');
      mountedRef.current = false;
      preflightRequestIdRef.current += 1;
      preflightAbortRef.current?.abort();
      preflightAbortRef.current = null;
      preflightInFlightRef.current = false;
      clearReconnectTimer();
      if (wsRef.current) {
        wsRef.current.close(1000, 'component unmount');
        wsRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // <-- 閸忔娊鏁敍姘涧閺堝瀵曟潪?閸楁瓕娴囩憴锕€褰傞敍灞炬￥閸忔湹绮笟婵婄

  const browseServerDirectories = useCallback(async (
    path: string,
  ): Promise<ServerDirectoryListing> => {
    const query = path.trim() ? `?path=${encodeURIComponent(path.trim())}` : '';
    const payload = await fetchJson<unknown>(`/filesystem/directories${query}`);
    return normalizeDirectoryListing(payload);
  }, []);

  const inspectRecoveryDirectory = useCallback(async (
    recoveryDirectory: string,
  ): Promise<RecoverySummary> => {
    const payload = await fetchJson<unknown>('/execution/recovery/inspect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recoveryDirectory: recoveryDirectory.trim() }),
    });
    return normalizeRecoverySummary(payload);
  }, []);

  const fetchRecoveryGraph = useCallback(async (
    recoveryDirectory: string,
  ): Promise<RecoveryOpenResponse> => {
    const payload = await fetchJson<unknown>('/execution/recovery/open', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recoveryDirectory: recoveryDirectory.trim() }),
    });
    return normalizeRecoveryOpenResponse(payload);
  }, []);

  const applyRecoveryGraph = useCallback((
    recoveryDirectory: string,
    opened: RecoveryOpenResponse,
  ) => {
    if (editableFlowRef.current === null) {
      editableFlowRef.current = serializeFlowForStorage(nodes, edges);
    }

    const serialized = executionGraphToSerializedFlow(opened.graph);
    const hydrated = hydrateFlowWithLatestSpecs(serialized, nodeDefs);
    preflightRequestIdRef.current += 1;
    preflightAbortRef.current?.abort();
    preflightAbortRef.current = null;
    preflightInFlightRef.current = false;
    pendingGraphRef.current = null;
    setIsPreflighting(false);
    executionPreflightRef.current = null;
    setExecutionPreflight(null);
    setNodes(hydrated.nodes);
    setEdges(hydrated.edges);
    setRecoveredGraphView({
      recoveryDirectory,
      summary: opened.recoverySummary,
      executionConfig: opened.executionConfig,
    });
    if (hydrated.invalidNodeTypes.length > 0) {
      addLog(
        `Recovery graph opened with ${hydrated.invalidNodeTypes.length} unavailable node type(s).`,
        'warning',
      );
    } else {
      addLog(`Opened recovery graph from ${recoveryDirectory} (read only).`, 'success');
    }
  }, [addLog, edges, nodeDefs, nodes, setEdges, setNodes]);

  const openRecoveryDirectory = useCallback(async (
    recoveryDirectory: string,
  ): Promise<RecoveryOpenResponse> => {
    const normalizedDirectory = recoveryDirectory.trim();
    const opened = await fetchRecoveryGraph(normalizedDirectory);
    applyRecoveryGraph(normalizedDirectory, opened);
    setIsRecoveryBrowserOpen(false);
    return opened;
  }, [applyRecoveryGraph, fetchRecoveryGraph]);

  const closeRecoveredGraph = useCallback(() => {
    if (blocksExecutionChanges(executionStateRef.current.phase)) {
      addLog('Cannot close the recovery graph while execution is active.', 'warning');
      return;
    }
    const editable = editableFlowRef.current;
    if (editable) {
      const hydrated = hydrateFlowWithLatestSpecs(editable, nodeDefs);
      setNodes(hydrated.nodes);
      setEdges(hydrated.edges);
    }
    editableFlowRef.current = null;
    setRecoveredGraphView(null);
    executionPreflightRef.current = null;
    setExecutionPreflight(null);
    pendingGraphRef.current = null;
    addLog('Returned to the editable workflow.', 'info');
  }, [addLog, nodeDefs, setEdges, setNodes]);

  const deleteRecoveryDirectory = useCallback(async (
    recoveryDirectory: string,
    expectedExecutionId: string,
  ): Promise<RecoveryDeleteResponse> => {
    if (blocksExecutionChanges(executionStateRef.current.phase)) {
      throw new Error('Cannot delete a recovery record while execution is active.');
    }
    const normalizedDirectory = recoveryDirectory.trim();
    const deleted = await fetchJson<RecoveryDeleteResponse>(
      '/execution/recovery/delete',
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          recoveryDirectory: normalizedDirectory,
          expectedExecutionId,
        }),
      },
    );
    if (
      deleted.deleted !== true
      || deleted.deletedExecutionId !== expectedExecutionId
      || typeof deleted.recoveryDirectory !== 'string'
      || deleted.recoveryDirectory.trim() === ''
      || !Array.isArray(deleted.outputsPreserved)
      || deleted.outputsPreserved.some(path => typeof path !== 'string')
      || typeof deleted.cleanupPending !== 'boolean'
      || (
        deleted.cleanupPending
        && (
          typeof deleted.cleanupDirectory !== 'string'
          || deleted.cleanupDirectory.trim() === ''
        )
      )
    ) {
      throw new Error('The backend did not confirm recovery-record deletion.');
    }
    if (sameServerPath(
      recoveredGraphView?.recoveryDirectory,
      deleted.recoveryDirectory,
    )) {
      closeRecoveredGraph();
    }
    addLog(
      deleted.cleanupPending
        ? `Deleted recovery record ${deleted.recoveryDirectory}; output data was not deleted. `
          + `Detached metadata still needs filesystem cleanup at ${deleted.cleanupDirectory}.`
        : `Deleted recovery record ${deleted.recoveryDirectory}; its output data was not deleted.`,
      deleted.cleanupPending ? 'warning' : 'success',
    );
    return deleted;
  }, [addLog, closeRecoveredGraph, recoveredGraphView]);

  const submitExecution = useCallback((
    graph: ExecutionGraph | null,
    executionConfig: ExecutionConfig,
  ): boolean => {
    if (isSubmittingRunRef.current) {
      addLog('Execution submission is already in progress.', 'warning');
      return false;
    }
    isSubmittingRunRef.current = true;

    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN || websocketStatus !== 'connected') {
      isSubmittingRunRef.current = false;
      addLog('Server not connected!', 'error');
      return false;
    }
    if (blocksExecutionChanges(executionStateRef.current.phase)) {
      isSubmittingRunRef.current = false;
      addLog('Another execution is already active.', 'warning');
      return false;
    }

    const executionId = crypto.randomUUID();
    setLastSubmittedExecutionConfig(executionConfig);
    finishedRef.current = false;
    windowProgressProtocolRef.current = createWindowProgressProtocolState(executionId);
    executionStateRef.current = {
      ...executionStateRef.current,
      phase: 'graph_building',
      executionId,
    };
    sessionStorage.setItem('WorkFlow_execution_id', executionId);
    dispatchExecution({ type: 'START', executionId });

    setEdges(currentEdges => currentEdges.map(edge => ({ ...edge, animated: true })));
    setNodes(currentNodes => currentNodes.map(node => ({
      ...node,
      data: {
        ...node.data,
        message: 'Pending...',
        runState: 'ready' as RunState,
        waitingFor: undefined,
        device: undefined,
        executionId,
      },
    })));

    try {
      ws.send(JSON.stringify({
        command: 'execute_graph',
        graph,
        executionId,
        executionConfig,
        ...workerResourcePayload(),
      }));
      addLog(
        executionConfig.mode === 'window' && executionConfig.resumeAction === 'resume'
          ? 'Resuming Workflow...'
          : executionConfig.mode === 'window' && executionConfig.resumeAction === 'restart'
            ? 'Restarting Window Workflow...'
            : 'Executing Workflow...',
        'info',
      );
      return true;
    } catch {
      setLastSubmittedExecutionConfig(null);
      isSubmittingRunRef.current = false;
      finishedRef.current = true;
      executionStateRef.current = { ...initialExecutionState };
      windowProgressProtocolRef.current = createWindowProgressProtocolState();
      sessionStorage.removeItem('WorkFlow_execution_id');
      dispatchExecution({ type: 'RESET' });
      setEdges(currentEdges => currentEdges.map(
        edge => edge.animated ? { ...edge, animated: false } : edge,
      ));
      resetRuntimeNodeState(setNodes);
      addLog('Failed to send execute command', 'error');
      return false;
    }
  }, [addLog, setEdges, setNodes, websocketStatus]);

  const executeRecoveryDirectory = useCallback(async (
    recoveryDirectory: string,
    action: Exclude<ResumeAction, 'new'>,
  ): Promise<boolean> => {
    if (recoveryRequestInFlightRef.current || isSubmittingRunRef.current) {
      return false;
    }
    recoveryRequestInFlightRef.current = true;
    try {
      const normalizedDirectory = recoveryDirectory.trim();
      const opened = await fetchRecoveryGraph(normalizedDirectory);
      applyRecoveryGraph(normalizedDirectory, opened);

      const request = buildRecoveryExecutionRequest(
        opened,
        normalizedDirectory,
        action,
      );
      const submitted = submitExecution(request.graph, request.executionConfig);
      if (submitted) setIsRecoveryBrowserOpen(false);
      return submitted;
    } finally {
      recoveryRequestInFlightRef.current = false;
    }
  }, [applyRecoveryGraph, fetchRecoveryGraph, submitExecution]);

  // =========================================================
  // Execution preflight and prepared submission
  // =========================================================
  const preflightFlow = useCallback(async (
    executionConfig: ExecutionConfig,
  ): Promise<ExecutionPreflightResponse> => {
    if (isSubmittingRunRef.current) {
      throw new Error('Execution submission is already in progress.');
    }
    if (preflightInFlightRef.current) {
      throw new Error('Execution preflight is already in progress.');
    }
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      const error = new Error('Server not connected!');
      addLog(error.message, 'error');
      throw error;
    }
    if (websocketStatus !== 'connected') {
      const error = new Error('Server not connected!');
      addLog(error.message, 'error');
      throw error;
    }
    if (blocksExecutionChanges(executionState.phase)) {
      throw new Error('Another execution is already active.');
    }

    const graph = buildExecutionGraph(nodes, edges);
    const requestId = preflightRequestIdRef.current + 1;
    preflightRequestIdRef.current = requestId;
    preflightAbortRef.current?.abort();
    const controller = new AbortController();
    preflightAbortRef.current = controller;
    preflightInFlightRef.current = true;
    setIsPreflighting(true);
    let didTimeout = false;
    const timeoutId = window.setTimeout(() => {
      didTimeout = true;
      controller.abort();
    }, PREFLIGHT_TIMEOUT_MS);

    try {
      const preflight = await fetchJson<ExecutionPreflightResponse>('/execution/preflight', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          graph,
          executionConfig,
          ...workerResourcePayload(),
        }),
        signal: controller.signal,
      });

      if (!mountedRef.current) {
        throw new Error('Execution preflight was cancelled because the editor was closed.');
      }
      if (controller.signal.aborted || preflightRequestIdRef.current !== requestId) {
        throw new Error('Execution preflight was cancelled.');
      }
      if (!preflight || typeof preflight.windowable !== 'boolean') {
        throw new Error('Invalid execution preflight response');
      }
      let normalizedPreflight = preflight;
      const outputShape = preflight.outputShape ?? preflight.output_shape;
      const shapeIsValid = outputShape == null || isValidOutputShape(outputShape);
      const outputs = Array.isArray(preflight.outputs) ? preflight.outputs : [];
      const outputsComplete = outputs.length > 0 && outputs.every(output => (
        typeof output?.nodeId === 'string'
        && typeof output.nodeType === 'string'
        && typeof output.displayName === 'string'
        && typeof output.pathInput === 'string'
        && typeof output.path === 'string'
      ));
      const windowMetadataComplete = (
        preflight.windowable
        && shapeIsValid
        && outputShape != null
        && outputShape.length > 0
        && (preflight.ndim == null || preflight.ndim === outputShape.length)
        && outputsComplete
      );
      if (!shapeIsValid || (preflight.windowable && !windowMetadataComplete)) {
        normalizedPreflight = {
          ...preflight,
          windowable: false,
          outputShape: null,
          output_shape: null,
          ndim: null,
          outputs,
          reason: outputs.length === 0
            ? 'The backend did not return persistent output paths for Window recovery. Full Graph Execution remains available.'
            : 'Window metadata could not be represented safely in this browser. Full Graph Execution remains available.',
        };
      } else {
        normalizedPreflight = {
          ...preflight,
          outputShape: outputShape ?? null,
          output_shape: outputShape ?? null,
          outputs,
        };
      }

      // Replace the prepared graph and summary together only after a complete,
      // valid response. A failed refresh therefore leaves the last preparation
      // available to the caller.
      pendingGraphRef.current = graph;
      executionPreflightRef.current = normalizedPreflight;
      setExecutionPreflight(normalizedPreflight);
      return normalizedPreflight;
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      const isCurrentRequest = preflightRequestIdRef.current === requestId;
      const wasExplicitlyCancelled = controller.signal.aborted && !didTimeout;
      if (isCurrentRequest && !wasExplicitlyCancelled) {
        addLog(
          didTimeout
            ? 'Execution preflight timed out after 60 seconds. Please try again.'
            : `Execution preflight failed: ${error.message}`,
          'error',
        );
      }
      if (didTimeout) {
        throw new Error('Execution preflight timed out after 60 seconds. Please try again.');
      }
      throw error;
    } finally {
      window.clearTimeout(timeoutId);
      if (preflightRequestIdRef.current === requestId) {
        preflightAbortRef.current = null;
        preflightInFlightRef.current = false;
        setIsPreflighting(false);
      }
    }
  }, [
    nodes,
    edges,
    addLog,
    websocketStatus,
    executionState.phase,
  ]);

  const clearPreparedExecution = useCallback(() => {
    preflightRequestIdRef.current += 1;
    preflightAbortRef.current?.abort();
    preflightAbortRef.current = null;
    preflightInFlightRef.current = false;
    pendingGraphRef.current = null;
    executionPreflightRef.current = null;
    setIsPreflighting(false);
    setExecutionPreflight(null);
  }, []);

  const submitPreparedExecution = useCallback((
    executionConfig: ExecutionConfig,
  ): boolean => {
    if (isSubmittingRunRef.current) return false;

    const graph = pendingGraphRef.current;
    const preflight = executionPreflightRef.current;
    if (!graph || !preflight) {
      addLog('Execution preflight has expired. Please run preflight again.', 'warning');
      return false;
    }
    if (!preflightResourcesAllowExecution(preflight)) {
      addLog(
        preflight.resourceError
          || 'The active Dask cluster cannot satisfy this workflow.',
        'error',
      );
      return false;
    }

    let normalizedConfig: ExecutionConfig = { mode: 'full_graph' };
    if (executionConfig.mode === 'window') {
      const outputShape = preflightOutputShape(preflight);
      const windowShape = [...(executionConfig.windowShape ?? [])];
      if (!preflight.windowable) {
        addLog(preflight.reason || 'Window execution is unavailable for this workflow.', 'warning');
        return false;
      }
      if (!isValidWindowShape(outputShape, windowShape)) {
        addLog('Window shape must contain one positive integer per output dimension.', 'warning');
        return false;
      }
      if (
        executionConfig.maxInFlightWindows !== undefined
        && !isValidMaxInFlightWindows(executionConfig.maxInFlightWindows)
      ) {
        addLog('Maximum in-flight Windows must be a positive integer.', 'warning');
        return false;
      }
      normalizedConfig = { ...executionConfig, windowShape } as ExecutionConfig;
    }

    if (submitExecution(graph, normalizedConfig)) {
      pendingGraphRef.current = null;
      executionPreflightRef.current = null;
      setExecutionPreflight(null);
      return true;
    }
    return false;
  }, [
    addLog,
    submitExecution,
  ]);

  // =========================================================
  // 閸嬫粍顒?(Stop)
  // =========================================================
  const stopFlow = useCallback(() => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      addLog('Cannot stop while the backend is disconnected.', 'warning');
      return;
    }
    dispatchExecution({ type: 'SET_PHASE', phase: 'cancelling' });
    wsRef.current.send(JSON.stringify({ command: 'stop_execution' }));
    setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
    addLog('Requesting Stop...', 'warning');
  }, [addLog, setEdges]);

  const openRecoveryBrowser = useCallback(() => {
    if (blocksExecutionChanges(executionStateRef.current.phase)) {
      addLog('Cannot open another recovery directory while execution is active.', 'warning');
      return;
    }
    setIsRecoveryBrowserOpen(true);
  }, [addLog]);

  const closeRecoveryBrowser = useCallback(() => {
    setIsRecoveryBrowserOpen(false);
  }, []);

  return {
    websocketStatus,
    nodeDefs,
    pluginDiagnostics,
    pluginStatusError,
    dashboardUrl,
    isReloadingNodes,
    isPreflighting,
    executionPreflight,
    lastSubmittedExecutionConfig,
    isRecoveryBrowserOpen,
    recoveredGraphView,
    executionState,
    preflightFlow,
    submitPreparedExecution,
    clearPreparedExecution,
    openRecoveryBrowser,
    closeRecoveryBrowser,
    browseServerDirectories,
    inspectRecoveryDirectory,
    openRecoveryDirectory,
    deleteRecoveryDirectory,
    executeRecoveryDirectory,
    closeRecoveredGraph,
    stopFlow,
    reloadNodes,
  };
};
