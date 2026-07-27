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
} from '../types';
import { hydrateNodesWithLatestSpecs, serializeNodesForStorage } from '../utils/workflowPersistence';
import { isValidOutputShape, isValidWindowShape } from '../utils/executionConfig';
import { normalizeWindowProgress, parseLegacyWindowProgress } from '../utils/windowProgress';

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
    throw new Error(String(payload.message || payload.error_message || `HTTP ${res.status} from ${url}`));
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
  | { type: 'FINISH'; status: 'succeeded' | 'failed' | 'cancelled'; message?: string }
  | { type: 'RESET' };

type TerminalStatus = 'succeeded' | 'failed' | 'cancelled';

const isTerminalStatus = (status: unknown): status is TerminalStatus =>
  status === 'succeeded' || status === 'failed' || status === 'cancelled';

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
    case 'FINISH':
      return {
        ...state,
        phase: action.status,
        finishedAt: Date.now(),
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

  // --- Execution state (reducer) ---
  const [executionState, dispatchExecution] = useReducer(executionReducer, initialExecutionState);
  const executionStateRef = useRef<ExecutionRuntimeState>(initialExecutionState);

  // --- Refs ---
  const wsRef = useRef<WebSocket | null>(null);
  const mountedRef = useRef(true);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const finishedRef = useRef(false);
  const restoredExecutionRef = useRef(false); // tracks if current session was restored via subscribe
  const isSubmittingRunRef = useRef(false); // prevents double-submit
  const pendingGraphRef = useRef<ExecutionGraph | null>(null);
  const preflightRequestIdRef = useRef(0);
  const preflightAbortRef = useRef<AbortController | null>(null);

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
    if (['graph_building', 'submitted', 'running', 'cancelling'].includes(executionStateRef.current.phase)) {
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

      ws.onopen = () => {
        if (!mountedRef.current) { ws.close(); return; }
        console.log('[useFlowEngine] ws open');
        setWebsocketStatus('connected');
        addLog('System Connected', 'success');

        // 闁插秷绻涢幁銏狀槻
        const storedExecutionId = sessionStorage.getItem('WorkFlow_execution_id');
        if (storedExecutionId) {
          restoredExecutionRef.current = true;
          ws.send(JSON.stringify({ command: 'subscribe', executionId: storedExecutionId }));
          addLog(`Attempting to restore execution ${storedExecutionId}...`, 'info');
        }
      };

      ws.onclose = (event) => {
        console.log('[useFlowEngine] ws close', event.code, event.reason);
        if (!mountedRef.current) return;
        setWebsocketStatus('disconnected');
        wsRef.current = null;
        setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));

        // Always reconnect unless component is unmounting
        addLog('Connection lost. Reconnecting...', 'warning');
        reconnectTimerRef.current = setTimeout(connectWs, 1000);
      };

      ws.onerror = (event) => {
        console.log('[useFlowEngine] ws error', event);
      };

      // ========================================================
      // Message handling uses refs for latest state.
      // ========================================================
      ws.onmessage = (e) => {
        let msg: WSMessage;
        try {
          msg = JSON.parse(e.data);
        } catch {
          console.error('[useFlowEngine] parse error', e.data);
          return;
        }

        const msgType = msg.type;

        // Log messages
        if (msgType === 'log') { addLog(msg.message || '', 'info'); return; }
        if (msgType === 'success') { addLog(msg.message || '', 'success'); return; }
        if (msgType === 'warning') { addLog(msg.message || '', 'warning'); return; }

        // execution_finished: authoritative terminal event
        if (msgType === 'error') {
          const msgText = msg.message || '';
          const hasStatus = msg.status === 'failed' || msg.status === 'cancelled';
          if (msgText.includes('not found')) {
            sessionStorage.removeItem('WorkFlow_execution_id');
            finishedRef.current = true;
            isSubmittingRunRef.current = false;
            dispatchExecution({ type: 'SET_PHASE', phase: 'idle' });
            setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
            resetRuntimeNodeState(setNodes);
            addLog('Old execution expired (project restarted). Starting fresh.', 'warning');
            return;
          }
          if (hasStatus && !finishedRef.current) {
            const status = msg.status;
            if (isTerminalStatus(status)) {
              finishedRef.current = true;
              sessionStorage.removeItem('WorkFlow_execution_id');
              setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
              dispatchExecution({ type: 'FINISH', status, message: msg.message });
              addLog(msgText || `Execution ${status}`, status === 'cancelled' ? 'warning' : 'error');
            }
            return;
          }
          // Generic error: reset execution state and node runtime state so user can retry.
          finishedRef.current = true;
          isSubmittingRunRef.current = false;
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
          isSubmittingRunRef.current = false;
          sessionStorage.setItem('WorkFlow_execution_id', msg.executionId);
          dispatchExecution({ type: 'START', executionId: msg.executionId });
          dispatchExecution({ type: 'SET_PHASE', phase: 'submitted' });
          return;
        }

        // execution_rejected: another execution already running
        if (msgType === 'execution_rejected') {
          isSubmittingRunRef.current = false;
          finishedRef.current = true;
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
          const snapshotPhase =
            msg.status === 'running' ? 'running'
            : msg.status === 'cancelling' ? 'cancelling'
            : msg.status === 'succeeded' ? 'succeeded'
            : msg.status === 'failed' ? 'failed'
            : msg.status === 'cancelled' ? 'cancelled'
            : msg.status === 'submitted' ? 'submitted'
            : msg.status === 'graph_building' ? 'graph_building'
            : 'idle';

          dispatchExecution({
            type: 'SET_SNAPSHOT',
            snapshot: {
              phase: snapshotPhase,
              executionId: msg.executionId ?? null,
              startedAt: msg.createdAt ?? null,
              finishedAt: msg.finishedAt ?? null,
              totalNodes: msg.nodeCount ?? 0,
              windowProgress: normalizeWindowProgress(msg.windowProgress),
            }
          });

          const terminal = ['succeeded', 'failed', 'cancelled'].includes(msg.status ?? '');
          if (terminal) {
            // Terminal state: clean up all execution state
            finishedRef.current = true;
            isSubmittingRunRef.current = false;
            restoredExecutionRef.current = false;
            sessionStorage.removeItem('WorkFlow_execution_id');
            setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          } else if (['running', 'submitted', 'graph_building', 'cancelling'].includes(msg.status ?? '')) {
            // Active state: restore edge animation
            setEdges(eds => eds.map(e => ({ ...e, animated: true })));
          }
          return;
        }

        if (msgType === 'window_progress') {
          const windowProgress = normalizeWindowProgress(msg);
          if (windowProgress) {
            dispatchExecution({ type: 'SET_WINDOW_PROGRESS', progress: windowProgress });
          }
          return;
        }

        // node_status messages carry simple node status updates.
        // Legacy progress remains a node status and also restores Window progress
        // for executions started by a backend that predates window_progress.
        if (msgType === 'node_status' || msgType === 'progress') {
          if (msgType === 'progress') {
            const legacyWindowProgress = parseLegacyWindowProgress(msg.message);
            if (legacyWindowProgress) {
              dispatchExecution({
                type: 'SET_WINDOW_PROGRESS',
                progress: legacyWindowProgress,
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
          finishedRef.current = true;
          isSubmittingRunRef.current = false;

          const status = msg.status;
          // cancelling is not terminal — handled separately
          if (status === 'cancelling') {
            return;
          }

          if (!isTerminalStatus(status)) {
            if (status) {
              dispatchExecution({ type: 'SET_PHASE', phase: status });
            }
            return;
          }
          sessionStorage.removeItem('WorkFlow_execution_id');
          dispatchExecution({ type: 'FINISH', status, message: msg.message });

          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          resetRuntimeNodeState(setNodes);

          const logType: LogEntry['type'] = status === 'failed' || status === 'cancelled' ? 'error' : 'success';
          addLog(msg.message || `Execution ${status}`, logType);
          return;
        }

        // LEGACY: done means succeeded
        if (msgType === 'done') {
          if (finishedRef.current) return;
          finishedRef.current = true;
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
            addLog(`Restored execution ${msg.executionId}`, 'success');
            // Only restore edge animation if execution is still active
            const phase = executionStateRef.current.phase;
            if (['running', 'submitted', 'graph_building', 'cancelling'].includes(phase)) {
              setEdges(eds => eds.map(e => ({ ...e, animated: true })));
            } else {
              // Terminal or idle — stop animation and clean up
              setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
              resetRuntimeNodeState(setNodes);
            }
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
      clearReconnectTimer();
      if (wsRef.current) {
        wsRef.current.close(1000, 'component unmount');
        wsRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // <-- 閸忔娊鏁敍姘涧閺堝瀵曟潪?閸楁瓕娴囩憴锕€褰傞敍灞炬￥閸忔湹绮笟婵婄

  // =========================================================
  // Execution preflight and confirmed Run
  // =========================================================
  const runFlow = useCallback(async () => {
    if (isSubmittingRunRef.current || isPreflighting || executionPreflight) return;

    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      addLog('Server not connected!', 'error');
      return;
    }
    if (websocketStatus !== 'connected') {
      addLog('Server not connected!', 'error');
      return;
    }
    if (executionState.phase === 'graph_building' || executionState.phase === 'submitted' ||
        executionState.phase === 'running' || executionState.phase === 'cancelling') {
      return;
    }

    const graph = buildExecutionGraph(nodes, edges);
    const requestId = preflightRequestIdRef.current + 1;
    preflightRequestIdRef.current = requestId;
    preflightAbortRef.current?.abort();
    const controller = new AbortController();
    preflightAbortRef.current = controller;
    pendingGraphRef.current = graph;
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
        body: JSON.stringify({ graph }),
        signal: controller.signal,
      });

      if (!mountedRef.current || controller.signal.aborted || preflightRequestIdRef.current !== requestId) {
        return;
      }
      if (!preflight || typeof preflight.windowable !== 'boolean') {
        throw new Error('Invalid execution preflight response');
      }
      let normalizedPreflight = preflight;
      const shapeIsValid = preflight.output_shape == null
        || isValidOutputShape(preflight.output_shape);
      const windowMetadataComplete = (
        preflight.windowable
        && shapeIsValid
        && preflight.output_shape != null
        && preflight.output_shape.length > 0
        && (preflight.ndim == null || preflight.ndim === preflight.output_shape.length)
      );
      if (!shapeIsValid || (preflight.windowable && !windowMetadataComplete)) {
        normalizedPreflight = {
          windowable: false,
          output_shape: null,
          ndim: null,
          reason: 'Window metadata could not be represented safely in this browser. Full Graph Execution remains available.',
        };
      }

      setExecutionPreflight(normalizedPreflight);
    } catch (err) {
      if (preflightRequestIdRef.current !== requestId) return;
      if (controller.signal.aborted && !didTimeout) return;
      pendingGraphRef.current = null;
      addLog(
        didTimeout
          ? 'Execution preflight timed out after 60 seconds. Please try again.'
          : `Execution preflight failed: ${(err as Error).message}`,
        'error',
      );
    } finally {
      window.clearTimeout(timeoutId);
      if (preflightRequestIdRef.current === requestId) {
        preflightAbortRef.current = null;
        setIsPreflighting(false);
      }
    }
  }, [
    nodes,
    edges,
    addLog,
    websocketStatus,
    executionState.phase,
    isPreflighting,
    executionPreflight,
  ]);

  const cancelExecutionDialog = useCallback(() => {
    preflightRequestIdRef.current += 1;
    preflightAbortRef.current?.abort();
    preflightAbortRef.current = null;
    pendingGraphRef.current = null;
    setIsPreflighting(false);
    setExecutionPreflight(null);
  }, []);

  const confirmExecution = useCallback((executionConfig: ExecutionConfig) => {
    if (isSubmittingRunRef.current) return;

    const graph = pendingGraphRef.current;
    const preflight = executionPreflight;
    if (!graph || !preflight) {
      addLog('Execution preflight has expired. Please click Run again.', 'warning');
      cancelExecutionDialog();
      return;
    }

    let normalizedConfig: ExecutionConfig = { mode: 'full_graph' };
    if (executionConfig.mode === 'window') {
      const outputShape = preflight.output_shape ?? [];
      const windowShape = [...executionConfig.windowShape];
      if (!preflight.windowable) {
        addLog(preflight.reason || 'Window execution is unavailable for this workflow.', 'warning');
        return;
      }
      if (!isValidWindowShape(outputShape, windowShape)) {
        addLog('Window shape must contain one positive integer per output dimension.', 'warning');
        return;
      }
      normalizedConfig = { mode: 'window', windowShape };
    }

    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN || websocketStatus !== 'connected') {
      addLog('Server not connected!', 'error');
      return;
    }
    if (executionState.phase === 'graph_building' || executionState.phase === 'submitted' ||
        executionState.phase === 'running' || executionState.phase === 'cancelling') {
      addLog('Another execution is already active.', 'warning');
      return;
    }

    const executionId = crypto.randomUUID();
    finishedRef.current = false;
    isSubmittingRunRef.current = true;
    pendingGraphRef.current = null;
    setExecutionPreflight(null);
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
        executionConfig: normalizedConfig,
      }));
      addLog('Executing Workflow...', 'info');
    } catch {
      isSubmittingRunRef.current = false;
      finishedRef.current = true;
      sessionStorage.removeItem('WorkFlow_execution_id');
      dispatchExecution({ type: 'RESET' });
      pendingGraphRef.current = graph;
      setExecutionPreflight(preflight);
      setEdges(currentEdges => currentEdges.map(edge => edge.animated ? { ...edge, animated: false } : edge));
      resetRuntimeNodeState(setNodes);
      addLog('Failed to send execute command', 'error');
    }
  }, [
    addLog,
    cancelExecutionDialog,
    executionPreflight,
    executionState.phase,
    setEdges,
    setNodes,
    websocketStatus,
  ]);

  // =========================================================
  // 閸嬫粍顒?(Stop)
  // =========================================================
  const stopFlow = useCallback(() => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) return;
    dispatchExecution({ type: 'SET_PHASE', phase: 'cancelling' });
    wsRef.current.send(JSON.stringify({ command: 'stop_execution' }));
    setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
    addLog('Requesting Stop...', 'warning');
  }, [addLog, setEdges]);

  return {
    websocketStatus,
    nodeDefs,
    pluginDiagnostics,
    pluginStatusError,
    dashboardUrl,
    isReloadingNodes,
    isPreflighting,
    executionPreflight,
    executionState,
    runFlow,
    confirmExecution,
    cancelExecutionDialog,
    stopFlow,
    reloadNodes,
  };
};
