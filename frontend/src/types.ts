import type { Node, Edge } from '@xyflow/react';

// Deprecated legacy wire type. Runtime UI no longer uses progress bars.
export type ProgressType = 'chunk_count' | 'state_only' | 'stage_based';

export type RunState = 'ready' | 'submitted' | 'running' | 'done' | 'failed' | 'cancelled';

export type ExecutionPhase =
  | 'idle'
  | 'graph_building'
  | 'submitted'
  | 'running'
  | 'cancelling'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export type WebSocketStatus = 'connected' | 'reconnecting' | 'disconnected';

export interface NodeRuntimeData {
  runState?: RunState;
  waitingFor?: string[];
  device?: string;
  executionId?: string | null;
}

export interface ExecutionRuntimeState {
  phase: ExecutionPhase;
  executionId: string | null;
  startedAt: number | null;
  finishedAt: number | null;
  totalNodes: number;
  lastError: string | null;
}

export interface NodeInputConfig {
  [key: string]: [string | string[], Record<string, unknown>?];
}

export interface NodeSpec {
  type: string;
  name?: string;
  display_name: string;
  category: string;
  description?: string;
  input: { required: NodeInputConfig; optional?: NodeInputConfig };
  output: string[];
  output_name?: string[];
  output_node?: boolean;
}

export interface PluginLoadedEntry {
  module: string;
  file?: string | null;
  timestamp?: string;
}

export interface PluginImportFailure {
  stage: 'import';
  module: string;
  file?: string | null;
  error_type: string;
  message: string;
  traceback?: string;
  timestamp?: string;
}

export interface PluginNodeInfoError {
  stage: 'object_info';
  node: string;
  class_name: string;
  module?: string | null;
  file?: string | null;
  error_type: string;
  message: string;
  traceback?: string;
  timestamp?: string;
}

export interface PluginWarningEntry {
  stage?: string;
  module?: string | null;
  file?: string | null;
  message: string;
  timestamp?: string;
}

export interface PluginDiagnostics {
  ok: boolean;
  loaded_count: number;
  failed_count: number;
  node_info_error_count?: number;
  loaded: PluginLoadedEntry[];
  failed_imports: PluginImportFailure[];
  warnings: PluginWarningEntry[];
  node_info_errors?: PluginNodeInfoError[];
}

export interface NodeData extends Record<string, unknown> {
  opType: string;
  nodeSpec: NodeSpec;
  values: Record<string, unknown>;
  message?: string;
  runState?: RunState;
  waitingFor?: string[];
  device?: string;
  executionId?: string | null;
  updateValue?: (id: string, key: string, val: unknown) => void;
  _invalid?: boolean;
  _warning?: string;
}

export type WSMessageType =
  | 'log'
  | 'success'
  | 'warning'
  | 'node_status'
  | 'progress'
  | 'error'
  | 'execution_rejected'
  | 'done'
  | 'executed'
  | 'execution_started'
  | 'execution_finished'
  | 'execution_snapshot'
  | 'execution_control_ack'
  | 'subscribed'
  | 'ping'
  | 'pong';

export interface WSMessage {
  type: WSMessageType;
  message?: string;
  taskId?: string;
  executionId?: string;

  runState?: RunState;
  waitingFor?: string[];
  device?: string;

  // Deprecated legacy progress-bar wire fields. Incoming values are ignored.
  progress?: number | null;
  progressType?: ProgressType;
  progressRole?: string;
  totalChunks?: number;
  processedChunks?: number;
  completedInferenceChunks?: number;
  skippedChunks?: number;
  failedChunks?: number;

  status?: 'graph_building' | 'submitted' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'cancelling';
  createdAt?: number;
  finishedAt?: number;
  nodeCount?: number;
  logCount?: number;
  action?: string;
}

export interface Workflow {
  id: string;
  name: string;
  nodes: Node<NodeData>[];
  edges: Edge[];
  timestamp: number;
}

export interface LogEntry {
  id: string;
  timestamp: string;
  type: 'info' | 'success' | 'error' | 'warning';
  message: string;
}
