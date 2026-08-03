import type { Node, Edge } from '@xyflow/react';

// Legacy node-level progress classification retained for wire compatibility.
export type ProgressType = 'chunk_count' | 'state_only' | 'stage_based';

export type RunState = 'ready' | 'submitted' | 'running' | 'done' | 'failed' | 'cancelled';

export type ExecutionPhase =
  | 'idle'
  | 'graph_building'
  | 'submitted'
  | 'running'
  | 'cancelling'
  | 'disconnected'
  | 'interrupted'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export type WebSocketStatus = 'connected' | 'reconnecting' | 'disconnected';

export type ExecutionMode = 'full_graph' | 'window';

export type ResumeAction = 'new' | 'resume' | 'restart';

export type RecoveryLocation =
  | { mode: 'output_sidecar'; anchorNodeId: string }
  | { mode: 'custom'; directory: string };

export type ExecutionConfig =
  | { mode: 'full_graph' }
  | {
      mode: 'window';
      windowShape: number[];
      maxInFlightWindows?: number;
      resumeAction: 'new' | 'restart';
      recoveryLocation: RecoveryLocation;
    }
  | {
      mode: 'window';
      windowShape?: number[];
      maxInFlightWindows?: number;
      resumeAction: 'resume';
      recoveryLocation: RecoveryLocation;
    };

export interface ExecutionOutput {
  nodeId: string;
  nodeType: string;
  displayName: string;
  pathInput: string;
  path: string;
}

export interface WindowPlan {
  outputShape: number[];
  windowShape: number[];
  windowGridShape: number[];
  totalWindows: number;
}

export type WindowProgressStatus = 'running' | 'finalizing';

export interface WindowExecutionProgress {
  currentWindow: number;
  completedWindows: number;
  totalWindows: number;
  progress: number;
  windowStatus: WindowProgressStatus;
  message: string;
}

export interface ExecutionPreflightResponse {
  windowable: boolean;
  outputShape?: number[] | null;
  windowShape?: number[] | null;
  windowGridShape?: number[] | null;
  totalWindows?: number | null;
  outputs?: ExecutionOutput[];
  // Transitional aliases accepted from older backends.
  output_shape?: number[] | null;
  ndim?: number | null;
  reason?: string | null;
  requiredResources?: {
    requiresCpu: boolean;
    requiresGpu: boolean;
    cpuWorkers: number;
    gpuWorkers: number;
    cpuNodes: ResourceNodeRequirement[];
    gpuNodes: ResourceNodeRequirement[];
    anyNodes?: ResourceNodeRequirement[];
  };
  availableResources?: {
    cpuWorkers: number | null;
    gpuWorkers: number | null;
    cpuSlots: number | null;
    gpuSlots: number | null;
  };
  resourcesSatisfied?: boolean | null;
  resourceError?: string | null;
}

export type ExecutionResource = 'any' | 'cpu' | 'gpu';

export interface ResourceNodeRequirement {
  nodeId: string;
  nodeType: string;
  displayName: string;
  workers: number;
  resource?: ExecutionResource;
}

export type RecoveryManifestStatus =
  | 'prepared'
  | 'running'
  | 'interrupted'
  | 'failed'
  | 'cancelled'
  | 'succeeded';

export interface RecoverySummary {
  found: boolean;
  valid: boolean;
  compatible: boolean;
  status: RecoveryManifestStatus;
  recoveryDirectory: string;
  completedWindows: number;
  totalWindows: number;
  remainingWindows: number;
  outputShape: number[];
  windowShape: number[];
  windowGridShape: number[];
  outputs: ExecutionOutput[];
  message?: string | null;
}

export interface RecoveryOpenResponse {
  graph: Record<string, { type: string; inputs: Record<string, unknown> }>;
  readOnly: true;
  executionConfig: ExecutionConfig;
  recoverySummary: RecoverySummary;
}

export interface ServerDirectoryEntry {
  name: string;
  path: string;
  isRecoveryDirectory: boolean;
}

export interface ServerDirectoryListing {
  path: string;
  parent: string | null;
  directories: ServerDirectoryEntry[];
}

export interface RecoveredGraphView {
  recoveryDirectory: string;
  summary: RecoverySummary;
  executionConfig: ExecutionConfig;
}

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
  windowProgress: WindowExecutionProgress | null;
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
  execution_resource?: ExecutionResource;
  execution_workers?: number;
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

export interface ReloadNodesResponse {
  ok: boolean;
  code?: string;
  message?: string;
  error_type?: string;
  error_message?: string;
  loaded?: string[];
  failed?: string[];
  plugin_status?: PluginDiagnostics;
  object_info?: Record<string, NodeSpec>;
  dashboard_url?: string | null;
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
  | 'execution_not_found'
  | 'window_progress'
  | 'execution_control_ack'
  | 'cluster_ready'
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

  // Node-level progress fields retained for backward compatibility.
  progress?: number | null;
  progressType?: ProgressType;
  progressRole?: string;
  totalChunks?: number;
  processedChunks?: number;
  completedInferenceChunks?: number;
  skippedChunks?: number;
  failedChunks?: number;

  status?: 'graph_building' | 'submitted' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'cancelling' | 'interrupted';
  code?: string;
  createdAt?: number;
  finishedAt?: number;
  nodeCount?: number;
  logCount?: number;
  currentWindow?: number;
  completedWindows?: number;
  totalWindows?: number;
  windowStatus?: WindowProgressStatus;
  windowProgress?: WindowExecutionProgress | null;
  action?: string;
  dashboardUrl?: string | null;
  cpuWorkers?: number;
  gpuWorkers?: number;
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
  timestamp: number;
  type: 'info' | 'success' | 'error' | 'warning';
  message: string;
}
