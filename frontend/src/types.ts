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

export interface WorkerPhysicalResources {
  cpu: number;
  memory: string;
  gpu: number;
}

export interface WorkerProfile {
  name: string;
  physical_resources: WorkerPhysicalResources;
  logical_resources: Record<string, number>;
  capabilities: string[];
  threads: number;
}

export interface WorkerPool {
  profile: string;
  processes: number;
  scale: number;
}

export interface SlurmAllocationPlan {
  partition: string;
  partitions: string[];
  timeLimit: string;
  requiredWorkerProfiles: Record<string, number>;
  workerCounts: Record<string, number>;
  totalWorkers: number;
  totalCpu: number;
  totalGpu: number;
  totalMemoryGiB: number;
  jobs: Array<{
    allocationId: string;
    profile: string;
    node: string;
    partition: string;
    workers: number;
    processes: number;
    threads: number;
    slurm: {
      nodes: 1;
      cpus: number;
      memoryGiB: number;
      gpus: number;
      partition: string;
      nodelist: string[];
    };
    logicalResources: Record<string, number>;
  }>;
  nodes: Array<{
    node: string;
    partition: string;
    workers: Record<string, number>;
    cpu: number;
    memoryGiB: number;
    gpu: number;
    jobs: string[];
  }>;
}

/**
 * Informational result of the most recent successful, read-only preflight.
 * This is safe to persist with a workflow; it is never used as a substitute
 * for running preflight again before execution.
 */
export interface LastPreflightSummary {
  outputShape?: number[];
  totalWindows?: number;
  requiredWorkerProfiles?: Record<string, number>;
  /** @deprecated Read-only migration support for settings saved before Phase 2. */
  cpuWorkers?: number;
  /** @deprecated Read-only migration support for settings saved before Phase 2. */
  gpuWorkers?: number;
  validatedAt?: number;
}

/**
 * Persistent settings for an ordinary execution of an editable workflow.
 *
 * Recovery actions and runtime state deliberately do not belong here.  In
 * particular, this type must never grow a resumeAction or executionId field.
 */
export interface WorkflowExecutionSettings {
  version: 1;
  mode: ExecutionMode;
  windowShape?: number[];
  maxInFlightWindows?: number;
  newRunRecoveryLocation?: RecoveryLocation;
  lastPreflight?: LastPreflightSummary;
}

export interface WorkflowMetadata {
  executionSettings?: WorkflowExecutionSettings;
  /** Preserve unrelated/future workflow metadata when loading and saving. */
  [key: string]: unknown;
}

export type WorkflowExecutionSettingsSource = 'metadata' | 'local' | 'default';

export type WorkflowExecutionSettingsField =
  | 'version'
  | 'mode'
  | 'windowShape'
  | 'maxInFlightWindows'
  | 'newRunRecoveryLocation'
  | 'anchorNodeId'
  | 'directory';

export interface WorkflowExecutionSettingsValidation {
  isValid: boolean;
  fieldErrors: Partial<Record<WorkflowExecutionSettingsField, string>>;
  generalError?: string;
}

export interface ResolvedWorkflowExecutionSettings {
  settings: WorkflowExecutionSettings;
  source: WorkflowExecutionSettingsSource;
  /** False only when application defaults were used because nothing was saved. */
  isConfigured: boolean;
  validation: WorkflowExecutionSettingsValidation;
}

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
    requiredWorkerProfiles: Record<string, number>;
    profileRequirements: WorkerProfileRequirement[];
  };
  availableResources?: {
    cpuWorkers: number | null;
    gpuWorkers: number | null;
    cpuSlots: number | null;
    gpuSlots: number | null;
  };
  resourcesSatisfied?: boolean | null;
  resourceError?: string | null;
  allocationPlan?: SlurmAllocationPlan | null;
  preflightError?: {
    type: string;
    message: string;
  } | null;
}

export interface WorkerProfileRequirement {
  nodeId: string;
  nodeType: string;
  displayName: string;
  workerProfile: string;
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
  executionId: string;
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

export interface RecoveryDeleteResponse {
  deleted: true;
  recoveryDirectory: string;
  deletedExecutionId: string;
  outputsPreserved: string[];
  cleanupPending: boolean;
  cleanupDirectory?: string;
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
  required_worker_profile?: string;
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
  | 'dashboard_ready'
  | 'cluster_ready'
  | 'slurm_job_submitted'
  | 'slurm_job_state'
  | 'subscribed'
  | 'ping'
  | 'pong';

export interface WSMessage {
  type: WSMessageType;
  message?: string;
  taskId?: string;
  executionId?: string;
  jobId?: string;
  state?: string;
  node?: string | null;
  reason?: string | null;
  resources?: Record<string, unknown>;

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
  /**
   * Stable persisted identity. Optional only while legacy in-memory workflows
   * are being normalized by the workflow-loading layer.
   */
  workflowId?: string;
  /** Workflow document metadata, including persistent execution settings. */
  metadata?: WorkflowMetadata;
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
