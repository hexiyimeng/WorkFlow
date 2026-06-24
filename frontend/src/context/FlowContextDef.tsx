import React from 'react';
import type { Node, Edge, OnNodesChange, OnEdgesChange, OnConnectStart, OnConnectEnd, Connection } from '@xyflow/react';
import type { NodeSpec, Workflow, LogEntry, NodeData, ExecutionRuntimeState, WebSocketStatus, PluginDiagnostics } from '../types';

export interface FlowContextType {
  // === State ===
  nodes: Node<NodeData>[];
  edges: Edge[];
  nodeDefs: Record<string, NodeSpec>;
  pluginDiagnostics: PluginDiagnostics | null;
  pluginStatusError: string | null;
  theme: 'light' | 'dark';
  isConsoleOpen: boolean;
  workflows: Workflow[];
  activeWorkflowId: string;
  logs: LogEntry[];
  // --- Execution state (新增) ---
  executionState: ExecutionRuntimeState;
  websocketStatus: WebSocketStatus;
  currentExecutionId: string | null;
  isExecuting: boolean;        // phase in ['graph_building','submitted','running','cancelling']
  isCancelling: boolean;       // phase === 'cancelling'
  isExecutionLocked: boolean;   // 运行中是否禁止修改（值、连线、增删节点）
  // Legacy
  isConnected: boolean;        // 保持兼容，等价于 websocketStatus === 'connected'

  // === Node/Edge Changes ===
  onNodesChange: OnNodesChange<Node<NodeData>>;
  onEdgesChange: OnEdgesChange<Edge>;
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;

  // === Theme / UI ===
  toggleTheme: () => void;
  toggleConsole: () => void;

  // === Connection ===
  onConnectStart: OnConnectStart;
  onConnectEnd: OnConnectEnd;
  connectingType: string | null;
  onConnect: (connection: Connection) => void;
  isValidConnection: (connection: Connection | Edge) => boolean;

  // === Node manipulation ===
  addNode: (type: string) => void;
  addNodeAt: (type: string, position: {x: number, y: number}) => void;
  updateNodeData: (id: string, data: Partial<NodeData>) => void;

  // === Execution ===
  runFlow: () => void;
  stopFlow: () => void;
  clearLogs: () => void;
  addLog: (message: string, level?: 'info' | 'warning' | 'error' | 'success') => void;

  // === Undo/Redo ===
  undo: () => void;
  redo: () => void;

  // === Clipboard ===
  handleCopy: () => void;
  handlePaste: () => void;
  handleDelete: () => void;

  // === Workflow ===
  createWorkflow: () => void;
  switchWorkflow: (id: string) => void;
  deleteWorkflow: (id: string) => void;
  renameWorkflow: (id: string, name: string) => void;
  saveCurrentWorkflow: () => void;
}

export const FlowContext = React.createContext<FlowContextType | null>(null);
