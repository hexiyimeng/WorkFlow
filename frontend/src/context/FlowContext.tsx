// src/context/FlowContext.tsx
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { useNodesState, useEdgesState, addEdge, type Connection, type Node, type Edge, type OnConnectStart, type OnConnectEnd } from '@xyflow/react';
import type { LogEntry, NodeData } from '../types';
import { FlowContext } from './FlowContextDef';

import { useUndoRedo } from '../hooks/useUndoRedo';
import { useAutoSave } from '../hooks/useAutoSave';
import { useFlowOperations } from '../hooks/useFlowOperations';
import { useFlowEngine } from '../hooks/useFlowEngine';
import { useWorkflows } from '../hooks/useWorkflows';
import { canConnectPorts, resolveNodeOutputTypes } from '../utils/portTypes';

export const FlowProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  // ===========================================
  // 1. Base State
  // ===========================================
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<NodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const [isConsoleOpen, setIsConsoleOpen] = useState(true);
  const [connectingType, setConnectingType] = useState<string | null>(null);

  // Log system
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const logBufferRef = useRef<LogEntry[]>([]);

  const addLog = useCallback((message: string, type: 'info' | 'success' | 'error' | 'warning' = 'info') => {
    logBufferRef.current.push({ id: Date.now().toString() + Math.random(), timestamp: new Date().toLocaleTimeString(), type, message });
  }, []);

  const clearLogs = useCallback(() => {
    logBufferRef.current = [];
    setLogs([]);
  }, []);

  // Log tick loop — batch updates to avoid high-frequency setState
  useEffect(() => {
    const tick = setInterval(() => {
      if (logBufferRef.current.length > 0) {
        const newLogs = [...logBufferRef.current];
        logBufferRef.current = [];
        setLogs(prev => [...prev, ...newLogs].slice(-100));
      }
    }, 100);
    return () => clearInterval(tick);
  }, []);

  // Theme effect
  useEffect(() => { document.documentElement.classList.toggle('dark', theme === 'dark'); }, [theme]);

  const toggleTheme = useCallback(() => setTheme(t => t === 'light' ? 'dark' : 'light'), []);
  const toggleConsole = useCallback(() => setIsConsoleOpen(p => !p), []);

  // ===========================================
  // 2. Engine core — must come first so nodeDefs is available
  //    to downstream hooks (autosave / workflows / undo-redo / operations)
  // ===========================================

  const {
    websocketStatus,
    nodeDefs,
    executionState,
    runFlow,
    stopFlow,
  } = useFlowEngine(nodes, edges, setNodes, setEdges, addLog);

  // ===========================================
  // 3. Undo/Redo — snapshots store stripped nodes, restore rehydrates
  // ===========================================
  const { undo, redo, takeSnapshot, syncCurrentState } = useUndoRedo(
    [], [], (nds) => setNodes(nds), (eds) => setEdges(eds), nodeDefs
  );

  // ===========================================
  // 4. Autosave — restores stripped data with latest specs
  // ===========================================
  useAutoSave(nodes, edges, setNodes, setEdges, nodeDefs);

  // ===========================================
  // 5. Derived execution state — must precede workflow wrappers
  // ===========================================
  const isExecuting = executionState.phase === 'graph_building'
    || executionState.phase === 'submitted'
    || executionState.phase === 'running'
    || executionState.phase === 'cancelling';
  const isCancelling = executionState.phase === 'cancelling';
  const isExecutionLocked = isExecuting;
  const isConnected = websocketStatus === 'connected';

  // ===========================================
  // 6. Workflows — stored as stripped nodes, hydrated on switch
  // ===========================================
  const {
    workflows, activeWorkflowId, createWorkflow: _createWorkflow, switchWorkflow: _switchWorkflow,
    deleteWorkflow: _deleteWorkflow, renameWorkflow, saveCurrentWorkflow
  } = useWorkflows(nodes, edges, setNodes, setEdges, nodeDefs, addLog);

  // Wrap workflow ops with execution lock
  const createWorkflow = useCallback(() => {
    if (isExecutionLocked) { addLog('Cannot create workflow while executing', 'warning'); return; }
    _createWorkflow();
  }, [isExecutionLocked, addLog, _createWorkflow]);

  const switchWorkflow = useCallback((id: string) => {
    if (isExecutionLocked) { addLog('Cannot switch workflow while executing', 'warning'); return; }
    _switchWorkflow(id);
  }, [isExecutionLocked, addLog, _switchWorkflow]);

  const deleteWorkflow = useCallback((id: string) => {
    if (isExecutionLocked) { addLog('Cannot delete workflow while executing', 'warning'); return; }
    _deleteWorkflow(id);
  }, [isExecutionLocked, addLog, _deleteWorkflow]);

  // ===========================================
  // 6. Snapshot trigger — only on non-running state changes
  // ===========================================
  useEffect(() => {
    syncCurrentState(nodes, edges);
    const hasActiveExecution = nodes.some(
      n => n.data.runState === 'submitted' || n.data.runState === 'running'
    );
    if (!hasActiveExecution) takeSnapshot();
  }, [nodes, edges, syncCurrentState, takeSnapshot]);

  // ===========================================
  // 7. Snapshot trigger — only on non-running state changes
  // ===========================================

  // ===========================================
  // 8. Flow operations — paste hydrates with fresh specs
  // ===========================================
  const { handleCopy, handlePaste, handleDelete } = useFlowOperations(
    nodes, edges, setNodes, setEdges,
    undo, redo, addLog, isExecutionLocked, nodeDefs
  );

  // ===========================================
  // 9. Connection validation helpers
  // ===========================================
  const getConnectionTypeError = useCallback((
    sourceId: string | null | undefined,
    targetId: string | null | undefined,
    sourceHandle: string | null | undefined,
    targetHandle: string | null | undefined,
  ) => {
    const sourceNode = nodes.find(n => n.id === sourceId);
    const targetNode = nodes.find(n => n.id === targetId);
    if (!sourceNode || !targetNode || !targetHandle) return 'Invalid Connection';

    const sourceIndex = parseInt(sourceHandle || '0');
    const sourceType = resolveNodeOutputTypes(sourceNode.data.nodeSpec, sourceNode.data.values)[sourceIndex] || 'unknown';
    const targetConfig = targetNode.data.nodeSpec.input?.required?.[targetHandle] || targetNode.data.nodeSpec.input?.optional?.[targetHandle];
    const targetType = Array.isArray(targetConfig) && typeof targetConfig[0] === 'string'
      ? targetConfig[0]
      : 'unknown';

    return canConnectPorts(sourceType, targetType).reason || 'Invalid Connection';
  }, [nodes]);

  const isValidConnection = useCallback((connection: Connection | Edge) => {
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);
    if (!sourceNode || !targetNode) return false;

    const sourceSpec = sourceNode.data.nodeSpec;
    const targetSpec = targetNode.data.nodeSpec;
    const sourceHandleIndex = parseInt(connection.sourceHandle || '0');
    const targetHandleName = connection.targetHandle;

    if (!sourceSpec?.output?.[sourceHandleIndex] || !targetSpec || !targetHandleName) return false;

    const outputTypes = resolveNodeOutputTypes(sourceSpec, sourceNode.data.values);
    const outputType = outputTypes[sourceHandleIndex];
    const inputConfig = targetSpec.input?.required?.[targetHandleName] || targetSpec.input?.optional?.[targetHandleName];
    if (!outputType || !inputConfig) return false;

    const inputType = Array.isArray(inputConfig) ? inputConfig[0] : inputConfig;
    if (typeof inputType !== 'string') return false;
    return canConnectPorts(outputType, inputType).ok;
  }, [nodes]);

  const onConnect = useCallback((params: Connection) => {
    if (isExecutionLocked) { addLog('Cannot connect while executing', 'warning'); return; }
    if (!params.targetHandle) return;
    if (!isValidConnection(params)) {
      addLog(getConnectionTypeError(params.source, params.target, params.sourceHandle, params.targetHandle), 'error');
      return;
    }
    setEdges(eds => {
      const withoutExisting = eds.filter(
        e => !(e.target === params.target && e.targetHandle === params.targetHandle)
      );
      return addEdge({ ...params, animated: false, style: { stroke: '#94a3b8', strokeWidth: 2 } }, withoutExisting);
    });
  }, [setEdges, isValidConnection, addLog, isExecutionLocked, getConnectionTypeError]);

  const onConnectStart: OnConnectStart = useCallback((_, { nodeId, handleId, handleType }) => {
    if (isExecutionLocked) return;
    if (handleType !== 'source') return;
    const node = nodes.find(n => n.id === nodeId);
    if (node) setConnectingType(resolveNodeOutputTypes(node.data.nodeSpec, node.data.values)[parseInt(handleId || '0')] || null);
  }, [nodes, isExecutionLocked]);

  const onConnectEnd: OnConnectEnd = useCallback((_, connectionState) => {
    setConnectingType(null);
    if (
      connectionState.isValid === false
      && connectionState.fromNode
      && connectionState.toNode
      && connectionState.fromHandle
      && connectionState.toHandle
    ) {
      addLog(
        getConnectionTypeError(
          connectionState.fromNode.id,
          connectionState.toNode.id,
          connectionState.fromHandle.id,
          connectionState.toHandle.id,
        ),
        'error',
      );
    }
  }, [addLog, getConnectionTypeError]);

  const addNodeAt = useCallback((type: string, position: {x: number, y: number}) => {
    if (isExecutionLocked) { addLog('Cannot add node while executing', 'warning'); return; }
    const spec = nodeDefs[type];
    if (!spec) return;
    setNodes(nds => nds.concat({
      id: `${type}_${Date.now()}`,
      type: 'dynamic',
      position,
      data: {
        opType: type,
        nodeSpec: spec,
        values: {},
        message: '',
      }
    }));
  }, [nodeDefs, setNodes, addLog, isExecutionLocked]);

  const addNode = useCallback((type: string) => addNodeAt(type, { x: Math.random() * 400 + 200, y: Math.random() * 300 + 100 }), [addNodeAt]);

  const updateNodeData = useCallback((id: string, newData: Partial<NodeData>) => {
    setNodes(nds => nds.map(n => n.id === id ? { ...n, data: { ...n.data, ...newData } } : n));
  }, [setNodes]);

  // ===========================================
  // 10. Context memoization
  // ===========================================
  const contextValue = useMemo(() => ({
    nodes, edges, nodeDefs, isConnected: isConnected, logs, workflows, activeWorkflowId,
    executionState,
    websocketStatus,
    currentExecutionId: executionState.executionId,
    isExecuting,
    isCancelling,
    isExecutionLocked,
    setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
    addNode, addNodeAt, updateNodeData,
    runFlow, stopFlow, clearLogs, addLog,
    createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
    theme, toggleTheme, isConsoleOpen, toggleConsole,
    isValidConnection, undo, redo,
    onConnectStart, onConnectEnd, connectingType,
    handleCopy, handlePaste, handleDelete,
  }), [
    nodes, edges, nodeDefs, isConnected, logs, workflows, activeWorkflowId,
    theme, isConsoleOpen, connectingType,
    executionState, websocketStatus, isExecuting, isCancelling, isExecutionLocked,
    setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
    addNode, addNodeAt, updateNodeData, runFlow, stopFlow, clearLogs, addLog,
    createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
    toggleTheme, toggleConsole, isValidConnection, undo, redo,
    onConnectStart, onConnectEnd, handleCopy, handlePaste, handleDelete,
  ]);

  return (
    <FlowContext.Provider value={contextValue}>
      {children}
    </FlowContext.Provider>
  );
};
