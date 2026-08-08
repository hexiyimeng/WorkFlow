// src/context/FlowContext.tsx
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { useNodesState, useEdgesState, addEdge, type Connection, type Node, type Edge, type OnConnectStart, type OnConnectEnd, type OnNodesChange, type OnEdgesChange } from '@xyflow/react';
import type {
  LogEntry,
  NodeData,
  ExecutionPreflightResponse,
  WorkflowExecutionSettings,
  WorkflowExecutionSettingsValidation,
} from '../types';
import { FlowContext } from './FlowContextDef';

import { useUndoRedo } from '../hooks/useUndoRedo';
import { useAutoSave } from '../hooks/useAutoSave';
import { useFlowOperations } from '../hooks/useFlowOperations';
import { useFlowEngine } from '../hooks/useFlowEngine';
import { useWorkflows } from '../hooks/useWorkflows';
import { useWorkflowExecutionSettingsStore } from '../hooks/useWorkflowExecutionSettingsStore';
import { canConnectPorts, resolveNodeOutputTypes } from '../utils/portTypes';
import { visibleNodeInputNames } from '../utils/inputVisibility';
import {
  blocksExecutionChanges,
  filterLockedEdgeChanges,
  filterLockedNodeChanges,
  isLiveExecutionPhase,
} from '../utils/executionRuntime';
import {
  decideNormalRun,
  normalRunRecoveryFailureValidation,
  preflightFailureValidation,
  validationContextFromPreflight,
} from '../utils/executionExperience';
import {
  buildNewRunExecutionConfig,
  validateWorkflowExecutionSettings,
  withLastPreflightSummary,
} from '../utils/workflowExecutionSettings';

export const FlowProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  // ===========================================
  // 1. Base State
  // ===========================================
  const [nodes, setNodes, applyNodeChanges] = useNodesState<Node<NodeData>>([]);
  const [edges, setEdges, applyEdgeChanges] = useEdgesState<Edge>([]);
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const [isConsoleOpen, setIsConsoleOpen] = useState(true);
  const [connectingType, setConnectingType] = useState<string | null>(null);
  const [isExecutionSettingsOpen, setIsExecutionSettingsOpen] = useState(false);
  const [executionSettingsValidation, setExecutionSettingsValidation] = useState<
    WorkflowExecutionSettingsValidation | null
  >(null);

  // Log system
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const logBufferRef = useRef<LogEntry[]>([]);

  const addLog = useCallback((message: string, type: 'info' | 'success' | 'error' | 'warning' = 'info') => {
    logBufferRef.current.push({ id: Date.now().toString() + Math.random(), timestamp: Date.now(), type, message });
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
    openRecoveryBrowser: _openRecoveryBrowser,
    closeRecoveryBrowser,
    browseServerDirectories,
    inspectRecoveryDirectory,
    openRecoveryDirectory,
    deleteRecoveryDirectory,
    executeRecoveryDirectory,
    closeRecoveredGraph,
    stopFlow,
    reloadNodes,
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
  const isRecoveryGraphReadOnly = recoveredGraphView !== null;
  const {
    workflows,
    activeWorkflow,
    activeWorkflowId,
    createWorkflow: _createWorkflow,
    switchWorkflow: _switchWorkflow,
    deleteWorkflow: _deleteWorkflow,
    renameWorkflow,
    saveCurrentWorkflow,
    updateWorkflowMetadata,
    loadWorkflowDocument: _loadWorkflowDocument,
  } = useWorkflows(
    nodes,
    edges,
    setNodes,
    setEdges,
    nodeDefs,
    addLog,
    !isRecoveryGraphReadOnly,
  );

  useAutoSave(
    nodes,
    edges,
    setNodes,
    setEdges,
    nodeDefs,
    activeWorkflow,
    _loadWorkflowDocument,
    !isRecoveryGraphReadOnly,
  );

  const {
    executionSettingsByWorkflowId,
    resolvedExecutionSettingsByWorkflowId,
    activeWorkflowDocumentId,
    activeExecutionSettings,
    activeExecutionSettingsConfigured,
    activeExecutionSettingsSource,
    activeExecutionSettingsStoredValidation,
    saveActiveExecutionSettings,
  } = useWorkflowExecutionSettingsStore(
    workflows,
    activeWorkflowId,
    updateWorkflowMetadata,
    nodes,
    edges,
  );

  // ===========================================
  // 5. Derived execution state — must precede workflow wrappers
  // ===========================================
  const isExecuting = isLiveExecutionPhase(executionState.phase);
  const isCancelling = executionState.phase === 'cancelling';
  const isExecutionLocked = blocksExecutionChanges(executionState.phase)
    || isPreflighting
    || isRecoveryGraphReadOnly;
  const isConnected = websocketStatus === 'connected';

  const onNodesChange = useCallback<OnNodesChange<Node<NodeData>>>((changes) => {
    const permittedChanges = isExecutionLocked
      ? filterLockedNodeChanges(changes)
      : changes;
    if (permittedChanges.length > 0) applyNodeChanges(permittedChanges);
  }, [applyNodeChanges, isExecutionLocked]);

  const onEdgesChange = useCallback<OnEdgesChange<Edge>>((changes) => {
    const permittedChanges = isExecutionLocked
      ? filterLockedEdgeChanges(changes)
      : changes;
    if (permittedChanges.length > 0) applyEdgeChanges(permittedChanges);
  }, [applyEdgeChanges, isExecutionLocked]);

  // ===========================================
  // 6. Workflows — stored as stripped nodes, hydrated on switch
  // ===========================================
  const resetExecutionSettingsUi = useCallback(() => {
    setIsExecutionSettingsOpen(false);
    setExecutionSettingsValidation(null);
    clearPreparedExecution();
  }, [clearPreparedExecution]);

  const closeExecutionSettings = resetExecutionSettingsUi;

  const openRecoveryBrowser = useCallback(() => {
    resetExecutionSettingsUi();
    _openRecoveryBrowser();
  }, [_openRecoveryBrowser, resetExecutionSettingsUi]);

  // Wrap workflow ops with execution lock
  const createWorkflow = useCallback(() => {
    if (isExecutionLocked) { addLog('Cannot create workflow while executing', 'warning'); return; }
    resetExecutionSettingsUi();
    _createWorkflow();
  }, [isExecutionLocked, addLog, resetExecutionSettingsUi, _createWorkflow]);

  const switchWorkflow = useCallback((id: string) => {
    if (isExecutionLocked) { addLog('Cannot switch workflow while executing', 'warning'); return; }
    resetExecutionSettingsUi();
    _switchWorkflow(id);
  }, [isExecutionLocked, addLog, resetExecutionSettingsUi, _switchWorkflow]);

  const deleteWorkflow = useCallback((id: string) => {
    if (isExecutionLocked) { addLog('Cannot delete workflow while executing', 'warning'); return; }
    resetExecutionSettingsUi();
    _deleteWorkflow(id);
  }, [isExecutionLocked, addLog, resetExecutionSettingsUi, _deleteWorkflow]);

  const loadWorkflowDocument = useCallback<typeof _loadWorkflowDocument>((
    document,
    hydratedNodes,
    hydratedEdges,
  ) => {
    resetExecutionSettingsUi();
    _loadWorkflowDocument(document, hydratedNodes, hydratedEdges);
  }, [_loadWorkflowDocument, resetExecutionSettingsUi]);

  const preflightExecutionSettings = useCallback(async (
    settings: WorkflowExecutionSettings,
  ): Promise<{
    preflight: ExecutionPreflightResponse;
    validation: WorkflowExecutionSettingsValidation;
  }> => {
    // Always discover current output rank and terminal anchors before sending
    // a Window plan. Otherwise a stale-rank Window config can be rejected by
    // the backend before it returns enough metadata to identify the bad field.
    const metadataPreflight = await preflightFlow({ mode: 'full_graph' });
    let validation = validateWorkflowExecutionSettings(
      settings,
      validationContextFromPreflight(metadataPreflight),
    );
    if (settings.mode !== 'window' || !validation.isValid) {
      return { preflight: metadataPreflight, validation };
    }

    const detailedPreflight = await preflightFlow(buildNewRunExecutionConfig(
      settings,
      validationContextFromPreflight(metadataPreflight),
    ));
    validation = validateWorkflowExecutionSettings(
      settings,
      validationContextFromPreflight(detailedPreflight),
    );
    return { preflight: detailedPreflight, validation };
  }, [preflightFlow]);

  const refreshExecutionSettingsPreflight = useCallback(async (
    settings: WorkflowExecutionSettings = activeExecutionSettings,
  ): Promise<void> => {
    try {
      const result = await preflightExecutionSettings(settings);
      if (
        settings === activeExecutionSettings
        && !activeExecutionSettingsStoredValidation.isValid
      ) {
        setExecutionSettingsValidation({
          isValid: false,
          fieldErrors: {
            ...result.validation.fieldErrors,
            ...activeExecutionSettingsStoredValidation.fieldErrors,
          },
          generalError: result.validation.generalError
            ?? activeExecutionSettingsStoredValidation.generalError,
        });
      } else {
        setExecutionSettingsValidation(result.validation);
      }
    } catch (error) {
      setExecutionSettingsValidation(preflightFailureValidation(error));
      throw error;
    }
  }, [
    activeExecutionSettings,
    activeExecutionSettingsStoredValidation,
    preflightExecutionSettings,
  ]);

  const openExecutionSettings = useCallback(() => {
    if (isRecoveryGraphReadOnly) {
      addLog('Close the recovered workflow before editing normal execution settings.', 'warning');
      return;
    }
    closeRecoveryBrowser();
    setExecutionSettingsValidation(activeExecutionSettingsStoredValidation);
    setIsExecutionSettingsOpen(true);
    void refreshExecutionSettingsPreflight(activeExecutionSettings).catch(() => undefined);
  }, [
    activeExecutionSettings,
    activeExecutionSettingsStoredValidation,
    addLog,
    closeRecoveryBrowser,
    isRecoveryGraphReadOnly,
    refreshExecutionSettingsPreflight,
  ]);

  const saveExecutionSettings = useCallback(async (
    settings: WorkflowExecutionSettings,
  ): Promise<boolean> => {
    const structuralValidation = validateWorkflowExecutionSettings(settings);
    if (!structuralValidation.isValid) {
      setExecutionSettingsValidation(structuralValidation);
      return false;
    }

    if (settings.mode === 'full_graph') {
      let settingsToSave = settings;
      try {
        const result = await preflightExecutionSettings(settings);
        if (!result.validation.isValid) {
          setExecutionSettingsValidation(result.validation);
          return false;
        }
        settingsToSave = withLastPreflightSummary(settings, result.preflight);
      } catch (error) {
        // Full Graph has no graph-specific references, so a valid choice can
        // still be saved while the current DAG is incomplete or disconnected.
        addLog(`Settings saved; preflight is unavailable: ${(error as Error).message}`, 'warning');
      }
      saveActiveExecutionSettings(settingsToSave);
      setExecutionSettingsValidation(null);
      clearPreparedExecution();
      return true;
    }

    try {
      const result = await preflightExecutionSettings(settings);
      if (!result.validation.isValid) {
        setExecutionSettingsValidation(result.validation);
        return false;
      }
      saveActiveExecutionSettings(withLastPreflightSummary(settings, result.preflight));
      setExecutionSettingsValidation(null);
      clearPreparedExecution();
      return true;
    } catch (error) {
      setExecutionSettingsValidation(preflightFailureValidation(error));
      return false;
    }
  }, [
    addLog,
    clearPreparedExecution,
    preflightExecutionSettings,
    saveActiveExecutionSettings,
  ]);

  const normalRunInFlightRef = useRef(false);
  const runFlow = useCallback(async (): Promise<void> => {
    if (normalRunInFlightRef.current) return;
    normalRunInFlightRef.current = true;
    try {
      if (isRecoveryGraphReadOnly) {
        addLog('Normal Run always uses the editable workflow. Close Recovery first.', 'warning');
        return;
      }

      const resolved = resolvedExecutionSettingsByWorkflowId[activeWorkflowDocumentId];
      if (!resolved || !activeExecutionSettingsConfigured) {
        setExecutionSettingsValidation({
          isValid: false,
          fieldErrors: {},
          generalError: 'Configure and save Execution Settings before the first run.',
        });
        setIsExecutionSettingsOpen(true);
        try {
          await preflightExecutionSettings(activeExecutionSettings);
        } catch (error) {
          setExecutionSettingsValidation(previous => ({
            ...(previous ?? { isValid: false, fieldErrors: {} }),
            isValid: false,
            generalError: (error as Error).message,
          }));
        }
        return;
      }

      try {
        const result = await preflightExecutionSettings(activeExecutionSettings);
        const decision = decideNormalRun(resolved, result.preflight);
        if (decision.kind === 'open_settings') {
          setExecutionSettingsValidation(decision.validation);
          setIsExecutionSettingsOpen(true);
          return;
        }

        saveActiveExecutionSettings(withLastPreflightSummary(
          activeExecutionSettings,
          result.preflight,
        ));
        if (!submitPreparedExecution(decision.config)) {
          setExecutionSettingsValidation({
            isValid: false,
            fieldErrors: {},
            generalError: 'Execution could not be submitted. Review the saved settings and try again.',
          });
          setIsExecutionSettingsOpen(true);
        }
      } catch (error) {
        setExecutionSettingsValidation(preflightFailureValidation(error));
        setIsExecutionSettingsOpen(true);
      }
    } finally {
      normalRunInFlightRef.current = false;
    }
  }, [
    activeExecutionSettings,
    activeExecutionSettingsConfigured,
    activeWorkflowDocumentId,
    addLog,
    isRecoveryGraphReadOnly,
    preflightExecutionSettings,
    resolvedExecutionSettingsByWorkflowId,
    saveActiveExecutionSettings,
    submitPreparedExecution,
  ]);

  const handledNewRunFailureRef = useRef<string | null>(null);
  useEffect(() => {
    if (
      executionState.phase !== 'failed'
      || !executionState.lastError
      || lastSubmittedExecutionConfig?.mode !== 'window'
      || lastSubmittedExecutionConfig.resumeAction !== 'new'
    ) {
      return;
    }
    const failureKey = `${executionState.executionId ?? ''}:${executionState.lastError}`;
    if (handledNewRunFailureRef.current === failureKey) return;
    handledNewRunFailureRef.current = failureKey;
    const validation = normalRunRecoveryFailureValidation(
      executionState.lastError,
      activeExecutionSettings,
    );
    if (!validation) return;
    setExecutionSettingsValidation(validation);
    setIsExecutionSettingsOpen(true);
  }, [
    activeExecutionSettings,
    executionState.executionId,
    executionState.lastError,
    executionState.phase,
    lastSubmittedExecutionConfig,
  ]);

  // ===========================================
  // 6. Snapshot trigger — only on non-running state changes
  // ===========================================
  useEffect(() => {
    if (isRecoveryGraphReadOnly) return;
    syncCurrentState(nodes, edges);
    const hasActiveExecution = nodes.some(
      n => n.data.runState === 'submitted' || n.data.runState === 'running'
    );
    if (!hasActiveExecution) takeSnapshot();
  }, [nodes, edges, isRecoveryGraphReadOnly, syncCurrentState, takeSnapshot]);

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
    if (isExecutionLocked) return;
    const targetNode = nodes.find(n => n.id === id);
    if (targetNode) {
      const nextData = { ...targetNode.data, ...newData };
      const visibleInputs = visibleNodeInputNames(nextData.nodeSpec, nextData.values ?? {});
      setEdges(eds => eds.filter(edge => (
        edge.target !== id
        || !edge.targetHandle
        || visibleInputs.has(edge.targetHandle)
      )));
    }
    setNodes(nds => nds.map(n => n.id === id ? { ...n, data: { ...n.data, ...newData } } : n));
  }, [isExecutionLocked, nodes, setEdges, setNodes]);

  // ===========================================
  // 10. Context memoization
  // ===========================================
  const contextValue = useMemo(() => ({
    nodes, edges, nodeDefs, pluginDiagnostics, pluginStatusError, dashboardUrl, isReloadingNodes, isConnected: isConnected, logs, workflows, activeWorkflow, activeWorkflowId,
    activeWorkflowDocumentId,
    executionSettingsByWorkflowId,
    activeExecutionSettings,
    activeExecutionSettingsConfigured,
    activeExecutionSettingsSource,
    executionState,
    websocketStatus,
    currentExecutionId: executionState.executionId,
    isExecuting,
    isCancelling,
    isPreflighting,
    executionPreflight,
    isExecutionSettingsOpen,
    executionSettingsValidation,
    isRecoveryBrowserOpen,
    recoveredGraphView,
    isRecoveryGraphReadOnly,
    isExecutionLocked,
    setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
    addNode, addNodeAt, updateNodeData,
    runFlow,
    openExecutionSettings, closeExecutionSettings,
    saveExecutionSettings, refreshExecutionSettingsPreflight,
    openRecoveryBrowser, closeRecoveryBrowser, browseServerDirectories,
    inspectRecoveryDirectory, openRecoveryDirectory, deleteRecoveryDirectory,
    executeRecoveryDirectory,
    closeRecoveredGraph, stopFlow, reloadNodes, clearLogs, addLog,
    createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
    loadWorkflowDocument,
    theme, toggleTheme, isConsoleOpen, toggleConsole,
    isValidConnection, undo, redo,
    onConnectStart, onConnectEnd, connectingType,
    handleCopy, handlePaste, handleDelete,
  }), [
    nodes, edges, nodeDefs, pluginDiagnostics, pluginStatusError, dashboardUrl, isReloadingNodes, isConnected, logs, workflows, activeWorkflow, activeWorkflowId,
    activeWorkflowDocumentId, executionSettingsByWorkflowId,
    activeExecutionSettings, activeExecutionSettingsConfigured, activeExecutionSettingsSource,
    theme, isConsoleOpen, connectingType,
    executionState, websocketStatus, isExecuting, isCancelling, isPreflighting,
    executionPreflight, isExecutionSettingsOpen, executionSettingsValidation,
    isRecoveryBrowserOpen, recoveredGraphView,
    isRecoveryGraphReadOnly, isExecutionLocked,
    setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
    addNode, addNodeAt, updateNodeData, runFlow,
    openExecutionSettings, closeExecutionSettings,
    saveExecutionSettings, refreshExecutionSettingsPreflight,
    openRecoveryBrowser, closeRecoveryBrowser, browseServerDirectories,
    inspectRecoveryDirectory, openRecoveryDirectory, deleteRecoveryDirectory,
    executeRecoveryDirectory,
    closeRecoveredGraph, stopFlow, reloadNodes, clearLogs, addLog,
    createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
    loadWorkflowDocument,
    toggleTheme, toggleConsole, isValidConnection, undo, redo,
    onConnectStart, onConnectEnd, handleCopy, handlePaste, handleDelete,
  ]);

  return (
    <FlowContext.Provider value={contextValue}>
      {children}
    </FlowContext.Provider>
  );
};
