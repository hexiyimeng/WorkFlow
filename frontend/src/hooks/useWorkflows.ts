// src/hooks/useWorkflows.ts
import { useState, useCallback, useEffect, useMemo } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type {
  Workflow,
  WorkflowMetadata,
  NodeData,
  NodeSpec,
} from '../types';
import {
  serializeFlowForStorage,
  parseStoredFlow,
  parseWorkflowDocument,
  hydrateFlowWithLatestSpecs,
  cloneWorkflowMetadata,
  type ParsedWorkflowDocument,
} from '../utils/workflowPersistence';
import { createWorkflowId } from '../utils/workflowExecutionSettings';

const AUTOSAVE_KEY = 'WorkFlow_AUTOSAVE';

const createTabId = (): string => `tab-${createWorkflowId()}`;

const cloneMetadata = (metadata: WorkflowMetadata): WorkflowMetadata => (
  cloneWorkflowMetadata(metadata)
);

const defaultWorkflow = (): Workflow => ({
  id: createTabId(),
  workflowId: createWorkflowId(),
  metadata: {},
  name: 'Workflow 1',
  nodes: [],
  edges: [],
  timestamp: Date.now(),
});

/**
 * Read identity and metadata synchronously so the first render already has the
 * stable workflowId used by the settings store. Graph hydration remains the
 * responsibility of useAutoSave once current node definitions are available.
 */
const initialWorkflowFromAutosave = (): Workflow => {
  let raw: string | null = null;
  try {
    raw = typeof localStorage === 'undefined'
      ? null
      : localStorage.getItem(AUTOSAVE_KEY);
  } catch {
    return defaultWorkflow();
  }
  if (!raw) return defaultWorkflow();

  try {
    const parsed = parseWorkflowDocument(JSON.parse(raw));
    if (!parsed) return defaultWorkflow();
    return {
      id: createTabId(),
      workflowId: parsed.workflowId,
      metadata: cloneMetadata(parsed.metadata),
      name: parsed.workflowName?.trim() || 'Workflow 1',
      nodes: parsed.nodes as unknown as Node<NodeData>[],
      edges: parsed.edges,
      timestamp: parsed.timestamp ?? Date.now(),
    };
  } catch (error) {
    console.warn('[Workflows] Could not read autosaved workflow metadata:', error);
    return defaultWorkflow();
  }
};

export type LoadWorkflowDocument = (
  document: ParsedWorkflowDocument,
  hydratedNodes: Node<NodeData>[],
  hydratedEdges: Edge[],
) => void;

export const useWorkflows = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  nodeDefs: Record<string, NodeSpec>,
  addLog: (msg: string, type?: 'info' | 'success' | 'error' | 'warning') => void,
  persistenceEnabled = true,
) => {
  const [initialWorkflow] = useState<Workflow>(initialWorkflowFromAutosave);

  const [workflows, setWorkflows] = useState<Workflow[]>(() => [initialWorkflow]);
  const [activeWorkflowId, setActiveWorkflowId] = useState<string>(initialWorkflow.id);

  const nodeDefsReady = Object.keys(nodeDefs).length > 0;
  const activeWorkflow = useMemo(
    () => workflows.find(workflow => workflow.id === activeWorkflowId) ?? null,
    [activeWorkflowId, workflows],
  );

  // === Core: save current canvas to active workflow (metadata is preserved) ===
  const saveCurrentWorkflow = useCallback(() => {
    if (!persistenceEnabled || !nodeDefsReady) return;
    const serialized = serializeFlowForStorage(nodes, edges);
    setWorkflows(previous => previous.map(workflow => (
      workflow.id === activeWorkflowId
        ? {
            ...workflow,
            nodes: serialized.nodes as unknown as Node<NodeData>[],
            edges: serialized.edges,
            timestamp: Date.now(),
          }
        : workflow
    )));
  }, [
    activeWorkflowId,
    edges,
    nodeDefsReady,
    nodes,
    persistenceEnabled,
  ]);

  // Auto-sync graph changes without replacing identity or metadata. Waiting for
  // node definitions also prevents the empty initial canvas from overwriting a
  // saved graph before useAutoSave completes hydration.
  useEffect(() => {
    if (!persistenceEnabled || !nodeDefsReady) return;
    const timer = window.setTimeout(() => saveCurrentWorkflow(), 500);
    return () => window.clearTimeout(timer);
  }, [nodeDefsReady, persistenceEnabled, saveCurrentWorkflow]);

  const updateWorkflowMetadata = useCallback((
    tabId: string,
    metadata: WorkflowMetadata,
  ) => {
    const normalized = cloneMetadata(metadata);
    setWorkflows(previous => previous.map(workflow => (
      workflow.id === tabId
        ? { ...workflow, metadata: normalized, timestamp: Date.now() }
        : workflow
    )));
  }, []);

  /** Atomically apply an imported/autosaved document to the active tab. */
  const loadWorkflowDocument = useCallback<LoadWorkflowDocument>((
    document,
    hydratedNodes,
    hydratedEdges,
  ) => {
    const serialized = serializeFlowForStorage(hydratedNodes, hydratedEdges);
    const metadata = cloneMetadata(document.metadata);
    setWorkflows(previous => previous.map(workflow => (
      workflow.id === activeWorkflowId
        ? {
            ...workflow,
            workflowId: document.workflowId,
            metadata,
            name: document.workflowName?.trim() || workflow.name,
            nodes: serialized.nodes as unknown as Node<NodeData>[],
            edges: serialized.edges,
            timestamp: document.timestamp ?? Date.now(),
          }
        : workflow
    )));
    setNodes(hydratedNodes);
    setEdges(hydratedEdges);
  }, [activeWorkflowId, setEdges, setNodes]);

  const switchWorkflow = useCallback((id: string) => {
    saveCurrentWorkflow();

    const target = workflows.find(workflow => workflow.id === id);
    if (!target) return;
    setActiveWorkflowId(id);

    if (nodeDefsReady) {
      const flow = parseStoredFlow({ nodes: target.nodes, edges: target.edges });
      if (!flow) {
        setNodes([]);
        setEdges([]);
      } else {
        const result = hydrateFlowWithLatestSpecs(flow, nodeDefs);
        setNodes(result.nodes);
        setEdges(result.edges);
        result.warnings.forEach(warning => console.warn('[Workflows]', warning));
        if (result.invalidNodeTypes.length > 0) {
          addLog(
            `${result.invalidNodeTypes.length} unavailable node type(s) marked invalid`,
            'warning',
          );
        }
        if (result.removedEdges > 0) {
          addLog(`Removed ${result.removedEdges} invalid connection(s)`, 'info');
        }
      }
    } else {
      setNodes(target.nodes as unknown as Node<NodeData>[]);
      setEdges(target.edges);
    }
    addLog(`Switched to ${target.name}`, 'info');
  }, [
    addLog,
    nodeDefs,
    nodeDefsReady,
    saveCurrentWorkflow,
    setEdges,
    setNodes,
    workflows,
  ]);

  const createWorkflow = useCallback(() => {
    saveCurrentWorkflow();

    const newName = `Workflow ${workflows.length + 1}`;
    const newWorkflow: Workflow = {
      id: createTabId(),
      workflowId: createWorkflowId(),
      metadata: {},
      name: newName,
      nodes: [],
      edges: [],
      timestamp: Date.now(),
    };

    setWorkflows(previous => [...previous, newWorkflow]);
    setActiveWorkflowId(newWorkflow.id);
    setNodes([]);
    setEdges([]);
    addLog(`Created ${newName}`, 'success');
  }, [addLog, saveCurrentWorkflow, setEdges, setNodes, workflows.length]);

  const deleteWorkflow = useCallback((id: string) => {
    if (workflows.length <= 1) {
      addLog('Cannot delete the last workflow', 'warning');
      return;
    }

    const targetName = workflows.find(workflow => workflow.id === id)?.name;
    const remaining = workflows.filter(workflow => workflow.id !== id);
    setWorkflows(remaining);

    if (activeWorkflowId === id) {
      const nextWorkflow = remaining[0];
      setActiveWorkflowId(nextWorkflow.id);
      if (nodeDefsReady) {
        const flow = parseStoredFlow({
          nodes: nextWorkflow.nodes,
          edges: nextWorkflow.edges,
        });
        if (flow) {
          const result = hydrateFlowWithLatestSpecs(flow, nodeDefs);
          setNodes(result.nodes);
          setEdges(result.edges);
        } else {
          setNodes([]);
          setEdges([]);
        }
      } else {
        setNodes(nextWorkflow.nodes as unknown as Node<NodeData>[]);
        setEdges(nextWorkflow.edges);
      }
    }
    addLog(`Deleted ${targetName}`, 'info');
  }, [
    activeWorkflowId,
    addLog,
    nodeDefs,
    nodeDefsReady,
    setEdges,
    setNodes,
    workflows,
  ]);

  const renameWorkflow = useCallback((id: string, name: string) => {
    setWorkflows(previous => previous.map(workflow => (
      workflow.id === id
        ? { ...workflow, name, timestamp: Date.now() }
        : workflow
    )));
  }, []);

  return {
    workflows,
    activeWorkflow,
    activeWorkflowId,
    createWorkflow,
    switchWorkflow,
    deleteWorkflow,
    renameWorkflow,
    saveCurrentWorkflow,
    updateWorkflowMetadata,
    loadWorkflowDocument,
  };
};
