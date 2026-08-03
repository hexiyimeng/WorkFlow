// src/hooks/useWorkflows.ts
import { useState, useCallback, useEffect } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { Workflow, NodeData, NodeSpec } from '../types';
import {
  serializeFlowForStorage,
  parseStoredFlow,
  hydrateFlowWithLatestSpecs,
} from '../utils/workflowPersistence';

export const useWorkflows = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  nodeDefs: Record<string, NodeSpec>,
  addLog: (msg: string, type?: 'info' | 'success' | 'error' | 'warning') => void,
  persistenceEnabled = true,
) => {
  // Initial state: one default workflow — store serialized (empty) form
  const [workflows, setWorkflows] = useState<Workflow[]>(() => [
    {
      id: '1',
      name: 'Workflow 1',
      nodes: [],
      edges: [],
      timestamp: Date.now(),
    },
  ]);
  const [activeWorkflowId, setActiveWorkflowId] = useState<string>('1');

  const nodeDefsReady = Object.keys(nodeDefs).length > 0;

  // === Core: save current canvas to activeWorkflow (stripped nodes) ===
  const saveCurrentWorkflow = useCallback(() => {
    if (!persistenceEnabled) return;
    const serialized = serializeFlowForStorage(nodes, edges);
    setWorkflows(prev =>
      prev.map(w =>
        w.id === activeWorkflowId
          ? { ...w, nodes: serialized.nodes as unknown as Node<NodeData>[], edges: serialized.edges, timestamp: Date.now() }
          : w
      )
    );
  }, [nodes, edges, activeWorkflowId, persistenceEnabled]);

  // === Auto-sync (debounced 500ms) ===
  useEffect(() => {
    if (!persistenceEnabled) return;
    const timer = setTimeout(() => saveCurrentWorkflow(), 500);
    return () => clearTimeout(timer);
  }, [nodes, edges, activeWorkflowId, persistenceEnabled, saveCurrentWorkflow]);

  // === 1. Switch workflow ===
  const switchWorkflow = useCallback(
    (id: string) => {
      saveCurrentWorkflow();

      const target = workflows.find(w => w.id === id);
      if (!target) return;

      setActiveWorkflowId(id);

      if (nodeDefsReady) {
        const flow = parseStoredFlow({ nodes: target.nodes, edges: target.edges });
        if (!flow) { setNodes([]); setEdges([]); }
        else {
          const result = hydrateFlowWithLatestSpecs(flow, nodeDefs);
          setNodes(result.nodes);
          setEdges(result.edges);
          if (result.warnings.length > 0) {
            result.warnings.forEach(w => console.warn('[Workflows]', w));
          }
          if (result.invalidNodeTypes.length > 0) {
            addLog(`${result.invalidNodeTypes.length} unavailable node type(s) marked invalid`, 'warning');
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
    },
    [workflows, saveCurrentWorkflow, nodeDefsReady, nodeDefs, setNodes, setEdges, addLog]
  );

  // === 2. Create new workflow ===
  const createWorkflow = useCallback(() => {
    saveCurrentWorkflow();

    const newId = Date.now().toString();
    const newName = `Workflow ${workflows.length + 1}`;

    const newWf: Workflow = {
      id: newId,
      name: newName,
      nodes: [],
      edges: [],
      timestamp: Date.now(),
    };

    setWorkflows(prev => [...prev, newWf]);
    setActiveWorkflowId(newId);
    setNodes([]);
    setEdges([]);
    addLog(`Created ${newName}`, 'success');
  }, [workflows.length, saveCurrentWorkflow, setNodes, setEdges, addLog]);

  // === 3. Delete workflow ===
  const deleteWorkflow = useCallback(
    (id: string) => {
      if (workflows.length <= 1) {
        addLog('Cannot delete the last workflow', 'warning');
        return;
      }

      const targetName = workflows.find(w => w.id === id)?.name;
      const newWfs = workflows.filter(w => w.id !== id);
      setWorkflows(newWfs);

      if (activeWorkflowId === id) {
        const nextWf = newWfs[0];
        setActiveWorkflowId(nextWf.id);
        if (nodeDefsReady) {
          const flow = parseStoredFlow({ nodes: nextWf.nodes, edges: nextWf.edges });
          if (flow) {
            const result = hydrateFlowWithLatestSpecs(flow, nodeDefs);
            setNodes(result.nodes);
            setEdges(result.edges);
          }
        } else {
          setNodes(nextWf.nodes as unknown as Node<NodeData>[]);
          setEdges(nextWf.edges);
        }
      }

      addLog(`Deleted ${targetName}`, 'info');
    },
    [workflows, activeWorkflowId, nodeDefsReady, nodeDefs, setNodes, setEdges, addLog]
  );

  // === 4. Rename workflow ===
  const renameWorkflow = useCallback((id: string, name: string) => {
    setWorkflows(prev => prev.map(w => (w.id === id ? { ...w, name } : w)));
  }, []);

  return {
    workflows,
    activeWorkflowId,
    createWorkflow,
    switchWorkflow,
    deleteWorkflow,
    renameWorkflow,
    saveCurrentWorkflow,
  };
}
