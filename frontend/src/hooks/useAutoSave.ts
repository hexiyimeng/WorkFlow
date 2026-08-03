// src/hooks/useAutoSave.ts
import { useEffect, useRef } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { NodeData, NodeSpec } from '../types';
import {
  serializeFlowForStorage,
  parseStoredFlow,
  hydrateFlowWithLatestSpecs,
} from '../utils/workflowPersistence';

export const useAutoSave = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  nodeDefs: Record<string, NodeSpec>,
  persistenceEnabled = true,
) => {
  // Track whether initial restore has happened — only restore once on mount
  const hasRestoredRef = useRef(false);

  // ============================================================
  // 1. Restore from autosave (once, on mount, after nodeDefs is ready)
  // ============================================================
  useEffect(() => {
    if (hasRestoredRef.current) return;
    if (Object.keys(nodeDefs).length === 0) return; // nodeDefs not loaded yet

    hasRestoredRef.current = true;

    const savedData = localStorage.getItem('WorkFlow_AUTOSAVE');
    if (!savedData) return;

    try {
      const parsed = JSON.parse(savedData);
      const flow = parseStoredFlow(parsed);
      if (!flow || flow.nodes.length === 0) return;

      const result = hydrateFlowWithLatestSpecs(flow, nodeDefs);
      setNodes(result.nodes);
      setEdges(result.edges);

      if (result.warnings.length > 0) {
        result.warnings.forEach(w => console.warn('[AutoSave]', w));
      }
      if (result.invalidNodeTypes.length > 0) {
        console.warn(`[AutoSave] ${result.invalidNodeTypes.length} unavailable node type(s) marked invalid`);
      }
      if (result.removedEdges > 0) {
        console.warn(`[AutoSave] Removed ${result.removedEdges} invalid connection(s) after updating node definitions`);
      }
    } catch (e) {
      console.error('[AutoSave] Restore failed:', e);
    }
  }, [nodeDefs, setNodes, setEdges]);

  // ============================================================
  // 2. Autosave (save stripped/serialized data only)
  // ============================================================
  useEffect(() => {
    if (!persistenceEnabled) return;
    const timer = setTimeout(() => {
      const stripped = serializeFlowForStorage(nodes, edges);
      const dataToSave = { ...stripped, timestamp: Date.now() };
      localStorage.setItem('WorkFlow_AUTOSAVE', JSON.stringify(dataToSave));
    }, 1000);
    return () => clearTimeout(timer);
  }, [nodes, edges, persistenceEnabled]);
};
