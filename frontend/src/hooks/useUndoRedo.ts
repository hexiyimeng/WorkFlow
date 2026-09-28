// src/hooks/useUndoRedo.ts
import { useState, useCallback, useEffect, useRef } from 'react';
import type { Node, Edge } from '@xyflow/react';
import { debounce } from 'lodash-es';
import type { NodeData, NodeSpec } from '../types';
import { serializeNodesForStorage, hydrateNodesWithLatestSpecs, validateEdgesWithLatestSpecs } from '../utils/workflowPersistence';
import type { SerializedNode } from '../utils/workflowPersistence';

interface HistoryState {
  nodes: SerializedNode[];
  edges: Edge[];
}

// NOTE: useUndoRedo always operates on Node<NodeData> internally.
// The caller (FlowContext) passes T=NodeData so the hook composes with the rest
// of the context, but internally we cast to Node<NodeData> for hydration compatibility.
export const useUndoRedo = (
  _initialNodes: Node<NodeData>[],
  _initialEdges: Edge[],
  setNodes: (nodes: Node<NodeData>[]) => void,
  setEdges: (edges: Edge[]) => void,
  nodeDefs: Record<string, NodeSpec> = {},
) => {
  const [past, setPast] = useState<HistoryState[]>([]);
  const [future, setFuture] = useState<HistoryState[]>([]);

  const currentState = useRef<{ nodes: Node<NodeData>[]; edges: Edge[] }>({
    nodes: [] as Node<NodeData>[],
    edges: [] as Edge[],
  });

  const syncCurrentState = useCallback((nodes: Node<NodeData>[], edges: Edge[]) => {
    currentState.current = { nodes, edges };
  }, []);

  // Snapshot: store stripped (no runtime data) nodes
  const saveToHistory = useCallback(() => {
    const stripped = serializeNodesForStorage(currentState.current.nodes);
    const newEntry: HistoryState = {
      nodes: stripped,
      edges: currentState.current.edges,
    };

    setPast(prev => {
      // Skip if identical to the last entry (e.g. position-only drag updates)
      const lastEntry = prev[prev.length - 1];
      if (lastEntry && (
        (lastEntry.nodes === newEntry.nodes || JSON.stringify(lastEntry.nodes) === JSON.stringify(newEntry.nodes)) &&
        lastEntry.edges === newEntry.edges
      )) {
        return prev;
      }
      const newPast = [...prev, newEntry];
      return newPast.length > 30 ? newPast.slice(newPast.length - 30) : newPast;
    });
    setFuture([]);
  }, []);

  const debouncedSaveHistoryRef = useRef<ReturnType<typeof debounce> | null>(null);

  useEffect(() => {
    // Save initial empty-canvas state on first mount so the first user action
    // (add / delete / connect / move) is undoable with a single Ctrl+Z.
    const stripped = serializeNodesForStorage(currentState.current.nodes);
    setPast([{ nodes: stripped, edges: currentState.current.edges }]);
  }, []); // intentionally once on mount

  useEffect(() => {
    if (debouncedSaveHistoryRef.current) {
      debouncedSaveHistoryRef.current.cancel();
    }
    debouncedSaveHistoryRef.current = debounce(saveToHistory, 500);
    return () => {
      if (debouncedSaveHistoryRef.current) {
        debouncedSaveHistoryRef.current.cancel();
      }
    };
  }, [saveToHistory]);

  const takeSnapshot = useCallback(() => {
    if (debouncedSaveHistoryRef.current) {
      debouncedSaveHistoryRef.current();
    }
  }, []);

  const undo = useCallback(() => {
    setPast(prev => {
      // Need at least 2 entries: [oldStates..., currentState]
      // past[last] is the current state — pop it and restore past[length-1]
      if (prev.length <= 1) return prev;
      const newPast = [...prev];
      newPast.pop(); // remove current state
      const previousEntry = newPast[newPast.length - 1]; // now the last entry is the restore target

      setFuture(f => [{
        nodes: serializeNodesForStorage(currentState.current.nodes),
        edges: currentState.current.edges,
      }, ...f]);

      if (Object.keys(nodeDefs).length > 0) {
        const hydratedNodes = hydrateNodesWithLatestSpecs(previousEntry.nodes, nodeDefs);
        const { validEdges } = validateEdgesWithLatestSpecs(previousEntry.edges, hydratedNodes);
        const castNodes = hydratedNodes as unknown as Node<NodeData>[];
        setNodes(castNodes);
        setEdges(validEdges);
        currentState.current = { nodes: castNodes, edges: validEdges };
      } else {
        const fallbackNodes = previousEntry.nodes as unknown as Node<NodeData>[];
        setNodes(fallbackNodes);
        setEdges(previousEntry.edges);
        currentState.current = { nodes: fallbackNodes, edges: previousEntry.edges };
      }

      return newPast;
    });
  }, [setNodes, setEdges, nodeDefs]);

  const redo = useCallback(() => {
    setFuture(prev => {
      if (prev.length === 0) return prev;
      const newFuture = [...prev];
      const nextEntry = newFuture.shift();
      if (!nextEntry) return prev;

      setPast(p => [
        ...p,
        {
          nodes: serializeNodesForStorage(currentState.current.nodes),
          edges: currentState.current.edges,
        },
      ]);

      if (Object.keys(nodeDefs).length > 0) {
        const hydratedNodes = hydrateNodesWithLatestSpecs(nextEntry.nodes, nodeDefs);
        const { validEdges } = validateEdgesWithLatestSpecs(nextEntry.edges, hydratedNodes);
        const castNodes = hydratedNodes as unknown as Node<NodeData>[];
        setNodes(castNodes);
        setEdges(validEdges);
        currentState.current = { nodes: castNodes, edges: validEdges };
      } else {
        setNodes(currentState.current.nodes);
        setEdges(currentState.current.edges);
      }

      return newFuture;
    });
  }, [setNodes, setEdges, nodeDefs]);

  return {
    undo,
    redo,
    takeSnapshot,
    syncCurrentState,
    canUndo: past.length > 0,
    canRedo: future.length > 0,
  };
};
