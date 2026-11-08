import { useCallback, useEffect, useRef } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { NodeData, NodeSpec } from '../types';
import { serializeFlowForStorage, hydrateFlowWithLatestSpecs, type SerializedFlow } from '../utils/workflowPersistence';

export const useUndoRedo = (
  _initialNodes: Node<NodeData>[], _initialEdges: Edge[],
  setNodes: (nodes: Node<NodeData>[]) => void, setEdges: (edges: Edge[]) => void,
  nodeDefs: Record<string, NodeSpec> = {},
) => {
  const current = useRef({ nodes: _initialNodes, edges: _initialEdges });
  const history = useRef<SerializedFlow[]>([{ nodes: [], edges: [] }]);
  const future = useRef<SerializedFlow[]>([]);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const syncCurrentState = useCallback((nodes: Node<NodeData>[], edges: Edge[]) => { current.current = { nodes, edges }; }, []);
  const snapshot = useCallback(() => serializeFlowForStorage(current.current.nodes, current.current.edges), []);
  const commit = useCallback(() => {
    clearTimeout(timer.current);
    const next = snapshot();
    if (JSON.stringify(history.current.at(-1)) === JSON.stringify(next)) return;
    history.current = [...history.current, next].slice(-50);
    future.current = [];
  }, [snapshot]);
  const takeSnapshot = useCallback(() => { clearTimeout(timer.current); timer.current = setTimeout(commit, 300); }, [commit]);
  const resetHistory = useCallback((nodes: Node<NodeData>[], edges: Edge[]) => {
    clearTimeout(timer.current); current.current = { nodes, edges };
    history.current = [serializeFlowForStorage(nodes, edges)]; future.current = [];
  }, []);
  const restore = useCallback((entry: SerializedFlow) => {
    const hydrated = hydrateFlowWithLatestSpecs(entry, nodeDefs);
    current.current = hydrated;
    setNodes(hydrated.nodes); setEdges(hydrated.edges);
  }, [nodeDefs, setNodes, setEdges]);
  const undo = useCallback(() => {
    commit();
    if (history.current.length < 2) return;
    future.current.unshift(history.current.pop()!);
    restore(history.current.at(-1)!);
  }, [commit, restore]);
  const redo = useCallback(() => {
    clearTimeout(timer.current);
    const next = future.current.shift();
    if (!next) return;
    history.current.push(next); restore(next);
  }, [restore]);
  useEffect(() => () => clearTimeout(timer.current), []);
  return { undo, redo, takeSnapshot, syncCurrentState, resetHistory };
};
