import { useCallback, useMemo, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import type { Edge, Node } from '@xyflow/react';
import type { NodeData } from '../types';
import { graphAtPath, updateGraphAtPath, type Graph } from '../utils/subgraphs';

export function useSubgraphEditor(root: Graph, setRoot: Dispatch<SetStateAction<Graph>>) {
  const [path, setPath] = useState<string[]>([]);
  // Undo, deleting a tab, and loading a different document may remove an ancestor.
  const validPath = useMemo(() => {
    const result: string[] = [];
    let graph = root;
    for (const id of path) {
      const inner = graph.nodes.find(n => n.id === id)?.data.subgraph;
      if (!inner) break;
      result.push(id); graph = inner;
    }
    return result;
  }, [path, root]);
  // Clamp navigation when an undo/load removes an ancestor. This guarded
  // adjustment settles in one render and does not become part of document undo.
  if (validPath.length !== path.length) setPath(validPath);
  const graph = graphAtPath(root, validPath);
  const editGraph = useCallback((change: (graph: Graph) => Graph) => {
    // Validate before entering a React updater so a rejected operation can be
    // reported by the toolbar without throwing during React rendering.
    const next = updateGraphAtPath(root, validPath, change);
    setRoot(next);
  }, [root, validPath, setRoot]);
  const setNodes = useCallback<Dispatch<SetStateAction<Node<NodeData>[]>>>(action => {
    setRoot(previous => updateGraphAtPath(previous, validPath,
      current => ({ ...current, nodes: typeof action === 'function' ? action(current.nodes) : action })));
  }, [validPath, setRoot]);
  const setEdges = useCallback<Dispatch<SetStateAction<Edge[]>>>(action => {
    setRoot(previous => updateGraphAtPath(previous, validPath,
      current => ({ ...current, edges: typeof action === 'function' ? action(current.edges) : action })));
  }, [validPath, setRoot]);
  const breadcrumbs = validPath.map((id, index) => {
    const parent = graphAtPath(root, validPath.slice(0, index));
    return { id, name: parent.nodes.find(n => n.id === id)?.data.title || 'Subgraph' };
  });
  return { nodes: graph.nodes, edges: graph.edges, setNodes, setEdges, editGraph,
    subgraphPath: validPath, breadcrumbs,
    enterSubgraph: (id: string) => {
      if (graph.nodes.find(n => n.id === id)?.data.subgraph) setPath([...validPath, id]);
    },
    navigateSubgraph: (depth: number) => setPath(validPath.slice(0, depth)),
    resetSubgraph: useCallback(() => setPath([]), []) };
}
