// src/hooks/useFlowOperations.ts
import { useRef, useCallback, useEffect } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { NodeData, LogEntry, NodeSpec } from '../types';
import { stripRuntimeNodeData, hydrateNodeFromSpec } from '../utils/workflowPersistence';
import { cloneGraphIds, deleteSelection, type Graph } from '../utils/subgraphs';
import { selectionWithContents } from '../utils/groups';
import type { SerializedNode } from '../utils/workflowPersistence';

export const useFlowOperations = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  editGraph: (change: (graph: Graph) => Graph) => void,
  undo: () => void,
  redo: () => void,
  addLog: (msg: string, type?: LogEntry['type']) => void,
  isExecutionLocked: boolean = false,
  nodeDefs: Record<string, NodeSpec> = {},
) => {
  const clipboardRef = useRef<{ nodes: SerializedNode[]; edges: Edge[] } | null>(null);

  // === Copy ===
  const handleCopy = useCallback(() => {
    if (isExecutionLocked) return;
    const selectedNodes = selectionWithContents(nodes).filter(n => !n.data.boundary);
    if (selectedNodes.length === 0) return;
    const selectedIds = new Set(selectedNodes.map(n => n.id));
    const selectedEdges = edges.filter(e => selectedIds.has(e.source) && selectedIds.has(e.target));

    // Store stripped (no runtime data) nodes + structural edges
    const strippedNodes = selectedNodes.map(n => stripRuntimeNodeData(n));
    clipboardRef.current = { nodes: strippedNodes, edges: selectedEdges };
    addLog(`Copied ${selectedNodes.length} nodes`, 'info');
  }, [nodes, edges, addLog, isExecutionLocked]);

  // === Paste ===
  const handlePaste = useCallback(() => {
    if (isExecutionLocked) return;
    if (!clipboardRef.current) return;
    const original = clipboardRef.current;
    const cloned = cloneGraphIds({ nodes: original.nodes as unknown as Node<NodeData>[], edges: original.edges });
    const cpNodes = cloned.nodes as unknown as SerializedNode[];
    const cpEdges = cloned.edges;

    const idMap = new Map<string, string>();
    const newNodes: Node<NodeData>[] = [];

    cpNodes.forEach(n => {
      const newId = `${n.data?.opType ?? 'node'}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
      idMap.set(n.id, newId);
    });

    // Remap node ids before hydrating so pasted nodes get fresh ids
    cpNodes.forEach(n => {
      const newId = idMap.get(n.id)!;
      const offsetX = 40;
      const offsetY = 40;
      const remappedNode = { ...n, id: newId };
      const hydrated = hydrateNodeFromSpec(remappedNode, nodeDefs, {
        x: n.position.x + offsetX,
        y: n.position.y + offsetY,
      });
      hydrated.selected = true;
      newNodes.push(hydrated);
    });

    // Remap edge references; defensively filter edges whose endpoints are not in idMap
    const newEdges = cpEdges
      .filter(e => idMap.has(e.source) && idMap.has(e.target))
      .map(e => ({
        ...e,
        id: `e_${idMap.get(e.source)}-${idMap.get(e.target)}_${Math.random().toString(36).substr(2, 5)}`,
        source: idMap.get(e.source)!,
        target: idMap.get(e.target)!,
        selected: true,
      }));

    setNodes(nds => (nds.map(n => ({ ...n, selected: false } as Node<NodeData>))).concat(newNodes));
    setEdges(eds => eds.map(e => ({ ...e, selected: false })).concat(newEdges));
    addLog(`Pasted ${newNodes.length} nodes`, 'info');
  }, [setNodes, setEdges, addLog, isExecutionLocked, nodeDefs]);

  // === Delete ===
  const handleDelete = useCallback(() => {
    if (isExecutionLocked) return;
    const selectedNodes = nodes.filter(n => n.selected);
    const selectedEdges = edges.filter(e => e.selected);
    if (selectedNodes.length === 0 && selectedEdges.length === 0) return;
    editGraph(deleteSelection);
  }, [nodes, edges, editGraph, isExecutionLocked]);

  // === Keyboard shortcuts ===
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      if (document.querySelector('[aria-modal="true"]')) return;
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName) || target.isContentEditable) return;
      const isCtrl = e.metaKey || e.ctrlKey;
      if (isExecutionLocked) return;
      if (isCtrl && e.key.toLowerCase() === 'z') { e.preventDefault(); if (e.shiftKey) redo(); else undo(); }
      else if (isCtrl && e.key.toLowerCase() === 'y') { e.preventDefault(); redo(); }
      else if (isCtrl && e.key.toLowerCase() === 'c') { e.preventDefault(); handleCopy(); }
      else if (isCtrl && e.key.toLowerCase() === 'v') { e.preventDefault(); handlePaste(); }
      else if (e.key === 'Delete' || e.key === 'Backspace') { e.preventDefault(); handleDelete(); }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [undo, redo, handleCopy, handlePaste, handleDelete, isExecutionLocked]);

  return { handleCopy, handlePaste, handleDelete };
};
