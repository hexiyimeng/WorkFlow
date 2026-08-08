// src/hooks/useAutoSave.ts
import { useEffect, useRef, useState } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { Workflow, NodeData, NodeSpec } from '../types';
import {
  hydrateFlowWithLatestSpecs,
  parseWorkflowDocument,
  serializeWorkflowDocument,
  type ParsedWorkflowDocument,
} from '../utils/workflowPersistence';
import { createWorkflowId } from '../utils/workflowExecutionSettings';

const AUTOSAVE_KEY = 'WorkFlow_AUTOSAVE';

export type AutoSaveDocumentLoader = (
  document: ParsedWorkflowDocument,
  hydratedNodes: Node<NodeData>[],
  hydratedEdges: Edge[],
) => void;

/**
 * Restore and persist the complete active workflow document. Callers should
 * create workflow state first and pass its activeWorkflow plus the atomic
 * loader returned by useWorkflows.
 */
export const useAutoSave = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  nodeDefs: Record<string, NodeSpec>,
  activeWorkflow: Workflow | null,
  loadWorkflowDocument: AutoSaveDocumentLoader,
  persistenceEnabled = true,
) => {
  const hasRestoredRef = useRef(false);
  const [restoreComplete, setRestoreComplete] = useState(false);

  // Restore once, after node definitions and the bootstrapped active workflow
  // identity are both available.
  useEffect(() => {
    if (hasRestoredRef.current) return;
    if (!persistenceEnabled || Object.keys(nodeDefs).length === 0 || !activeWorkflow) return;

    let savedData: string | null = null;
    try {
      savedData = localStorage.getItem(AUTOSAVE_KEY);
    } catch (error) {
      console.error('[AutoSave] Restore failed:', error);
      hasRestoredRef.current = true;
      setRestoreComplete(true);
      return;
    }

    if (!savedData) {
      hasRestoredRef.current = true;
      setRestoreComplete(true);
      return;
    }

    try {
      const parsed = parseWorkflowDocument(JSON.parse(savedData), {
        // useWorkflows already migrated a legacy autosave synchronously. Reuse
        // that ID instead of generating a second identity while hydrating it.
        createWorkflowId: () => activeWorkflow.workflowId
          || activeWorkflow.id
          || createWorkflowId(),
      });
      if (!parsed) {
        console.warn('[AutoSave] Ignoring an unrecognized autosave document.');
        return;
      }

      const result = hydrateFlowWithLatestSpecs(parsed, nodeDefs);
      loadWorkflowDocument(parsed, result.nodes, result.edges);

      result.warnings.forEach(warning => console.warn('[AutoSave]', warning));
      if (result.invalidNodeTypes.length > 0) {
        console.warn(
          `[AutoSave] ${result.invalidNodeTypes.length} unavailable node type(s) marked invalid`,
        );
      }
      if (result.removedEdges > 0) {
        console.warn(
          `[AutoSave] Removed ${result.removedEdges} invalid connection(s) after updating node definitions`,
        );
      }
    } catch (error) {
      console.error('[AutoSave] Restore failed:', error);
    } finally {
      // The write effect is deliberately gated on this flag so an empty canvas
      // can never overwrite saved data while node definitions are loading.
      hasRestoredRef.current = true;
      setRestoreComplete(true);
    }
  }, [
    activeWorkflow,
    loadWorkflowDocument,
    nodeDefs,
    persistenceEnabled,
    setEdges,
    setNodes,
  ]);

  // Persist stable identity, metadata, and stripped graph as one document.
  useEffect(() => {
    if (
      !persistenceEnabled
      || !restoreComplete
      || !activeWorkflow
      || Object.keys(nodeDefs).length === 0
    ) {
      return;
    }

    const timer = window.setTimeout(() => {
      const workflowId = activeWorkflow.workflowId || activeWorkflow.id;
      if (!workflowId) return;
      const document = serializeWorkflowDocument({
        workflowId,
        metadata: activeWorkflow.metadata ?? {},
        nodes,
        edges,
        workflowName: activeWorkflow.name,
        timestamp: Date.now(),
      });
      try {
        localStorage.setItem(AUTOSAVE_KEY, JSON.stringify(document));
      } catch (error) {
        console.error('[AutoSave] Save failed:', error);
      }
    }, 1000);
    return () => window.clearTimeout(timer);
  }, [activeWorkflow, edges, nodeDefs, nodes, persistenceEnabled, restoreComplete]);
};
