import { useCallback, useEffect, useMemo } from 'react';
import type { Edge, Node } from '@xyflow/react';
import type {
  NodeData,
  ResolvedWorkflowExecutionSettings,
  Workflow,
  WorkflowExecutionSettings,
  WorkflowMetadata,
} from '../types';
import { serializeWorkflowDocument } from '../utils/workflowPersistence';
import {
  resolveWorkflowExecutionSettings,
  commitWorkflowExecutionSettings,
  mirrorWorkflowExecutionSettingsIntoAutosave,
  saveLocalWorkflowExecutionSettings,
} from '../utils/workflowExecutionSettings';

const AUTOSAVE_KEY = 'WorkFlow_AUTOSAVE';

const browserStorage = (): Storage | null => {
  try {
    return typeof localStorage === 'undefined' ? null : localStorage;
  } catch {
    return null;
  }
};

const workflowDocumentId = (workflow: Workflow): string => (
  workflow.workflowId?.trim() || workflow.id
);

export const useWorkflowExecutionSettingsStore = (
  workflows: Workflow[],
  activeWorkflowId: string,
  updateWorkflowMetadata: (workflowTabId: string, metadata: WorkflowMetadata) => void,
  currentNodes: Node<NodeData>[],
  currentEdges: Edge[],
) => {
  const resolvedByWorkflowId = useMemo<Record<string, ResolvedWorkflowExecutionSettings>>(
    () => Object.fromEntries(workflows.map(workflow => {
      const documentId = workflowDocumentId(workflow);
      return [
        documentId,
        resolveWorkflowExecutionSettings({
          workflowId: documentId,
          metadata: workflow.metadata,
        }),
      ];
    })),
    [workflows],
  );

  const executionSettingsByWorkflowId = useMemo<Record<string, WorkflowExecutionSettings>>(
    () => Object.fromEntries(Object.entries(resolvedByWorkflowId).map(
      ([workflowId, resolved]) => [workflowId, resolved.settings],
    )),
    [resolvedByWorkflowId],
  );

  const activeWorkflow = workflows.find(workflow => workflow.id === activeWorkflowId);
  const activeWorkflowDocumentId = activeWorkflow
    ? workflowDocumentId(activeWorkflow)
    : activeWorkflowId;
  const activeResolvedExecutionSettings = resolvedByWorkflowId[activeWorkflowDocumentId]
    ?? resolveWorkflowExecutionSettings({
      workflowId: activeWorkflowDocumentId,
      metadata: activeWorkflow?.metadata,
    });

  // A valid local fallback bridges an unsaved/temporary session. Promote it
  // once into primary workflow metadata so manual export and subsequent
  // autosaves carry the settings even after the fallback cache is cleared.
  useEffect(() => {
    workflows.forEach(workflow => {
      const documentId = workflowDocumentId(workflow);
      const resolved = resolvedByWorkflowId[documentId];
      if (
        !resolved
        || resolved.source !== 'local'
        || !resolved.validation.isValid
        || Object.hasOwn(workflow.metadata ?? {}, 'executionSettings')
      ) {
        return;
      }
      updateWorkflowMetadata(workflow.id, {
        ...(workflow.metadata ?? {}),
        executionSettings: resolved.settings,
      });
      if (workflow.id === activeWorkflowId) {
        const storage = browserStorage();
        if (storage) {
          try {
            mirrorWorkflowExecutionSettingsIntoAutosave({
              storage,
              autosaveKey: AUTOSAVE_KEY,
              workflowId: documentId,
              settings: resolved.settings,
            });
          } catch (error) {
            console.warn('[Execution Settings] Could not promote settings into autosave:', error);
          }
        }
      }
    });
  }, [
    activeWorkflowId,
    resolvedByWorkflowId,
    updateWorkflowMetadata,
    workflows,
  ]);

  const saveActiveExecutionSettings = useCallback((
    settings: WorkflowExecutionSettings,
  ): WorkflowExecutionSettings => {
    if (!activeWorkflow) {
      throw new Error('No active workflow is available for execution settings.');
    }
    const storage = browserStorage();
    return commitWorkflowExecutionSettings(
      settings,
      sanitized => {
        updateWorkflowMetadata(activeWorkflow.id, {
          ...(activeWorkflow.metadata ?? {}),
          executionSettings: sanitized,
        });
      },
      storage
        ? [
            sanitized => saveLocalWorkflowExecutionSettings(
              activeWorkflowDocumentId,
              sanitized,
              storage,
            ),
            sanitized => {
              const document = serializeWorkflowDocument({
                workflowId: activeWorkflowDocumentId,
                metadata: {
                  ...(activeWorkflow.metadata ?? {}),
                  executionSettings: sanitized,
                },
                nodes: currentNodes,
                edges: currentEdges,
                workflowName: activeWorkflow.name,
                timestamp: Date.now(),
              });
              storage.setItem(AUTOSAVE_KEY, JSON.stringify(document));
            },
          ]
        : [],
      error => {
        console.warn('[Execution Settings] Secondary persistence failed:', error);
      },
    );
  }, [
    activeWorkflow,
    activeWorkflowDocumentId,
    currentEdges,
    currentNodes,
    updateWorkflowMetadata,
  ]);

  return {
    executionSettingsByWorkflowId,
    resolvedExecutionSettingsByWorkflowId: resolvedByWorkflowId,
    activeWorkflowDocumentId,
    activeExecutionSettings: activeResolvedExecutionSettings.settings,
    activeExecutionSettingsConfigured: activeResolvedExecutionSettings.isConfigured,
    activeExecutionSettingsSource: activeResolvedExecutionSettings.source,
    activeExecutionSettingsStoredValidation: activeResolvedExecutionSettings.validation,
    saveActiveExecutionSettings,
  };
};
