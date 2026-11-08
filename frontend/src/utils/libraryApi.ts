import type { LibraryItem, LibraryReference, NodeData } from '../types';
import type { SerializedWorkflowDocument, SerializedNode } from './workflowPersistence';
import type { Node, Edge } from '@xyflow/react';
import { workflowServer } from './workflowDrafts.ts';

const base = workflowServer();
export type LibraryKind = 'workflows' | 'subgraphs';
export interface LibraryRecord extends LibraryItem { document: SerializedWorkflowDocument }

export async function libraryRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${base}/library/${path}`, init);
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(typeof body?.detail === 'string' ? body.detail : `Library request failed (${response.status}).`);
  }
  return response.json() as Promise<T>;
}

export const documentFingerprint = (document: SerializedWorkflowDocument): string => {
  const normalize = (nodes: (SerializedNode | Node<NodeData>)[], edges: Edge[]): unknown => ({ nodes: nodes.map(node => ({
    id: node.id, type: node.type || 'dynamic', position: node.position, data: { ...node.data, selected: undefined,
      bypassed: node.data.bypassed || undefined,
      subgraph: node.data.subgraph ? { ...node.data.subgraph, ...normalize(node.data.subgraph.nodes, node.data.subgraph.edges) as object } : undefined },
  })), edges });
  const canonical = (value: unknown): unknown => Array.isArray(value) ? value.map(canonical)
    : value && typeof value === 'object' ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => [k, canonical(v)])) : value;
  return JSON.stringify(canonical({ workflowId: document.workflowId, workflow_name: document.workflow_name || '',
    metadata: document.metadata || {}, graph: normalize(document.nodes, document.edges) }));
};

export const libraryReference = (record: LibraryRecord, kind: LibraryKind = 'workflows'): LibraryReference => ({
  kind,
  id: record.id, revision: record.revision, folder: record.folder, fingerprint: documentFingerprint(record.document),
});
