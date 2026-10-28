/**
 * workflowPersistence.ts
 *
 * Centralized hydration and serialization for WorkFlow workflow data.
 *
 * Design principles:
 * - `nodeSpec` is runtime-derived from backend `/object_info`. It must NOT be
 *   stored as persistent workflow schema — backend node definitions evolve and
 *   old stored specs become stale, causing false type mismatches.
 * - When loading/restoring nodes, always rehydrate `nodeSpec` from the latest
 *   `nodeDefs` using `node.data.opType` as the lookup key.
 * - Serialize only the user graph: node id, type, position, opType, values,
 *   dimensions, and edges.
 * - Strip all runtime state: nodeSpec, runState,
 *   executionId, animated edge state, etc.
 */

import type { Node, Edge } from '@xyflow/react';
import type {
  NodeData,
  NodeSpec,
  WorkflowExecutionSettingsValidation,
  WorkflowMetadata,
  SubgraphGraph,
  InputDefinition,
} from '../types';
import { subgraphSpec, normalizeGraph } from './subgraphs.ts';
import { canConnectPorts, resolveNodeOutputTypes } from './portTypes.ts';
import { nodeInputValues } from './nodeInputs.ts';
import { GROUP_SPEC, isGroup } from './groups.ts';
import {
  cloneWorkflowExecutionSettings,
  createWorkflowId,
  sanitizeWorkflowExecutionSettings,
} from './workflowExecutionSettings.ts';

// ============================================================
// Types
// ============================================================

export interface SerializedFlow {
  nodes: SerializedNode[];
  edges: Edge[];
}

/** Persistent JSON document used by manual save/load and browser autosave. */
export interface SerializedWorkflowDocument extends SerializedFlow {
  schemaVersion?: number;
  workflowId: string;
  metadata: WorkflowMetadata;
  workflow_name?: string;
  timestamp?: number;
}

export interface ParsedWorkflowDocument extends SerializedFlow {
  workflowId: string;
  metadata: WorkflowMetadata;
  workflowName?: string;
  timestamp?: number;
  /** True when a legacy document had no stable workflowId and received one. */
  migratedWorkflowId: boolean;
  executionSettingsValidation?: WorkflowExecutionSettingsValidation;
}

export interface SerializeWorkflowDocumentInput {
  workflowId: string;
  metadata?: WorkflowMetadata;
  nodes: Node<NodeData>[];
  edges: Edge[];
  workflowName?: string;
  timestamp?: number;
}

export interface ParseWorkflowDocumentOptions {
  createWorkflowId?: () => string;
}

export interface SerializedNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: {
    opType: string;
    group?: { width: number; height: number };
    values?: Record<string, unknown>;
    dimensions?: Record<string, number>;
    selected?: boolean;
    title?: string;
    color?: string;
    inputLabels?: Record<string, string>;
    outputLabels?: Record<string, string>;
    showAdvanced?: boolean;
    boundaryConfig?: InputDefinition;
    bypassed?: boolean;
    boundary?: 'input' | 'output';
    subgraph?: SubgraphGraph;
  };
}

export interface HydrationResult {
  nodes: Node<NodeData>[];
  edges: Edge[];
  invalidNodeTypes: string[];
  removedEdges: number;
  warnings: string[];
}

export interface SerializedNodeForStorage {
  id: string;
  type?: string;
  position: { x: number; y: number };
  data: {
    opType: string;
    group?: { width: number; height: number };
    values?: Record<string, unknown>;
    dimensions?: Record<string, number>;
    selected?: boolean;
    title?: string;
    color?: string;
    inputLabels?: Record<string, string>;
    outputLabels?: Record<string, string>;
    showAdvanced?: boolean;
    boundaryConfig?: InputDefinition;
    bypassed?: boolean;
    boundary?: 'input' | 'output';
    subgraph?: SubgraphGraph;
  };
}

function parseGroup(value: unknown): NodeData['group'] {
  if (value === undefined) return undefined;
  const box = value as NodeData['group'];
  if (!box || !Number.isFinite(box.width) || !Number.isFinite(box.height) || box.width < 240 || box.height < 160)
    throw new Error('Invalid group dimensions.');
  return { width: box.width, height: box.height };
}

function parseDisplayLabels(value: unknown): Record<string, string> | undefined {
  if (value === undefined) return undefined;
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid input/output display labels.');
  if (Object.values(value).some(label => typeof label !== 'string')) throw new Error('Invalid input/output display label.');
  return { ...value } as Record<string, string>;
}

function parseInputDefinition(value: unknown): InputDefinition | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length < 1 || value.length > 2
    || !(typeof value[0] === 'string' || (Array.isArray(value[0]) && value[0].length && value[0].every(v => typeof v === 'string')))
    || (value[1] !== undefined && (!value[1] || typeof value[1] !== 'object' || Array.isArray(value[1])))) throw new Error('Invalid subgraph parameter definition.');
  return value as InputDefinition;
}

// ============================================================
// Fallback node spec for invalid/missing node types
// ============================================================

const FALLBACK_NODE_SPEC: NodeSpec = {
  type: '__fallback__',
  display_name: 'Unknown Node',
  category: 'Unknown',
  input: { required: {} },
  output: [],
};

// ============================================================
// 1. Strip runtime data from a single node (returns plain object)
// ============================================================

/**
 * Strip all runtime-derived fields from a node, returning a clean
 * serialized representation that contains only the user graph data.
 * Returns a plain object (not a ReactFlow Node) for storage.
 */
export function stripRuntimeNodeData(node: Node<NodeData>): SerializedNode {
  return {
    id: node.id,
    type: node.type ?? 'dynamic',
    position: { x: node.position.x, y: node.position.y },
    data: {
      opType: node.data.opType,
      group: node.data.group,
      values: node.data.values,
      dimensions: node.data.dimensions as Record<string, number> | undefined,
      selected: node.selected,
      title: node.data.title,
      color: node.data.color,
      inputLabels: node.data.inputLabels,
      outputLabels: node.data.outputLabels,
      showAdvanced: node.data.showAdvanced || undefined,
      boundaryConfig: node.data.boundaryConfig,
      bypassed: node.data.bypassed || undefined,
      boundary: node.data.boundary,
      subgraph: node.data.subgraph ? { ...node.data.subgraph, ...serializeFlowForStorage(node.data.subgraph.nodes, node.data.subgraph.edges) } as unknown as SubgraphGraph : undefined,
    },
  };
}

// ============================================================
// 2. Serialize a node array for storage
// ============================================================

/**
 * Convert a node array to a storage-safe format, stripping all runtime fields.
 * Does NOT include edges — those are serialized separately by serializeFlowForStorage.
 */
export function serializeNodesForStorage(nodes: Node<NodeData>[]): SerializedNode[] {
  return nodes.map(n => stripRuntimeNodeData(n));
}

// ============================================================
// 3. Serialize the full flow (nodes + edges) for storage
// ============================================================

/**
 * Serialize both nodes and edges into a storage-safe format.
 * Nodes are stripped of runtime data; edges keep their structural fields.
 */
export function serializeFlowForStorage(
  nodes: Node<NodeData>[],
  edges: Edge[]
): SerializedFlow {
  return {
    nodes: serializeNodesForStorage(nodes),
    edges: edges.map(e => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle,
      targetHandle: e.targetHandle,
      type: e.type,
    })),
  };
}

export const cloneWorkflowMetadata = (metadata?: WorkflowMetadata): WorkflowMetadata => {
  if (!metadata) return {};
  const cloned: WorkflowMetadata = { ...metadata };
  if (Object.hasOwn(metadata, 'executionSettings')) {
    // Rebuild from the explicit persistent schema so runtime/recovery fields
    // cannot hitchhike into a valid workflow document. Unsupported or
    // malformed schemas remain byte-for-byte representable until the user
    // explicitly resets/saves them; silently coercing them to v1 Full Graph
    // would make an invalid configuration executable.
    const sanitized = sanitizeWorkflowExecutionSettings(metadata.executionSettings);
    cloned.executionSettings = sanitized.validation.isValid
      ? cloneWorkflowExecutionSettings(sanitized.settings)
      : metadata.executionSettings;
  }
  return cloned;
};

/** Serialize graph, stable identity, and metadata as one workflow document. */
export function serializeWorkflowDocument({
  workflowId,
  metadata,
  nodes,
  edges,
  workflowName,
  timestamp,
}: SerializeWorkflowDocumentInput): SerializedWorkflowDocument {
  const normalizedWorkflowId = workflowId.trim();
  if (!normalizedWorkflowId) {
    throw new Error('workflowId must be a non-empty string.');
  }
  const flow = serializeFlowForStorage(nodes, edges);
  return {
    schemaVersion: 2,
    workflowId: normalizedWorkflowId,
    metadata: cloneWorkflowMetadata(metadata),
    ...flow,
    ...(workflowName === undefined ? {} : { workflow_name: workflowName }),
    ...(timestamp === undefined ? {} : { timestamp }),
  };
}

// ============================================================
// 4. Hydrate a single node with latest nodeDefs
// ============================================================

/**
 * Restore runtime fields (nodeSpec, runtime state) on a deserialized node
 * using the latest nodeDefs. Returns a fully-hydrated ReactFlow Node.
 *
 * Missing node types are marked _invalid instead of crashing the canvas.
 */
export function hydrateNodeFromSpec(
  serialized: SerializedNode,
  nodeDefs: Record<string, NodeSpec>,
  positionOverride?: { x: number; y: number }
): Node<NodeData> {
  const opType = serialized.data?.opType;
  const subgraph = serialized.data?.subgraph;
  const boundary = serialized.data?.boundary;
  const group = serialized.data?.group;
  const latestSpec = group ? GROUP_SPEC : subgraph ? subgraphSpec(subgraph) : boundary ? { type: opType, display_name: serialized.data.title || 'Interface', category: 'Interface', input: { required: boundary === 'output' ? { value: [String(serialized.data.values?.portType || '*')] } : { ...(serialized.data.boundaryConfig ? { value: serialized.data.boundaryConfig } : {}) } }, output: boundary === 'input' ? [String(serialized.data.values?.portType || '*')] : [] } as NodeSpec : opType ? nodeDefs[opType] : undefined;

  const baseData: NodeData = {
    group,
    opType: opType ?? '',
    title: serialized.data.title,
    color: serialized.data.color,
    inputLabels: serialized.data.inputLabels,
    outputLabels: serialized.data.outputLabels,
    showAdvanced: serialized.data.showAdvanced,
    boundaryConfig: serialized.data.boundaryConfig,
    bypassed: serialized.data.bypassed,
    boundary,
    subgraph: subgraph ? { ...subgraph, nodes: hydrateNodesWithLatestSpecs(subgraph.nodes as unknown as SerializedNode[], nodeDefs) } : undefined,
    values: serialized.data?.values ?? {},
    message: '',
    runState: undefined,
    waitingFor: undefined,
    device: undefined,
    executionId: undefined,
    _invalid: false,
    _warning: undefined,
    // Always set nodeSpec — from latest defs, or fallback for invalid types
    nodeSpec: latestSpec ?? {
      ...FALLBACK_NODE_SPEC,
      type: opType ?? '__unknown__',
      display_name: opType ?? 'Unknown Node',
    },
  };

  // If spec not in latest defs, mark node invalid
  if (!latestSpec && opType) {
    baseData._invalid = true;
    baseData._warning = `Node '${opType}' is no longer available`;
  }

  return {
    id: serialized.id,
    type: group ? 'groupFrame' : serialized.type ?? 'dynamic',
    position: positionOverride ?? serialized.position,
    data: baseData,
    selected: serialized.data?.selected ?? false,
  } as Node<NodeData>;
}

// ============================================================
// 5. Hydrate all nodes in a flow
// ============================================================

/**
 * Convert deserialized nodes (with no runtime data) into fully-hydrated
 * WorkFlow nodes using the latest nodeDefs.
 */
export function hydrateNodesWithLatestSpecs(
  serializedNodes: SerializedNode[],
  nodeDefs: Record<string, NodeSpec>
): Node<NodeData>[] {
  return serializedNodes.map(n => hydrateNodeFromSpec(n, nodeDefs));
}

// ============================================================
// 6. Validate edges against hydrated nodes
// ============================================================

/**
 * After hydrating nodes with latest specs, validate all edges and remove
 * those that are type-incompatible under the current frontend type system.
 *
 * Returns the filtered edges plus a report of what was removed and why.
 *
 * DASK_ARRAY[uint16] -> DASK_ARRAY[any] is always valid (any accepts all).
 * DaskTypeCast dynamic output resolves from values.target_dtype.
 */
export function validateEdgesWithLatestSpecs(
  edges: Edge[],
  nodes: Node<NodeData>[]
): { validEdges: Edge[]; removedCount: number; warnings: string[] } {
  const nodeMap = new Map(nodes.map(n => [n.id, n]));
  const warnings: string[] = [];

  const validEdges = edges.filter(edge => {
    const sourceNode = nodeMap.get(edge.source);
    const targetNode = nodeMap.get(edge.target);

    if (!sourceNode || !targetNode) {
      warnings.push(`Removed orphaned edge ${edge.id} (node not found)`);
      return false;
    }

    if (sourceNode.data._invalid || targetNode.data._invalid) return true;
    if (isGroup(sourceNode) || isGroup(targetNode)) return false;
    const sourceHandleIndex = parseInt(edge.sourceHandle || '0');
    const targetHandleName = edge.targetHandle;

    if (!targetHandleName) return true;

    const outputTypes = resolveNodeOutputTypes(sourceNode.data.nodeSpec, nodeInputValues(sourceNode, { nodes, edges }));
    const outputType = outputTypes[sourceHandleIndex] || 'unknown';

    const targetSpec = targetNode.data.nodeSpec;
    const inputConfig = targetSpec?.input?.required?.[targetHandleName]
      || targetSpec?.input?.optional?.[targetHandleName];
    const inputType = Array.isArray(inputConfig?.[0]) ? 'STRING' : typeof inputConfig?.[0] === 'string' ? inputConfig[0] : 'unknown';

    const result = canConnectPorts(outputType, inputType);
    if (!result.ok) {
      warnings.push(
        `Removed invalid connection ${edge.source}:${edge.sourceHandle} → ${edge.target}:${edge.targetHandle}: ${result.reason}`
      );
      return false;
    }
    return true;
  });

  return {
    validEdges,
    removedCount: edges.length - validEdges.length,
    warnings,
  };
}

// ============================================================
// 7. Full flow hydration — main entry point for load paths
// ============================================================

/**
 * Primary load path: takes raw stored/serialized flow data plus the latest
 * nodeDefs and returns fully-hydrated nodes and validated edges.
 *
 * This is the single entry point for:
 * - autosave restore
 * - workflow switch
 * - undo/redo restore
 * - import JSON
 * - clipboard paste (partial, see #9)
 */
export function hydrateFlowWithLatestSpecs(
  flow: { nodes: SerializedNode[]; edges: Edge[] },
  nodeDefs: Record<string, NodeSpec>
): HydrationResult {
  const warnings: string[] = [];

  // Stored interface types are hints. Recompute from current plugin specs and
  // nested defaults before validating external links, including older documents.
  const hydratedNodes = normalizeGraph({ nodes: hydrateNodesWithLatestSpecs(flow.nodes, nodeDefs), edges: flow.edges }).nodes;

  const unavailable = (nodes: Node<NodeData>[]): string[] => nodes.flatMap(n => n.data.subgraph
    ? unavailable(n.data.subgraph.nodes) : n.data._invalid ? [n.data.opType] : []);
  const invalidNodeTypes = unavailable(hydratedNodes);

  const { validEdges, removedCount, warnings: edgeWarnings } = validateEdgesWithLatestSpecs(
    flow.edges,
    hydratedNodes
  );
  warnings.push(...edgeWarnings);

  return {
    nodes: hydratedNodes,
    edges: validEdges,
    invalidNodeTypes,
    removedEdges: removedCount,
    warnings,
  };
}

// ============================================================
// 8. Partial hydration for paste (only nodes, new IDs)
// ============================================================

/**
 * Hydrate pasted nodes (which already have new IDs) with fresh specs.
 * Unlike full flow hydration, this handles ID remapping and does not
 * validate edges — edges are mapped separately by the caller.
 */
export function hydratePastedNodes(
  serializedNodes: SerializedNode[],
  nodeDefs: Record<string, NodeSpec>
): Node<NodeData>[] {
  return serializedNodes.map(n => {
    const newPosition = {
      x: n.position.x + 30 + Math.random() * 20,
      y: n.position.y + 30 + Math.random() * 20,
    };
    return hydrateNodeFromSpec(n, nodeDefs, newPosition);
  });
}

// ============================================================
// 9. Parse any stored flow format (handles legacy with nodeSpec)
// ============================================================

/**
 * Parse stored JSON into a normalized SerializedFlow regardless of
 * whether it contains legacy `nodeSpec` fields or the new stripped format.
 *
 * Returns null if the format is completely unrecognizable.
 */
export function parseStoredFlow(raw: unknown, depth = 0): SerializedFlow | null {
  if (depth > 32) throw new Error('Subgraph nesting exceeds 32 levels.');
  if (!raw || typeof raw !== 'object') return null;

  const obj = raw as Record<string, unknown>;
  if (obj.schemaVersion !== undefined && obj.schemaVersion !== 1 && obj.schemaVersion !== 2) {
    throw new Error('Unsupported workflow schema version.');
  }
  const graphEnvelope = obj.graph;
  const graph = (
    graphEnvelope !== null
    && typeof graphEnvelope === 'object'
    && !Array.isArray(graphEnvelope)
    && Array.isArray((graphEnvelope as Record<string, unknown>).nodes)
  )
    ? graphEnvelope as Record<string, unknown>
    : obj;
  const rawNodes = graph.nodes;
  if (!Array.isArray(rawNodes)) return null;

  const parseSubgraph = (value: unknown): SubgraphGraph | undefined => {
    if (value === undefined) return undefined;
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid subgraph document.');
    const candidate = value as Record<string, unknown>;
    const inner = parseStoredFlow(candidate, depth + 1);
    if (!inner || !Array.isArray(candidate.inputs) || !Array.isArray(candidate.outputs)) throw new Error('Invalid subgraph interface.');
    const ports = (values: unknown[]) => values.map(value => {
      if (!value || typeof value !== 'object') throw new Error('Invalid subgraph port.');
      const p = value as Record<string, unknown>;
      if (!['id', 'name', 'type', 'nodeId'].every(key => typeof p[key] === 'string')) throw new Error('Invalid subgraph port.');
      if (!inner.nodes.some(n => n.id === p.nodeId)) throw new Error('Subgraph interface references a missing node.');
      const config = parseInputDefinition(p.config);
      return { id: p.id as string, name: p.name as string, type: p.type as string, nodeId: p.nodeId as string, ...(config ? { config } : {}) };
    });
    return { nodes: inner.nodes as unknown as Node<NodeData>[], edges: inner.edges,
      inputs: ports(candidate.inputs), outputs: ports(candidate.outputs) };
  };

  const serializedNodes: SerializedNode[] = rawNodes.map((n: unknown) => {
    if (!n || typeof n !== 'object') return null;
    const node = n as Record<string, unknown>;
    const nodeData = node.data as Record<string, unknown> | undefined;
    return {
      id: String(node.id ?? ''),
      type: String(node.type ?? 'dynamic'),
      position: {
        x: Number((node.position as Record<string, unknown>)?.x ?? 0),
        y: Number((node.position as Record<string, unknown>)?.y ?? 0),
      },
      data: {
        opType: String(nodeData?.opType ?? ''),
        group: parseGroup(nodeData?.group),
        values: nodeData?.values as Record<string, unknown> | undefined,
        dimensions: nodeData?.dimensions as Record<string, number> | undefined,
        selected: Boolean(nodeData?.selected),
        title: typeof nodeData?.title === 'string' ? nodeData.title : undefined,
        inputLabels: parseDisplayLabels(nodeData?.inputLabels),
        outputLabels: parseDisplayLabels(nodeData?.outputLabels),
        showAdvanced: nodeData?.showAdvanced === true || undefined,
        boundaryConfig: parseInputDefinition(nodeData?.boundaryConfig),
        color: typeof nodeData?.color === 'string' && /^#[0-9a-fA-F]{6}$/.test(nodeData.color) ? nodeData.color : undefined,
        bypassed: nodeData?.bypassed === true || undefined,
        boundary: nodeData?.boundary === 'input' || nodeData?.boundary === 'output' ? nodeData.boundary : undefined,
        subgraph: parseSubgraph(nodeData?.subgraph),
      },
    };
  }).filter(Boolean) as SerializedNode[];

  const rawEdges = graph.edges;
  const edges: Edge[] = Array.isArray(rawEdges) ? rawEdges.map((e: unknown) => {
    if (!e || typeof e !== 'object') return null;
    const edge = e as Record<string, unknown>;
    return {
      id: String(edge.id ?? ''),
      source: String(edge.source ?? ''),
      target: String(edge.target ?? ''),
      sourceHandle: edge.sourceHandle as string | null,
      targetHandle: edge.targetHandle as string | null,
      type: edge.type as string | undefined,
      animated: false,
    };
  }).filter(Boolean) as Edge[] : [];

  return { nodes: serializedNodes, edges };
}

/**
 * Parse both current workflow documents and legacy flat `{nodes, edges}` JSON.
 * Legacy documents receive a stable ID exactly once; callers should retain and
 * write the returned ID on their next autosave/export.
 */
export function parseWorkflowDocument(
  raw: unknown,
  options: ParseWorkflowDocumentOptions = {},
): ParsedWorkflowDocument | null {
  const flow = parseStoredFlow(raw);
  if (!flow || !raw || typeof raw !== 'object' || Array.isArray(raw)) return null;

  const obj = raw as Record<string, unknown>;
  const savedWorkflowId = typeof obj.workflowId === 'string'
    ? obj.workflowId.trim()
    : '';
  const idFactory = options.createWorkflowId ?? createWorkflowId;
  const workflowId = savedWorkflowId || idFactory();
  if (!workflowId.trim()) {
    throw new Error('The workflow ID factory returned an empty workflowId.');
  }

  const rawMetadata = (
    obj.metadata !== null
    && typeof obj.metadata === 'object'
    && !Array.isArray(obj.metadata)
  )
    ? obj.metadata as Record<string, unknown>
    : {};
  const metadata: WorkflowMetadata = { ...rawMetadata };
  let executionSettingsValidation: WorkflowExecutionSettingsValidation | undefined;
  if (Object.hasOwn(rawMetadata, 'executionSettings')) {
    const sanitized = sanitizeWorkflowExecutionSettings(rawMetadata.executionSettings);
    executionSettingsValidation = sanitized.validation;
  }

  const workflowName = typeof obj.workflow_name === 'string'
    ? obj.workflow_name
    : typeof obj.name === 'string'
      ? obj.name
      : undefined;
  const timestamp = typeof obj.timestamp === 'number' && Number.isFinite(obj.timestamp)
    ? obj.timestamp
    : undefined;

  return {
    workflowId,
    metadata,
    nodes: flow.nodes,
    edges: flow.edges,
    migratedWorkflowId: !savedWorkflowId,
    ...(workflowName === undefined ? {} : { workflowName }),
    ...(timestamp === undefined ? {} : { timestamp }),
    ...(executionSettingsValidation === undefined
      ? {}
      : { executionSettingsValidation }),
  };
}
