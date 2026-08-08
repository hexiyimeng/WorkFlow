import type { Edge } from '@xyflow/react';
import type {
  ExecutionConfig,
  ExecutionOutput,
  RecoveryOpenResponse,
  RecoverySummary,
  ServerDirectoryEntry,
  ServerDirectoryListing,
} from '../types';
import type { SerializedFlow, SerializedNode } from './workflowPersistence';
import {
  isAbsoluteServerPath,
  isValidMaxInFlightWindows,
  isValidOutputShape,
  isValidWindowShape,
} from './executionConfig.ts';

type JsonObject = Record<string, unknown>;

const isObject = (value: unknown): value is JsonObject => (
  value !== null && typeof value === 'object' && !Array.isArray(value)
);

const isNonNegativeInteger = (value: unknown): value is number => (
  Number.isSafeInteger(value) && Number(value) >= 0
);

const RECOVERY_STATUSES = new Set([
  'prepared',
  'running',
  'interrupted',
  'failed',
  'cancelled',
  'succeeded',
]);

const parseOutput = (value: unknown): ExecutionOutput | null => {
  if (!isObject(value)) return null;
  const nodeId = value.nodeId;
  const nodeType = value.nodeType;
  const displayName = value.displayName;
  const pathInput = value.pathInput ?? '';
  const path = value.path;
  if (
    typeof nodeId !== 'string'
    || typeof nodeType !== 'string'
    || typeof displayName !== 'string'
    || typeof pathInput !== 'string'
    || typeof path !== 'string'
  ) {
    return null;
  }
  return { nodeId, nodeType, displayName, pathInput, path };
};

export const normalizeRecoverySummary = (value: unknown): RecoverySummary => {
  if (!isObject(value)) throw new Error('Invalid recovery summary response.');

  const outputShape = value.outputShape;
  const windowShape = value.windowShape;
  const windowGridShape = value.windowGridShape;
  const completedWindows = value.completedWindows;
  const totalWindows = value.totalWindows;
  const remainingWindows = value.remainingWindows;
  const outputs = Array.isArray(value.outputs)
    ? value.outputs.map(parseOutput)
    : [];
  const calculatedTotal = isValidOutputShape(windowGridShape)
    ? windowGridShape.reduce((total, size) => total * BigInt(size), 1n)
    : null;
  const shapesAreCompatible = (
    isValidOutputShape(outputShape)
    && isValidOutputShape(windowShape)
    && isValidWindowShape(outputShape, windowShape)
    && isValidOutputShape(windowGridShape)
    && windowGridShape.length === outputShape.length
  );
  if (
    value.found !== true
    || value.valid !== true
    || value.compatible !== true
    || typeof value.executionId !== 'string'
    || value.executionId.trim() === ''
    || typeof value.status !== 'string'
    || !RECOVERY_STATUSES.has(value.status)
    || typeof value.recoveryDirectory !== 'string'
    || !isNonNegativeInteger(completedWindows)
    || !isNonNegativeInteger(totalWindows)
    || !isNonNegativeInteger(remainingWindows)
    || completedWindows + remainingWindows !== totalWindows
    || !shapesAreCompatible
    || calculatedTotal !== BigInt(totalWindows)
    || outputs.length === 0
    || outputs.some(output => output === null)
  ) {
    throw new Error('Backend returned an incompatible recovery summary.');
  }

  return {
    found: true,
    valid: true,
    compatible: true,
    executionId: value.executionId,
    status: value.status as RecoverySummary['status'],
    recoveryDirectory: value.recoveryDirectory,
    completedWindows,
    totalWindows,
    remainingWindows,
    outputShape: outputShape as number[],
    windowShape: windowShape as number[],
    windowGridShape: windowGridShape as number[],
    outputs: outputs as ExecutionOutput[],
    message: typeof value.message === 'string' ? value.message : null,
  };
};

export const normalizeDirectoryListing = (value: unknown): ServerDirectoryListing => {
  if (
    !isObject(value)
    || (value.path !== null && typeof value.path !== 'string')
    || !Array.isArray(value.directories)
  ) {
    throw new Error('Invalid server directory listing response.');
  }

  const directories: ServerDirectoryEntry[] = value.directories.map(item => {
    if (
      !isObject(item)
      || typeof item.name !== 'string'
      || typeof item.path !== 'string'
      || typeof item.isRecoveryDirectory !== 'boolean'
    ) {
      throw new Error('Invalid directory entry returned by the server.');
    }
    return {
      name: item.name,
      path: item.path,
      isRecoveryDirectory: item.isRecoveryDirectory,
    };
  });

  return {
    // The top-level response represents the configured roots and therefore
    // intentionally has no single current directory.
    path: typeof value.path === 'string' ? value.path : '',
    parent: typeof value.parent === 'string' ? value.parent : null,
    directories,
  };
};

const isExecutionConnection = (value: unknown): value is [string, number] => (
  Array.isArray(value)
  && value.length === 2
  && typeof value[0] === 'string'
  && typeof value[1] === 'number'
  && Number.isSafeInteger(value[1])
  && value[1] >= 0
);

const RECOVERY_LAYOUT_ORIGIN = { x: 80, y: 80 };
const RECOVERY_LAYOUT_LAYER_GAP = 440;
const RECOVERY_LAYOUT_NODE_GAP = 80;
const RECOVERY_LAYOUT_MIN_NODE_HEIGHT = 180;

const estimateRecoveryNodeHeight = (node: SerializedNode): number => {
  const parameterCount = Object.keys(node.data.values ?? {}).length;
  return Math.max(
    RECOVERY_LAYOUT_MIN_NODE_HEIGHT,
    112 + parameterCount * 34,
  );
};

/**
 * Lay out a recovered execution graph in deterministic, left-to-right DAG
 * layers. Node heights are estimated conservatively from their saved inputs so
 * parameter-heavy nodes do not overlap other nodes in the same layer.
 */
export const layoutRecoveryDag = (
  nodes: SerializedNode[],
  edges: Edge[],
): SerializedNode[] => {
  if (nodes.length === 0) return [];

  const nodeById = new Map(nodes.map(node => [node.id, node]));
  if (nodeById.size !== nodes.length) {
    throw new Error('Recovery graph contains duplicate node identifiers.');
  }

  const nodeIds = [...nodeById.keys()].sort((left, right) => left.localeCompare(right));
  const indegree = new Map(nodeIds.map(nodeId => [nodeId, 0]));
  const outgoing = new Map(nodeIds.map(nodeId => [nodeId, new Set<string>()]));

  edges.forEach(edge => {
    if (!nodeById.has(edge.source) || !nodeById.has(edge.target)) {
      throw new Error(`Recovery edge '${edge.id}' references a missing node.`);
    }
    const targets = outgoing.get(edge.source)!;
    if (!targets.has(edge.target)) {
      targets.add(edge.target);
      indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1);
    }
  });

  const layerById = new Map(nodeIds.map(nodeId => [nodeId, 0]));
  const ready = nodeIds.filter(nodeId => indegree.get(nodeId) === 0);
  let processedCount = 0;

  while (ready.length > 0) {
    const nodeId = ready.shift()!;
    processedCount += 1;
    const nextLayer = (layerById.get(nodeId) ?? 0) + 1;

    [...outgoing.get(nodeId)!]
      .sort((left, right) => left.localeCompare(right))
      .forEach(targetId => {
        layerById.set(
          targetId,
          Math.max(layerById.get(targetId) ?? 0, nextLayer),
        );
        const remaining = (indegree.get(targetId) ?? 0) - 1;
        indegree.set(targetId, remaining);
        if (remaining === 0) {
          ready.push(targetId);
          ready.sort((left, right) => left.localeCompare(right));
        }
      });
  }

  if (processedCount !== nodes.length) {
    throw new Error('Recovery graph must be acyclic.');
  }

  const idsByLayer = new Map<number, string[]>();
  nodeIds.forEach(nodeId => {
    const layer = layerById.get(nodeId) ?? 0;
    const layerIds = idsByLayer.get(layer) ?? [];
    layerIds.push(nodeId);
    idsByLayer.set(layer, layerIds);
  });

  const stackHeightByLayer = new Map<number, number>();
  idsByLayer.forEach((layerIds, layer) => {
    const contentHeight = layerIds.reduce(
      (height, nodeId) => height + estimateRecoveryNodeHeight(nodeById.get(nodeId)!),
      0,
    );
    stackHeightByLayer.set(
      layer,
      contentHeight + Math.max(0, layerIds.length - 1) * RECOVERY_LAYOUT_NODE_GAP,
    );
  });
  const maximumStackHeight = Math.max(...stackHeightByLayer.values());

  const positionById = new Map<string, { x: number; y: number }>();
  idsByLayer.forEach((layerIds, layer) => {
    let y = RECOVERY_LAYOUT_ORIGIN.y
      + (maximumStackHeight - (stackHeightByLayer.get(layer) ?? 0)) / 2;
    layerIds.forEach(nodeId => {
      positionById.set(nodeId, {
        x: RECOVERY_LAYOUT_ORIGIN.x + layer * RECOVERY_LAYOUT_LAYER_GAP,
        y,
      });
      y += estimateRecoveryNodeHeight(nodeById.get(nodeId)!) + RECOVERY_LAYOUT_NODE_GAP;
    });
  });

  return nodes.map(node => ({
    ...node,
    position: positionById.get(node.id) ?? node.position,
  }));
};

export const executionGraphToSerializedFlow = (graph: unknown): SerializedFlow => {
  if (!isObject(graph)) throw new Error('Recovery graph must be an object.');

  const nodes: SerializedNode[] = [];
  const edges: Edge[] = [];
  const orderedNodeIds = Object.keys(graph).sort((left, right) => left.localeCompare(right));
  const nodeIds = new Set(orderedNodeIds);

  orderedNodeIds.forEach(nodeId => {
    const rawNode = graph[nodeId];
    if (!isObject(rawNode) || typeof rawNode.type !== 'string' || !isObject(rawNode.inputs)) {
      throw new Error(`Recovery graph node '${nodeId}' is invalid.`);
    }

    const rawInputs = rawNode.inputs;
    const values: Record<string, unknown> = {};
    Object.keys(rawInputs)
      .sort((left, right) => left.localeCompare(right))
      .forEach(inputName => {
      const inputValue = rawInputs[inputName];
      if (isExecutionConnection(inputValue)) {
        const [source, outputIndex] = inputValue;
        if (!nodeIds.has(source)) {
          throw new Error(`Recovery graph node '${nodeId}' references missing node '${source}'.`);
        }
        edges.push({
          id: `recovery-${source}-${outputIndex}-${nodeId}-${inputName}`,
          source,
          sourceHandle: String(outputIndex),
          target: nodeId,
          targetHandle: inputName,
          type: 'default',
          animated: false,
        });
      } else {
        values[inputName] = inputValue;
      }
    });

    nodes.push({
      id: nodeId,
      type: 'dynamic',
      position: RECOVERY_LAYOUT_ORIGIN,
      data: {
        opType: rawNode.type,
        values,
      },
    });
  });

  return { nodes: layoutRecoveryDag(nodes, edges), edges };
};

export const normalizeRecoveryOpenResponse = (value: unknown): RecoveryOpenResponse => {
  if (!isObject(value) || value.readOnly !== true || !isObject(value.graph)) {
    throw new Error('Invalid recovery graph response.');
  }
  const executionConfig = value.executionConfig;
  if (!isObject(executionConfig) || executionConfig.mode !== 'window') {
    throw new Error('Recovery graph has an invalid execution configuration.');
  }

  return {
    graph: value.graph as RecoveryOpenResponse['graph'],
    readOnly: true,
    executionConfig: executionConfig as ExecutionConfig,
    recoverySummary: normalizeRecoverySummary(value.recoverySummary),
  };
};

export type RecoveryExecutionAction = 'resume' | 'restart';

export interface RecoveryExecutionRequest {
  /** Recovery graph selection is server-authoritative for both actions. */
  graph: null;
  executionConfig: ExecutionConfig;
}

/**
 * Build a recovery-only execution configuration from the immutable recovery
 * record. The action and selected custom directory are the only intentional
 * overrides; Window shape and valid concurrency settings remain those saved
 * with the recovery record.
 */
export const buildRecoveryExecutionConfig = (
  opened: RecoveryOpenResponse,
  recoveryDirectory: string,
  action: RecoveryExecutionAction,
): ExecutionConfig => {
  const savedConfig = opened.executionConfig;
  if (savedConfig.mode !== 'window') {
    throw new Error('Recovery execution requires a saved Window configuration.');
  }

  const normalizedDirectory = recoveryDirectory.trim();
  if (!isAbsoluteServerPath(normalizedDirectory)) {
    throw new Error('Recovery execution requires an absolute server directory.');
  }

  const savedWindowShape = savedConfig.windowShape;
  const manifestWindowShape = opened.recoverySummary.windowShape;
  if (
    !savedWindowShape
    || !isValidWindowShape(opened.recoverySummary.outputShape, savedWindowShape)
    || savedWindowShape.length !== manifestWindowShape.length
    || savedWindowShape.some((size, index) => size !== manifestWindowShape[index])
  ) {
    throw new Error('Saved recovery Window shape does not match the recovery manifest.');
  }

  const maxInFlightWindows = savedConfig.maxInFlightWindows;
  const common = {
    mode: 'window' as const,
    windowShape: [...savedWindowShape],
    ...(maxInFlightWindows !== undefined
      && isValidMaxInFlightWindows(maxInFlightWindows)
      ? { maxInFlightWindows }
      : {}),
    recoveryLocation: {
      mode: 'custom' as const,
      directory: normalizedDirectory,
    },
  };

  return action === 'resume'
    ? { ...common, resumeAction: 'resume' }
    : { ...common, resumeAction: 'restart' };
};

/** Ask the server to select the immutable saved graph, never the edited DAG. */
export const buildRecoveryExecutionRequest = (
  opened: RecoveryOpenResponse,
  recoveryDirectory: string,
  action: RecoveryExecutionAction,
): RecoveryExecutionRequest => ({
  graph: null,
  executionConfig: buildRecoveryExecutionConfig(
    opened,
    recoveryDirectory,
    action,
  ),
});
