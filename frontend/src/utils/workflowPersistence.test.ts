import type { Edge, Node } from '@xyflow/react';
import type {
  NodeData,
  WorkflowExecutionSettings,
  WorkflowMetadata,
} from '../types.ts';
import {
  parseStoredFlow,
  parseWorkflowDocument,
  serializeWorkflowDocument,
} from './workflowPersistence.ts';

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

const makeNode = (
  id: string,
  value: number,
): Node<NodeData> => ({
  id,
  type: 'dynamic',
  position: { x: value, y: value + 1 },
  data: {
    opType: 'ExampleNode',
    values: { threshold: value },
    nodeSpec: {
      type: 'ExampleNode',
      display_name: 'Example',
      category: 'Tests',
      input: { required: {} },
      output: ['VALUE'],
    },
    runState: 'failed',
    executionId: 'temporary-run-id',
    message: 'temporary error',
  },
});

const settings: WorkflowExecutionSettings = {
  version: 1,
  mode: 'window',
  windowShape: [64, 256, 256],
  maxInFlightWindows: 8,
  newRunRecoveryLocation: {
    mode: 'output_sidecar',
    anchorNodeId: 'writer',
  },
  lastPreflight: {
    outputShape: [293, 1077, 1050],
    totalWindows: 125,
    cpuWorkers: 1,
    gpuWorkers: 8,
    validatedAt: 100,
  },
};
const metadata: WorkflowMetadata = {
  executionSettings: settings,
  description: 'metadata outside execution settings must survive',
};
const nodes = [makeNode('source', 10), makeNode('writer', 20)];
const edges: Edge[] = [{
  id: 'source-writer',
  source: 'source',
  target: 'writer',
  sourceHandle: '0',
  targetHandle: 'image',
  animated: true,
}];

const serialized = serializeWorkflowDocument({
  workflowId: 'b0c7e94d-6c61-4c21-a8a5-0b4973b54a41',
  metadata,
  nodes,
  edges,
  workflowName: 'Cells',
  timestamp: 123,
});
assert(
  serialized.workflowId === 'b0c7e94d-6c61-4c21-a8a5-0b4973b54a41',
  'workflow document must contain stable workflowId',
);
assert(serialized.metadata.executionSettings?.mode === 'window', 'settings must be stored in workflow metadata');
assert(serialized.workflow_name === 'Cells', 'workflow name should round-trip in the existing file format');
assert(serialized.nodes[0]?.data.values?.threshold === 10, 'node parameters should be serialized');
assert(!Object.hasOwn(serialized.nodes[0]?.data ?? {}, 'runState'), 'runtime node state must be stripped');
assert(serialized.edges[0]?.animated === undefined, 'runtime edge animation must be stripped');

// Serialization must clone settings rather than hand callers mutable aliases.
serialized.metadata.executionSettings?.windowShape?.splice(0, 1, 999);
assert(settings.windowShape?.[0] === 64, 'workflow serialization must not mutate or alias source settings');

const cleanSerialized = serializeWorkflowDocument({
  workflowId: 'b0c7e94d-6c61-4c21-a8a5-0b4973b54a41',
  metadata,
  nodes,
  edges,
  workflowName: 'Cells',
  timestamp: 123,
});
const reopened = parseWorkflowDocument(JSON.parse(JSON.stringify(cleanSerialized)), {
  createWorkflowId: () => { throw new Error('saved workflowId should be reused'); },
});
assert(reopened !== null, 'current workflow document should parse');
assert(
  reopened.workflowId === 'b0c7e94d-6c61-4c21-a8a5-0b4973b54a41',
  'saved workflowId must remain unchanged after reopen',
);
assert(!reopened.migratedWorkflowId, 'current workflow must not be marked as migrated');
assert(reopened.metadata.executionSettings?.windowShape?.join(',') === '64,256,256', 'settings must load from metadata');
assert(reopened.metadata.description === metadata.description, 'unrelated workflow metadata must survive');
assert(reopened.workflowName === 'Cells' && reopened.timestamp === 123, 'document attributes must round-trip');
assert(reopened.executionSettingsValidation?.isValid === true, 'valid saved settings should validate on load');

const legacy = {
  nodes: cleanSerialized.nodes,
  edges: cleanSerialized.edges,
  workflow_name: 'Legacy Cells',
  timestamp: 456,
};
let generatedCount = 0;
const migrated = parseWorkflowDocument(legacy, {
  createWorkflowId: () => {
    generatedCount += 1;
    return 'generated-stable-id';
  },
});
assert(migrated !== null, 'legacy flat workflow should remain loadable');
assert(migrated.workflowId === 'generated-stable-id', 'legacy workflow should receive a generated stable ID');
assert(migrated.migratedWorkflowId, 'legacy workflow ID migration should be explicit');
assert(generatedCount === 1, 'legacy workflow ID should be generated exactly once per load');
assert(Object.keys(migrated.metadata).length === 0, 'legacy workflow should start without invented metadata settings');

const migratedSaved = serializeWorkflowDocument({
  workflowId: migrated.workflowId,
  metadata: migrated.metadata,
  nodes: migrated.nodes as unknown as Node<NodeData>[],
  edges: migrated.edges,
  workflowName: migrated.workflowName,
  timestamp: migrated.timestamp,
});
const migratedReopened = parseWorkflowDocument(migratedSaved, {
  createWorkflowId: () => { throw new Error('migrated ID must be persisted'); },
});
assert(migratedReopened?.workflowId === 'generated-stable-id', 'migrated ID must survive save and reopen');
assert(!migratedReopened?.migratedWorkflowId, 'saved migrated workflow is now a current document');

const envelope = {
  workflowId: 'enveloped-workflow',
  metadata: { executionSettings: settings },
  graph: {
    nodes: cleanSerialized.nodes,
    edges: cleanSerialized.edges,
  },
};
const envelopedFlow = parseStoredFlow(envelope);
assert(envelopedFlow?.nodes.length === 2, 'graph-envelope workflows should be accepted');
const envelopedDocument = parseWorkflowDocument(envelope);
assert(envelopedDocument?.workflowId === 'enveloped-workflow', 'document parser should retain envelope identity');
assert(envelopedDocument?.metadata.executionSettings?.mode === 'window', 'envelope metadata should load');

const flatFlow = parseStoredFlow({
  nodes: cleanSerialized.nodes,
  edges: cleanSerialized.edges,
});
assert(flatFlow?.nodes.length === 2 && flatFlow.edges.length === 1, 'flat graph parsing must remain backward-compatible');

const unsafeMetadata = {
  executionSettings: {
    ...settings,
    resumeAction: 'resume',
    executionId: 'runtime-id',
    selectedRecoveryRecord: '/old/recovery',
  },
};
const safeDocument = serializeWorkflowDocument({
  workflowId: 'safe-workflow',
  metadata: unsafeMetadata as unknown as WorkflowMetadata,
  nodes,
  edges,
});
const savedSettings = safeDocument.metadata.executionSettings as unknown as Record<string, unknown>;
assert(!Object.hasOwn(savedSettings, 'resumeAction'), 'workflow metadata must never cache resume action');
assert(!Object.hasOwn(savedSettings, 'executionId'), 'workflow metadata must never cache execution ID');
assert(!Object.hasOwn(savedSettings, 'selectedRecoveryRecord'), 'workflow metadata must never cache selected recovery state');

const changedNodes = [makeNode('source', 99), makeNode('middle', 30), makeNode('writer', 20)];
const changedEdges: Edge[] = [
  { id: 'source-middle', source: 'source', target: 'middle' },
  { id: 'middle-writer', source: 'middle', target: 'writer' },
];
const afterGraphChanges = serializeWorkflowDocument({
  workflowId: cleanSerialized.workflowId,
  metadata: cleanSerialized.metadata,
  nodes: changedNodes,
  edges: changedEdges,
});
assert(afterGraphChanges.workflowId === cleanSerialized.workflowId, 'DAG edits must preserve workflowId');
assert(
  JSON.stringify(afterGraphChanges.metadata.executionSettings)
    === JSON.stringify(cleanSerialized.metadata.executionSettings),
  'parameter and topology changes must preserve execution settings',
);
assert(afterGraphChanges.nodes[0]?.data.values?.threshold === 99, 'changed parameters should still save');
assert(afterGraphChanges.nodes.length === 3 && afterGraphChanges.edges.length === 2, 'changed topology should still save');

const invalidMetadataDocument = parseWorkflowDocument({
  workflowId: 'invalid-settings',
  metadata: {
    executionSettings: {
      version: 1,
      mode: 'window',
      windowShape: [64, 256, 256],
      maxInFlightWindows: 8,
      newRunRecoveryLocation: {
        mode: 'output_sidecar',
        // missing anchorNodeId: retain the rest and mark only the anchor
      },
    },
  },
  nodes: cleanSerialized.nodes,
  edges: cleanSerialized.edges,
});
assert(invalidMetadataDocument !== null, 'incomplete settings must not make the graph unloadable');
assert(
  invalidMetadataDocument.metadata.executionSettings?.windowShape?.join(',') === '64,256,256',
  'valid values in incomplete metadata must be retained',
);
assert(
  Object.keys(invalidMetadataDocument.executionSettingsValidation?.fieldErrors ?? {}).join(',')
    === 'anchorNodeId',
  'document load should report only the invalid saved reference',
);

const futureSettingsDocument = parseWorkflowDocument({
  workflowId: 'future-settings-workflow',
  metadata: {
    executionSettings: { version: 2, mode: 'full_graph', futureOption: true },
  },
  nodes: cleanSerialized.nodes,
  edges: cleanSerialized.edges,
});
assert(futureSettingsDocument !== null, 'a future settings schema must not hide the workflow graph');
const futureRawSettings = futureSettingsDocument.metadata.executionSettings as unknown as Record<string, unknown>;
assert(futureRawSettings.version === 2, 'unsupported settings must remain unmodified until migration');
assert(
  futureSettingsDocument.executionSettingsValidation?.fieldErrors.version !== undefined,
  'unsupported settings must carry an actionable version error',
);
const futureSettingsResaved = serializeWorkflowDocument({
  workflowId: futureSettingsDocument.workflowId,
  metadata: futureSettingsDocument.metadata,
  nodes: futureSettingsDocument.nodes as unknown as Node<NodeData>[],
  edges: futureSettingsDocument.edges,
});
assert(
  (futureSettingsResaved.metadata.executionSettings as unknown as Record<string, unknown>).version === 2,
  'autosave/export must not silently rewrite an unsupported schema to v1',
);

assert(parseWorkflowDocument(null) === null, 'non-object workflow data should be rejected');
assert(parseWorkflowDocument({ metadata: {} }) === null, 'documents without a recognizable graph should be rejected');

let threwForEmptyId = false;
try {
  serializeWorkflowDocument({ workflowId: ' ', nodes: [], edges: [] });
} catch {
  threwForEmptyId = true;
}
assert(threwForEmptyId, 'serializer must reject an empty stable workflowId');
