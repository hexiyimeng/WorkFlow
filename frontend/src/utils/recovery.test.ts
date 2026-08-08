import {
  buildRecoveryExecutionRequest,
  executionGraphToSerializedFlow,
  normalizeDirectoryListing,
  normalizeRecoveryOpenResponse,
  normalizeRecoverySummary,
} from './recovery.ts';

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

const summaryPayload = {
  found: true,
  valid: true,
  compatible: true,
  executionId: 'run-001',
  status: 'interrupted',
  recoveryDirectory: '/data/result.zarr.workflow',
  completedWindows: 2,
  totalWindows: 8,
  remainingWindows: 6,
  outputShape: [4, 4, 4],
  windowShape: [2, 2, 2],
  windowGridShape: [2, 2, 2],
  outputs: [{
    nodeId: 'writer',
    nodeType: 'ZarrWriter',
    displayName: 'Zarr Writer',
    pathInput: 'output_path',
    path: '/data/result.zarr',
  }],
};

const summary = normalizeRecoverySummary(summaryPayload);
assert(summary.remainingWindows === 6, 'recovery summary should preserve noncontiguous totals');
assert(summary.outputs[0]?.displayName === 'Zarr Writer', 'output DISPLAY_NAME should be retained');

const listing = normalizeDirectoryListing({
  path: '/data',
  parent: '/',
  directories: [{
    name: 'result.zarr.workflow',
    path: '/data/result.zarr.workflow',
    isRecoveryDirectory: true,
  }],
});
assert(listing.directories[0]?.isRecoveryDirectory === true, 'manifest-recognized recovery should be retained');

const rootListing = normalizeDirectoryListing({
  path: null,
  parent: null,
  directories: [{
    name: 'data',
    path: '/data',
    isRecoveryDirectory: false,
  }],
});
assert(rootListing.path === '', 'filesystem-root listings should not require a current path');

const graph = {
  reader: { type: 'OMEZarrReader', inputs: { path: '/data/input.zarr' } },
  writer: {
    type: 'ZarrWriter',
    inputs: {
      dask_arr: ['reader', 0],
      output_path: '/data/result.zarr',
    },
  },
};
const flow = executionGraphToSerializedFlow(graph);
assert(flow.nodes.length === 2, 'saved execution graph should become two read-only nodes');
assert(flow.edges.length === 1, 'saved execution connection should become one editor edge');
assert(flow.edges[0]?.source === 'reader', 'recovered edge should preserve its source node');
const readerPosition = flow.nodes.find(node => node.id === 'reader')?.position;
const writerPosition = flow.nodes.find(node => node.id === 'writer')?.position;
assert(
  readerPosition !== undefined
  && writerPosition !== undefined
  && writerPosition.x > readerPosition.x,
  'recovery dependencies should be laid out from left to right',
);

const branchingGraph = {
  writer_b: {
    type: 'ZarrWriter',
    inputs: {
      array: ['segment', 0],
      output_path: '/data/b.zarr',
      overwrite: true,
      axes: 'Z,Y,X',
      store_kind: 'ome_zarr',
      write_metadata: true,
    },
  },
  segment: {
    type: 'Cellpose',
    inputs: {
      image: ['source', 0],
      diameter: 30,
      model_name: 'cpsam',
      do_3d: 'auto',
      normalize: true,
      gpu_batch_size: 4,
      primary_channel: 0,
      secondary_channel: -1,
      flow_threshold: 0.4,
      cellprob_threshold: 0,
    },
  },
  source: {
    type: 'OMEZarrReader',
    inputs: { file_path: '/data/input.zarr' },
  },
  writer_a: {
    type: 'WriteParquetCellTable',
    inputs: {
      mask: ['segment', 0],
      output_dir: '/data/cells',
      overwrite: true,
      compression: 'zstd',
      axes: 'Z,Y,X',
      tile_z: 16,
      tile_y: 256,
      tile_x: 256,
      row_group_size: 100000,
      sort_by_spatial_key: true,
    },
  },
};
const branchingFlow = executionGraphToSerializedFlow(branchingGraph);
const branchingPositions = new Map(
  branchingFlow.nodes.map(node => [node.id, node.position]),
);
const sourcePosition = branchingPositions.get('source');
const segmentPosition = branchingPositions.get('segment');
const writerAPosition = branchingPositions.get('writer_a');
const writerBPosition = branchingPositions.get('writer_b');
assert(
  sourcePosition !== undefined
  && segmentPosition !== undefined
  && writerAPosition !== undefined
  && writerBPosition !== undefined,
  'every recovered DAG node should receive a position',
);
assert(
  sourcePosition!.x < segmentPosition!.x
  && segmentPosition!.x < writerAPosition!.x
  && writerAPosition!.x === writerBPosition!.x,
  'DAG layers should follow dependencies rather than JSON insertion order',
);
assert(
  Math.abs(writerAPosition!.y - writerBPosition!.y) >= 400,
  'parameter-heavy terminal nodes in one layer should not overlap',
);

const reorderedBranchingGraph = Object.fromEntries(
  Object.entries(branchingGraph).reverse(),
);
const reorderedFlow = executionGraphToSerializedFlow(reorderedBranchingGraph);
const reorderedPositions = new Map(
  reorderedFlow.nodes.map(node => [node.id, node.position]),
);
branchingPositions.forEach((position, nodeId) => {
  const reordered = reorderedPositions.get(nodeId);
  assert(
    reordered?.x === position.x && reordered?.y === position.y,
    'recovery layout should be deterministic across object insertion orders',
  );
});

const opened = normalizeRecoveryOpenResponse({
  graph,
  readOnly: true,
  executionConfig: {
    mode: 'window',
    windowShape: [2, 2, 2],
    maxInFlightWindows: 8,
    resumeAction: 'new',
    recoveryLocation: {
      mode: 'output_sidecar',
      anchorNodeId: 'writer',
    },
  },
  recoverySummary: summaryPayload,
});
assert(opened.readOnly, 'opened recovery graph must be read-only');

const resumeRequest = buildRecoveryExecutionRequest(
  opened,
  '/data/result.zarr.workflow',
  'resume',
);
const restartRequest = buildRecoveryExecutionRequest(
  opened,
  '/data/result.zarr.workflow',
  'restart',
);

assert(
  resumeRequest.graph === null,
  'Resume must delegate immutable graph selection to the recovery service',
);
assert(
  restartRequest.graph === null,
  'Restart must delegate immutable graph selection to the recovery service',
);

if (resumeRequest.executionConfig.mode !== 'window') {
  throw new Error('Resume must build a Window execution configuration');
}
if (restartRequest.executionConfig.mode !== 'window') {
  throw new Error('Restart must build a Window execution configuration');
}
assert(
  resumeRequest.executionConfig.resumeAction === 'resume',
  'Resume must be an explicit recovery-only action',
);
assert(
  restartRequest.executionConfig.resumeAction === 'restart',
  'Restart must remain separate from Resume',
);
assert(
  resumeRequest.executionConfig.maxInFlightWindows === 8
  && restartRequest.executionConfig.maxInFlightWindows === 8,
  'Recovery actions must preserve the valid saved maximum in-flight Window count',
);
assert(
  resumeRequest.executionConfig.recoveryLocation.mode === 'custom'
  && resumeRequest.executionConfig.recoveryLocation.directory === '/data/result.zarr.workflow',
  'Recovery execution must target the explicitly selected custom recovery directory',
);
assert(
  opened.executionConfig.mode === 'window'
  && opened.executionConfig.resumeAction === 'new'
  && opened.executionConfig.recoveryLocation.mode === 'output_sidecar',
  'Building a recovery request must not mutate the immutable saved execution configuration',
);
