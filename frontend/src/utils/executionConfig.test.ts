import {
  calculateWindowGridShape,
  estimateWindowCount,
  isAbsoluteServerPath,
  isValidOutputShape,
  isValidMaxInFlightWindows,
  isValidWindowShape,
  preflightResourcesAllowExecution,
  resolveRecoveryDirectory,
  sameServerPath,
} from './executionConfig.ts';
import type {
  ExecutionPreflightResponse,
  NodeSpec,
  WSMessage,
} from '../types.ts';

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

assert(isValidOutputShape([391, 391, 391]), 'positive integer output shape should be valid');
assert(isValidOutputShape([0, 8]), 'zero-sized output axes should be valid');
assert(!isValidOutputShape([Number.MAX_SAFE_INTEGER + 1]), 'unsafe output dimensions must be rejected');

assert(isValidWindowShape([391, 391, 391], [4, 4, 4]), 'matching positive window shape should be valid');
assert(!isValidWindowShape([391, 391, 391], [4, 4]), 'window rank mismatch must be rejected');
assert(!isValidWindowShape([391, 391, 391], [4, 0, 4]), 'zero window dimensions must be rejected');
assert(!isValidWindowShape([391, 391, 391], [4, -1, 4]), 'negative window dimensions must be rejected');
assert(isValidMaxInFlightWindows(16), 'positive integer concurrency should be valid');
assert(!isValidMaxInFlightWindows(0), 'zero concurrency should be rejected');
assert(!isValidMaxInFlightWindows(1.5), 'fractional concurrency should be rejected');
assert(!isValidMaxInFlightWindows(true), 'boolean concurrency should be rejected');
assert(!isValidMaxInFlightWindows('2'), 'string concurrency should be rejected');

assert(
  !preflightResourcesAllowExecution({ resourcesSatisfied: false }),
  'an explicitly unsatisfied resource plan must block execution',
);
assert(
  preflightResourcesAllowExecution({ resourcesSatisfied: true }),
  'a satisfied resource plan should allow execution',
);
assert(
  preflightResourcesAllowExecution({ resourcesSatisfied: null }),
  'unknown availability should defer to formal backend validation',
);
assert(
  preflightResourcesAllowExecution({}),
  'older preflight responses without resource metadata should remain compatible',
);

const countedResourcePreflight: ExecutionPreflightResponse = {
  windowable: true,
  requiredResources: {
    requiresCpu: true,
    requiresGpu: true,
    cpuWorkers: 3,
    gpuWorkers: 2,
    cpuNodes: [{
      nodeId: 'reader',
      nodeType: 'OMEZarrReader',
      displayName: 'OME-Zarr Reader',
      workers: 3,
      resource: 'cpu',
    }],
    gpuNodes: [{
      nodeId: 'cellpose',
      nodeType: 'Cellpose',
      displayName: 'Cellpose',
      workers: 2,
      resource: 'gpu',
    }],
    anyNodes: [],
  },
  availableResources: {
    cpuWorkers: 4,
    gpuWorkers: 8,
    cpuSlots: 4,
    gpuSlots: 8,
  },
  resourcesSatisfied: null,
};
assert(
  countedResourcePreflight.requiredResources?.cpuWorkers === 3
    && countedResourcePreflight.requiredResources.gpuWorkers === 2,
  'preflight types should retain planned CPU and GPU Worker counts',
);
assert(
  countedResourcePreflight.requiredResources?.gpuNodes[0]?.workers === 2,
  'resource node requirements should retain their Worker count',
);
assert(
  preflightResourcesAllowExecution(countedResourcePreflight),
  'unknown live-cluster availability must remain submit-able with planned counts',
);

const gpuOnlyWithUnconstrainedIo: ExecutionPreflightResponse = {
  windowable: true,
  requiredResources: {
    requiresCpu: false,
    requiresGpu: true,
    cpuWorkers: 0,
    gpuWorkers: 1,
    cpuNodes: [],
    gpuNodes: [{
      nodeId: 'cellpose',
      nodeType: 'Cellpose',
      displayName: 'Cellpose',
      workers: 1,
      resource: 'gpu',
    }],
    anyNodes: [{
      nodeId: 'reader',
      nodeType: 'OMEZarrReader',
      displayName: 'OME-Zarr Reader',
      workers: 0,
      resource: 'any',
    }, {
      nodeId: 'writer',
      nodeType: 'ZarrWriter',
      displayName: 'Zarr Writer',
      workers: 0,
      resource: 'any',
    }],
  },
  resourcesSatisfied: true,
};
assert(
  gpuOnlyWithUnconstrainedIo.requiredResources?.cpuWorkers === 0
    && gpuOnlyWithUnconstrainedIo.requiredResources.gpuWorkers === 1,
  'preflight types must preserve an exact 0 CPU + 1 GPU Worker plan',
);
assert(
  gpuOnlyWithUnconstrainedIo.requiredResources?.anyNodes?.length === 2
    && gpuOnlyWithUnconstrainedIo.requiredResources.anyNodes.every(
      node => node.resource === 'any' && node.workers === 0,
    ),
  'unconstrained Reader and Writer nodes should be represented without adding Workers',
);

const countedNodeSpec: NodeSpec = {
  type: 'Cellpose',
  display_name: 'Cellpose',
  category: 'Segmentation',
  input: { required: {} },
  output: ['MASK'],
  execution_resource: 'gpu',
  execution_workers: 2,
};
assert(
  countedNodeSpec.execution_workers === 2,
  'node specifications should expose their requested Worker count',
);

const unconstrainedNodeSpec: NodeSpec = {
  type: 'OMEZarrReader',
  display_name: 'OME-Zarr Reader',
  category: 'Input',
  input: { required: {} },
  output: ['IMAGE'],
  execution_resource: 'any',
  execution_workers: 0,
};
assert(
  unconstrainedNodeSpec.execution_resource === 'any'
    && unconstrainedNodeSpec.execution_workers === 0,
  'node specifications should expose unconstrained nodes that add no Workers',
);

const clusterReadyMessage: WSMessage = {
  type: 'cluster_ready',
  dashboardUrl: 'http://127.0.0.1:8787/status',
  cpuWorkers: 3,
  gpuWorkers: 2,
};
assert(
  Boolean(clusterReadyMessage.dashboardUrl?.endsWith('/status'))
    && clusterReadyMessage.cpuWorkers === 3
    && clusterReadyMessage.gpuWorkers === 2,
  'cluster_ready messages should carry dashboard and Worker-count metadata',
);

const estimated = estimateWindowCount([391, 391, 391], [4, 4, 4]);
assert(estimated === 941192n, `expected 941192 windows, received ${String(estimated)}`);

const partialWindows = estimateWindowCount([10, 8, 6], [4, 4, 4]);
assert(partialWindows === 12n, `expected 12 partial windows, received ${String(partialWindows)}`);

assert(estimateWindowCount([10, 8], [4]) === null, 'invalid shape should not produce an estimate');

const grid = calculateWindowGridShape([20, 20, 20], [2, 2, 2]);
assert(grid?.join(',') === '10,10,10', 'Window plan should expose the C-order grid shape');

const outputs = [{
  nodeId: 'writer-id',
  nodeType: 'ZarrWriter',
  displayName: 'Zarr Writer',
  pathInput: 'output_path',
  path: '/data/results/segmentation.zarr',
}];
assert(
  resolveRecoveryDirectory(
    { mode: 'output_sidecar', anchorNodeId: 'writer-id' },
    outputs,
  ) === '/data/results/segmentation.zarr.workflow',
  'sidecar recovery should use the selected complete output path',
);
assert(
  resolveRecoveryDirectory(
    { mode: 'custom', directory: '/data/recovery/run.workflow' },
    outputs,
  ) === '/data/recovery/run.workflow',
  'custom recovery directory should be preserved',
);
assert(isAbsoluteServerPath('/shared/run.workflow'), 'POSIX server paths should be accepted');
assert(isAbsoluteServerPath('D:\\runs\\run.workflow'), 'Windows server paths should be accepted');
assert(!isAbsoluteServerPath('../run.workflow'), 'relative recovery paths should be rejected');
assert(
  sameServerPath('d:/data/run.workflow/', 'D:\\data\\run.workflow'),
  'Windows recovery path identity should ignore slash style, case, and a trailing separator',
);
assert(
  !sameServerPath('/data/Run.workflow', '/data/run.workflow'),
  'POSIX recovery path identity must remain case-sensitive',
);
