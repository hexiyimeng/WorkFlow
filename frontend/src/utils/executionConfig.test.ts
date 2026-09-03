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

const profilePreflight: ExecutionPreflightResponse = {
  windowable: true,
  requiredResources: {
    requiredWorkerProfiles: {
      'cpu-general': 3,
      'gpu-inference': 2,
    },
    profileRequirements: [{
      nodeId: 'cellpose',
      nodeType: 'Cellpose',
      displayName: 'Cellpose',
      workerProfile: 'gpu-inference',
    }],
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
  profilePreflight.requiredResources?.requiredWorkerProfiles['gpu-inference'] === 2,
  'preflight types should retain Worker Profile requirement counts',
);
assert(
  profilePreflight.requiredResources?.profileRequirements[0]?.workerProfile === 'gpu-inference',
  'node requirements should expose their Worker Profile',
);
assert(
  preflightResourcesAllowExecution(profilePreflight),
  'unknown live-cluster availability must remain submit-able',
);

const profileNodeSpec: NodeSpec = {
  type: 'Cellpose',
  display_name: 'Cellpose',
  category: 'Segmentation',
  input: { required: {} },
  output: ['MASK'],
  required_worker_profile: 'gpu-cellpose',
};
assert(
  profileNodeSpec.required_worker_profile === 'gpu-cellpose',
  'node specifications should expose their Worker Profile',
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
