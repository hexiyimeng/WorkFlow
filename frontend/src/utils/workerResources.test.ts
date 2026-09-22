import {
  WORKER_POOLS_STORAGE_KEY,
  WORKER_PROFILES_STORAGE_KEY,
  defaultWorkerPool,
  defaultWorkerProfile,
  loadWorkerPools,
  loadWorkerProfiles,
  saveRequiredWorkerResources,
  saveWorkerResources,
  synchronizeLogicalResources,
  workerResourcePayload,
} from './workerResources.ts';

const values = new Map<string, string>();
Object.defineProperty(globalThis, 'localStorage', {
  configurable: true,
  value: {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
    clear: () => values.clear(),
  },
});

const assert = (condition: boolean, message: string) => {
  if (!condition) throw new Error(message);
};

const gpu = synchronizeLogicalResources({
  ...defaultWorkerProfile('GPU'),
  physical_resources: { cpu: 4, memory: '32GB', gpu: 1 },
  threads: 1,
});
const gpuPool = { ...defaultWorkerPool('GPU'), minimum_jobs: 8, maximum_jobs: 8 };
saveWorkerResources([gpu], [gpuPool]);

assert(loadWorkerProfiles()[0]?.logical_resources['GPU'] === 1,
  'Profile capability must be persisted');
assert(loadWorkerProfiles()[0]?.logical_resources.GPU === 1,
  'Physical GPU must be reflected in logical resources');
assert(loadWorkerPools()[0]?.minimum_jobs === 8, 'Minimum Jobs must be persisted');
assert(loadWorkerPools()[0]?.maximum_jobs === 8, 'Maximum Jobs must be persisted');
let invalidRangeRejected = false;
try {
  saveWorkerResources([gpu], [{ ...gpuPool, minimum_jobs: 3, maximum_jobs: 2 }]);
} catch {
  invalidRangeRejected = true;
}
assert(invalidRangeRejected, 'Maximum Jobs must not be below Minimum Jobs');
assert(workerResourcePayload().workerProfiles[0]?.name === 'GPU',
  'Run payload must load browser Worker Profiles');

values.set(WORKER_PROFILES_STORAGE_KEY, JSON.stringify([{ ...gpu, threads: 1 }]));
assert(loadWorkerProfiles()[0]?.threads === 4,
  'Legacy independent thread values must migrate to CPU / Worker');
saveWorkerResources(loadWorkerProfiles(), [gpuPool]);

let gpuProcessesRejected = false;
try {
  saveWorkerResources([gpu], [{ ...gpuPool, processes: 2 }]);
} catch {
  gpuProcessesRejected = true;
}
assert(gpuProcessesRejected, 'GPU Pool must enforce one process per Slurm job');

let detailedProfileError = '';
try {
  saveWorkerResources([{
    ...gpu,
    physical_resources: { ...gpu.physical_resources, memory: '0GB' },
  }], [gpuPool]);
} catch (error) {
  detailedProfileError = (error as Error).message;
}
assert(detailedProfileError.includes('Worker Profile "GPU"'),
  'Validation errors must identify the invalid Profile');
assert(detailedProfileError.includes('Memory / Worker'),
  'Validation errors must identify the invalid field');

const reader = defaultWorkerProfile('CPU');
assert(reader.logical_resources.CPU === reader.physical_resources.cpu,
  'CPU capacity must equal cores per Worker');
assert(Object.keys(reader.logical_resources).length === 1,
  'CPU Workers must advertise only CPU task capacity');
assert(gpu.logical_resources.CPU === undefined && Object.keys(gpu.logical_resources).length === 1,
  'GPU Workers must advertise only GPU task capacity');
saveRequiredWorkerResources([reader], [defaultWorkerPool('CPU')]);
assert(loadWorkerProfiles().some(profile => profile.name === 'GPU'),
  'Saving one workflow requirement must preserve Profiles used by other workflows');
assert(loadWorkerProfiles().some(profile => profile.name === 'CPU'),
  'Saving required resources must add the current workflow Profile');
assert(values.has(WORKER_PROFILES_STORAGE_KEY) && values.has(WORKER_POOLS_STORAGE_KEY),
  'Worker resources must use the dedicated localStorage keys');

const staleCpuGeneral = {
  ...defaultWorkerProfile('CPU'),
  physical_resources: { cpu: 8, memory: '32GB', gpu: 1 },
  logical_resources: { CPU: 8, GPU: 1 },
};
values.set(WORKER_PROFILES_STORAGE_KEY, JSON.stringify([staleCpuGeneral]));
values.set(WORKER_POOLS_STORAGE_KEY, JSON.stringify([
  { profile: 'CPU', processes: 1, minimum_jobs: 1, maximum_jobs: 1 },
]));
const migratedCpuGeneral = loadWorkerProfiles()[0];
assert(migratedCpuGeneral?.physical_resources.gpu === 0,
  'stale CPU Profiles must migrate to zero physical GPUs');
assert(migratedCpuGeneral?.logical_resources.GPU === undefined,
  'stale CPU Profiles must drop logical GPU capability');
saveWorkerResources([migratedCpuGeneral!], [
  { profile: 'CPU', processes: 4, minimum_jobs: 1, maximum_jobs: 1 },
]);
assert(loadWorkerPools()[0]?.processes === 4,
  'CPU must allow multiple Worker processes per Slurm Job');

let builtInGpuMismatchRejected = false;
try {
  saveWorkerResources([staleCpuGeneral], [
    { profile: 'CPU', processes: 1, minimum_jobs: 1, maximum_jobs: 1 },
  ]);
} catch {
  builtInGpuMismatchRejected = true;
}
assert(builtInGpuMismatchRejected,
  'built-in CPU must reject GPU allocation');
