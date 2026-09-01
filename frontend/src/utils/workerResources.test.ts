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
  ...defaultWorkerProfile('gpu-cellpose'),
  physical_resources: { cpu: 4, memory: '32GB', gpu: 1 },
  threads: 1,
});
const gpuPool = { ...defaultWorkerPool('gpu-cellpose'), scale: 8 };
saveWorkerResources([gpu], [gpuPool]);

assert(loadWorkerProfiles()[0]?.logical_resources['gpu-cellpose'] === 1,
  'Profile capability must be persisted');
assert(loadWorkerProfiles()[0]?.logical_resources.GPU === 1,
  'Physical GPU must be reflected in logical resources');
assert(loadWorkerPools()[0]?.scale === 8, 'Pool scale must be persisted');
assert(workerResourcePayload().workerProfiles[0]?.name === 'gpu-cellpose',
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
assert(detailedProfileError.includes('Worker Profile "gpu-cellpose"'),
  'Validation errors must identify the invalid Profile');
assert(detailedProfileError.includes('Memory / Worker'),
  'Validation errors must identify the invalid field');

const reader = defaultWorkerProfile('cpu-reader');
saveRequiredWorkerResources([reader], [defaultWorkerPool('cpu-reader')]);
assert(loadWorkerProfiles().some(profile => profile.name === 'gpu-cellpose'),
  'Saving one workflow requirement must preserve Profiles used by other workflows');
assert(loadWorkerProfiles().some(profile => profile.name === 'cpu-reader'),
  'Saving required resources must add the current workflow Profile');
assert(values.has(WORKER_PROFILES_STORAGE_KEY) && values.has(WORKER_POOLS_STORAGE_KEY),
  'Worker resources must use the dedicated localStorage keys');

const staleCpuGeneral = {
  ...defaultWorkerProfile('cpu-general'),
  physical_resources: { cpu: 8, memory: '32GB', gpu: 1 },
  logical_resources: { 'cpu-general': 1, CPU: 8, GPU: 1 },
};
values.set(WORKER_PROFILES_STORAGE_KEY, JSON.stringify([staleCpuGeneral]));
values.set(WORKER_POOLS_STORAGE_KEY, JSON.stringify([
  { profile: 'cpu-general', processes: 1, scale: 1 },
]));
const migratedCpuGeneral = loadWorkerProfiles()[0];
assert(migratedCpuGeneral?.physical_resources.gpu === 0,
  'stale cpu-general Profiles must migrate to zero physical GPUs');
assert(migratedCpuGeneral?.logical_resources.GPU === undefined,
  'stale cpu-general Profiles must drop logical GPU capability');
saveWorkerResources([migratedCpuGeneral!], [
  { profile: 'cpu-general', processes: 4, scale: 1 },
]);
assert(loadWorkerPools()[0]?.processes === 4,
  'cpu-general must allow multiple Worker processes per Slurm Job');

let builtInGpuMismatchRejected = false;
try {
  saveWorkerResources([staleCpuGeneral], [
    { profile: 'cpu-general', processes: 1, scale: 1 },
  ]);
} catch {
  builtInGpuMismatchRejected = true;
}
assert(builtInGpuMismatchRejected,
  'built-in cpu-general must reject GPU allocation');
