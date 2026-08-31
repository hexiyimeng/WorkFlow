import type { WorkerPool, WorkerProfile } from '../types';

export const WORKER_PROFILES_STORAGE_KEY = 'worker_profiles';
export const WORKER_POOLS_STORAGE_KEY = 'worker_pools';

const PROFILE_NAME = /^[a-z0-9][a-z0-9-]{0,63}$/;
const MEMORY = /^([0-9]+(?:\.[0-9]+)?)\s*(GB|GiB)$/i;

const positiveMemory = (value: unknown): value is string => {
  if (typeof value !== 'string') return false;
  const match = MEMORY.exec(value.trim());
  return match !== null && Number(match[1]) > 0;
};

const isRecord = (value: unknown): value is Record<string, unknown> => (
  typeof value === 'object' && value !== null && !Array.isArray(value)
);

const positiveInteger = (value: unknown): value is number => (
  Number.isSafeInteger(value) && Number(value) > 0
);

const nonnegativeInteger = (value: unknown): value is number => (
  Number.isSafeInteger(value) && Number(value) >= 0
);

export const isWorkerProfile = (value: unknown): value is WorkerProfile => {
  if (!isRecord(value) || typeof value.name !== 'string' || !PROFILE_NAME.test(value.name)) {
    return false;
  }
  const physical = value.physical_resources;
  if (!isRecord(physical)) return false;
  const logical = value.logical_resources;
  const gpu = Number(physical.gpu);
  return positiveInteger(physical.cpu)
    && positiveMemory(physical.memory)
    && nonnegativeInteger(physical.gpu)
    && Number(physical.gpu) <= 1
    && Number(value.threads) === Number(physical.cpu)
    && isRecord(logical)
    && Number(logical[value.name]) === 1
    && Number(logical.CPU) === Number(physical.cpu)
    && (gpu > 0 ? Number(logical.GPU) === gpu : logical.GPU === undefined)
    && Array.isArray(value.capabilities)
    && value.capabilities.includes(value.name);
};

export const isWorkerPool = (value: unknown): value is WorkerPool => (
  isRecord(value)
  && typeof value.profile === 'string'
  && PROFILE_NAME.test(value.profile)
  && positiveInteger(value.processes)
  && positiveInteger(value.scale)
);

const workerProfileError = (value: unknown, index: number): string | null => {
  const label = isRecord(value) && typeof value.name === 'string' && value.name.trim()
    ? `Worker Profile "${value.name}"`
    : `Worker Profile at position ${index + 1}`;
  if (!isRecord(value)) return `${label} must be an object.`;
  if (typeof value.name !== 'string' || !PROFILE_NAME.test(value.name)) {
    return `${label}: Profile name is invalid.`;
  }
  const physical = value.physical_resources;
  if (!isRecord(physical)) return `${label}: physical resources are missing.`;
  if (!positiveInteger(physical.cpu)) return `${label}: CPU / Worker must be a positive integer.`;
  if (!positiveMemory(physical.memory)) {
    return `${label}: Memory / Worker must be a positive GB value.`;
  }
  if (!nonnegativeInteger(physical.gpu) || Number(physical.gpu) > 1) {
    return `${label}: GPU / Worker must be 0 or 1.`;
  }
  if (Number(value.threads) !== Number(physical.cpu)) {
    return `${label}: Threads / Worker must equal CPU / Worker for SLURMCluster.`;
  }
  const logical = value.logical_resources;
  if (!isRecord(logical) || Number(logical[value.name]) !== 1) {
    return `${label}: logical Profile capability must equal 1.`;
  }
  if (Number(logical.CPU) !== Number(physical.cpu)) {
    return `${label}: logical CPU does not match CPU / Worker.`;
  }
  const gpu = Number(physical.gpu);
  if (gpu > 0 ? Number(logical.GPU) !== gpu : logical.GPU !== undefined) {
    return `${label}: logical GPU does not match GPU / Worker.`;
  }
  if (!Array.isArray(value.capabilities) || !value.capabilities.includes(value.name)) {
    return `${label}: capabilities must include its Profile name.`;
  }
  return null;
};

const workerPoolError = (value: unknown, index: number): string | null => {
  const label = isRecord(value) && typeof value.profile === 'string' && value.profile.trim()
    ? `Worker Pool "${value.profile}"`
    : `Worker Pool at position ${index + 1}`;
  if (!isRecord(value)) return `${label} must be an object.`;
  if (typeof value.profile !== 'string' || !PROFILE_NAME.test(value.profile)) {
    return `${label}: Profile reference is invalid.`;
  }
  if (!positiveInteger(value.processes)) return `${label}: Processes / Job must be a positive integer.`;
  if (!positiveInteger(value.scale)) return `${label}: Scale must be a positive integer.`;
  return null;
};

const parseArray = <T>(key: string, validate: (value: unknown) => value is T): T[] => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter(validate) : [];
  } catch {
    return [];
  }
};

export const loadWorkerProfiles = (): WorkerProfile[] => {
  try {
    const raw = localStorage.getItem(WORKER_PROFILES_STORAGE_KEY);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.map(value => {
      if (!isRecord(value) || !isRecord(value.physical_resources)) return value;
      return { ...value, threads: Number(value.physical_resources.cpu) };
    }).filter(isWorkerProfile);
  } catch {
    return [];
  }
};

export const loadWorkerPools = (): WorkerPool[] => (
  parseArray(WORKER_POOLS_STORAGE_KEY, isWorkerPool)
);

export const saveWorkerResources = (
  profiles: WorkerProfile[],
  pools: WorkerPool[],
): void => {
  for (const [index, profile] of profiles.entries()) {
    const error = workerProfileError(profile, index);
    if (error) throw new Error(error);
  }
  for (const [index, pool] of pools.entries()) {
    const error = workerPoolError(pool, index);
    if (error) throw new Error(error);
  }
  if (new Set(profiles.map(profile => profile.name)).size !== profiles.length) {
    throw new Error('Worker Profile names must be unique.');
  }
  if (new Set(pools.map(pool => pool.profile)).size !== pools.length) {
    throw new Error('Only one Worker Pool may be configured per Profile.');
  }
  const profileByName = new Map(profiles.map(profile => [profile.name, profile]));
  for (const pool of pools) {
    const profile = profileByName.get(pool.profile);
    if (!profile) throw new Error(`Worker Pool ${pool.profile} has no matching Profile.`);
    if (profile.physical_resources.gpu > 0 && pool.processes !== 1) {
      throw new Error(`GPU Worker Pool ${pool.profile} must use processes=1.`);
    }
  }
  localStorage.setItem(WORKER_PROFILES_STORAGE_KEY, JSON.stringify(profiles));
  localStorage.setItem(WORKER_POOLS_STORAGE_KEY, JSON.stringify(pools));
};

export const saveRequiredWorkerResources = (
  profiles: WorkerProfile[],
  pools: WorkerPool[],
): void => {
  const requiredNames = new Set(profiles.map(profile => profile.name));
  const mergedProfiles = [
    ...loadWorkerProfiles().filter(profile => !requiredNames.has(profile.name)),
    ...profiles,
  ];
  const mergedPools = [
    ...loadWorkerPools().filter(pool => !requiredNames.has(pool.profile)),
    ...pools,
  ];
  saveWorkerResources(mergedProfiles, mergedPools);
};

export const workerResourcePayload = (): {
  workerProfiles: WorkerProfile[];
  workerPools: WorkerPool[];
} => ({
  workerProfiles: loadWorkerProfiles(),
  workerPools: loadWorkerPools(),
});

export const defaultWorkerProfile = (name: string): WorkerProfile => {
  const gpu = name.startsWith('gpu-') ? 1 : 0;
  const cpu = gpu > 0 ? 4 : 8;
  return {
    name,
    physical_resources: { cpu, memory: '32GB', gpu },
    logical_resources: { [name]: 1, CPU: cpu, ...(gpu ? { GPU: gpu } : {}) },
    capabilities: [name],
    threads: cpu,
  };
};

export const defaultWorkerPool = (profile: string): WorkerPool => ({
  profile,
  processes: 1,
  scale: 1,
});

export const synchronizeLogicalResources = (profile: WorkerProfile): WorkerProfile => ({
  ...profile,
  threads: profile.physical_resources.cpu,
  logical_resources: {
    [profile.name]: 1,
    CPU: profile.physical_resources.cpu,
    ...(profile.physical_resources.gpu > 0
      ? { GPU: profile.physical_resources.gpu }
      : {}),
  },
  capabilities: [profile.name],
});
